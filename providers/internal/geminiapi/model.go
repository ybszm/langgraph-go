package geminiapi

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"

	"github.com/wahanbo/langgraph-go/graph"
	"github.com/wahanbo/langgraph-go/prebuilt"
	"github.com/wahanbo/langgraph-go/providers/internal/sse"
	"github.com/wahanbo/langgraph-go/providers/openaicompat"
)

const maxBodyBytes = 16 << 20

type Config[S any] struct {
	Provider    string
	Endpoint    string
	APIKey      string
	AccessToken string
	HTTPClient  *http.Client
	Messages    openaicompat.MessageReader[S]
	EditRequest openaicompat.RequestEditor
	Streaming   bool
}

type Model[S any] struct {
	config Config[S]
	tools  []prebuilt.ToolDefinition
}

func New[S any](config Config[S]) (*Model[S], error) {
	if config.Provider == "" || config.Endpoint == "" || config.Messages == nil {
		return nil, errors.New("Google model provider, endpoint, and message reader are required")
	}
	parsed, err := url.Parse(config.Endpoint)
	if err != nil || parsed.Scheme == "" || parsed.Host == "" {
		return nil, errors.New("Google model endpoint must be absolute")
	}
	if config.HTTPClient == nil {
		config.HTTPClient = http.DefaultClient
	}
	return &Model[S]{config: config}, nil
}

func (m *Model[S]) BindTools(definitions []prebuilt.ToolDefinition) (prebuilt.ChatModel[S], error) {
	if m == nil {
		return nil, errors.New("Google model is nil")
	}
	bound := *m
	bound.tools = make([]prebuilt.ToolDefinition, len(definitions))
	for i, definition := range definitions {
		if definition.Name == "" || !json.Valid(definition.InputSchema) {
			return nil, fmt.Errorf("Google tool definition %d is invalid", i)
		}
		bound.tools[i] = definition
		bound.tools[i].InputSchema = append(json.RawMessage(nil), definition.InputSchema...)
	}
	return &bound, nil
}

func (m *Model[S]) Invoke(ctx context.Context, state S, runtime graph.Runtime) (prebuilt.AssistantMessage, error) {
	if m == nil {
		return prebuilt.AssistantMessage{}, errors.New("Google model is nil")
	}
	if m.config.Streaming {
		return m.Stream(ctx, state, runtime, nil)
	}
	history, err := m.config.Messages(ctx, state)
	if err != nil {
		return prebuilt.AssistantMessage{}, fmt.Errorf("read %s messages: %w", m.config.Provider, err)
	}
	request, err := encodeRequest(history, m.tools)
	if err != nil {
		return prebuilt.AssistantMessage{}, err
	}
	body, err := json.Marshal(request)
	if err != nil {
		return prebuilt.AssistantMessage{}, fmt.Errorf("encode %s request: %w", m.config.Provider, err)
	}
	httpRequest, err := http.NewRequestWithContext(ctx, http.MethodPost, m.config.Endpoint, bytes.NewReader(body))
	if err != nil {
		return prebuilt.AssistantMessage{}, err
	}
	httpRequest.Header.Set("Content-Type", "application/json")
	if m.config.APIKey != "" {
		httpRequest.Header.Set("x-goog-api-key", m.config.APIKey)
	}
	if m.config.AccessToken != "" {
		httpRequest.Header.Set("Authorization", "Bearer "+m.config.AccessToken)
	}
	if m.config.EditRequest != nil {
		if err := m.config.EditRequest(ctx, httpRequest, body); err != nil {
			return prebuilt.AssistantMessage{}, fmt.Errorf("prepare %s request: %w", m.config.Provider, err)
		}
	}
	response, err := m.config.HTTPClient.Do(httpRequest)
	if err != nil {
		return prebuilt.AssistantMessage{}, fmt.Errorf("call %s: %w", m.config.Provider, err)
	}
	defer response.Body.Close()
	data, err := io.ReadAll(io.LimitReader(response.Body, maxBodyBytes+1))
	if err != nil {
		return prebuilt.AssistantMessage{}, err
	}
	if len(data) > maxBodyBytes {
		return prebuilt.AssistantMessage{}, fmt.Errorf("%s response is too large", m.config.Provider)
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return prebuilt.AssistantMessage{}, &openaicompat.APIError{Provider: m.config.Provider, StatusCode: response.StatusCode, Body: strings.TrimSpace(string(data))}
	}
	var decoded responseBody
	if err := json.Unmarshal(data, &decoded); err != nil {
		return prebuilt.AssistantMessage{}, fmt.Errorf("decode %s response: %w", m.config.Provider, err)
	}
	if len(decoded.Candidates) == 0 {
		return prebuilt.AssistantMessage{}, fmt.Errorf("%s response contains no candidates", m.config.Provider)
	}
	result := prebuilt.AssistantMessage{ID: decoded.ResponseID}
	for i, part := range decoded.Candidates[0].Content.Parts {
		result.Content += part.Text
		if part.FunctionCall != nil {
			arguments, err := json.Marshal(part.FunctionCall.Args)
			if err != nil || part.FunctionCall.Name == "" {
				return prebuilt.AssistantMessage{}, fmt.Errorf("%s function call %d is invalid", m.config.Provider, i)
			}
			result.ToolCalls = append(result.ToolCalls, prebuilt.ToolCall{ID: fmt.Sprintf("%s-call-%d", decoded.ResponseID, i), Name: part.FunctionCall.Name, Arguments: arguments})
		}
	}
	return result, nil
}

// Stream consumes Gemini's streamGenerateContent SSE protocol.
func (m *Model[S]) Stream(ctx context.Context, state S, runtime graph.Runtime, emit func(prebuilt.AssistantMessageChunk) error) (prebuilt.AssistantMessage, error) {
	if m == nil {
		return prebuilt.AssistantMessage{}, errors.New("Google model is nil")
	}
	adapter := prebuilt.StreamingChatModel[S, prebuilt.AssistantMessageChunk]{
		Metadata: map[string]any{"provider": m.config.Provider},
		Emit:     emit,
		Adapter: prebuilt.MessageChunkAdapterFunc[prebuilt.AssistantMessageChunk](func(_ context.Context, chunk prebuilt.AssistantMessageChunk) (prebuilt.AssistantMessageChunk, error) {
			return chunk, nil
		}),
		Stream: func(streamCtx context.Context, streamState S, _ graph.Runtime, publish func(prebuilt.AssistantMessageChunk) error) error {
			return m.streamChunks(streamCtx, streamState, publish)
		},
	}
	return adapter.Invoke(ctx, state, runtime)
}

func (m *Model[S]) streamChunks(ctx context.Context, state S, emit func(prebuilt.AssistantMessageChunk) error) error {
	history, err := m.config.Messages(ctx, state)
	if err != nil {
		return fmt.Errorf("read %s messages: %w", m.config.Provider, err)
	}
	request, err := encodeRequest(history, m.tools)
	if err != nil {
		return err
	}
	body, err := json.Marshal(request)
	if err != nil {
		return fmt.Errorf("encode %s request: %w", m.config.Provider, err)
	}
	endpoint, err := streamingEndpoint(m.config.Endpoint)
	if err != nil {
		return err
	}
	httpRequest, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		return err
	}
	httpRequest.Header.Set("Content-Type", "application/json")
	httpRequest.Header.Set("Accept", "text/event-stream")
	if m.config.APIKey != "" {
		httpRequest.Header.Set("x-goog-api-key", m.config.APIKey)
	}
	if m.config.AccessToken != "" {
		httpRequest.Header.Set("Authorization", "Bearer "+m.config.AccessToken)
	}
	if m.config.EditRequest != nil {
		if err := m.config.EditRequest(ctx, httpRequest, body); err != nil {
			return fmt.Errorf("prepare %s request: %w", m.config.Provider, err)
		}
	}
	response, err := m.config.HTTPClient.Do(httpRequest)
	if err != nil {
		return fmt.Errorf("call %s: %w", m.config.Provider, err)
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		data, readErr := io.ReadAll(io.LimitReader(response.Body, maxBodyBytes+1))
		if readErr != nil {
			return readErr
		}
		return &openaicompat.APIError{Provider: m.config.Provider, StatusCode: response.StatusCode, Body: strings.TrimSpace(string(data))}
	}
	toolIndex := 0
	streamID := ""
	streamIDLocked := false
	return sse.Scan(ctx, response.Body, maxBodyBytes, func(_ string, data string) error {
		var decoded responseBody
		if err := json.Unmarshal([]byte(data), &decoded); err != nil {
			return fmt.Errorf("decode %s stream event: %w", m.config.Provider, err)
		}
		if len(decoded.Candidates) == 0 {
			return nil
		}
		// Gemini may omit responseId on early chunks. Lock to the first emitted
		// event so a later responseId cannot conflict with the stable ID assigned
		// by StreamingChatModel.
		if !streamIDLocked {
			streamID = decoded.ResponseID
			streamIDLocked = true
		}
		chunk := prebuilt.AssistantMessageChunk{ID: streamID}
		candidate := decoded.Candidates[0]
		for _, part := range candidate.Content.Parts {
			chunk.Content += part.Text
			if part.FunctionCall != nil {
				arguments, err := json.Marshal(part.FunctionCall.Args)
				if err != nil || part.FunctionCall.Name == "" {
					return fmt.Errorf("%s streamed function call is invalid", m.config.Provider)
				}
				idPrefix := streamID
				if idPrefix == "" {
					idPrefix = "gemini"
				}
				id := fmt.Sprintf("%s-call-%d", idPrefix, toolIndex)
				chunk.ToolCallChunks = append(chunk.ToolCallChunks, prebuilt.ToolCallChunk{Index: toolIndex, ID: id, Name: part.FunctionCall.Name, Arguments: string(arguments)})
				toolIndex++
			}
		}
		if candidate.FinishReason != "" || decoded.UsageMetadata != nil {
			chunk.ResponseMetadata = map[string]any{}
			if candidate.FinishReason != "" {
				chunk.ResponseMetadata["finish_reason"] = candidate.FinishReason
			}
			if decoded.UsageMetadata != nil {
				chunk.ResponseMetadata["usage"] = decoded.UsageMetadata
			}
		}
		return emit(chunk)
	})
}

func streamingEndpoint(endpoint string) (string, error) {
	parsed, err := url.Parse(endpoint)
	if err != nil {
		return "", err
	}
	if !strings.HasSuffix(parsed.Path, ":generateContent") {
		return "", fmt.Errorf("Google streaming endpoint must end with :generateContent")
	}
	parsed.Path = strings.TrimSuffix(parsed.Path, ":generateContent") + ":streamGenerateContent"
	query := parsed.Query()
	query.Set("alt", "sse")
	parsed.RawQuery = query.Encode()
	return parsed.String(), nil
}

type requestBody struct {
	SystemInstruction *content  `json:"systemInstruction,omitempty"`
	Contents          []content `json:"contents"`
	Tools             []tool    `json:"tools,omitempty"`
}

type content struct {
	Role  string `json:"role,omitempty"`
	Parts []part `json:"parts"`
}

type part struct {
	Text             string            `json:"text,omitempty"`
	FunctionCall     *functionCall     `json:"functionCall,omitempty"`
	FunctionResponse *functionResponse `json:"functionResponse,omitempty"`
}

type functionCall struct {
	Name string         `json:"name"`
	Args map[string]any `json:"args"`
}

type functionResponse struct {
	Name     string         `json:"name"`
	Response map[string]any `json:"response"`
}

type tool struct {
	FunctionDeclarations []functionDeclaration `json:"functionDeclarations"`
}

type functionDeclaration struct {
	Name        string          `json:"name"`
	Description string          `json:"description,omitempty"`
	Parameters  json.RawMessage `json:"parameters"`
}

type responseBody struct {
	ResponseID string `json:"responseId"`
	Candidates []struct {
		Content      content `json:"content"`
		FinishReason string  `json:"finishReason"`
	} `json:"candidates"`
	UsageMetadata map[string]any `json:"usageMetadata"`
}

func encodeRequest(history []prebuilt.Message, definitions []prebuilt.ToolDefinition) (requestBody, error) {
	var request requestBody
	for i, message := range history {
		switch value := message.(type) {
		case prebuilt.SystemMessage:
			if request.SystemInstruction == nil {
				request.SystemInstruction = &content{Parts: []part{{Text: value.Content}}}
			} else {
				request.SystemInstruction.Parts = append(request.SystemInstruction.Parts, part{Text: value.Content})
			}
		case prebuilt.UserMessage:
			request.Contents = append(request.Contents, content{Role: "user", Parts: []part{{Text: value.Content}}})
		case prebuilt.AssistantMessage:
			parts := make([]part, 0, 1+len(value.ToolCalls))
			if value.Content != "" {
				parts = append(parts, part{Text: value.Content})
			}
			for _, call := range value.ToolCalls {
				var args map[string]any
				if err := json.Unmarshal(call.Arguments, &args); err != nil {
					return requestBody{}, fmt.Errorf("Google assistant tool arguments: %w", err)
				}
				parts = append(parts, part{FunctionCall: &functionCall{Name: call.Name, Args: args}})
			}
			request.Contents = append(request.Contents, content{Role: "model", Parts: parts})
		case prebuilt.ToolMessage:
			response := map[string]any{"result": value.Content}
			var decoded map[string]any
			if json.Unmarshal([]byte(value.Content), &decoded) == nil && decoded != nil {
				response = decoded
			}
			request.Contents = append(request.Contents, content{Role: "user", Parts: []part{{FunctionResponse: &functionResponse{Name: value.Name, Response: response}}}})
		default:
			return requestBody{}, fmt.Errorf("Google message %d has unsupported type %T", i, message)
		}
	}
	if len(definitions) > 0 {
		declarations := make([]functionDeclaration, len(definitions))
		for i, definition := range definitions {
			declarations[i] = functionDeclaration{Name: definition.Name, Description: definition.Description, Parameters: append(json.RawMessage(nil), definition.InputSchema...)}
		}
		request.Tools = []tool{{FunctionDeclarations: declarations}}
	}
	return request, nil
}
