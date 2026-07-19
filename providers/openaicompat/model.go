// Package openaicompat adapts OpenAI-compatible chat-completion APIs to the
// provider-neutral prebuilt.ChatModel contract.
package openaicompat

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

	"github.com/ybszm/langgraph-go/graph"
	"github.com/ybszm/langgraph-go/prebuilt"
	"github.com/ybszm/langgraph-go/providers/internal/sse"
)

const maxResponseBytes = 16 << 20

// MessageReader projects application state into provider-neutral history.
type MessageReader[S any] func(context.Context, S) ([]prebuilt.Message, error)

// RequestEditor adds provider-specific authentication or headers.
type RequestEditor func(context.Context, *http.Request, []byte) error

// Config controls an OpenAI-compatible chat model.
type Config[S any] struct {
	Provider    string
	BaseURL     string
	Path        string
	APIKey      string
	Model       string
	HTTPClient  *http.Client
	Messages    MessageReader[S]
	Headers     map[string]string
	EditRequest RequestEditor
	MaxTokens   int
	// LegacyMaxTokens sends max_tokens instead of OpenAI's newer
	// max_completion_tokens field. Most OpenAI-compatible APIs still expose
	// the legacy spelling.
	LegacyMaxTokens bool
	Temperature     *float64
	// Streaming makes Invoke use the provider's SSE protocol. Stream can be
	// called explicitly regardless of this setting.
	Streaming bool
}

// APIError is a non-2xx provider response.
type APIError struct {
	Provider   string
	StatusCode int
	Body       string
}

func (e *APIError) Error() string {
	return fmt.Sprintf("%s API returned HTTP %d: %s", e.Provider, e.StatusCode, e.Body)
}

// Model implements ChatModel and ToolBindingChatModel.
type Model[S any] struct {
	config Config[S]
	tools  []prebuilt.ToolDefinition
}

// New validates config and creates a model adapter.
func New[S any](config Config[S]) (*Model[S], error) {
	config.Provider = strings.TrimSpace(config.Provider)
	if config.Provider == "" {
		config.Provider = "OpenAI-compatible"
	}
	config.BaseURL = strings.TrimRight(strings.TrimSpace(config.BaseURL), "/")
	if config.BaseURL == "" {
		return nil, errors.New("provider base URL is required")
	}
	parsed, err := url.Parse(config.BaseURL)
	if err != nil || parsed.Scheme == "" || parsed.Host == "" {
		return nil, fmt.Errorf("provider base URL must be absolute: %q", config.BaseURL)
	}
	if config.Path == "" {
		config.Path = "/chat/completions"
	}
	if !strings.HasPrefix(config.Path, "/") {
		return nil, errors.New("provider request path must start with /")
	}
	if strings.TrimSpace(config.Model) == "" {
		return nil, errors.New("provider model is required")
	}
	if config.Messages == nil {
		return nil, errors.New("provider message reader is required")
	}
	if config.MaxTokens < 0 {
		return nil, errors.New("provider max tokens cannot be negative")
	}
	if config.HTTPClient == nil {
		config.HTTPClient = http.DefaultClient
	}
	config.Headers = cloneHeaders(config.Headers)
	return &Model[S]{config: config}, nil
}

// BindTools returns an independent adapter with the supplied tool definitions.
func (m *Model[S]) BindTools(definitions []prebuilt.ToolDefinition) (prebuilt.ChatModel[S], error) {
	if m == nil {
		return nil, errors.New("provider model is nil")
	}
	cloned, err := cloneTools(definitions)
	if err != nil {
		return nil, err
	}
	bound := *m
	bound.tools = cloned
	return &bound, nil
}

// Invoke sends one non-streaming chat-completion request.
func (m *Model[S]) Invoke(ctx context.Context, state S, runtime graph.Runtime) (prebuilt.AssistantMessage, error) {
	if m == nil {
		return prebuilt.AssistantMessage{}, errors.New("provider model is nil")
	}
	if err := ctx.Err(); err != nil {
		return prebuilt.AssistantMessage{}, err
	}
	if m.config.Streaming {
		return m.Stream(ctx, state, runtime, nil)
	}
	history, err := m.config.Messages(ctx, state)
	if err != nil {
		return prebuilt.AssistantMessage{}, fmt.Errorf("read %s messages: %w", m.config.Provider, err)
	}
	messages, err := encodeMessages(history)
	if err != nil {
		return prebuilt.AssistantMessage{}, fmt.Errorf("encode %s messages: %w", m.config.Provider, err)
	}
	request := chatRequest{Model: m.config.Model, Messages: messages, Temperature: m.config.Temperature}
	if m.config.LegacyMaxTokens {
		request.MaxTokens = m.config.MaxTokens
	} else {
		request.MaxCompletionTokens = m.config.MaxTokens
	}
	if len(m.tools) > 0 {
		request.Tools = make([]wireTool, len(m.tools))
		for i, definition := range m.tools {
			request.Tools[i] = wireTool{Type: "function", Function: wireFunction{Name: definition.Name, Description: definition.Description, Parameters: definition.InputSchema}}
		}
	}
	body, err := json.Marshal(request)
	if err != nil {
		return prebuilt.AssistantMessage{}, fmt.Errorf("encode %s request: %w", m.config.Provider, err)
	}
	httpRequest, err := http.NewRequestWithContext(ctx, http.MethodPost, m.config.BaseURL+m.config.Path, bytes.NewReader(body))
	if err != nil {
		return prebuilt.AssistantMessage{}, err
	}
	httpRequest.Header.Set("Content-Type", "application/json")
	if m.config.APIKey != "" {
		httpRequest.Header.Set("Authorization", "Bearer "+m.config.APIKey)
	}
	for key, value := range m.config.Headers {
		httpRequest.Header.Set(key, value)
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
	responseBody, err := io.ReadAll(io.LimitReader(response.Body, maxResponseBytes+1))
	if err != nil {
		return prebuilt.AssistantMessage{}, fmt.Errorf("read %s response: %w", m.config.Provider, err)
	}
	if len(responseBody) > maxResponseBytes {
		return prebuilt.AssistantMessage{}, fmt.Errorf("%s response exceeds %d bytes", m.config.Provider, maxResponseBytes)
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return prebuilt.AssistantMessage{}, &APIError{Provider: m.config.Provider, StatusCode: response.StatusCode, Body: strings.TrimSpace(string(responseBody))}
	}
	var decoded chatResponse
	if err := json.Unmarshal(responseBody, &decoded); err != nil {
		return prebuilt.AssistantMessage{}, fmt.Errorf("decode %s response: %w", m.config.Provider, err)
	}
	if len(decoded.Choices) == 0 {
		return prebuilt.AssistantMessage{}, fmt.Errorf("%s response contains no choices", m.config.Provider)
	}
	message := decoded.Choices[0].Message
	result := prebuilt.AssistantMessage{ID: decoded.ID, Content: message.Content}
	for i, call := range message.ToolCalls {
		if call.ID == "" || call.Function.Name == "" || !json.Valid(call.Function.Arguments) {
			return prebuilt.AssistantMessage{}, fmt.Errorf("%s response tool call %d is invalid", m.config.Provider, i)
		}
		result.ToolCalls = append(result.ToolCalls, prebuilt.ToolCall{ID: call.ID, Name: call.Function.Name, Arguments: append(json.RawMessage(nil), call.Function.Arguments...)})
	}
	return result, nil
}

// Stream sends a streaming chat-completion request, publishes normalized
// chunks through Runtime.WriteMessage, and returns their merged final message.
func (m *Model[S]) Stream(ctx context.Context, state S, runtime graph.Runtime, emit func(prebuilt.AssistantMessageChunk) error) (prebuilt.AssistantMessage, error) {
	if m == nil {
		return prebuilt.AssistantMessage{}, errors.New("provider model is nil")
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
	if err := ctx.Err(); err != nil {
		return err
	}
	history, err := m.config.Messages(ctx, state)
	if err != nil {
		return fmt.Errorf("read %s messages: %w", m.config.Provider, err)
	}
	messages, err := encodeMessages(history)
	if err != nil {
		return fmt.Errorf("encode %s messages: %w", m.config.Provider, err)
	}
	request := chatRequest{Model: m.config.Model, Messages: messages, Temperature: m.config.Temperature, Stream: true, StreamOptions: &streamOptions{IncludeUsage: true}}
	if m.config.LegacyMaxTokens {
		request.MaxTokens = m.config.MaxTokens
	} else {
		request.MaxCompletionTokens = m.config.MaxTokens
	}
	if len(m.tools) > 0 {
		request.Tools = make([]wireTool, len(m.tools))
		for i, definition := range m.tools {
			request.Tools[i] = wireTool{Type: "function", Function: wireFunction{Name: definition.Name, Description: definition.Description, Parameters: definition.InputSchema}}
		}
	}
	body, err := json.Marshal(request)
	if err != nil {
		return fmt.Errorf("encode %s request: %w", m.config.Provider, err)
	}
	httpRequest, err := http.NewRequestWithContext(ctx, http.MethodPost, m.config.BaseURL+m.config.Path, bytes.NewReader(body))
	if err != nil {
		return err
	}
	httpRequest.Header.Set("Content-Type", "application/json")
	httpRequest.Header.Set("Accept", "text/event-stream")
	if m.config.APIKey != "" {
		httpRequest.Header.Set("Authorization", "Bearer "+m.config.APIKey)
	}
	for key, value := range m.config.Headers {
		httpRequest.Header.Set(key, value)
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
		data, readErr := io.ReadAll(io.LimitReader(response.Body, maxResponseBytes+1))
		if readErr != nil {
			return readErr
		}
		return &APIError{Provider: m.config.Provider, StatusCode: response.StatusCode, Body: strings.TrimSpace(string(data))}
	}
	streamID := ""
	streamIDLocked := false
	return sse.Scan(ctx, response.Body, maxResponseBytes, func(_ string, data string) error {
		if data == "[DONE]" {
			return nil
		}
		var decoded streamResponse
		if err := json.Unmarshal([]byte(data), &decoded); err != nil {
			return fmt.Errorf("decode %s stream event: %w", m.config.Provider, err)
		}
		if len(decoded.Error) > 0 && string(decoded.Error) != "null" {
			return fmt.Errorf("%s stream error: %s", m.config.Provider, decoded.Error)
		}
		if len(decoded.Choices) == 0 && decoded.Usage == nil {
			return nil
		}
		if !streamIDLocked {
			streamID = decoded.ID
			streamIDLocked = true
		}
		chunk := prebuilt.AssistantMessageChunk{ID: streamID}
		if decoded.Usage != nil {
			chunk.ResponseMetadata = map[string]any{"usage": decoded.Usage}
		}
		for _, choice := range decoded.Choices {
			if choice.Index != 0 {
				return fmt.Errorf("%s stream returned unsupported choice index %d", m.config.Provider, choice.Index)
			}
			chunk.Content += choice.Delta.Content
			if choice.FinishReason != "" {
				if chunk.ResponseMetadata == nil {
					chunk.ResponseMetadata = map[string]any{}
				}
				chunk.ResponseMetadata["finish_reason"] = choice.FinishReason
			}
			for _, call := range choice.Delta.ToolCalls {
				chunk.ToolCallChunks = append(chunk.ToolCallChunks, prebuilt.ToolCallChunk{Index: call.Index, ID: call.ID, Name: call.Function.Name, Arguments: call.Function.Arguments})
			}
		}
		return emit(chunk)
	})
}

type chatRequest struct {
	Model               string         `json:"model"`
	Messages            []wireMessage  `json:"messages"`
	Tools               []wireTool     `json:"tools,omitempty"`
	MaxCompletionTokens int            `json:"max_completion_tokens,omitempty"`
	MaxTokens           int            `json:"max_tokens,omitempty"`
	Temperature         *float64       `json:"temperature,omitempty"`
	Stream              bool           `json:"stream,omitempty"`
	StreamOptions       *streamOptions `json:"stream_options,omitempty"`
}

type streamOptions struct {
	IncludeUsage bool `json:"include_usage"`
}

type wireMessage struct {
	Role       string         `json:"role"`
	Content    string         `json:"content,omitempty"`
	Name       string         `json:"name,omitempty"`
	ToolCallID string         `json:"tool_call_id,omitempty"`
	ToolCalls  []wireToolCall `json:"tool_calls,omitempty"`
}

type wireTool struct {
	Type     string       `json:"type"`
	Function wireFunction `json:"function"`
}

type wireFunction struct {
	Name        string          `json:"name"`
	Description string          `json:"description,omitempty"`
	Parameters  json.RawMessage `json:"parameters"`
}

type wireToolCall struct {
	ID       string           `json:"id"`
	Type     string           `json:"type"`
	Function wireFunctionCall `json:"function"`
}

type wireFunctionCall struct {
	Name      string          `json:"name"`
	Arguments json.RawMessage `json:"arguments"`
}

type chatResponse struct {
	ID      string `json:"id"`
	Choices []struct {
		Message struct {
			Content   string         `json:"content"`
			ToolCalls []wireToolCall `json:"tool_calls"`
		} `json:"message"`
	} `json:"choices"`
}

type streamResponse struct {
	ID      string `json:"id"`
	Choices []struct {
		Index int `json:"index"`
		Delta struct {
			Content   string `json:"content"`
			ToolCalls []struct {
				Index    int    `json:"index"`
				ID       string `json:"id"`
				Function struct {
					Name      string `json:"name"`
					Arguments string `json:"arguments"`
				} `json:"function"`
			} `json:"tool_calls"`
		} `json:"delta"`
		FinishReason string `json:"finish_reason"`
	} `json:"choices"`
	Usage map[string]any  `json:"usage"`
	Error json.RawMessage `json:"error"`
}

func encodeMessages(messages []prebuilt.Message) ([]wireMessage, error) {
	result := make([]wireMessage, len(messages))
	for i, message := range messages {
		switch value := message.(type) {
		case prebuilt.SystemMessage:
			result[i] = wireMessage{Role: "system", Content: value.Content}
		case prebuilt.UserMessage:
			result[i] = wireMessage{Role: "user", Content: value.Content}
		case prebuilt.AssistantMessage:
			encoded := wireMessage{Role: "assistant", Content: value.Content}
			for _, call := range value.ToolCalls {
				encoded.ToolCalls = append(encoded.ToolCalls, wireToolCall{ID: call.ID, Type: "function", Function: wireFunctionCall{Name: call.Name, Arguments: append(json.RawMessage(nil), call.Arguments...)}})
			}
			result[i] = encoded
		case prebuilt.ToolMessage:
			result[i] = wireMessage{Role: "tool", Content: value.Content, Name: value.Name, ToolCallID: value.ToolCallID}
		default:
			return nil, fmt.Errorf("message %d has unsupported type %T", i, message)
		}
	}
	return result, nil
}

func cloneTools(source []prebuilt.ToolDefinition) ([]prebuilt.ToolDefinition, error) {
	result := make([]prebuilt.ToolDefinition, len(source))
	seen := make(map[string]struct{}, len(source))
	for i, definition := range source {
		if definition.Name == "" || !json.Valid(definition.InputSchema) {
			return nil, fmt.Errorf("tool definition %d is invalid", i)
		}
		if _, duplicate := seen[definition.Name]; duplicate {
			return nil, fmt.Errorf("duplicate tool definition %q", definition.Name)
		}
		seen[definition.Name] = struct{}{}
		result[i] = definition
		result[i].InputSchema = append(json.RawMessage(nil), definition.InputSchema...)
	}
	return result, nil
}

func cloneHeaders(source map[string]string) map[string]string {
	if source == nil {
		return nil
	}
	result := make(map[string]string, len(source))
	for key, value := range source {
		result[key] = value
	}
	return result
}
