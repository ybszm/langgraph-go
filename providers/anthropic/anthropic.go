// Package anthropic adapts the Anthropic Messages API to prebuilt.ChatModel.
package anthropic

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

const (
	DefaultBaseURL = "https://api.anthropic.com"
	DefaultVersion = "2023-06-01"
	maxBodyBytes   = 16 << 20
)

type Config[S any] struct {
	APIKey     string
	Model      string
	BaseURL    string
	Version    string
	HTTPClient *http.Client
	Messages   openaicompat.MessageReader[S]
	MaxTokens  int
	Streaming  bool
}

type Model[S any] struct {
	config Config[S]
	tools  []prebuilt.ToolDefinition
}

func New[S any](config Config[S]) (*Model[S], error) {
	if config.BaseURL == "" {
		config.BaseURL = DefaultBaseURL
	}
	config.BaseURL = strings.TrimRight(config.BaseURL, "/")
	parsed, err := url.Parse(config.BaseURL)
	if err != nil || parsed.Scheme == "" || parsed.Host == "" {
		return nil, errors.New("Anthropic base URL must be absolute")
	}
	if config.Version == "" {
		config.Version = DefaultVersion
	}
	if config.Model == "" || config.Messages == nil {
		return nil, errors.New("Anthropic model and message reader are required")
	}
	if config.MaxTokens == 0 {
		config.MaxTokens = 1024
	}
	if config.MaxTokens < 0 {
		return nil, errors.New("Anthropic max tokens cannot be negative")
	}
	if config.HTTPClient == nil {
		config.HTTPClient = http.DefaultClient
	}
	return &Model[S]{config: config}, nil
}

func (m *Model[S]) BindTools(definitions []prebuilt.ToolDefinition) (prebuilt.ChatModel[S], error) {
	if m == nil {
		return nil, errors.New("Anthropic model is nil")
	}
	bound := *m
	bound.tools = make([]prebuilt.ToolDefinition, len(definitions))
	for i, definition := range definitions {
		if definition.Name == "" || !json.Valid(definition.InputSchema) {
			return nil, fmt.Errorf("Anthropic tool definition %d is invalid", i)
		}
		bound.tools[i] = definition
		bound.tools[i].InputSchema = append(json.RawMessage(nil), definition.InputSchema...)
	}
	return &bound, nil
}

func (m *Model[S]) Invoke(ctx context.Context, state S, runtime graph.Runtime) (prebuilt.AssistantMessage, error) {
	if m == nil {
		return prebuilt.AssistantMessage{}, errors.New("Anthropic model is nil")
	}
	if m.config.Streaming {
		return m.Stream(ctx, state, runtime, nil)
	}
	history, err := m.config.Messages(ctx, state)
	if err != nil {
		return prebuilt.AssistantMessage{}, fmt.Errorf("read Anthropic messages: %w", err)
	}
	request, err := encodeRequest(m.config.Model, m.config.MaxTokens, history, m.tools)
	if err != nil {
		return prebuilt.AssistantMessage{}, err
	}
	body, err := json.Marshal(request)
	if err != nil {
		return prebuilt.AssistantMessage{}, fmt.Errorf("encode Anthropic request: %w", err)
	}
	httpRequest, err := http.NewRequestWithContext(ctx, http.MethodPost, m.config.BaseURL+"/v1/messages", bytes.NewReader(body))
	if err != nil {
		return prebuilt.AssistantMessage{}, err
	}
	httpRequest.Header.Set("Content-Type", "application/json")
	httpRequest.Header.Set("x-api-key", m.config.APIKey)
	httpRequest.Header.Set("anthropic-version", m.config.Version)
	response, err := m.config.HTTPClient.Do(httpRequest)
	if err != nil {
		return prebuilt.AssistantMessage{}, fmt.Errorf("call Anthropic: %w", err)
	}
	defer response.Body.Close()
	data, err := io.ReadAll(io.LimitReader(response.Body, maxBodyBytes+1))
	if err != nil {
		return prebuilt.AssistantMessage{}, err
	}
	if len(data) > maxBodyBytes {
		return prebuilt.AssistantMessage{}, errors.New("Anthropic response is too large")
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return prebuilt.AssistantMessage{}, &openaicompat.APIError{Provider: "Anthropic", StatusCode: response.StatusCode, Body: strings.TrimSpace(string(data))}
	}
	var decoded responseBody
	if err := json.Unmarshal(data, &decoded); err != nil {
		return prebuilt.AssistantMessage{}, fmt.Errorf("decode Anthropic response: %w", err)
	}
	result := prebuilt.AssistantMessage{ID: decoded.ID}
	for i, block := range decoded.Content {
		switch block.Type {
		case "text":
			result.Content += block.Text
		case "tool_use":
			if block.ID == "" || block.Name == "" || !json.Valid(block.Input) {
				return prebuilt.AssistantMessage{}, fmt.Errorf("Anthropic tool block %d is invalid", i)
			}
			result.ToolCalls = append(result.ToolCalls, prebuilt.ToolCall{ID: block.ID, Name: block.Name, Arguments: append(json.RawMessage(nil), block.Input...)})
		}
	}
	return result, nil
}

// Stream consumes Anthropic's Messages SSE protocol and returns the merged
// assistant response while publishing each normalized chunk to the graph.
func (m *Model[S]) Stream(ctx context.Context, state S, runtime graph.Runtime, emit func(prebuilt.AssistantMessageChunk) error) (prebuilt.AssistantMessage, error) {
	if m == nil {
		return prebuilt.AssistantMessage{}, errors.New("Anthropic model is nil")
	}
	adapter := prebuilt.StreamingChatModel[S, prebuilt.AssistantMessageChunk]{
		Metadata: map[string]any{"provider": "Anthropic"},
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
		return fmt.Errorf("read Anthropic messages: %w", err)
	}
	request, err := encodeRequest(m.config.Model, m.config.MaxTokens, history, m.tools)
	if err != nil {
		return err
	}
	request.Stream = true
	body, err := json.Marshal(request)
	if err != nil {
		return fmt.Errorf("encode Anthropic request: %w", err)
	}
	httpRequest, err := http.NewRequestWithContext(ctx, http.MethodPost, m.config.BaseURL+"/v1/messages", bytes.NewReader(body))
	if err != nil {
		return err
	}
	httpRequest.Header.Set("Content-Type", "application/json")
	httpRequest.Header.Set("Accept", "text/event-stream")
	httpRequest.Header.Set("x-api-key", m.config.APIKey)
	httpRequest.Header.Set("anthropic-version", m.config.Version)
	response, err := m.config.HTTPClient.Do(httpRequest)
	if err != nil {
		return fmt.Errorf("call Anthropic: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		data, readErr := io.ReadAll(io.LimitReader(response.Body, maxBodyBytes+1))
		if readErr != nil {
			return readErr
		}
		return &openaicompat.APIError{Provider: "Anthropic", StatusCode: response.StatusCode, Body: strings.TrimSpace(string(data))}
	}
	messageID := ""
	return sse.Scan(ctx, response.Body, maxBodyBytes, func(_ string, data string) error {
		var event anthropicStreamEvent
		if err := json.Unmarshal([]byte(data), &event); err != nil {
			return fmt.Errorf("decode Anthropic stream event: %w", err)
		}
		chunk := prebuilt.AssistantMessageChunk{ID: messageID}
		switch event.Type {
		case "ping", "message_stop":
			return nil
		case "error":
			return fmt.Errorf("Anthropic stream error: %s", strings.TrimSpace(string(event.Error)))
		case "message_start":
			messageID = event.Message.ID
			if messageID == "" {
				return errors.New("Anthropic message_start has no ID")
			}
			chunk.ID = messageID
			if event.Message.Usage != nil {
				chunk.ResponseMetadata = map[string]any{"usage": event.Message.Usage}
			}
		case "content_block_start":
			if event.ContentBlock.Type != "tool_use" {
				return nil
			}
			chunk.ToolCallChunks = []prebuilt.ToolCallChunk{{Index: event.Index, ID: event.ContentBlock.ID, Name: event.ContentBlock.Name}}
		case "content_block_delta":
			switch event.Delta.Type {
			case "text_delta":
				chunk.Content = event.Delta.Text
			case "input_json_delta":
				chunk.ToolCallChunks = []prebuilt.ToolCallChunk{{Index: event.Index, Arguments: event.Delta.PartialJSON}}
			default:
				return nil
			}
		case "message_delta":
			chunk.ResponseMetadata = map[string]any{"finish_reason": event.Delta.StopReason}
			if event.Usage != nil {
				chunk.ResponseMetadata["usage"] = event.Usage
			}
		default:
			return nil
		}
		return emit(chunk)
	})
}

type requestBody struct {
	Model     string        `json:"model"`
	MaxTokens int           `json:"max_tokens"`
	System    string        `json:"system,omitempty"`
	Messages  []wireMessage `json:"messages"`
	Tools     []wireTool    `json:"tools,omitempty"`
	Stream    bool          `json:"stream,omitempty"`
}

type anthropicStreamEvent struct {
	Type    string `json:"type"`
	Index   int    `json:"index"`
	Message struct {
		ID    string         `json:"id"`
		Usage map[string]any `json:"usage"`
	} `json:"message"`
	ContentBlock wireBlock `json:"content_block"`
	Delta        struct {
		Type        string `json:"type"`
		Text        string `json:"text"`
		PartialJSON string `json:"partial_json"`
		StopReason  string `json:"stop_reason"`
	} `json:"delta"`
	Usage map[string]any  `json:"usage"`
	Error json.RawMessage `json:"error"`
}

type wireMessage struct {
	Role    string      `json:"role"`
	Content []wireBlock `json:"content"`
}

type wireBlock struct {
	Type      string          `json:"type"`
	Text      string          `json:"text,omitempty"`
	ID        string          `json:"id,omitempty"`
	Name      string          `json:"name,omitempty"`
	Input     json.RawMessage `json:"input,omitempty"`
	ToolUseID string          `json:"tool_use_id,omitempty"`
	Content   string          `json:"content,omitempty"`
}

type wireTool struct {
	Name        string          `json:"name"`
	Description string          `json:"description,omitempty"`
	InputSchema json.RawMessage `json:"input_schema"`
}

type responseBody struct {
	ID      string      `json:"id"`
	Content []wireBlock `json:"content"`
}

func encodeRequest(model string, maxTokens int, history []prebuilt.Message, tools []prebuilt.ToolDefinition) (requestBody, error) {
	request := requestBody{Model: model, MaxTokens: maxTokens}
	for i, message := range history {
		switch value := message.(type) {
		case prebuilt.SystemMessage:
			if request.System != "" {
				request.System += "\n\n"
			}
			request.System += value.Content
		case prebuilt.UserMessage:
			request.Messages = append(request.Messages, wireMessage{Role: "user", Content: []wireBlock{{Type: "text", Text: value.Content}}})
		case prebuilt.AssistantMessage:
			blocks := make([]wireBlock, 0, 1+len(value.ToolCalls))
			if value.Content != "" {
				blocks = append(blocks, wireBlock{Type: "text", Text: value.Content})
			}
			for _, call := range value.ToolCalls {
				blocks = append(blocks, wireBlock{Type: "tool_use", ID: call.ID, Name: call.Name, Input: append(json.RawMessage(nil), call.Arguments...)})
			}
			request.Messages = append(request.Messages, wireMessage{Role: "assistant", Content: blocks})
		case prebuilt.ToolMessage:
			request.Messages = append(request.Messages, wireMessage{Role: "user", Content: []wireBlock{{Type: "tool_result", ToolUseID: value.ToolCallID, Content: value.Content}}})
		default:
			return requestBody{}, fmt.Errorf("Anthropic message %d has unsupported type %T", i, message)
		}
	}
	for _, definition := range tools {
		request.Tools = append(request.Tools, wireTool{Name: definition.Name, Description: definition.Description, InputSchema: append(json.RawMessage(nil), definition.InputSchema...)})
	}
	return request, nil
}
