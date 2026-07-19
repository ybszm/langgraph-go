// Package bedrock adapts Amazon Bedrock's Converse API to prebuilt.ChatModel.
package bedrock

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
	"github.com/ybszm/langgraph-go/providers/openaicompat"
)

const maxBodyBytes = 16 << 20

// RequestSigner applies AWS SigV4 credentials to a fully built request. This
// boundary permits AWS SDK, workload identity, or custom credential providers
// without coupling the adapter to one credential implementation.
type RequestSigner func(context.Context, *http.Request, []byte) error

type Config[S any] struct {
	Region      string
	ModelID     string
	Endpoint    string
	HTTPClient  *http.Client
	Messages    openaicompat.MessageReader[S]
	SignRequest RequestSigner
	MaxTokens   int
}

type Model[S any] struct {
	config Config[S]
	tools  []prebuilt.ToolDefinition
}

func New[S any](config Config[S]) (*Model[S], error) {
	if config.Region == "" || config.ModelID == "" || config.Messages == nil || config.SignRequest == nil {
		return nil, errors.New("Bedrock region, model ID, message reader, and SigV4 signer are required")
	}
	if config.Endpoint == "" {
		config.Endpoint = "https://bedrock-runtime." + config.Region + ".amazonaws.com"
	}
	config.Endpoint = strings.TrimRight(config.Endpoint, "/")
	parsed, err := url.Parse(config.Endpoint)
	if err != nil || parsed.Scheme == "" || parsed.Host == "" {
		return nil, errors.New("Bedrock endpoint must be absolute")
	}
	if config.MaxTokens < 0 {
		return nil, errors.New("Bedrock max tokens cannot be negative")
	}
	if config.HTTPClient == nil {
		config.HTTPClient = http.DefaultClient
	}
	return &Model[S]{config: config}, nil
}

func (m *Model[S]) BindTools(definitions []prebuilt.ToolDefinition) (prebuilt.ChatModel[S], error) {
	if m == nil {
		return nil, errors.New("Bedrock model is nil")
	}
	bound := *m
	bound.tools = make([]prebuilt.ToolDefinition, len(definitions))
	for i, definition := range definitions {
		if definition.Name == "" || !json.Valid(definition.InputSchema) {
			return nil, fmt.Errorf("Bedrock tool definition %d is invalid", i)
		}
		bound.tools[i] = definition
		bound.tools[i].InputSchema = append(json.RawMessage(nil), definition.InputSchema...)
	}
	return &bound, nil
}

func (m *Model[S]) Invoke(ctx context.Context, state S, _ graph.Runtime) (prebuilt.AssistantMessage, error) {
	if m == nil {
		return prebuilt.AssistantMessage{}, errors.New("Bedrock model is nil")
	}
	history, err := m.config.Messages(ctx, state)
	if err != nil {
		return prebuilt.AssistantMessage{}, fmt.Errorf("read Bedrock messages: %w", err)
	}
	request, err := encodeRequest(history, m.tools, m.config.MaxTokens)
	if err != nil {
		return prebuilt.AssistantMessage{}, err
	}
	body, err := json.Marshal(request)
	if err != nil {
		return prebuilt.AssistantMessage{}, fmt.Errorf("encode Bedrock request: %w", err)
	}
	endpoint := m.config.Endpoint + "/model/" + url.PathEscape(m.config.ModelID) + "/converse"
	httpRequest, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		return prebuilt.AssistantMessage{}, err
	}
	httpRequest.Header.Set("Content-Type", "application/json")
	if err := m.config.SignRequest(ctx, httpRequest, body); err != nil {
		return prebuilt.AssistantMessage{}, fmt.Errorf("sign Bedrock request: %w", err)
	}
	response, err := m.config.HTTPClient.Do(httpRequest)
	if err != nil {
		return prebuilt.AssistantMessage{}, fmt.Errorf("call Bedrock: %w", err)
	}
	defer response.Body.Close()
	data, err := io.ReadAll(io.LimitReader(response.Body, maxBodyBytes+1))
	if err != nil {
		return prebuilt.AssistantMessage{}, err
	}
	if len(data) > maxBodyBytes {
		return prebuilt.AssistantMessage{}, errors.New("Bedrock response is too large")
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return prebuilt.AssistantMessage{}, &openaicompat.APIError{Provider: "Bedrock", StatusCode: response.StatusCode, Body: strings.TrimSpace(string(data))}
	}
	var decoded responseBody
	if err := json.Unmarshal(data, &decoded); err != nil {
		return prebuilt.AssistantMessage{}, fmt.Errorf("decode Bedrock response: %w", err)
	}
	result := prebuilt.AssistantMessage{}
	for i, block := range decoded.Output.Message.Content {
		result.Content += block.Text
		if block.ToolUse != nil {
			arguments, err := json.Marshal(block.ToolUse.Input)
			if err != nil || block.ToolUse.ToolUseID == "" || block.ToolUse.Name == "" {
				return prebuilt.AssistantMessage{}, fmt.Errorf("Bedrock tool call %d is invalid", i)
			}
			result.ToolCalls = append(result.ToolCalls, prebuilt.ToolCall{ID: block.ToolUse.ToolUseID, Name: block.ToolUse.Name, Arguments: arguments})
		}
	}
	return result, nil
}

type requestBody struct {
	System          []systemBlock    `json:"system,omitempty"`
	Messages        []message        `json:"messages"`
	ToolConfig      *toolConfig      `json:"toolConfig,omitempty"`
	InferenceConfig *inferenceConfig `json:"inferenceConfig,omitempty"`
}
type systemBlock struct {
	Text string `json:"text"`
}
type message struct {
	Role    string         `json:"role"`
	Content []contentBlock `json:"content"`
}
type contentBlock struct {
	Text       string      `json:"text,omitempty"`
	ToolUse    *toolUse    `json:"toolUse,omitempty"`
	ToolResult *toolResult `json:"toolResult,omitempty"`
}
type toolUse struct {
	ToolUseID string         `json:"toolUseId"`
	Name      string         `json:"name"`
	Input     map[string]any `json:"input"`
}
type toolResult struct {
	ToolUseID string          `json:"toolUseId"`
	Content   []resultContent `json:"content"`
}
type resultContent struct {
	Text string         `json:"text,omitempty"`
	JSON map[string]any `json:"json,omitempty"`
}
type toolConfig struct {
	Tools []tool `json:"tools"`
}
type tool struct {
	ToolSpec toolSpec `json:"toolSpec"`
}
type toolSpec struct {
	Name        string      `json:"name"`
	Description string      `json:"description,omitempty"`
	InputSchema inputSchema `json:"inputSchema"`
}
type inputSchema struct {
	JSON map[string]any `json:"json"`
}
type inferenceConfig struct {
	MaxTokens int `json:"maxTokens,omitempty"`
}
type responseBody struct {
	Output struct {
		Message message `json:"message"`
	} `json:"output"`
}

func encodeRequest(history []prebuilt.Message, definitions []prebuilt.ToolDefinition, maxTokens int) (requestBody, error) {
	var request requestBody
	if maxTokens > 0 {
		request.InferenceConfig = &inferenceConfig{MaxTokens: maxTokens}
	}
	for i, item := range history {
		switch value := item.(type) {
		case prebuilt.SystemMessage:
			request.System = append(request.System, systemBlock{Text: value.Content})
		case prebuilt.UserMessage:
			request.Messages = append(request.Messages, message{Role: "user", Content: []contentBlock{{Text: value.Content}}})
		case prebuilt.AssistantMessage:
			blocks := make([]contentBlock, 0, 1+len(value.ToolCalls))
			if value.Content != "" {
				blocks = append(blocks, contentBlock{Text: value.Content})
			}
			for _, call := range value.ToolCalls {
				var input map[string]any
				if err := json.Unmarshal(call.Arguments, &input); err != nil {
					return requestBody{}, fmt.Errorf("Bedrock assistant arguments: %w", err)
				}
				blocks = append(blocks, contentBlock{ToolUse: &toolUse{ToolUseID: call.ID, Name: call.Name, Input: input}})
			}
			request.Messages = append(request.Messages, message{Role: "assistant", Content: blocks})
		case prebuilt.ToolMessage:
			content := resultContent{Text: value.Content}
			var object map[string]any
			if json.Unmarshal([]byte(value.Content), &object) == nil && object != nil {
				content = resultContent{JSON: object}
			}
			request.Messages = append(request.Messages, message{Role: "user", Content: []contentBlock{{ToolResult: &toolResult{ToolUseID: value.ToolCallID, Content: []resultContent{content}}}}})
		default:
			return requestBody{}, fmt.Errorf("Bedrock message %d has unsupported type %T", i, item)
		}
	}
	if len(definitions) > 0 {
		request.ToolConfig = &toolConfig{}
		for i, definition := range definitions {
			var schema map[string]any
			if err := json.Unmarshal(definition.InputSchema, &schema); err != nil {
				return requestBody{}, fmt.Errorf("Bedrock tool schema %d: %w", i, err)
			}
			request.ToolConfig.Tools = append(request.ToolConfig.Tools, tool{ToolSpec: toolSpec{Name: definition.Name, Description: definition.Description, InputSchema: inputSchema{JSON: schema}}})
		}
	}
	return request, nil
}
