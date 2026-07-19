package anthropic_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/ybszm/langgraph-go/graph"
	"github.com/ybszm/langgraph-go/prebuilt"
	"github.com/ybszm/langgraph-go/providers/anthropic"
)

func TestAnthropicToolRoundTrip(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Header.Get("x-api-key") != "secret" || request.Header.Get("anthropic-version") == "" {
			t.Fatalf("headers=%v", request.Header)
		}
		var body map[string]any
		_ = json.NewDecoder(request.Body).Decode(&body)
		if body["system"] != "be safe" || len(body["tools"].([]any)) != 1 {
			t.Fatalf("body=%+v", body)
		}
		_, _ = writer.Write([]byte(`{"id":"msg-1","content":[{"type":"text","text":"checking"},{"type":"tool_use","id":"call-1","name":"weather","input":{"city":"Paris"}}]}`))
	}))
	defer server.Close()
	model, err := anthropic.New(anthropic.Config[int]{BaseURL: server.URL, APIKey: "secret", Model: "claude", Messages: func(context.Context, int) ([]prebuilt.Message, error) {
		return []prebuilt.Message{prebuilt.SystemMessage{Content: "be safe"}, prebuilt.UserMessage{Content: "weather"}}, nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	bound, err := model.BindTools([]prebuilt.ToolDefinition{{Name: "weather", InputSchema: json.RawMessage(`{"type":"object"}`)}})
	if err != nil {
		t.Fatal(err)
	}
	message, err := bound.Invoke(context.Background(), 0, graph.Runtime{})
	if err != nil {
		t.Fatal(err)
	}
	if message.Content != "checking" || len(message.ToolCalls) != 1 || message.ToolCalls[0].Name != "weather" {
		t.Fatalf("message=%+v", message)
	}
}

func TestAnthropicStreamingTextToolAndUsage(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		var body map[string]any
		if err := json.NewDecoder(request.Body).Decode(&body); err != nil {
			t.Fatal(err)
		}
		if body["stream"] != true || request.Header.Get("Accept") != "text/event-stream" {
			t.Fatalf("body=%v accept=%q", body, request.Header.Get("Accept"))
		}
		writer.Header().Set("Content-Type", "text/event-stream")
		events := []struct{ name, data string }{
			{"message_start", `{"type":"message_start","message":{"id":"msg-1","usage":{"input_tokens":2}}}`},
			{"content_block_delta", `{"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"hello"}}`},
			{"content_block_start", `{"type":"content_block_start","index":1,"content_block":{"type":"tool_use","id":"tool-1","name":"weather","input":{}}}`},
			{"content_block_delta", `{"type":"content_block_delta","index":1,"delta":{"type":"input_json_delta","partial_json":"{\"city\":\"Paris\"}"}}`},
			{"message_delta", `{"type":"message_delta","delta":{"stop_reason":"tool_use"},"usage":{"output_tokens":3}}`},
			{"message_stop", `{"type":"message_stop"}`},
		}
		for _, event := range events {
			_, _ = fmt.Fprintf(writer, "event: %s\ndata: %s\n\n", event.name, event.data)
		}
	}))
	defer server.Close()
	model, err := anthropic.New(anthropic.Config[int]{BaseURL: server.URL, Model: "model", Streaming: true, Messages: func(context.Context, int) ([]prebuilt.Message, error) {
		return []prebuilt.Message{prebuilt.UserMessage{Content: "weather"}}, nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	var chunks []prebuilt.AssistantMessageChunk
	message, err := model.Stream(context.Background(), 0, graph.Runtime{}, func(chunk prebuilt.AssistantMessageChunk) error { chunks = append(chunks, chunk); return nil })
	if err != nil {
		t.Fatal(err)
	}
	if message.ID != "msg-1" || message.Content != "hello" || len(message.ToolCalls) != 1 || string(message.ToolCalls[0].Arguments) != `{"city":"Paris"}` {
		t.Fatalf("chunks=%+v message=%+v", chunks, message)
	}
	if chunks[len(chunks)-1].ResponseMetadata["finish_reason"] != "tool_use" {
		t.Fatalf("metadata=%+v", chunks[len(chunks)-1].ResponseMetadata)
	}
}

func TestAnthropicRejectsMalformedToolHistoryBeforeTransport(t *testing.T) {
	model, err := anthropic.New(anthropic.Config[int]{
		BaseURL: "https://example.invalid", Model: "claude",
		Messages: func(context.Context, int) ([]prebuilt.Message, error) {
			return []prebuilt.Message{prebuilt.AssistantMessage{ToolCalls: []prebuilt.ToolCall{{ID: "call", Name: "tool", Arguments: json.RawMessage(`{`)}}}}, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = model.Invoke(context.Background(), 0, graph.Runtime{})
	if err == nil || !strings.Contains(err.Error(), "encode Anthropic request") {
		t.Fatalf("err=%v", err)
	}
}
