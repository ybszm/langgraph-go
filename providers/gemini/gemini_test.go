package gemini_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/wahanbo/langgraph-go/graph"
	"github.com/wahanbo/langgraph-go/prebuilt"
	"github.com/wahanbo/langgraph-go/providers/gemini"
)

func TestGeminiFunctionCall(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Header.Get("x-goog-api-key") != "secret" || request.URL.Path != "/models/gemini-test:generateContent" {
			t.Fatalf("request=%s headers=%v", request.URL.Path, request.Header)
		}
		var body map[string]any
		_ = json.NewDecoder(request.Body).Decode(&body)
		if len(body["tools"].([]any)) != 1 {
			t.Fatalf("body=%+v", body)
		}
		_, _ = writer.Write([]byte(`{"responseId":"response-1","candidates":[{"content":{"role":"model","parts":[{"text":"checking"},{"functionCall":{"name":"weather","args":{"city":"Paris"}}}]}}]}`))
	}))
	defer server.Close()
	model, err := gemini.New(gemini.Config[int]{BaseURL: server.URL, APIKey: "secret", Model: "gemini-test", Messages: func(context.Context, int) ([]prebuilt.Message, error) {
		return []prebuilt.Message{prebuilt.UserMessage{Content: "weather"}}, nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	bound, _ := model.BindTools([]prebuilt.ToolDefinition{{Name: "weather", InputSchema: json.RawMessage(`{"type":"object"}`)}})
	message, err := bound.Invoke(context.Background(), 0, graph.Runtime{})
	if err != nil {
		t.Fatal(err)
	}
	if message.Content != "checking" || len(message.ToolCalls) != 1 || message.ToolCalls[0].ID != "response-1-call-1" {
		t.Fatalf("message=%+v", message)
	}
}

func TestGeminiStreamingTextFunctionCallAndUsage(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.URL.Path != "/models/gemini-test:streamGenerateContent" || request.URL.Query().Get("alt") != "sse" || request.Header.Get("Accept") != "text/event-stream" {
			t.Fatalf("url=%s accept=%q", request.URL.String(), request.Header.Get("Accept"))
		}
		writer.Header().Set("Content-Type", "text/event-stream")
		for _, event := range []string{
			`{"candidates":[{"content":{"parts":[{"text":"hel"}]}}]}`,
			`{"responseId":"response-1","candidates":[{"content":{"parts":[{"text":"lo"},{"functionCall":{"name":"weather","args":{"city":"Paris"}}}]},"finishReason":"STOP"}],"usageMetadata":{"totalTokenCount":5}}`,
		} {
			_, _ = fmt.Fprintf(writer, "data: %s\n\n", event)
		}
	}))
	defer server.Close()
	model, err := gemini.New(gemini.Config[int]{BaseURL: server.URL, APIKey: "secret", Model: "gemini-test", Streaming: true, Messages: func(context.Context, int) ([]prebuilt.Message, error) {
		return []prebuilt.Message{prebuilt.UserMessage{Content: "hello"}}, nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	var chunks []prebuilt.AssistantMessageChunk
	message, err := model.Stream(context.Background(), 0, graph.Runtime{}, func(chunk prebuilt.AssistantMessageChunk) error { chunks = append(chunks, chunk); return nil })
	if err != nil {
		t.Fatal(err)
	}
	if message.Content != "hello" || len(message.ToolCalls) != 1 || message.ToolCalls[0].Name != "weather" || string(message.ToolCalls[0].Arguments) != `{"city":"Paris"}` {
		t.Fatalf("chunks=%+v message=%+v", chunks, message)
	}
	if chunks[0].ID == "" || chunks[0].ID != chunks[1].ID || chunks[0].ID != message.ID {
		t.Fatalf("unstable generated IDs: chunks=%+v message=%+v", chunks, message)
	}
	if chunks[1].ResponseMetadata["finish_reason"] != "STOP" || chunks[1].ResponseMetadata["usage"] == nil {
		t.Fatalf("metadata=%+v", chunks[1].ResponseMetadata)
	}
}
