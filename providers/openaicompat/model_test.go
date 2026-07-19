package openaicompat_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/ybszm/langgraph-go/graph"
	"github.com/ybszm/langgraph-go/prebuilt"
	"github.com/ybszm/langgraph-go/providers/openaicompat"
)

func TestModelEncodesHistoryToolsAndResponse(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.URL.Path != "/v1/chat/completions" || request.Header.Get("Authorization") != "Bearer secret" {
			t.Fatalf("request=%s auth=%q", request.URL.Path, request.Header.Get("Authorization"))
		}
		var body map[string]any
		if err := json.NewDecoder(request.Body).Decode(&body); err != nil {
			t.Fatal(err)
		}
		if body["model"] != "test-model" || len(body["messages"].([]any)) != 2 || len(body["tools"].([]any)) != 1 {
			t.Fatalf("body=%+v", body)
		}
		writer.Header().Set("Content-Type", "application/json")
		_, _ = writer.Write([]byte(`{"id":"response-1","choices":[{"message":{"content":"","tool_calls":[{"id":"call-1","type":"function","function":{"name":"weather","arguments":{"city":"Paris"}}}]}}]}`))
	}))
	defer server.Close()

	model, err := openaicompat.New(openaicompat.Config[int]{
		BaseURL: server.URL + "/v1", APIKey: "secret", Model: "test-model",
		Messages: func(context.Context, int) ([]prebuilt.Message, error) {
			return []prebuilt.Message{prebuilt.SystemMessage{Content: "safe"}, prebuilt.UserMessage{Content: "weather"}}, nil
		},
	})
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
	if message.ID != "response-1" || len(message.ToolCalls) != 1 || message.ToolCalls[0].Name != "weather" || string(message.ToolCalls[0].Arguments) != `{"city":"Paris"}` {
		t.Fatalf("message=%+v", message)
	}
}

func TestModelStreamsTextToolArgumentsUsageAndMergedResult(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		var body map[string]any
		if err := json.NewDecoder(request.Body).Decode(&body); err != nil {
			t.Fatal(err)
		}
		if body["stream"] != true || body["stream_options"].(map[string]any)["include_usage"] != true || request.Header.Get("Accept") != "text/event-stream" {
			t.Fatalf("request body=%v accept=%q", body, request.Header.Get("Accept"))
		}
		writer.Header().Set("Content-Type", "text/event-stream")
		for _, event := range []string{
			`{"id":"response-1","choices":[{"index":0,"delta":{"content":"hel"}}]}`,
			`{"id":"response-1","choices":[{"index":0,"delta":{"content":"lo","tool_calls":[{"index":0,"id":"call-1","function":{"name":"weather","arguments":"{\"city\":"}}]}}]}`,
			`{"id":"response-1","choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"function":{"arguments":"\"Paris\"}"}}]},"finish_reason":"tool_calls"}]}`,
			`{"id":"response-1","choices":[],"usage":{"prompt_tokens":3,"completion_tokens":4}}`,
		} {
			_, _ = fmt.Fprintf(writer, "data: %s\n\n", event)
		}
		_, _ = fmt.Fprint(writer, "data: [DONE]\n\n")
	}))
	defer server.Close()
	model, err := openaicompat.New(openaicompat.Config[int]{Provider: "test", BaseURL: server.URL, Model: "model", Streaming: true, Messages: func(context.Context, int) ([]prebuilt.Message, error) {
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
	if len(chunks) != 4 || message.ID != "response-1" || message.Content != "hello" || len(message.ToolCalls) != 1 || string(message.ToolCalls[0].Arguments) != `{"city":"Paris"}` {
		t.Fatalf("chunks=%+v message=%+v", chunks, message)
	}
	if chunks[2].ResponseMetadata["finish_reason"] != "tool_calls" || chunks[3].ResponseMetadata["usage"] == nil {
		t.Fatalf("metadata=%+v %+v", chunks[2].ResponseMetadata, chunks[3].ResponseMetadata)
	}
	invoked, err := model.Invoke(context.Background(), 0, graph.Runtime{})
	if err != nil || invoked.Content != "hello" {
		t.Fatalf("Invoke=%+v err=%v", invoked, err)
	}
}

func TestStreamingProviderPublishesGraphMessageEvents(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		writer.Header().Set("Content-Type", "text/event-stream")
		_, _ = fmt.Fprint(writer, "data: {\"id\":\"stream-message\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"one\"}}]}\n\n")
		_, _ = fmt.Fprint(writer, "data: {\"id\":\"stream-message\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\" two\"},\"finish_reason\":\"stop\"}]}\n\ndata: [DONE]\n\n")
	}))
	defer server.Close()
	model, err := openaicompat.New(openaicompat.Config[prebuilt.AgentState]{BaseURL: server.URL, Model: "model", Streaming: true, Messages: prebuilt.AgentModelMessages})
	if err != nil {
		t.Fatal(err)
	}
	agent, err := prebuilt.NewAgent(model, nil, prebuilt.ReactAgentConfig{})
	if err != nil {
		t.Fatal(err)
	}
	var streamed string
	for event := range agent.StreamWithOptions(context.Background(), prebuilt.NewAgentState("hello"), graph.RunConfig{}, graph.StreamOptions{Modes: []graph.StreamMode{graph.StreamMessages}}) {
		if event.Err != nil {
			t.Fatal(event.Err)
		}
		if event.Message == nil {
			continue
		}
		if chunk, ok := event.Message.Message.(prebuilt.AssistantMessageChunk); ok {
			streamed += chunk.Content
		}
	}
	if streamed != "one two" {
		t.Fatalf("streamed=%q", streamed)
	}
}

func TestModelReturnsTypedAPIError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		http.Error(writer, `{"error":"denied"}`, http.StatusUnauthorized)
	}))
	defer server.Close()
	model, _ := openaicompat.New(openaicompat.Config[int]{BaseURL: server.URL, Model: "model", Messages: func(context.Context, int) ([]prebuilt.Message, error) { return nil, nil }})
	_, err := model.Invoke(context.Background(), 0, graph.Runtime{})
	apiError, ok := err.(*openaicompat.APIError)
	if !ok || apiError.StatusCode != http.StatusUnauthorized {
		t.Fatalf("err=%T %v", err, err)
	}
}

func TestModelSelectsProviderTokenField(t *testing.T) {
	for _, test := range []struct {
		name       string
		legacy     bool
		want       string
		unexpected string
	}{
		{name: "openai", want: "max_completion_tokens", unexpected: "max_tokens"},
		{name: "compatible", legacy: true, want: "max_tokens", unexpected: "max_completion_tokens"},
	} {
		t.Run(test.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
				var body map[string]any
				if err := json.NewDecoder(request.Body).Decode(&body); err != nil {
					t.Fatal(err)
				}
				if body[test.want] != float64(42) {
					t.Fatalf("%s = %v, want 42; body=%v", test.want, body[test.want], body)
				}
				if _, exists := body[test.unexpected]; exists {
					t.Fatalf("unexpected %s in body=%v", test.unexpected, body)
				}
				_, _ = writer.Write([]byte(`{"choices":[{"message":{"content":"ok"}}]}`))
			}))
			defer server.Close()

			model, err := openaicompat.New(openaicompat.Config[int]{
				BaseURL: server.URL, Model: "model", MaxTokens: 42, LegacyMaxTokens: test.legacy,
				Messages: func(context.Context, int) ([]prebuilt.Message, error) { return nil, nil },
			})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := model.Invoke(context.Background(), 0, graph.Runtime{}); err != nil {
				t.Fatal(err)
			}
		})
	}
}
