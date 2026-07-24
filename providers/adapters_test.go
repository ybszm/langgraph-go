package providers

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/ybszm/langgraph-go/graph"
	"github.com/ybszm/langgraph-go/prebuilt"
	"github.com/ybszm/langgraph-go/providers/azureopenai"
	"github.com/ybszm/langgraph-go/providers/dashscope"
	"github.com/ybszm/langgraph-go/providers/deepseek"
	"github.com/ybszm/langgraph-go/providers/mistral"
	"github.com/ybszm/langgraph-go/providers/ollama"
	"github.com/ybszm/langgraph-go/providers/openai"
)

type invokingModel interface {
	Invoke(context.Context, int, graph.Runtime) (prebuilt.AssistantMessage, error)
}

func emptyMessages(context.Context, int) ([]prebuilt.Message, error) {
	return nil, nil
}

func TestOpenAICompatibleAdaptersConfigureRequests(t *testing.T) {
	tests := []struct {
		name          string
		path          string
		authorization string
		apiKey        string
		tokenField    string
		newModel      func(string, *http.Client) (invokingModel, error)
	}{
		{
			name: "OpenAI", path: "/chat/completions", authorization: "Bearer secret", tokenField: "max_completion_tokens",
			newModel: func(baseURL string, client *http.Client) (invokingModel, error) {
				return openai.New(openai.Config[int]{BaseURL: baseURL, APIKey: "secret", Model: "test-model", HTTPClient: client, Messages: emptyMessages, MaxTokens: 42})
			},
		},
		{
			name: "DeepSeek", path: "/chat/completions", authorization: "Bearer secret", tokenField: "max_tokens",
			newModel: func(baseURL string, client *http.Client) (invokingModel, error) {
				return deepseek.New(deepseek.Config[int]{BaseURL: baseURL, APIKey: "secret", Model: "test-model", HTTPClient: client, Messages: emptyMessages, MaxTokens: 42})
			},
		},
		{
			name: "DashScope", path: "/chat/completions", authorization: "Bearer secret", tokenField: "max_tokens",
			newModel: func(baseURL string, client *http.Client) (invokingModel, error) {
				return dashscope.New(dashscope.Config[int]{BaseURL: baseURL, APIKey: "secret", Model: "test-model", HTTPClient: client, Messages: emptyMessages, MaxTokens: 42})
			},
		},
		{
			name: "Mistral", path: "/chat/completions", authorization: "Bearer secret", tokenField: "max_tokens",
			newModel: func(baseURL string, client *http.Client) (invokingModel, error) {
				return mistral.New(mistral.Config[int]{BaseURL: baseURL, APIKey: "secret", Model: "test-model", HTTPClient: client, Messages: emptyMessages, MaxTokens: 42})
			},
		},
		{
			name: "Ollama", path: "/chat/completions", tokenField: "max_tokens",
			newModel: func(baseURL string, client *http.Client) (invokingModel, error) {
				return ollama.New(ollama.Config[int]{BaseURL: baseURL, Model: "test-model", HTTPClient: client, Messages: emptyMessages, MaxTokens: 42})
			},
		},
		{
			name: "Azure API key", path: "/openai/v1/chat/completions", apiKey: "secret", tokenField: "max_completion_tokens",
			newModel: func(baseURL string, client *http.Client) (invokingModel, error) {
				return azureopenai.New(azureopenai.Config[int]{Endpoint: baseURL, APIKey: "secret", Model: "test-model", HTTPClient: client, Messages: emptyMessages, MaxTokens: 42})
			},
		},
		{
			name: "Azure access token", path: "/openai/v1/chat/completions", authorization: "Bearer secret", tokenField: "max_completion_tokens",
			newModel: func(baseURL string, client *http.Client) (invokingModel, error) {
				return azureopenai.New(azureopenai.Config[int]{Endpoint: baseURL, AccessToken: "secret", Model: "test-model", HTTPClient: client, Messages: emptyMessages, MaxTokens: 42})
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
				if request.URL.Path != test.path {
					t.Errorf("path = %q, want %q", request.URL.Path, test.path)
				}
				if got := request.Header.Get("Authorization"); got != test.authorization {
					t.Errorf("Authorization = %q, want %q", got, test.authorization)
				}
				if got := request.Header.Get("api-key"); got != test.apiKey {
					t.Errorf("api-key = %q, want %q", got, test.apiKey)
				}
				var body map[string]any
				if err := json.NewDecoder(request.Body).Decode(&body); err != nil {
					t.Errorf("decode request: %v", err)
				}
				if body["model"] != "test-model" || body[test.tokenField] != float64(42) {
					t.Errorf("body = %v, want model and %s", body, test.tokenField)
				}
				writer.Header().Set("Content-Type", "application/json")
				_, _ = writer.Write([]byte(`{"id":"response-1","choices":[{"message":{"content":"ok"}}]}`))
			}))
			defer server.Close()

			model, err := test.newModel(server.URL, server.Client())
			if err != nil {
				t.Fatal(err)
			}
			message, err := model.Invoke(context.Background(), 0, graph.Runtime{})
			if err != nil {
				t.Fatal(err)
			}
			if message.ID != "response-1" || message.Content != "ok" {
				t.Fatalf("message = %+v", message)
			}
		})
	}
}
