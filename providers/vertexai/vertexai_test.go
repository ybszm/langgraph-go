package vertexai_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/wahanbo/langgraph-go/graph"
	"github.com/wahanbo/langgraph-go/prebuilt"
	"github.com/wahanbo/langgraph-go/providers/vertexai"
)

func TestVertexAIRequestEditorSupportsRefreshingCredentials(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Header.Get("Authorization") != "Bearer refreshed" {
			t.Fatalf("authorization=%q", request.Header.Get("Authorization"))
		}
		_, _ = writer.Write([]byte(`{"responseId":"response","candidates":[{"content":{"role":"model","parts":[{"text":"ok"}]}}]}`))
	}))
	defer server.Close()

	model, err := vertexai.New(vertexai.Config[int]{
		ProjectID: "project", Location: "us-central1", Model: "gemini", Endpoint: server.URL,
		Messages: func(context.Context, int) ([]prebuilt.Message, error) { return nil, nil },
		EditRequest: func(_ context.Context, request *http.Request, _ []byte) error {
			request.Header.Set("Authorization", "Bearer refreshed")
			return nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	message, err := model.Invoke(context.Background(), 0, graph.Runtime{})
	if err != nil || message.Content != "ok" {
		t.Fatalf("message=%+v err=%v", message, err)
	}
}
