package bedrock_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/wahanbo/langgraph-go/graph"
	"github.com/wahanbo/langgraph-go/prebuilt"
	"github.com/wahanbo/langgraph-go/providers/bedrock"
)

func TestBedrockConverseToolCallAndSigner(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Header.Get("Authorization") != "signed" {
			t.Fatalf("headers=%v", request.Header)
		}
		var body map[string]any
		_ = json.NewDecoder(request.Body).Decode(&body)
		if body["toolConfig"] == nil {
			t.Fatalf("body=%+v", body)
		}
		_, _ = writer.Write([]byte(`{"output":{"message":{"role":"assistant","content":[{"text":"checking"},{"toolUse":{"toolUseId":"call-1","name":"weather","input":{"city":"Paris"}}}]}}}`))
	}))
	defer server.Close()
	model, err := bedrock.New(bedrock.Config[int]{Region: "us-east-1", ModelID: "model", Endpoint: server.URL, Messages: func(context.Context, int) ([]prebuilt.Message, error) {
		return []prebuilt.Message{prebuilt.UserMessage{Content: "weather"}}, nil
	}, SignRequest: func(_ context.Context, request *http.Request, _ []byte) error {
		request.Header.Set("Authorization", "signed")
		return nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	bound, _ := model.BindTools([]prebuilt.ToolDefinition{{Name: "weather", InputSchema: json.RawMessage(`{"type":"object"}`)}})
	message, err := bound.Invoke(context.Background(), 0, graph.Runtime{})
	if err != nil {
		t.Fatal(err)
	}
	if message.Content != "checking" || len(message.ToolCalls) != 1 || message.ToolCalls[0].ID != "call-1" {
		t.Fatalf("message=%+v", message)
	}
}
