package mcpclient_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/ybszm/langgraph-go/graph"
	"github.com/ybszm/langgraph-go/mcpclient"
	"github.com/ybszm/langgraph-go/prebuilt"
)

type addInput struct {
	A int `json:"a"`
	B int `json:"b"`
}
type addOutput struct {
	Total int `json:"total"`
}

func TestMCPToolsDiscoveryAndCall(t *testing.T) {
	serverTransport, clientTransport := mcp.NewInMemoryTransports()
	server := mcp.NewServer(&mcp.Implementation{Name: "test-server", Version: "1"}, nil)
	mcp.AddTool(server, &mcp.Tool{Name: "add", Description: "add numbers"}, func(_ context.Context, _ *mcp.CallToolRequest, input addInput) (*mcp.CallToolResult, addOutput, error) {
		return nil, addOutput{Total: input.A + input.B}, nil
	})
	serverSession, err := server.Connect(context.Background(), serverTransport, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer serverSession.Close()
	client, err := mcpclient.Connect(context.Background(), clientTransport, mcpclient.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	tools, err := mcpclient.Tools[int, string](context.Background(), client)
	if err != nil || len(tools) != 1 || tools[0].Name() != "add" {
		t.Fatalf("tools=%v err=%v", tools, err)
	}
	result, err := tools[0].Invoke(context.Background(), prebuilt.ToolCall{ID: "call", Name: "add", Arguments: json.RawMessage(`{"a":20,"b":22}`)}, prebuilt.ToolRuntime[int]{Graph: graph.Runtime{}})
	if err != nil || result.Message == nil || result.Message.Content != `{"total":42}` || result.Message.Artifact == nil {
		t.Fatalf("result=%+v err=%v", result, err)
	}
}
