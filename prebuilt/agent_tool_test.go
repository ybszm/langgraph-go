package prebuilt_test

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/wahanbo/langgraph-go/graph"
	"github.com/wahanbo/langgraph-go/prebuilt"
)

func TestAgentAsToolInvokesChildGraph(t *testing.T) {
	builder := graph.NewStateGraph(func(_ context.Context, state int, updates []int) (int, error) {
		for _, update := range updates {
			state += update
		}
		return state, nil
	})
	_ = builder.AddNode("double", func(context.Context, int, graph.Runtime) (graph.Command[int], error) { return graph.Update(21), nil })
	_ = builder.AddEdge(graph.START, "double")
	_ = builder.AddEdge("double", graph.END)
	child, err := builder.Compile()
	if err != nil {
		t.Fatal(err)
	}
	tool, err := prebuilt.AgentAsTool(prebuilt.AgentToolConfig[string, string, int, int]{Name: "answer", InputSchema: json.RawMessage(`{"type":"object"}`), Agent: child, Input: func(context.Context, prebuilt.ToolCall, prebuilt.ToolRuntime[string]) (int, graph.RunConfig, error) {
		return 21, graph.RunConfig{}, nil
	}, Output: func(_ context.Context, state int) (prebuilt.ToolResult[string], error) {
		return prebuilt.TextResult[string](fmt.Sprint(state)), nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	result, err := tool.Invoke(context.Background(), prebuilt.ToolCall{ID: "call", Name: "answer", Arguments: json.RawMessage(`{}`)}, prebuilt.ToolRuntime[string]{})
	if err != nil || result.Message == nil || result.Message.Content != "42" {
		t.Fatalf("result=%+v err=%v", result, err)
	}
}
