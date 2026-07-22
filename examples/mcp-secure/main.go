// Example: secure MCP tool binding pattern (offline).
//
// This program does not dial a real MCP server. It shows the production
// pattern: discover tools → allowlist/deny via GuardTools → attach to QuickAgent.
package main

import (
	"context"
	"fmt"

	"github.com/ybszm/langgraph-go/graph"
	"github.com/ybszm/langgraph-go/prebuilt"
)

// In production:
//
//	client, err := mcpclient.ConnectHTTP(ctx, os.Getenv("MCP_URL"), nil, mcpclient.Options{})
//	tools, err := mcpclient.Tools[prebuilt.AgentState, prebuilt.AgentDelta](ctx, client)
//	tools, err = prebuilt.GuardTools(tools, prebuilt.ToolGuardPolicy{
//	    Allowed: []string{"get_metrics", "search_logs"},
//	    Denied:  []string{"run_shell"},
//	})

type demoModel struct{}

func (demoModel) Invoke(_ context.Context, _ prebuilt.AgentState, _ graph.Runtime) (prebuilt.AssistantMessage, error) {
	return prebuilt.AssistantMessage{Content: "mcp-secure pattern ready"}, nil
}

type demoTool struct{ name string }

func (t demoTool) Name() string { return t.name }
func (t demoTool) Invoke(context.Context, prebuilt.ToolCall, prebuilt.ToolRuntime[prebuilt.AgentState]) (prebuilt.ToolResult[prebuilt.AgentDelta], error) {
	return prebuilt.TextResult[prebuilt.AgentDelta]("ok"), nil
}

func main() {
	raw := []prebuilt.Tool[prebuilt.AgentState, prebuilt.AgentDelta]{
		demoTool{name: "get_metrics"},
		demoTool{name: "run_shell"},
	}
	tools, err := prebuilt.GuardTools(raw, prebuilt.ToolGuardPolicy{
		Allowed: []string{"get_metrics"},
		Denied:  []string{"run_shell"},
	})
	if err != nil {
		panic(err)
	}
	// run_shell remains in slice but policy denies invocation at call time.
	_, _ = tools[1].Invoke(context.Background(), prebuilt.ToolCall{ID: "1", Name: "run_shell"}, prebuilt.ToolRuntime[prebuilt.AgentState]{})

	agent, err := prebuilt.NewQuickAgent(prebuilt.QuickAgentConfig{
		Model: demoModel{},
		Tools: tools[:1],
	})
	if err != nil {
		panic(err)
	}
	state, err := agent.Run(context.Background(), "status?", graph.RunConfig{})
	if err != nil {
		panic(err)
	}
	fmt.Println(state.FinalResponse())
	fmt.Println("tools after guard:", len(tools))
}
