package main

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/ybszm/langgraph-go/graph"
	"github.com/ybszm/langgraph-go/prebuilt"
)

type model struct{}

func (model) Invoke(_ context.Context, state prebuilt.AgentState, _ graph.Runtime) (prebuilt.AssistantMessage, error) {
	if len(state.Messages) < 3 {
		return prebuilt.AssistantMessage{ToolCalls: []prebuilt.ToolCall{{ID: "echo", Name: "echo", Arguments: json.RawMessage(`{"text":"hello"}`)}}}, nil
	}
	return prebuilt.AssistantMessage{Content: "done"}, nil
}

func main() {
	callback := prebuilt.AgentCallbackFuncs{
		ModelStart: func(_ context.Context, event prebuilt.AgentModelStartEvent) { fmt.Println("model start:", event.Agent) },
		ModelEnd:   func(_ context.Context, event prebuilt.AgentModelEndEvent) { fmt.Println("model end:", event.Agent) },
		ToolStart: func(_ context.Context, event prebuilt.AgentToolStartEvent) {
			fmt.Println("tool start:", event.Call.Name)
		},
		ToolEnd: func(_ context.Context, event prebuilt.AgentToolEndEvent) { fmt.Println("tool end:", event.Call.Name) },
	}
	tool := prebuilt.ToolFunc[prebuilt.AgentState, prebuilt.AgentDelta]{ToolName: "echo", Run: func(_ context.Context, call prebuilt.ToolCall, _ prebuilt.ToolRuntime[prebuilt.AgentState]) (prebuilt.ToolResult[prebuilt.AgentDelta], error) {
		return prebuilt.TextResult[prebuilt.AgentDelta](string(call.Arguments)), nil
	}}
	agent, _ := prebuilt.NewChatModelAgent(prebuilt.ChatModelAgentConfig{Model: model{}, Tools: []prebuilt.Tool[prebuilt.AgentState, prebuilt.AgentDelta]{tool}, Callbacks: []prebuilt.AgentCallback{callback}})
	_, _ = agent.Run(context.Background(), "start", graph.RunConfig{})
}
