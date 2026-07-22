package main

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/ybszm/langgraph-go/graph"
	"github.com/ybszm/langgraph-go/prebuilt"
)

type model struct{}

func (model) Invoke(_ context.Context, state prebuilt.AgentState, _ graph.Runtime) (prebuilt.AssistantMessage, error) {
	for _, message := range state.Messages {
		if message.Role == prebuilt.AgentRoleSystem && strings.Contains(message.Content, "Work autonomously") {
			return prebuilt.AssistantMessage{Content: "isolated research result"}, nil
		}
	}
	tools := make([]prebuilt.AgentMessage, 0)
	for _, message := range state.Messages {
		if message.Role == prebuilt.AgentRoleTool {
			tools = append(tools, message)
		}
	}
	switch len(tools) {
	case 0:
		return prebuilt.AssistantMessage{ToolCalls: []prebuilt.ToolCall{{ID: "plan", Name: prebuilt.TodoToolName, Arguments: json.RawMessage(`{"todos":[{"content":"delegate research","status":"in_progress"}]}`)}}}, nil
	case 1:
		return prebuilt.AssistantMessage{ToolCalls: []prebuilt.ToolCall{{ID: "task", Name: prebuilt.DelegateToolName, Arguments: json.RawMessage(`{"agent":"general-purpose","task":"research graph persistence"}`)}}}, nil
	default:
		return prebuilt.AssistantMessage{Content: "synthesis: " + tools[len(tools)-1].Content}, nil
	}
}

func main() {
	agent, err := prebuilt.NewDeepAgent(prebuilt.DeepAgentConfig{Model: model{}})
	if err != nil {
		panic(err)
	}
	result, err := agent.Run(context.Background(), "Explain persistence.", graph.RunConfig{})
	if err != nil {
		panic(err)
	}
	fmt.Printf("%s (todos: %d)\n", result.FinalResponse(), len(result.Todos))
}
