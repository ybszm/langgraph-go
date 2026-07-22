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
	for _, message := range state.Messages {
		if message.Role == prebuilt.AgentRoleTool {
			return prebuilt.AssistantMessage{Content: "answer: " + message.Content}, nil
		}
	}
	return prebuilt.AssistantMessage{ToolCalls: []prebuilt.ToolCall{{ID: "add", Name: "add", Arguments: json.RawMessage(`{"a":20,"b":22}`)}}}, nil
}

func main() {
	add, _ := prebuilt.WithToolDefinition[prebuilt.AgentState, prebuilt.AgentDelta](
		prebuilt.ToolFunc[prebuilt.AgentState, prebuilt.AgentDelta]{ToolName: "add", Run: func(_ context.Context, call prebuilt.ToolCall, _ prebuilt.ToolRuntime[prebuilt.AgentState]) (prebuilt.ToolResult[prebuilt.AgentDelta], error) {
			var input struct{ A, B int }
			_ = json.Unmarshal(call.Arguments, &input)
			return prebuilt.TextResult[prebuilt.AgentDelta](fmt.Sprint(input.A + input.B)), nil
		}},
		prebuilt.ToolDefinition{Name: "add", Description: "Add two integers.", InputSchema: json.RawMessage(`{"type":"object","properties":{"a":{"type":"integer"},"b":{"type":"integer"}},"required":["a","b"]}`)},
	)
	agent, _ := prebuilt.NewChatModelAgent(prebuilt.ChatModelAgentConfig{Name: "calculator", SystemPrompt: "Use tools for arithmetic.", Model: model{}, Tools: []prebuilt.Tool[prebuilt.AgentState, prebuilt.AgentDelta]{add}})
	result, err := agent.Run(context.Background(), "What is 20 + 22?", graph.RunConfig{})
	if err != nil {
		panic(err)
	}
	fmt.Println(result.FinalResponse())
}
