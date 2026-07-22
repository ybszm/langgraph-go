package main

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/ybszm/langgraph-go/graph"
	"github.com/ybszm/langgraph-go/prebuilt"
)

type workerModel struct{ role string }

func (model workerModel) Invoke(_ context.Context, state prebuilt.AgentState, _ graph.Runtime) (prebuilt.AssistantMessage, error) {
	request := state.Messages[len(state.Messages)-1].Content
	return prebuilt.AssistantMessage{Content: model.role + " completed: " + request}, nil
}

type supervisorModel struct{}

func (supervisorModel) Invoke(_ context.Context, state prebuilt.AgentState, _ graph.Runtime) (prebuilt.AssistantMessage, error) {
	results := make([]string, 0, 2)
	for _, message := range state.Messages {
		if message.Role == prebuilt.AgentRoleTool {
			results = append(results, message.Content)
		}
	}
	switch len(results) {
	case 0:
		return prebuilt.AssistantMessage{ToolCalls: []prebuilt.ToolCall{{
			ID: "plan", Name: prebuilt.TodoToolName,
			Arguments: json.RawMessage(`{"todos":[{"content":"research facts","status":"in_progress"},{"content":"draft summary","status":"in_progress"}]}`),
		}}}, nil
	case 1:
		return prebuilt.AssistantMessage{ToolCalls: []prebuilt.ToolCall{
			{ID: "research", Name: prebuilt.DelegateToolName, Arguments: json.RawMessage(`{"agent":"researcher","task":"Find the key graph runtime facts."}`)},
			{ID: "writer", Name: prebuilt.DelegateToolName, Arguments: json.RawMessage(`{"agent":"writer","task":"Draft a two-sentence project summary."}`)},
		}}, nil
	default:
		return prebuilt.AssistantMessage{Content: "Supervisor synthesis: " + strings.Join(results[1:], " | ")}, nil
	}
}

func mustAgent(name, prompt string, model prebuilt.ChatModel[prebuilt.AgentState]) *prebuilt.ChatModelAgent {
	agent, err := prebuilt.NewChatModelAgent(prebuilt.ChatModelAgentConfig{Name: name, SystemPrompt: prompt, Model: model})
	if err != nil {
		panic(err)
	}
	return agent
}

func main() {
	researcher := mustAgent("researcher", "Return verified facts only.", workerModel{role: "researcher"})
	writer := mustAgent("writer", "Write concise prose.", workerModel{role: "writer"})

	agent, err := prebuilt.NewDeepAgent(prebuilt.DeepAgentConfig{
		Name: "report-agent", SystemPrompt: "Coordinate a reliable project report.", Model: supervisorModel{},
		DisableGeneralPurpose: true,
		SubAgents: []prebuilt.SubAgent{
			{Name: "researcher", Description: "Finds and verifies technical facts.", Agent: researcher},
			{Name: "writer", Description: "Turns findings into concise prose.", Agent: writer},
		},
		React: prebuilt.ReactAgentConfig{ToolNode: prebuilt.ToolNodeConfig{MaxConcurrency: 2}},
	})
	if err != nil {
		panic(err)
	}

	result, err := agent.Run(context.Background(), "Explain this agent framework.", graph.RunConfig{})
	if err != nil {
		panic(err)
	}
	fmt.Println(result.FinalResponse())
}
