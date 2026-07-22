package main

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/ybszm/langgraph-go/graph"
	"github.com/ybszm/langgraph-go/prebuilt"
)

type salesModel struct{}

func (salesModel) Invoke(context.Context, prebuilt.AgentState, graph.Runtime) (prebuilt.AssistantMessage, error) {
	return prebuilt.AssistantMessage{ToolCalls: []prebuilt.ToolCall{{ID: "transfer", Name: "transfer_to_support", Arguments: json.RawMessage(`{}`)}}}, nil
}

type supportModel struct{}

func (supportModel) Invoke(context.Context, prebuilt.AgentState, graph.Runtime) (prebuilt.AssistantMessage, error) {
	return prebuilt.AssistantMessage{Content: "Support agent resolved the incident."}, nil
}

func main() {
	agent, err := prebuilt.NewHandoffAgent(prebuilt.HandoffAgentConfig{InitialAgent: "sales", Agents: []prebuilt.HandoffAgentSpec{
		{Name: "sales", Description: "Handles purchases.", SystemPrompt: "Help with sales.", Model: salesModel{}},
		{Name: "support", Description: "Handles incidents.", SystemPrompt: "Resolve incidents.", Model: supportModel{}},
	}})
	if err != nil {
		panic(err)
	}
	result, err := agent.Run(context.Background(), "My new device is broken.", graph.RunConfig{})
	if err != nil {
		panic(err)
	}
	fmt.Printf("active=%s response=%s\n", result.ActiveAgent, result.FinalResponse())
}
