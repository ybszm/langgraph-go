package main

import (
	"context"
	"fmt"

	"github.com/ybszm/langgraph-go/graph"
	"github.com/ybszm/langgraph-go/prebuilt"
)

// demoModel is a deterministic stand-in for a real ChatModel adapter.
type demoModel struct{}

func (demoModel) Invoke(_ context.Context, state prebuilt.AgentState, _ graph.Runtime) (prebuilt.AssistantMessage, error) {
	last := ""
	if n := len(state.Messages); n > 0 {
		last = state.Messages[n-1].Content
	}
	return prebuilt.AssistantMessage{Content: "quick-agent heard: " + last}, nil
}

func main() {
	agent, err := prebuilt.NewQuickAgent(prebuilt.QuickAgentConfig{
		Model:        demoModel{},
		SystemPrompt: "You are a tiny example agent.",
	})
	if err != nil {
		panic(err)
	}
	state, err := agent.Run(context.Background(), "hello from quick agent", graph.RunConfig{})
	if err != nil {
		panic(err)
	}
	fmt.Println(state.FinalResponse())
}
