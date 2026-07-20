package prebuilt_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/ybszm/langgraph-go/graph"
	"github.com/ybszm/langgraph-go/prebuilt"
)

func TestHandoffAgentTransfersActivePersona(t *testing.T) {
	sales := prebuilt.ChatModelFunc[prebuilt.AgentState]{Run: func(_ context.Context, state prebuilt.AgentState, _ graph.Runtime) (prebuilt.AssistantMessage, error) {
		if state.ActiveAgent != "sales" {
			t.Fatalf("sales state=%+v", state)
		}
		return prebuilt.AssistantMessage{ToolCalls: []prebuilt.ToolCall{{ID: "handoff", Name: "transfer_to_support", Arguments: json.RawMessage(`{}`)}}}, nil
	}}
	support := prebuilt.ChatModelFunc[prebuilt.AgentState]{Run: func(_ context.Context, state prebuilt.AgentState, _ graph.Runtime) (prebuilt.AssistantMessage, error) {
		if state.ActiveAgent != "support" || len(state.Messages) == 0 || state.Messages[0].Role != prebuilt.AgentRoleSystem || state.Messages[0].Content != "Solve support cases." {
			t.Fatalf("support state=%+v", state)
		}
		return prebuilt.AssistantMessage{Content: "support resolved"}, nil
	}}
	agent, err := prebuilt.NewHandoffAgent(prebuilt.HandoffAgentConfig{
		InitialAgent: "sales",
		Agents: []prebuilt.HandoffAgentSpec{
			{Name: "sales", Description: "Handles purchases.", SystemPrompt: "Handle sales.", Model: sales},
			{Name: "support", Description: "Handles incidents.", SystemPrompt: "Solve support cases.", Model: support},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	state, err := agent.Run(context.Background(), "my device is broken", graph.RunConfig{})
	if err != nil {
		t.Fatal(err)
	}
	if state.ActiveAgent != "support" || state.FinalResponse() != "support resolved" {
		t.Fatalf("state=%+v", state)
	}
}

func TestHandoffAgentValidation(t *testing.T) {
	model := prebuilt.ChatModelFunc[prebuilt.AgentState]{Run: func(context.Context, prebuilt.AgentState, graph.Runtime) (prebuilt.AssistantMessage, error) {
		return prebuilt.AssistantMessage{}, nil
	}}
	_, err := prebuilt.NewHandoffAgent(prebuilt.HandoffAgentConfig{Agents: []prebuilt.HandoffAgentSpec{{Name: "only", Description: "one", Model: model}}})
	if err == nil {
		t.Fatal("expected at least two personas error")
	}
}
