package prebuilt_test

import (
	"context"
	"testing"

	"github.com/ybszm/langgraph-go/graph"
	"github.com/ybszm/langgraph-go/prebuilt"
)

type echoModel struct{}

func (echoModel) Invoke(_ context.Context, state prebuilt.AgentState, _ graph.Runtime) (prebuilt.AssistantMessage, error) {
	return prebuilt.AssistantMessage{Content: "echo:" + state.FinalResponse()}, nil
}

func TestNewQuickAgentRunsWithoutTools(t *testing.T) {
	agent, err := prebuilt.NewQuickAgent(prebuilt.QuickAgentConfig{
		Model:        echoModel{},
		SystemPrompt: "sys",
	})
	if err != nil {
		t.Fatal(err)
	}
	// Seed user input; FinalResponse is empty until assistant replies, so model
	// sees prior empty assistant content — assert non-empty success path.
	state, err := agent.Run(context.Background(), "hello", graph.RunConfig{})
	if err != nil {
		t.Fatal(err)
	}
	if state.FinalResponse() == "" {
		t.Fatalf("empty response: %+v", state)
	}
	if agent.Name() != "quick-agent" {
		t.Fatalf("name=%q", agent.Name())
	}
	if agent.Graph() == nil {
		t.Fatal("graph is nil")
	}
}

func TestNewQuickAgentRequiresModel(t *testing.T) {
	if _, err := prebuilt.NewQuickAgent(prebuilt.QuickAgentConfig{}); err == nil {
		t.Fatal("expected error")
	}
}
