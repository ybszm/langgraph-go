package prebuilt_test

import (
	"context"
	"testing"

	"github.com/ybszm/langgraph-go/graph"
	"github.com/ybszm/langgraph-go/prebuilt"
)

func TestChatModelAgentInjectsSystemPromptAndRunsTextInput(t *testing.T) {
	model := prebuilt.ChatModelFunc[prebuilt.AgentState]{Run: func(_ context.Context, state prebuilt.AgentState, runtime graph.Runtime) (prebuilt.AssistantMessage, error) {
		if runtime.Node != prebuilt.AgentNodeID {
			t.Fatalf("node=%q", runtime.Node)
		}
		if len(state.Messages) != 2 || state.Messages[0].Role != prebuilt.AgentRoleSystem || state.Messages[0].Content != "Be precise." || state.Messages[1].Content != "hello" {
			t.Fatalf("state=%+v", state)
		}
		return prebuilt.AssistantMessage{Content: "hi"}, nil
	}}
	agent, err := prebuilt.NewChatModelAgent(prebuilt.ChatModelAgentConfig{Name: "assistant", SystemPrompt: "Be precise.", Model: model})
	if err != nil {
		t.Fatal(err)
	}
	state, err := agent.Run(context.Background(), "hello", graph.RunConfig{})
	if err != nil {
		t.Fatal(err)
	}
	if agent.Name() != "assistant" || agent.Graph() == nil || state.FinalResponse() != "hi" {
		t.Fatalf("name=%q graph=%v response=%q", agent.Name(), agent.Graph() != nil, state.FinalResponse())
	}
	message, ok := state.LastAssistant()
	if !ok || message.Content != "hi" {
		t.Fatalf("message=%+v ok=%v", message, ok)
	}
}

func TestChatModelAgentDoesNotDuplicateExistingSystemPrompt(t *testing.T) {
	model := prebuilt.ChatModelFunc[prebuilt.AgentState]{Run: func(_ context.Context, state prebuilt.AgentState, _ graph.Runtime) (prebuilt.AssistantMessage, error) {
		if len(state.Messages) != 2 || len(state.Todos) != 1 || state.Todos[0].Content != "keep" {
			t.Fatalf("state=%+v", state)
		}
		return prebuilt.AssistantMessage{Content: "ok"}, nil
	}}
	agent, err := prebuilt.NewChatModelAgent(prebuilt.ChatModelAgentConfig{SystemPrompt: "system", Model: model})
	if err != nil {
		t.Fatal(err)
	}
	_, err = agent.Invoke(context.Background(), prebuilt.AgentState{Messages: []prebuilt.AgentMessage{{Role: prebuilt.AgentRoleSystem, Content: "system"}, {Role: prebuilt.AgentRoleUser, Content: "input"}}, Todos: []prebuilt.Todo{{Content: "keep", Status: prebuilt.TodoPending}}}, graph.RunConfig{})
	if err != nil {
		t.Fatal(err)
	}
}
