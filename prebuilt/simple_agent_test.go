package prebuilt_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/ybszm/langgraph-go/checkpoint"
	checkpointmemory "github.com/ybszm/langgraph-go/checkpoint/memory"
	"github.com/ybszm/langgraph-go/graph"
	"github.com/ybszm/langgraph-go/prebuilt"
)

func TestNewAgentRunsToolLoopWithDefaultState(t *testing.T) {
	model := prebuilt.ChatModelFunc[prebuilt.AgentState]{Run: func(_ context.Context, state prebuilt.AgentState, _ graph.Runtime) (prebuilt.AssistantMessage, error) {
		messages, err := state.ProviderMessages()
		if err != nil {
			return prebuilt.AssistantMessage{}, err
		}
		if len(messages) == 1 {
			return prebuilt.AssistantMessage{ID: "plan", ToolCalls: []prebuilt.ToolCall{{ID: "call-1", Name: "add", Arguments: json.RawMessage(`{"a":20,"b":22}`)}}}, nil
		}
		return prebuilt.AssistantMessage{ID: "final", Content: "42"}, nil
	}}
	tool := prebuilt.ToolFunc[prebuilt.AgentState, prebuilt.AgentDelta]{ToolName: "add", Run: func(context.Context, prebuilt.ToolCall, prebuilt.ToolRuntime[prebuilt.AgentState]) (prebuilt.ToolResult[prebuilt.AgentDelta], error) {
		return prebuilt.TextResult[prebuilt.AgentDelta]("42"), nil
	}}
	saver := checkpointmemory.NewSaver()
	agent, err := prebuilt.NewAgent(model, []prebuilt.Tool[prebuilt.AgentState, prebuilt.AgentDelta]{tool}, prebuilt.ReactAgentConfig{}, graph.WithPersistence(graph.PersistenceConfig[prebuilt.AgentState, prebuilt.AgentDelta]{Saver: saver, StateCodec: checkpoint.MustJSONCodec[prebuilt.AgentState]("prebuilt.agent-state", 1), DeltaCodec: checkpoint.MustJSONCodec[prebuilt.AgentDelta]("prebuilt.agent-delta", 1)}))
	if err != nil {
		t.Fatal(err)
	}
	result, err := agent.Invoke(context.Background(), prebuilt.NewAgentState("20+22"), graph.RunConfig{ThreadID: "thread-1"})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Messages) != 4 || result.Messages[2].Role != prebuilt.AgentRoleTool || result.Messages[3].Content != "42" {
		t.Fatalf("state=%+v", result)
	}
	stored, found, err := saver.GetTuple(context.Background(), checkpoint.Config{ThreadID: "thread-1"})
	if err != nil || !found || stored.Checkpoint.ID == "" {
		t.Fatalf("found=%v err=%v", found, err)
	}
}

func TestAgentModelMessagesPreservesChronology(t *testing.T) {
	state := prebuilt.AgentState{Messages: []prebuilt.AgentMessage{{Role: prebuilt.AgentRoleSystem, Content: "safe"}, {Role: prebuilt.AgentRoleUser, Content: "hello"}, {Role: prebuilt.AgentRoleAssistant, Content: "hi"}}}
	messages, err := prebuilt.AgentModelMessages(context.Background(), state)
	if err != nil || len(messages) != 3 {
		t.Fatalf("messages=%v err=%v", messages, err)
	}
	if _, ok := messages[0].(prebuilt.SystemMessage); !ok {
		t.Fatalf("first=%T", messages[0])
	}
	if _, ok := messages[1].(prebuilt.UserMessage); !ok {
		t.Fatalf("second=%T", messages[1])
	}
}
