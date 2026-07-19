package prebuilt_test

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/ybszm/langgraph-go/graph"
	"github.com/ybszm/langgraph-go/prebuilt"
)

func TestValidateChatHistoryRequiresToolResults(t *testing.T) {
	assistants := []prebuilt.AssistantMessage{
		{ID: "a1", ToolCalls: []prebuilt.ToolCall{{ID: "call-1", Name: "one"}, {ID: "call-2", Name: "two"}}},
	}
	err := prebuilt.ValidateChatHistory(assistants, []prebuilt.ToolMessage{{ToolCallID: "call-1", Name: "one"}})
	if !errors.Is(err, prebuilt.ErrInvalidChatHistory) || !strings.Contains(err.Error(), "call-2") {
		t.Fatalf("error=%v", err)
	}
	if err := prebuilt.ValidateChatHistory(assistants, []prebuilt.ToolMessage{{ToolCallID: "call-2"}, {ToolCallID: "call-1"}}); err != nil {
		t.Fatal(err)
	}
}

func TestCreateReactAgentValidatesHistoryBeforeModel(t *testing.T) {
	var modelCalls atomic.Int32
	model := prebuilt.ChatModelFunc[reactState]{Run: func(context.Context, reactState, graph.Runtime) (prebuilt.AssistantMessage, error) {
		modelCalls.Add(1)
		return prebuilt.AssistantMessage{Content: "done"}, nil
	}}
	adapter := reactAdapter()
	adapter.ToolNode = prebuilt.ToolNodeAdapter[reactState, reactDelta]{}
	adapter.ChatHistory = func(_ context.Context, state reactState) ([]prebuilt.AssistantMessage, []prebuilt.ToolMessage, error) {
		return state.Assistant, state.Tools, nil
	}
	agent, err := prebuilt.CreateReactAgent(model, nil, reactReducer, adapter, prebuilt.ReactAgentConfig{})
	if err != nil {
		t.Fatal(err)
	}
	input := reactState{Assistant: []prebuilt.AssistantMessage{{ToolCalls: []prebuilt.ToolCall{{ID: "pending", Name: "lookup"}}}}}
	_, err = agent.Invoke(context.Background(), input, graph.RunConfig{})
	if !errors.Is(err, prebuilt.ErrInvalidChatHistory) || modelCalls.Load() != 0 {
		t.Fatalf("model calls=%d error=%v", modelCalls.Load(), err)
	}
	input.Tools = []prebuilt.ToolMessage{{ToolCallID: "pending", Name: "lookup"}}
	if _, err := agent.Invoke(context.Background(), input, graph.RunConfig{}); err != nil {
		t.Fatal(err)
	}
	if modelCalls.Load() != 1 {
		t.Fatalf("model calls=%d", modelCalls.Load())
	}
}

func TestCreateReactAgentHistoryAccessorError(t *testing.T) {
	model := prebuilt.ChatModelFunc[reactState]{Run: func(context.Context, reactState, graph.Runtime) (prebuilt.AssistantMessage, error) {
		return prebuilt.AssistantMessage{}, nil
	}}
	adapter := reactAdapter()
	adapter.ToolNode = prebuilt.ToolNodeAdapter[reactState, reactDelta]{}
	adapter.ChatHistory = func(context.Context, reactState) ([]prebuilt.AssistantMessage, []prebuilt.ToolMessage, error) {
		return nil, nil, errors.New("history unavailable")
	}
	agent, err := prebuilt.CreateReactAgent(model, nil, reactReducer, adapter, prebuilt.ReactAgentConfig{})
	if err != nil {
		t.Fatal(err)
	}
	_, err = agent.Invoke(context.Background(), reactState{}, graph.RunConfig{})
	if err == nil || !strings.Contains(err.Error(), "read chat history") {
		t.Fatalf("error=%v", err)
	}
}
