package memory_test

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/wahanbo/langgraph-go/graph"
	"github.com/wahanbo/langgraph-go/memory"
	"github.com/wahanbo/langgraph-go/prebuilt"
	"github.com/wahanbo/langgraph-go/retrieval"
)

type captureModel struct {
	state *prebuilt.AgentState
}

func (model captureModel) Invoke(_ context.Context, state prebuilt.AgentState, _ graph.Runtime) (prebuilt.AssistantMessage, error) {
	*model.state = state
	return prebuilt.AssistantMessage{Content: "ok"}, nil
}

func TestWindowMessagesPreservesSystemAndToolExchange(t *testing.T) {
	messages := []prebuilt.Message{
		prebuilt.SystemMessage{Content: "rules"},
		prebuilt.UserMessage{Content: "old"},
		prebuilt.AssistantMessage{Content: "old answer"},
		prebuilt.UserMessage{Content: "search"},
		prebuilt.AssistantMessage{ToolCalls: []prebuilt.ToolCall{{ID: "call-1", Name: "search", Arguments: json.RawMessage(`{}`)}}},
		prebuilt.ToolMessage{ToolCallID: "call-1", Name: "search", Content: "result"},
	}
	window := memory.WindowMessages(messages, 1)
	if len(window) != 3 {
		t.Fatalf("window=%+v", window)
	}
	if _, ok := window[0].(prebuilt.SystemMessage); !ok {
		t.Fatalf("first=%T", window[0])
	}
	if assistant, ok := window[1].(prebuilt.AssistantMessage); !ok || len(assistant.ToolCalls) != 1 {
		t.Fatalf("assistant=%+v", window[1])
	}
	if _, ok := window[2].(prebuilt.ToolMessage); !ok {
		t.Fatalf("tool=%T", window[2])
	}
}

func TestWindowMiddlewareProjectsWithoutMutatingState(t *testing.T) {
	original := prebuilt.AgentState{Messages: []prebuilt.AgentMessage{
		{Role: prebuilt.AgentRoleSystem, Content: "rules"},
		{Role: prebuilt.AgentRoleUser, Content: "old"},
		{Role: prebuilt.AgentRoleAssistant, Content: "answer"},
		{Role: prebuilt.AgentRoleUser, Content: "new"},
	}}
	window, err := memory.NewWindow(memory.AgentStateAdapter(), 1)
	if err != nil {
		t.Fatal(err)
	}
	var captured prebuilt.AgentState
	wrapped, err := prebuilt.WrapChatModel[prebuilt.AgentState](captureModel{state: &captured}, window)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := wrapped.Invoke(context.Background(), original, graph.Runtime{}); err != nil {
		t.Fatal(err)
	}
	if len(captured.Messages) != 2 || captured.Messages[1].Content != "new" {
		t.Fatalf("captured=%+v", captured.Messages)
	}
	if len(original.Messages) != 4 {
		t.Fatalf("original mutated: %+v", original.Messages)
	}
}

func TestSummaryMiddlewareCondensesEvictedMessages(t *testing.T) {
	state := prebuilt.AgentState{Messages: []prebuilt.AgentMessage{
		{Role: prebuilt.AgentRoleSystem, Content: "rules"},
		{Role: prebuilt.AgentRoleUser, Content: "one"},
		{Role: prebuilt.AgentRoleAssistant, Content: "two"},
		{Role: prebuilt.AgentRoleUser, Content: "three"},
		{Role: prebuilt.AgentRoleAssistant, Content: "four"},
	}}
	var summarized []prebuilt.Message
	summary, err := memory.NewSummary(memory.AgentStateAdapter(), memory.SummarizerFunc(func(_ context.Context, messages []prebuilt.Message) (string, error) {
		summarized = messages
		return "user said one; assistant said two", nil
	}), 4, 2)
	if err != nil {
		t.Fatal(err)
	}
	var captured prebuilt.AgentState
	wrapped, _ := prebuilt.WrapChatModel[prebuilt.AgentState](captureModel{state: &captured}, summary)
	if _, err := wrapped.Invoke(context.Background(), state, graph.Runtime{}); err != nil {
		t.Fatal(err)
	}
	if len(summarized) != 2 || len(captured.Messages) != 4 {
		t.Fatalf("summarized=%d captured=%+v", len(summarized), captured.Messages)
	}
	if captured.Messages[1].Role != prebuilt.AgentRoleSystem || !strings.Contains(captured.Messages[1].Content, "user said one") {
		t.Fatalf("summary=%+v", captured.Messages[1])
	}
	if state.Messages[1].Content != "one" {
		t.Fatal("original state was mutated")
	}
}

func TestRetrievalMiddlewareInjectsContextAfterSystem(t *testing.T) {
	index, err := retrieval.NewBM25([]retrieval.Document{{ID: "durability", Content: "Checkpoints make graph state durable.", Metadata: map[string]any{"source": "guide"}}}, retrieval.BM25Options{})
	if err != nil {
		t.Fatal(err)
	}
	middleware, err := memory.NewRetrieval(memory.AgentStateAdapter(), index, func(_ context.Context, _ prebuilt.AgentState) (retrieval.Query, error) {
		return retrieval.Query{Text: "durable graph", Limit: 1}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	state := prebuilt.AgentState{Messages: []prebuilt.AgentMessage{{Role: prebuilt.AgentRoleSystem, Content: "rules"}, {Role: prebuilt.AgentRoleUser, Content: "How?"}}}
	var captured prebuilt.AgentState
	wrapped, _ := prebuilt.WrapChatModel[prebuilt.AgentState](captureModel{state: &captured}, middleware)
	if _, err := wrapped.Invoke(context.Background(), state, graph.Runtime{}); err != nil {
		t.Fatal(err)
	}
	if len(captured.Messages) != 3 || captured.Messages[1].Role != prebuilt.AgentRoleSystem || !strings.Contains(captured.Messages[1].Content, "Checkpoints") {
		t.Fatalf("captured=%+v", captured.Messages)
	}
}
