package prebuilt_test

import (
	"context"
	"encoding/json"
	"sync"
	"testing"

	"github.com/ybszm/langgraph-go/graph"
	"github.com/ybszm/langgraph-go/prebuilt"
)

func TestAgentCallbacksObserveModelToolAndDelegation(t *testing.T) {
	var mu sync.Mutex
	counts := map[string]int{}
	callback := prebuilt.AgentCallbackFuncs{
		ModelStart: func(context.Context, prebuilt.AgentModelStartEvent) { mu.Lock(); counts["model_start"]++; mu.Unlock() },
		ModelEnd:   func(context.Context, prebuilt.AgentModelEndEvent) { mu.Lock(); counts["model_end"]++; mu.Unlock() },
		ToolStart:  func(context.Context, prebuilt.AgentToolStartEvent) { mu.Lock(); counts["tool_start"]++; mu.Unlock() },
		ToolEnd:    func(context.Context, prebuilt.AgentToolEndEvent) { mu.Lock(); counts["tool_end"]++; mu.Unlock() },
		DelegationStart: func(context.Context, prebuilt.AgentDelegationStartEvent) {
			mu.Lock()
			counts["delegation_start"]++
			mu.Unlock()
		},
		DelegationEnd: func(context.Context, prebuilt.AgentDelegationEndEvent) {
			mu.Lock()
			counts["delegation_end"]++
			mu.Unlock()
		},
	}
	workerModel := prebuilt.ChatModelFunc[prebuilt.AgentState]{Run: func(_ context.Context, _ prebuilt.AgentState, runtime graph.Runtime) (prebuilt.AssistantMessage, error) {
		if err := runtime.WriteMessage(prebuilt.AssistantMessage{ID: "worker-chunk", Content: "working"}); err != nil {
			return prebuilt.AssistantMessage{}, err
		}
		return prebuilt.AssistantMessage{ID: "worker-final", Content: "worker result"}, nil
	}}
	worker, err := prebuilt.NewChatModelAgent(prebuilt.ChatModelAgentConfig{Name: "worker", Model: workerModel, Callbacks: []prebuilt.AgentCallback{callback}})
	if err != nil {
		t.Fatal(err)
	}
	supervisorModel := prebuilt.ChatModelFunc[prebuilt.AgentState]{Run: func(_ context.Context, state prebuilt.AgentState, _ graph.Runtime) (prebuilt.AssistantMessage, error) {
		for _, message := range state.Messages {
			if message.Role == prebuilt.AgentRoleTool {
				return prebuilt.AssistantMessage{Content: message.Content}, nil
			}
		}
		return prebuilt.AssistantMessage{ToolCalls: []prebuilt.ToolCall{{ID: "delegate", Name: prebuilt.DelegateToolName, Arguments: json.RawMessage(`{"agent":"worker","task":"do work"}`)}}}, nil
	}}
	coordinator, err := prebuilt.NewMultiAgentCoordinator(prebuilt.MultiAgentCoordinatorConfig{
		Name: "supervisor", Model: supervisorModel, Callbacks: []prebuilt.AgentCallback{callback},
		Agents: []prebuilt.SubAgent{{Name: "worker", Description: "Works.", Agent: worker}},
	})
	if err != nil {
		t.Fatal(err)
	}
	var forwarded bool
	for event := range coordinator.Stream(context.Background(), "start", graph.RunConfig{}, graph.StreamOptions{Modes: []graph.StreamMode{graph.StreamMessages, graph.StreamDone}}) {
		if event.Mode == graph.StreamMessages && event.Message != nil && event.Message.Metadata["langgraph_subagent"] == "worker" {
			forwarded = true
		}
		if event.Mode == graph.StreamError {
			t.Fatal(event.Err)
		}
	}
	if !forwarded {
		t.Fatal("expected forwarded subagent message stream")
	}
	mu.Lock()
	defer mu.Unlock()
	for _, key := range []string{"model_start", "model_end", "tool_start", "tool_end", "delegation_start", "delegation_end"} {
		if counts[key] == 0 {
			t.Fatalf("callback %s was not observed: %v", key, counts)
		}
	}
}

func TestChatModelAgentStreamEventsConvenience(t *testing.T) {
	agent, err := prebuilt.NewChatModelAgent(prebuilt.ChatModelAgentConfig{Name: "stream-agent", Model: prebuilt.ChatModelFunc[prebuilt.AgentState]{Run: func(context.Context, prebuilt.AgentState, graph.Runtime) (prebuilt.AssistantMessage, error) {
		return prebuilt.AssistantMessage{Content: "done"}, nil
	}}})
	if err != nil {
		t.Fatal(err)
	}
	var start, end bool
	for event := range agent.StreamEvents(context.Background(), "hello", graph.RunConfig{}, graph.StreamEventsV3) {
		if event.Err != nil {
			t.Fatal(event.Err)
		}
		start = start || event.Event == "on_chain_start" && event.Name == "stream-agent"
		end = end || event.Event == "on_chain_end" && event.Name == "stream-agent"
	}
	if !start || !end {
		t.Fatalf("start=%v end=%v", start, end)
	}
}
