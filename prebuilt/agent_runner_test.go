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

func TestAgentRunnerQueryMapsMessagesCustomAndDone(t *testing.T) {
	model := prebuilt.ChatModelFunc[prebuilt.AgentState]{Run: func(_ context.Context, _ prebuilt.AgentState, runtime graph.Runtime) (prebuilt.AssistantMessage, error) {
		if err := runtime.WriteCustom("thinking"); err != nil {
			return prebuilt.AssistantMessage{}, err
		}
		if err := runtime.WriteMessage(prebuilt.AssistantMessage{ID: "chunk-1", Content: "hel"}); err != nil {
			return prebuilt.AssistantMessage{}, err
		}
		return prebuilt.AssistantMessage{ID: "final-1", Content: "hello"}, nil
	}}
	agent, err := prebuilt.NewChatModelAgent(prebuilt.ChatModelAgentConfig{Name: "assistant", Model: model})
	if err != nil {
		t.Fatal(err)
	}
	runner, err := prebuilt.NewAgentRunner(prebuilt.AgentRunnerConfig{Agent: agent})
	if err != nil {
		t.Fatal(err)
	}

	var kinds []prebuilt.AgentEventKind
	var final prebuilt.AgentState
	for event := range runner.Query(context.Background(), "hello") {
		kinds = append(kinds, event.Kind)
		if event.Kind == prebuilt.AgentEventDone {
			final = event.State
		}
		if event.Kind == prebuilt.AgentEventError {
			t.Fatalf("query error: %v", event.Err)
		}
	}
	if !containsAgentEvent(kinds, prebuilt.AgentEventCustom) || !containsAgentEvent(kinds, prebuilt.AgentEventMessage) || !containsAgentEvent(kinds, prebuilt.AgentEventDone) {
		t.Fatalf("event kinds=%v", kinds)
	}
	if containsAgentEvent(kinds, prebuilt.AgentEventGraph) {
		t.Fatalf("default runner leaked low-level graph event: %v", kinds)
	}
	if final.FinalResponse() != "hello" || runner.Agent().Name() != "assistant" {
		t.Fatalf("final=%+v agent=%q", final, runner.Agent().Name())
	}
}

func TestAgentRunnerWorksWithDeepAgent(t *testing.T) {
	model := prebuilt.ChatModelFunc[prebuilt.AgentState]{Run: func(_ context.Context, _ prebuilt.AgentState, _ graph.Runtime) (prebuilt.AssistantMessage, error) {
		return prebuilt.AssistantMessage{Content: "deep answer"}, nil
	}}
	agent, err := prebuilt.NewDeepAgent(prebuilt.DeepAgentConfig{Model: model})
	if err != nil {
		t.Fatal(err)
	}
	runner, err := prebuilt.NewAgentRunner(prebuilt.AgentRunnerConfig{Agent: agent})
	if err != nil {
		t.Fatal(err)
	}
	state, err := runner.Run(context.Background(), "complex task")
	if err != nil {
		t.Fatal(err)
	}
	if state.FinalResponse() != "deep answer" || runner.Agent().Name() != "deep-agent" {
		t.Fatalf("state=%+v name=%q", state, runner.Agent().Name())
	}
}

func TestAgentRunnerResumeMapsInterruptAndDone(t *testing.T) {
	model := prebuilt.ChatModelFunc[prebuilt.AgentState]{Run: func(_ context.Context, state prebuilt.AgentState, _ graph.Runtime) (prebuilt.AssistantMessage, error) {
		for _, message := range state.Messages {
			if message.Role == prebuilt.AgentRoleTool {
				return prebuilt.AssistantMessage{Content: "approved: " + message.Content}, nil
			}
		}
		return prebuilt.AssistantMessage{ToolCalls: []prebuilt.ToolCall{{ID: "approval-1", Name: "approval", Arguments: json.RawMessage(`{}`)}}}, nil
	}}
	approval := prebuilt.ToolFunc[prebuilt.AgentState, prebuilt.AgentDelta]{ToolName: "approval", Run: func(_ context.Context, _ prebuilt.ToolCall, runtime prebuilt.ToolRuntime[prebuilt.AgentState]) (prebuilt.ToolResult[prebuilt.AgentDelta], error) {
		answer, err := graph.AwaitResume[string](runtime.Graph, "approve?")
		if err != nil {
			return prebuilt.ToolResult[prebuilt.AgentDelta]{}, err
		}
		return prebuilt.TextResult[prebuilt.AgentDelta](answer), nil
	}}
	saver := checkpointmemory.NewSaver()
	agent, err := prebuilt.NewChatModelAgent(prebuilt.ChatModelAgentConfig{
		Model: model,
		Tools: []prebuilt.Tool[prebuilt.AgentState, prebuilt.AgentDelta]{approval},
		Compile: []graph.CompileOption[prebuilt.AgentState, prebuilt.AgentDelta]{graph.WithPersistence(graph.PersistenceConfig[prebuilt.AgentState, prebuilt.AgentDelta]{
			Saver: saver, StateCodec: checkpoint.MustJSONCodec[prebuilt.AgentState]("runner.agent-state", 1), DeltaCodec: checkpoint.MustJSONCodec[prebuilt.AgentDelta]("runner.agent-delta", 1),
		})},
	})
	if err != nil {
		t.Fatal(err)
	}
	runner, err := prebuilt.NewAgentRunner(prebuilt.AgentRunnerConfig{Agent: agent, RunConfig: graph.RunConfig{ThreadID: "runner-resume"}})
	if err != nil {
		t.Fatal(err)
	}
	interrupted := false
	for event := range runner.Query(context.Background(), "perform action") {
		if event.Kind == prebuilt.AgentEventInterrupt {
			interrupted = len(event.Interrupts) == 1
		}
		if event.Kind == prebuilt.AgentEventError {
			t.Fatalf("initial query error: %v", event.Err)
		}
	}
	if !interrupted {
		t.Fatal("expected interrupt event")
	}
	command, err := graph.Resume("yes")
	if err != nil {
		t.Fatal(err)
	}
	done := false
	for event := range runner.Resume(context.Background(), command) {
		if event.Kind == prebuilt.AgentEventDone {
			done = event.State.FinalResponse() == "approved: yes"
		}
		if event.Kind == prebuilt.AgentEventError {
			t.Fatalf("resume error: %v", event.Err)
		}
	}
	if !done {
		t.Fatal("resume did not produce the expected final state")
	}
}

func TestAgentRunnerRejectsNilAgent(t *testing.T) {
	if _, err := prebuilt.NewAgentRunner(prebuilt.AgentRunnerConfig{}); err == nil {
		t.Fatal("expected nil agent error")
	}
	var runner *prebuilt.AgentRunner
	event := <-runner.Query(context.Background(), "input")
	if event.Kind != prebuilt.AgentEventError || event.Err == nil {
		t.Fatalf("event=%+v", event)
	}
}

func containsAgentEvent(values []prebuilt.AgentEventKind, expected prebuilt.AgentEventKind) bool {
	for _, value := range values {
		if value == expected {
			return true
		}
	}
	return false
}
