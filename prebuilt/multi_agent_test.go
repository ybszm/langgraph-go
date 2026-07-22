package prebuilt_test

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ybszm/langgraph-go/graph"
	"github.com/ybszm/langgraph-go/prebuilt"
)

func TestMultiAgentCoordinatorDelegatesInParallelAndPreservesCallOrder(t *testing.T) {
	var mu sync.Mutex
	started := 0
	ready := make(chan struct{})
	worker := func(name string) *prebuilt.ChatModelAgent {
		model := prebuilt.ChatModelFunc[prebuilt.AgentState]{Run: func(_ context.Context, state prebuilt.AgentState, _ graph.Runtime) (prebuilt.AssistantMessage, error) {
			if len(state.Messages) != 2 || state.Messages[0].Role != prebuilt.AgentRoleSystem || state.Messages[1].Role != prebuilt.AgentRoleUser {
				return prebuilt.AssistantMessage{}, errors.New("worker context was not isolated")
			}
			mu.Lock()
			started++
			if started == 2 {
				close(ready)
			}
			mu.Unlock()
			select {
			case <-ready:
			case <-time.After(time.Second):
				return prebuilt.AssistantMessage{}, errors.New("workers did not execute concurrently")
			}
			return prebuilt.AssistantMessage{Content: name + ":" + state.Messages[1].Content}, nil
		}}
		agent, err := prebuilt.NewChatModelAgent(prebuilt.ChatModelAgentConfig{Name: name, SystemPrompt: "worker " + name, Model: model})
		if err != nil {
			t.Fatal(err)
		}
		return agent
	}

	supervisor := prebuilt.ChatModelFunc[prebuilt.AgentState]{Run: func(_ context.Context, state prebuilt.AgentState, _ graph.Runtime) (prebuilt.AssistantMessage, error) {
		results := make([]string, 0, 2)
		for _, message := range state.Messages {
			if message.Role == prebuilt.AgentRoleTool {
				results = append(results, message.Content)
			}
		}
		if len(results) == 0 {
			return prebuilt.AssistantMessage{ToolCalls: []prebuilt.ToolCall{
				{ID: "research-call", Name: prebuilt.DelegateToolName, Arguments: json.RawMessage(`{"agent":"researcher","task":"collect facts"}`)},
				{ID: "writer-call", Name: prebuilt.DelegateToolName, Arguments: json.RawMessage(`{"agent":"writer","task":"draft answer"}`)},
			}}, nil
		}
		return prebuilt.AssistantMessage{Content: strings.Join(results, " | ")}, nil
	}}
	coordinator, err := prebuilt.NewMultiAgentCoordinator(prebuilt.MultiAgentCoordinatorConfig{
		Name: "supervisor", Model: supervisor,
		Agents: []prebuilt.SubAgent{
			{Name: "researcher", Description: "Collects evidence.", Agent: worker("researcher")},
			{Name: "writer", Description: "Drafts prose.", Agent: worker("writer")},
		},
		React: prebuilt.ReactAgentConfig{ToolNode: prebuilt.ToolNodeConfig{MaxConcurrency: 2}},
	})
	if err != nil {
		t.Fatal(err)
	}
	state, err := coordinator.Run(context.Background(), "prepare report", graph.RunConfig{})
	if err != nil {
		t.Fatal(err)
	}
	if got := state.FinalResponse(); got != "researcher:collect facts | writer:draft answer" {
		t.Fatalf("response=%q", got)
	}
	if len(coordinator.Agents()) != 2 {
		t.Fatalf("agents=%v", coordinator.Agents())
	}
}

type deepHarnessModel struct {
	mu       *sync.Mutex
	bindings *[][]prebuilt.ToolDefinition
}

func (model deepHarnessModel) BindTools(definitions []prebuilt.ToolDefinition) (prebuilt.ChatModel[prebuilt.AgentState], error) {
	model.mu.Lock()
	*model.bindings = append(*model.bindings, definitions)
	model.mu.Unlock()
	return model, nil
}

func (model deepHarnessModel) Invoke(_ context.Context, state prebuilt.AgentState, _ graph.Runtime) (prebuilt.AssistantMessage, error) {
	for _, message := range state.Messages {
		if message.Role == prebuilt.AgentRoleSystem && strings.Contains(message.Content, "Work autonomously on the delegated task") {
			return prebuilt.AssistantMessage{Content: "worker result"}, nil
		}
	}
	tools := make([]prebuilt.AgentMessage, 0, 2)
	for _, message := range state.Messages {
		if message.Role == prebuilt.AgentRoleTool {
			tools = append(tools, message)
		}
	}
	switch len(tools) {
	case 0:
		return prebuilt.AssistantMessage{ToolCalls: []prebuilt.ToolCall{
			{ID: "plan", Name: prebuilt.TodoToolName, Arguments: json.RawMessage(`{"todos":[{"content":"delegate research","status":"in_progress"}]}`)},
			{ID: "delegate", Name: prebuilt.DelegateToolName, Arguments: json.RawMessage(`{"agent":"general-purpose","task":"research the topic"}`)},
		}}, nil
	default:
		return prebuilt.AssistantMessage{Content: "final: " + tools[len(tools)-1].Content}, nil
	}
}

func TestDeepAgentInstallsPlanningAndGeneralPurposeDelegation(t *testing.T) {
	var mu sync.Mutex
	bindings := make([][]prebuilt.ToolDefinition, 0, 1)
	agent, err := prebuilt.NewDeepAgent(prebuilt.DeepAgentConfig{Model: deepHarnessModel{mu: &mu, bindings: &bindings}})
	if err != nil {
		t.Fatal(err)
	}
	state, err := agent.Run(context.Background(), "investigate", graph.RunConfig{})
	if err != nil {
		t.Fatal(err)
	}
	if got := state.FinalResponse(); got != "final: worker result" {
		t.Fatalf("response=%q", got)
	}
	if len(state.Todos) != 1 || state.Todos[0].Content != "delegate research" || state.Todos[0].Status != prebuilt.TodoInProgress {
		t.Fatalf("todos=%+v", state.Todos)
	}
	if len(agent.Agents()) != 1 || agent.Agents()[0].Name != prebuilt.GeneralPurposeAgentName {
		t.Fatalf("agents=%+v", agent.Agents())
	}
	if len(bindings) != 1 || len(bindings[0]) != 2 || bindings[0][0].Name != prebuilt.TodoToolName || bindings[0][1].Name != prebuilt.DelegateToolName {
		t.Fatalf("bindings=%+v", bindings)
	}
}

func TestAgentHarnessValidation(t *testing.T) {
	model := prebuilt.ChatModelFunc[prebuilt.AgentState]{Run: func(context.Context, prebuilt.AgentState, graph.Runtime) (prebuilt.AssistantMessage, error) {
		return prebuilt.AssistantMessage{Content: "ok"}, nil
	}}
	if _, err := prebuilt.NewMultiAgentCoordinator(prebuilt.MultiAgentCoordinatorConfig{Model: model}); err == nil {
		t.Fatal("expected empty coordinator validation error")
	}
	if _, err := prebuilt.NewMultiAgentCoordinator(prebuilt.MultiAgentCoordinatorConfig{Model: model, Agents: []prebuilt.SubAgent{{Name: "bad name", Description: "bad", Agent: nil}}}); err == nil {
		t.Fatal("expected invalid subagent validation error")
	}
	todo, err := prebuilt.NewTodoListTool()
	if err != nil {
		t.Fatal(err)
	}
	_, err = todo.Invoke(context.Background(), prebuilt.ToolCall{Name: prebuilt.TodoToolName, Arguments: json.RawMessage(`{"todos":[{"content":"x","status":"unknown"}]}`)}, prebuilt.ToolRuntime[prebuilt.AgentState]{})
	if err == nil {
		t.Fatal("expected invalid todo status error")
	}
}

func TestRouterAgentSelectsRouteAndSynthesizes(t *testing.T) {
	worker, err := prebuilt.NewChatModelAgent(prebuilt.ChatModelAgentConfig{Model: prebuilt.ChatModelFunc[prebuilt.AgentState]{Run: func(context.Context, prebuilt.AgentState, graph.Runtime) (prebuilt.AssistantMessage, error) {
		return prebuilt.AssistantMessage{Content: "billing result"}, nil
	}}})
	if err != nil {
		t.Fatal(err)
	}
	routerModel := prebuilt.ChatModelFunc[prebuilt.AgentState]{Run: func(_ context.Context, state prebuilt.AgentState, _ graph.Runtime) (prebuilt.AssistantMessage, error) {
		for _, message := range state.Messages {
			if message.Role == prebuilt.AgentRoleTool {
				return prebuilt.AssistantMessage{Content: "routed: " + message.Content}, nil
			}
		}
		if len(state.Messages) == 0 || state.Messages[0].Role != prebuilt.AgentRoleSystem || !strings.Contains(state.Messages[0].Content, "Classify the request") {
			t.Fatalf("router prompt missing: %+v", state.Messages)
		}
		return prebuilt.AssistantMessage{ToolCalls: []prebuilt.ToolCall{{ID: "route", Name: prebuilt.DelegateToolName, Arguments: json.RawMessage(`{"agent":"billing","task":"check invoice"}`)}}}, nil
	}}
	router, err := prebuilt.NewRouterAgent(prebuilt.RouterAgentConfig{Model: routerModel, Routes: []prebuilt.SubAgent{{Name: "billing", Description: "Handles invoices.", Agent: worker}}})
	if err != nil {
		t.Fatal(err)
	}
	state, err := router.Run(context.Background(), "invoice question", graph.RunConfig{})
	if err != nil || state.FinalResponse() != "routed: billing result" || len(router.Routes()) != 1 {
		t.Fatalf("state=%+v routes=%v err=%v", state, router.Routes(), err)
	}
}
