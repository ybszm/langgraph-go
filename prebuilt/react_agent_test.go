package prebuilt_test

import (
	"context"
	"errors"
	"reflect"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ybszm/langgraph-go/graph"
	"github.com/ybszm/langgraph-go/prebuilt"
)

type reactState struct {
	Assistant  []prebuilt.AssistantMessage
	Tools      []prebuilt.ToolMessage
	Events     []string
	Structured string
	TaskCall   *prebuilt.ToolCall
}

type reactDelta struct {
	Assistant   *prebuilt.AssistantMessage
	ReplaceLast *prebuilt.AssistantMessage
	Tools       []prebuilt.ToolMessage
	Event       string
	Structured  *string
}

func reactReducer(_ context.Context, state reactState, updates []reactDelta) (reactState, error) {
	state.Assistant = append([]prebuilt.AssistantMessage(nil), state.Assistant...)
	state.Tools = append([]prebuilt.ToolMessage(nil), state.Tools...)
	state.Events = append([]string(nil), state.Events...)
	for _, update := range updates {
		if update.Assistant != nil {
			state.Assistant = append(state.Assistant, *update.Assistant)
		}
		if update.ReplaceLast != nil && len(state.Assistant) > 0 {
			state.Assistant[len(state.Assistant)-1] = *update.ReplaceLast
		}
		state.Tools = append(state.Tools, update.Tools...)
		if update.Event != "" {
			state.Events = append(state.Events, update.Event)
		}
		if update.Structured != nil {
			state.Structured = *update.Structured
		}
	}
	return state, nil
}

func reactAdapter() prebuilt.ReactAgentAdapter[reactState, reactDelta] {
	return prebuilt.ReactAgentAdapter[reactState, reactDelta]{
		ModelMessage: func(_ context.Context, _ reactState, message prebuilt.AssistantMessage) (reactDelta, error) {
			return reactDelta{Assistant: &message}, nil
		},
		ToolNode: prebuilt.ToolNodeAdapter[reactState, reactDelta]{
			Calls: func(_ context.Context, state reactState) ([]prebuilt.ToolCall, error) {
				if state.TaskCall != nil {
					return []prebuilt.ToolCall{*state.TaskCall}, nil
				}
				if len(state.Assistant) == 0 {
					return nil, nil
				}
				return prebuilt.LastAssistantToolCalls(state.Assistant), nil
			},
			Messages: func(_ context.Context, _ reactState, messages []prebuilt.ToolMessage) (reactDelta, error) {
				return reactDelta{Tools: append([]prebuilt.ToolMessage(nil), messages...)}, nil
			},
		},
		ToolMessages: func(_ context.Context, state reactState) ([]prebuilt.ToolMessage, error) {
			return append([]prebuilt.ToolMessage(nil), state.Tools...), nil
		},
	}
}

func TestCreateReactAgentLoopsModelToolsModel(t *testing.T) {
	var calls atomic.Int32
	model := prebuilt.ChatModelFunc[reactState]{Run: func(ctx context.Context, state reactState, _ graph.Runtime) (prebuilt.AssistantMessage, error) {
		if err := ctx.Err(); err != nil {
			return prebuilt.AssistantMessage{}, err
		}
		calls.Add(1)
		if len(state.Tools) == 0 {
			return prebuilt.AssistantMessage{ID: "plan", ToolCalls: []prebuilt.ToolCall{{ID: "call-1", Name: "weather"}}}, nil
		}
		return prebuilt.AssistantMessage{ID: "final", Content: "sunny"}, nil
	}}
	tool := prebuilt.ToolFunc[reactState, reactDelta]{ToolName: "weather", Run: func(context.Context, prebuilt.ToolCall, prebuilt.ToolRuntime[reactState]) (prebuilt.ToolResult[reactDelta], error) {
		return prebuilt.TextResult[reactDelta]("72F"), nil
	}}

	agent, err := prebuilt.CreateReactAgent(model, []prebuilt.Tool[reactState, reactDelta]{tool}, reactReducer, reactAdapter(), prebuilt.ReactAgentConfig{})
	if err != nil {
		t.Fatal(err)
	}
	result, err := agent.Invoke(context.Background(), reactState{}, graph.RunConfig{})
	if err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 2 || len(result.Assistant) != 2 || result.Assistant[1].Content != "sunny" {
		t.Fatalf("calls=%d state=%+v", calls.Load(), result)
	}
	if len(result.Tools) != 1 || result.Tools[0].ToolCallID != "call-1" || result.Tools[0].Content != "72F" {
		t.Fatalf("tools=%+v", result.Tools)
	}
	if got := agent.Edges(); !reflect.DeepEqual(got, []graph.Edge{{From: graph.START, To: prebuilt.AgentNodeID}, {From: prebuilt.ToolsNodeID, To: prebuilt.AgentNodeID}}) {
		t.Fatalf("static edges=%+v", got)
	}
}

func TestCreateReactAgentReturnDirectSkipsSecondModelCall(t *testing.T) {
	var calls atomic.Int32
	model := prebuilt.ChatModelFunc[reactState]{Run: func(context.Context, reactState, graph.Runtime) (prebuilt.AssistantMessage, error) {
		calls.Add(1)
		return prebuilt.AssistantMessage{ToolCalls: []prebuilt.ToolCall{{ID: "call-1", Name: "finish"}}}, nil
	}}
	tool := prebuilt.ToolFunc[reactState, reactDelta]{ToolName: "finish", Run: func(context.Context, prebuilt.ToolCall, prebuilt.ToolRuntime[reactState]) (prebuilt.ToolResult[reactDelta], error) {
		return prebuilt.TextResult[reactDelta]("done"), nil
	}}
	agent, err := prebuilt.CreateReactAgent(model, []prebuilt.Tool[reactState, reactDelta]{tool}, reactReducer, reactAdapter(), prebuilt.ReactAgentConfig{ReturnDirect: []string{"finish"}})
	if err != nil {
		t.Fatal(err)
	}
	result, err := agent.Invoke(context.Background(), reactState{}, graph.RunConfig{})
	if err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 1 || len(result.Tools) != 1 || result.Tools[0].Name != "finish" {
		t.Fatalf("calls=%d state=%+v", calls.Load(), result)
	}
}

func TestCreateReactAgentWithoutToolsIsSingleModelNode(t *testing.T) {
	model := prebuilt.ChatModelFunc[reactState]{Run: func(context.Context, reactState, graph.Runtime) (prebuilt.AssistantMessage, error) {
		// As in upstream, an empty tool set disables tool calling even if a
		// provider unexpectedly returns a tool call.
		return prebuilt.AssistantMessage{Content: "final", ToolCalls: []prebuilt.ToolCall{{ID: "unexpected", Name: "missing"}}}, nil
	}}
	adapter := reactAdapter()
	adapter.ToolNode = prebuilt.ToolNodeAdapter[reactState, reactDelta]{}
	agent, err := prebuilt.CreateReactAgent(model, nil, reactReducer, adapter, prebuilt.ReactAgentConfig{})
	if err != nil {
		t.Fatal(err)
	}
	result, err := agent.Invoke(context.Background(), reactState{}, graph.RunConfig{})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Assistant) != 1 || !reflect.DeepEqual(agent.Nodes(), []graph.NodeID{prebuilt.AgentNodeID}) {
		t.Fatalf("nodes=%v state=%+v", agent.Nodes(), result)
	}
}

func TestCreateReactAgentGracefullyStopsWhenStepsAreInsufficient(t *testing.T) {
	tests := []struct {
		name          string
		remaining     int
		returnDirect  []string
		wantToolCalls int
	}{
		{name: "regular tool needs two steps", remaining: 1},
		{name: "direct tool needs one step", remaining: 0, returnDirect: []string{"lookup"}},
		{name: "direct tool allowed with one step", remaining: 1, returnDirect: []string{"lookup"}, wantToolCalls: 1},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			state := reactState{}
			model := prebuilt.ChatModelFunc[reactState]{Run: func(context.Context, reactState, graph.Runtime) (prebuilt.AssistantMessage, error) {
				return prebuilt.AssistantMessage{ID: "model-id", Content: "thinking", ToolCalls: []prebuilt.ToolCall{{ID: "call-1", Name: "lookup"}}}, nil
			}}
			tool := prebuilt.ToolFunc[reactState, reactDelta]{ToolName: "lookup", Run: func(context.Context, prebuilt.ToolCall, prebuilt.ToolRuntime[reactState]) (prebuilt.ToolResult[reactDelta], error) {
				return prebuilt.TextResult[reactDelta]("found"), nil
			}}
			adapter := reactAdapter()
			adapter.RemainingSteps = func(context.Context, reactState) (int, bool, error) {
				return test.remaining, true, nil
			}
			agent, err := prebuilt.CreateReactAgent(model, []prebuilt.Tool[reactState, reactDelta]{tool}, reactReducer, adapter, prebuilt.ReactAgentConfig{ReturnDirect: test.returnDirect})
			if err != nil {
				t.Fatal(err)
			}
			result, err := agent.Invoke(context.Background(), state, graph.RunConfig{})
			if err != nil {
				t.Fatal(err)
			}
			first := result.Assistant[0]
			if len(first.ToolCalls) != test.wantToolCalls {
				t.Fatalf("tool calls=%+v", first.ToolCalls)
			}
			if test.wantToolCalls == 0 {
				if first.ID != "model-id" || first.Content != prebuilt.StepsExhaustedMessage || len(result.Tools) != 0 {
					t.Fatalf("state=%+v", result)
				}
			} else if len(result.Tools) != 1 {
				t.Fatalf("state=%+v", result)
			}
		})
	}
}

func TestCreateReactAgentRemainingStepsAdapterError(t *testing.T) {
	model := prebuilt.ChatModelFunc[reactState]{Run: func(context.Context, reactState, graph.Runtime) (prebuilt.AssistantMessage, error) {
		return prebuilt.AssistantMessage{ToolCalls: []prebuilt.ToolCall{{ID: "call-1", Name: "lookup"}}}, nil
	}}
	tool := prebuilt.ToolFunc[reactState, reactDelta]{ToolName: "lookup", Run: func(context.Context, prebuilt.ToolCall, prebuilt.ToolRuntime[reactState]) (prebuilt.ToolResult[reactDelta], error) {
		return prebuilt.TextResult[reactDelta]("found"), nil
	}}
	adapter := reactAdapter()
	adapter.RemainingSteps = func(context.Context, reactState) (int, bool, error) {
		return 0, false, errors.New("remaining unavailable")
	}
	agent, err := prebuilt.CreateReactAgent(model, []prebuilt.Tool[reactState, reactDelta]{tool}, reactReducer, adapter, prebuilt.ReactAgentConfig{})
	if err != nil {
		t.Fatal(err)
	}
	_, err = agent.Invoke(context.Background(), reactState{}, graph.RunConfig{})
	var nodeErr *graph.NodeExecutionError
	if err == nil || !errors.As(err, &nodeErr) || !strings.Contains(err.Error(), "read remaining steps") {
		t.Fatalf("error=%v", err)
	}
}

func TestCreateReactAgentRunsPreAndPostHooksOnEveryModelTurn(t *testing.T) {
	model := prebuilt.ChatModelFunc[reactState]{Run: func(_ context.Context, state reactState, _ graph.Runtime) (prebuilt.AssistantMessage, error) {
		if len(state.Tools) == 0 {
			return prebuilt.AssistantMessage{ToolCalls: []prebuilt.ToolCall{{ID: "call-1", Name: "lookup"}}}, nil
		}
		return prebuilt.AssistantMessage{Content: "done"}, nil
	}}
	tool := prebuilt.ToolFunc[reactState, reactDelta]{ToolName: "lookup", Run: func(context.Context, prebuilt.ToolCall, prebuilt.ToolRuntime[reactState]) (prebuilt.ToolResult[reactDelta], error) {
		return prebuilt.TextResult[reactDelta]("found"), nil
	}}
	adapter := reactAdapter()
	adapter.PreModelHook = func(_ context.Context, _ reactState, _ graph.Runtime) (graph.Command[reactDelta], error) {
		return graph.Update(reactDelta{Event: "pre"}), nil
	}
	adapter.PostModelHook = func(_ context.Context, _ reactState, _ graph.Runtime) (graph.Command[reactDelta], error) {
		return graph.Update(reactDelta{Event: "post"}), nil
	}
	agent, err := prebuilt.CreateReactAgent(model, []prebuilt.Tool[reactState, reactDelta]{tool}, reactReducer, adapter, prebuilt.ReactAgentConfig{})
	if err != nil {
		t.Fatal(err)
	}
	result, err := agent.Invoke(context.Background(), reactState{}, graph.RunConfig{})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(result.Events, []string{"pre", "post", "pre", "post"}) {
		t.Fatalf("events=%v", result.Events)
	}
	if got := agent.Nodes(); !reflect.DeepEqual(got, []graph.NodeID{prebuilt.AgentNodeID, prebuilt.PostModelHookNodeID, prebuilt.PreModelHookNodeID, prebuilt.ToolsNodeID}) {
		t.Fatalf("nodes=%v", got)
	}
}

func TestCreateReactAgentHooksWithoutTools(t *testing.T) {
	model := prebuilt.ChatModelFunc[reactState]{Run: func(context.Context, reactState, graph.Runtime) (prebuilt.AssistantMessage, error) {
		return prebuilt.AssistantMessage{Content: "done"}, nil
	}}
	adapter := reactAdapter()
	adapter.ToolNode = prebuilt.ToolNodeAdapter[reactState, reactDelta]{}
	adapter.PreModelHook = func(_ context.Context, _ reactState, _ graph.Runtime) (graph.Command[reactDelta], error) {
		return graph.Update(reactDelta{Event: "pre"}), nil
	}
	adapter.PostModelHook = func(_ context.Context, _ reactState, _ graph.Runtime) (graph.Command[reactDelta], error) {
		return graph.Update(reactDelta{Event: "post"}), nil
	}
	agent, err := prebuilt.CreateReactAgent(model, nil, reactReducer, adapter, prebuilt.ReactAgentConfig{})
	if err != nil {
		t.Fatal(err)
	}
	result, err := agent.Invoke(context.Background(), reactState{}, graph.RunConfig{})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(result.Events, []string{"pre", "post"}) {
		t.Fatalf("events=%v", result.Events)
	}
}

func TestCreateReactAgentPostHookCanStopToolRouting(t *testing.T) {
	var toolCalls atomic.Int32
	model := prebuilt.ChatModelFunc[reactState]{Run: func(context.Context, reactState, graph.Runtime) (prebuilt.AssistantMessage, error) {
		return prebuilt.AssistantMessage{ToolCalls: []prebuilt.ToolCall{{ID: "call-1", Name: "lookup"}}}, nil
	}}
	tool := prebuilt.ToolFunc[reactState, reactDelta]{ToolName: "lookup", Run: func(context.Context, prebuilt.ToolCall, prebuilt.ToolRuntime[reactState]) (prebuilt.ToolResult[reactDelta], error) {
		toolCalls.Add(1)
		return prebuilt.TextResult[reactDelta]("found"), nil
	}}
	adapter := reactAdapter()
	adapter.PostModelHook = func(_ context.Context, _ reactState, _ graph.Runtime) (graph.Command[reactDelta], error) {
		return graph.Update(reactDelta{ReplaceLast: &prebuilt.AssistantMessage{Content: "blocked"}}), nil
	}
	agent, err := prebuilt.CreateReactAgent(model, []prebuilt.Tool[reactState, reactDelta]{tool}, reactReducer, adapter, prebuilt.ReactAgentConfig{})
	if err != nil {
		t.Fatal(err)
	}
	result, err := agent.Invoke(context.Background(), reactState{}, graph.RunConfig{})
	if err != nil {
		t.Fatal(err)
	}
	if toolCalls.Load() != 0 || len(result.Tools) != 0 || result.Assistant[0].Content != "blocked" {
		t.Fatalf("tool calls=%d state=%+v", toolCalls.Load(), result)
	}
}

func TestCreateReactAgentGeneratesStructuredResponseAfterLoop(t *testing.T) {
	model := prebuilt.ChatModelFunc[reactState]{Run: func(_ context.Context, state reactState, _ graph.Runtime) (prebuilt.AssistantMessage, error) {
		if len(state.Tools) == 0 {
			return prebuilt.AssistantMessage{ToolCalls: []prebuilt.ToolCall{{ID: "call-1", Name: "lookup"}}}, nil
		}
		return prebuilt.AssistantMessage{Content: "raw final"}, nil
	}}
	tool := prebuilt.ToolFunc[reactState, reactDelta]{ToolName: "lookup", Run: func(context.Context, prebuilt.ToolCall, prebuilt.ToolRuntime[reactState]) (prebuilt.ToolResult[reactDelta], error) {
		return prebuilt.TextResult[reactDelta]("found"), nil
	}}
	adapter := reactAdapter()
	adapter.StructuredModel = prebuilt.StructuredModelFunc[reactState]{Run: func(_ context.Context, state reactState, _ graph.Runtime) (any, error) {
		if state.Assistant[len(state.Assistant)-1].Content != "raw final" {
			t.Fatalf("structured model state=%+v", state)
		}
		return "structured final", nil
	}}
	adapter.StructuredResponse = func(_ context.Context, _ reactState, value any) (reactDelta, error) {
		result, ok := value.(string)
		if !ok {
			return reactDelta{}, errors.New("unexpected structured type")
		}
		return reactDelta{Structured: &result}, nil
	}
	agent, err := prebuilt.CreateReactAgent(model, []prebuilt.Tool[reactState, reactDelta]{tool}, reactReducer, adapter, prebuilt.ReactAgentConfig{})
	if err != nil {
		t.Fatal(err)
	}
	result, err := agent.Invoke(context.Background(), reactState{}, graph.RunConfig{})
	if err != nil {
		t.Fatal(err)
	}
	if result.Structured != "structured final" || !slices.Contains(agent.Nodes(), prebuilt.GenerateStructuredResponseNodeID) {
		t.Fatalf("nodes=%v state=%+v", agent.Nodes(), result)
	}
}

func TestCreateReactAgentReturnDirectSkipsStructuredResponse(t *testing.T) {
	var structuredCalls atomic.Int32
	model := prebuilt.ChatModelFunc[reactState]{Run: func(context.Context, reactState, graph.Runtime) (prebuilt.AssistantMessage, error) {
		return prebuilt.AssistantMessage{ToolCalls: []prebuilt.ToolCall{{ID: "call-1", Name: "finish"}}}, nil
	}}
	tool := prebuilt.ToolFunc[reactState, reactDelta]{ToolName: "finish", Run: func(context.Context, prebuilt.ToolCall, prebuilt.ToolRuntime[reactState]) (prebuilt.ToolResult[reactDelta], error) {
		return prebuilt.TextResult[reactDelta]("done"), nil
	}}
	adapter := reactAdapter()
	adapter.StructuredModel = prebuilt.StructuredModelFunc[reactState]{Run: func(context.Context, reactState, graph.Runtime) (any, error) {
		structuredCalls.Add(1)
		return "unexpected", nil
	}}
	adapter.StructuredResponse = func(_ context.Context, _ reactState, value any) (reactDelta, error) {
		result := value.(string)
		return reactDelta{Structured: &result}, nil
	}
	agent, err := prebuilt.CreateReactAgent(model, []prebuilt.Tool[reactState, reactDelta]{tool}, reactReducer, adapter, prebuilt.ReactAgentConfig{ReturnDirect: []string{"finish"}})
	if err != nil {
		t.Fatal(err)
	}
	result, err := agent.Invoke(context.Background(), reactState{}, graph.RunConfig{})
	if err != nil {
		t.Fatal(err)
	}
	if structuredCalls.Load() != 0 || result.Structured != "" {
		t.Fatalf("structured calls=%d state=%+v", structuredCalls.Load(), result)
	}
}

func TestCreateReactAgentValidatesStructuredAdapterPair(t *testing.T) {
	model := prebuilt.ChatModelFunc[reactState]{Run: func(context.Context, reactState, graph.Runtime) (prebuilt.AssistantMessage, error) {
		return prebuilt.AssistantMessage{}, nil
	}}
	adapter := reactAdapter()
	adapter.StructuredModel = prebuilt.StructuredModelFunc[reactState]{Run: func(context.Context, reactState, graph.Runtime) (any, error) {
		return nil, nil
	}}
	if _, err := prebuilt.CreateReactAgent(model, nil, reactReducer, adapter, prebuilt.ReactAgentConfig{}); err == nil {
		t.Fatal("expected incomplete structured adapter error")
	}
}

func TestCreateReactAgentV2DistributesToolCallsAsSendTasks(t *testing.T) {
	var active atomic.Int32
	var maximum atomic.Int32
	bothStarted := make(chan struct{})
	var release sync.Once
	model := prebuilt.ChatModelFunc[reactState]{Run: func(_ context.Context, state reactState, _ graph.Runtime) (prebuilt.AssistantMessage, error) {
		if len(state.Tools) == 0 {
			return prebuilt.AssistantMessage{ToolCalls: []prebuilt.ToolCall{{ID: "slow-call", Name: "slow"}, {ID: "fast-call", Name: "fast"}}}, nil
		}
		return prebuilt.AssistantMessage{Content: "done"}, nil
	}}
	makeTool := func(name string, delay time.Duration) prebuilt.Tool[reactState, reactDelta] {
		return prebuilt.ToolFunc[reactState, reactDelta]{ToolName: name, Run: func(ctx context.Context, _ prebuilt.ToolCall, _ prebuilt.ToolRuntime[reactState]) (prebuilt.ToolResult[reactDelta], error) {
			current := active.Add(1)
			for {
				previous := maximum.Load()
				if current <= previous || maximum.CompareAndSwap(previous, current) {
					break
				}
			}
			defer active.Add(-1)
			if current == 2 {
				release.Do(func() { close(bothStarted) })
			}
			select {
			case <-bothStarted:
			case <-ctx.Done():
				return prebuilt.ToolResult[reactDelta]{}, ctx.Err()
			}
			select {
			case <-time.After(delay):
			case <-ctx.Done():
				return prebuilt.ToolResult[reactDelta]{}, ctx.Err()
			}
			return prebuilt.TextResult[reactDelta](name), nil
		}}
	}
	adapter := reactAdapter()
	adapter.ToolCallState = func(_ context.Context, state reactState, call prebuilt.ToolCall) (reactState, error) {
		state.TaskCall = &call
		return state, nil
	}
	agent, err := prebuilt.CreateReactAgent(
		model,
		[]prebuilt.Tool[reactState, reactDelta]{makeTool("slow", 20*time.Millisecond), makeTool("fast", time.Millisecond)},
		reactReducer,
		adapter,
		prebuilt.ReactAgentConfig{Version: prebuilt.ReactAgentV2},
	)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	result, err := agent.Invoke(ctx, reactState{}, graph.RunConfig{MaxConcurrency: 2})
	if err != nil {
		t.Fatal(err)
	}
	if maximum.Load() != 2 {
		t.Fatalf("maximum concurrency=%d", maximum.Load())
	}
	if len(result.Tools) != 2 || result.Tools[0].ToolCallID != "slow-call" || result.Tools[1].ToolCallID != "fast-call" {
		t.Fatalf("tool messages=%+v", result.Tools)
	}
}

func TestCreateReactAgentV2RequiresToolCallStateAdapter(t *testing.T) {
	model := prebuilt.ChatModelFunc[reactState]{Run: func(context.Context, reactState, graph.Runtime) (prebuilt.AssistantMessage, error) {
		return prebuilt.AssistantMessage{}, nil
	}}
	tool := prebuilt.ToolFunc[reactState, reactDelta]{ToolName: "lookup", Run: func(context.Context, prebuilt.ToolCall, prebuilt.ToolRuntime[reactState]) (prebuilt.ToolResult[reactDelta], error) {
		return prebuilt.TextResult[reactDelta]("found"), nil
	}}
	if _, err := prebuilt.CreateReactAgent(model, []prebuilt.Tool[reactState, reactDelta]{tool}, reactReducer, reactAdapter(), prebuilt.ReactAgentConfig{Version: prebuilt.ReactAgentV2}); err == nil {
		t.Fatal("expected v2 ToolCallState validation error")
	}
	if _, err := prebuilt.CreateReactAgent(model, nil, reactReducer, reactAdapter(), prebuilt.ReactAgentConfig{Version: "v3"}); err == nil {
		t.Fatal("expected invalid version error")
	}
}

func TestCreateReactAgentValidationAndCancellation(t *testing.T) {
	model := prebuilt.ChatModelFunc[reactState]{Run: func(ctx context.Context, _ reactState, _ graph.Runtime) (prebuilt.AssistantMessage, error) {
		<-ctx.Done()
		return prebuilt.AssistantMessage{}, ctx.Err()
	}}
	if _, err := prebuilt.CreateReactAgent[reactState, reactDelta](nil, nil, reactReducer, reactAdapter(), prebuilt.ReactAgentConfig{}); err == nil {
		t.Fatal("expected nil model validation error")
	}
	bad := reactAdapter()
	bad.ToolMessages = nil
	if _, err := prebuilt.CreateReactAgent(model, nil, reactReducer, bad, prebuilt.ReactAgentConfig{ReturnDirect: []string{"finish"}}); err == nil {
		t.Fatal("expected return-direct adapter validation error")
	}
	agent, err := prebuilt.CreateReactAgent(model, nil, reactReducer, reactAdapter(), prebuilt.ReactAgentConfig{})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = agent.Invoke(ctx, reactState{}, graph.RunConfig{})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error=%v", err)
	}
}
