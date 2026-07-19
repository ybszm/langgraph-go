package prebuilt_test

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"github.com/wahanbo/langgraph-go/checkpoint"
	checkpointmemory "github.com/wahanbo/langgraph-go/checkpoint/memory"
	"github.com/wahanbo/langgraph-go/graph"
	"github.com/wahanbo/langgraph-go/prebuilt"
	"github.com/wahanbo/langgraph-go/store"
	storememory "github.com/wahanbo/langgraph-go/store/memory"
)

type toolAgentState struct {
	Calls    []prebuilt.ToolCall
	Messages []prebuilt.ToolMessage
	Total    int
	User     string
}

type toolAgentDelta struct {
	Messages []prebuilt.ToolMessage
	Add      int
}

func toolAgentReducer(_ context.Context, state toolAgentState, updates []toolAgentDelta) (toolAgentState, error) {
	state.Calls = append([]prebuilt.ToolCall(nil), state.Calls...)
	state.Messages = append([]prebuilt.ToolMessage(nil), state.Messages...)
	for _, update := range updates {
		state.Messages = append(state.Messages, update.Messages...)
		state.Total += update.Add
	}
	return state, nil
}

func toolAdapter() prebuilt.ToolNodeAdapter[toolAgentState, toolAgentDelta] {
	return prebuilt.ToolNodeAdapter[toolAgentState, toolAgentDelta]{
		Calls: func(_ context.Context, state toolAgentState) ([]prebuilt.ToolCall, error) {
			return append([]prebuilt.ToolCall(nil), state.Calls...), nil
		},
		Messages: func(_ context.Context, _ toolAgentState, messages []prebuilt.ToolMessage) (toolAgentDelta, error) {
			return toolAgentDelta{Messages: append([]prebuilt.ToolMessage(nil), messages...)}, nil
		},
	}
}

func toolGraph(
	t *testing.T,
	node *prebuilt.ToolNode[toolAgentState, toolAgentDelta],
	options ...graph.CompileOption[toolAgentState, toolAgentDelta],
) *graph.CompiledGraph[toolAgentState, toolAgentDelta] {
	t.Helper()
	builder := graph.NewStateGraph(toolAgentReducer)
	if err := builder.AddNode("tools", node.Node()); err != nil {
		t.Fatal(err)
	}
	_ = builder.AddEdge(graph.START, "tools")
	_ = builder.AddEdge("tools", graph.END)
	compiled, err := builder.Compile(options...)
	if err != nil {
		t.Fatal(err)
	}
	return compiled
}

func updateMax(target *atomic.Int32, value int32) {
	for {
		current := target.Load()
		if value <= current || target.CompareAndSwap(current, value) {
			return
		}
	}
}

func TestToolNodeExecutesParallelAndReturnsCallOrder(t *testing.T) {
	var active atomic.Int32
	var maximum atomic.Int32
	makeTool := func(name string, delay time.Duration) prebuilt.Tool[toolAgentState, toolAgentDelta] {
		return prebuilt.ToolFunc[toolAgentState, toolAgentDelta]{ToolName: name, Run: func(ctx context.Context, call prebuilt.ToolCall, _ prebuilt.ToolRuntime[toolAgentState]) (prebuilt.ToolResult[toolAgentDelta], error) {
			current := active.Add(1)
			updateMax(&maximum, current)
			defer active.Add(-1)
			select {
			case <-time.After(delay):
			case <-ctx.Done():
				return prebuilt.ToolResult[toolAgentDelta]{}, ctx.Err()
			}
			return prebuilt.TextResult[toolAgentDelta]("result:" + call.ID), nil
		}}
	}
	node, err := prebuilt.NewToolNode(
		[]prebuilt.Tool[toolAgentState, toolAgentDelta]{makeTool("slow", 20*time.Millisecond), makeTool("fast", time.Millisecond)},
		toolAdapter(),
		prebuilt.ToolNodeConfig{MaxConcurrency: 2},
	)
	if err != nil {
		t.Fatal(err)
	}
	compiled := toolGraph(t, node)
	result, err := compiled.Invoke(context.Background(), toolAgentState{Calls: []prebuilt.ToolCall{
		{ID: "one", Name: "slow", Arguments: json.RawMessage(`{}`)},
		{ID: "two", Name: "fast", Arguments: json.RawMessage(`{}`)},
	}}, graph.RunConfig{})
	if err != nil {
		t.Fatal(err)
	}
	if maximum.Load() != 2 {
		t.Fatalf("maximum concurrency=%d", maximum.Load())
	}
	if len(result.Messages) != 2 || result.Messages[0].ToolCallID != "one" || result.Messages[1].ToolCallID != "two" {
		t.Fatalf("messages=%+v", result.Messages)
	}
}

func TestToolNodeUnknownAndHandledErrorsBecomeToolMessages(t *testing.T) {
	failing := prebuilt.ToolFunc[toolAgentState, toolAgentDelta]{ToolName: "fail", Run: func(context.Context, prebuilt.ToolCall, prebuilt.ToolRuntime[toolAgentState]) (prebuilt.ToolResult[toolAgentDelta], error) {
		return prebuilt.ToolResult[toolAgentDelta]{}, errors.New("boom")
	}}
	node, err := prebuilt.NewToolNode(
		[]prebuilt.Tool[toolAgentState, toolAgentDelta]{failing}, toolAdapter(),
		prebuilt.ToolNodeConfig{ErrorHandler: func(err error, _ prebuilt.ToolCall) (string, bool) {
			return "handled:" + err.Error(), true
		}},
	)
	if err != nil {
		t.Fatal(err)
	}
	result, err := toolGraph(t, node).Invoke(context.Background(), toolAgentState{Calls: []prebuilt.ToolCall{
		{ID: "missing-id", Name: "missing"}, {ID: "fail-id", Name: "fail"},
	}}, graph.RunConfig{})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Messages) != 2 || result.Messages[0].Status != prebuilt.ToolStatusError ||
		result.Messages[1].Content != "handled:boom" || result.Messages[1].ToolCallID != "fail-id" {
		t.Fatalf("messages=%+v", result.Messages)
	}
}

func TestToolNodePropagatesUnhandledErrorAndHandlesPanic(t *testing.T) {
	failing := prebuilt.ToolFunc[toolAgentState, toolAgentDelta]{ToolName: "fail", Run: func(context.Context, prebuilt.ToolCall, prebuilt.ToolRuntime[toolAgentState]) (prebuilt.ToolResult[toolAgentDelta], error) {
		return prebuilt.ToolResult[toolAgentDelta]{}, errors.New("unhandled")
	}}
	node, _ := prebuilt.NewToolNode([]prebuilt.Tool[toolAgentState, toolAgentDelta]{failing}, toolAdapter(), prebuilt.ToolNodeConfig{})
	if _, err := toolGraph(t, node).Invoke(context.Background(), toolAgentState{Calls: []prebuilt.ToolCall{{ID: "1", Name: "fail"}}}, graph.RunConfig{}); err == nil {
		t.Fatal("unhandled tool error was swallowed")
	}
	panicking := prebuilt.ToolFunc[toolAgentState, toolAgentDelta]{ToolName: "panic", Run: func(context.Context, prebuilt.ToolCall, prebuilt.ToolRuntime[toolAgentState]) (prebuilt.ToolResult[toolAgentDelta], error) {
		panic("bad tool")
	}}
	panicNode, _ := prebuilt.NewToolNode([]prebuilt.Tool[toolAgentState, toolAgentDelta]{panicking}, toolAdapter(), prebuilt.ToolNodeConfig{HandleToolErrors: true})
	result, err := toolGraph(t, panicNode).Invoke(context.Background(), toolAgentState{Calls: []prebuilt.ToolCall{{ID: "2", Name: "panic"}}}, graph.RunConfig{})
	if err != nil || len(result.Messages) != 1 || result.Messages[0].Status != prebuilt.ToolStatusError {
		t.Fatalf("panic result=%+v err=%v", result, err)
	}
}

func TestToolRuntimeInjectsStateContextAndStore(t *testing.T) {
	tenantTool := prebuilt.ToolFunc[toolAgentState, toolAgentDelta]{ToolName: "tenant", Run: func(ctx context.Context, call prebuilt.ToolCall, runtime prebuilt.ToolRuntime[toolAgentState]) (prebuilt.ToolResult[toolAgentDelta], error) {
		tenant, ok := runtime.Context.(string)
		if !ok || runtime.Store == nil || runtime.State.User == "" || runtime.Call.ID != call.ID {
			return prebuilt.ToolResult[toolAgentDelta]{}, errors.New("injected runtime is incomplete")
		}
		if err := runtime.Store.Put(ctx, store.Namespace{"tool"}, call.ID, store.Value{"user": runtime.State.User, "tenant": tenant}); err != nil {
			return prebuilt.ToolResult[toolAgentDelta]{}, err
		}
		return prebuilt.TextResult[toolAgentDelta](runtime.State.User + ":" + tenant), nil
	}}
	node, _ := prebuilt.NewToolNode([]prebuilt.Tool[toolAgentState, toolAgentDelta]{tenantTool}, toolAdapter(), prebuilt.ToolNodeConfig{})
	memoryStore := storememory.New()
	compiled := toolGraph(t, node, graph.WithStore[toolAgentState, toolAgentDelta](memoryStore))
	result, err := compiled.Invoke(context.Background(), toolAgentState{User: "alice", Calls: []prebuilt.ToolCall{{ID: "call", Name: "tenant"}}}, graph.RunConfig{Context: "acme"})
	if err != nil || result.Messages[0].Content != "alice:acme" {
		t.Fatalf("result=%+v err=%v", result, err)
	}
	item, err := memoryStore.Get(context.Background(), store.Namespace{"tool"}, "call")
	if err != nil || item == nil || item.Value["tenant"] != "acme" {
		t.Fatalf("store item=%+v err=%v", item, err)
	}
}

func TestToolCommandUpdatesAndRoutesGraph(t *testing.T) {
	commandTool := prebuilt.ToolFunc[toolAgentState, toolAgentDelta]{ToolName: "command", Run: func(context.Context, prebuilt.ToolCall, prebuilt.ToolRuntime[toolAgentState]) (prebuilt.ToolResult[toolAgentDelta], error) {
		return prebuilt.CommandResult(graph.UpdateAndGoto(toolAgentDelta{Add: 2}, "finish")), nil
	}}
	node, _ := prebuilt.NewToolNode([]prebuilt.Tool[toolAgentState, toolAgentDelta]{commandTool}, toolAdapter(), prebuilt.ToolNodeConfig{})
	builder := graph.NewStateGraph(toolAgentReducer)
	_ = builder.AddNode("tools", node.Node())
	_ = builder.AddNode("finish", func(context.Context, toolAgentState, graph.Runtime) (graph.Command[toolAgentDelta], error) {
		return graph.Update(toolAgentDelta{Add: 3}), nil
	})
	_ = builder.AddEdge(graph.START, "tools")
	_ = builder.AddEdge("tools", graph.END)
	_ = builder.AddCommandDestinations("tools", "finish")
	_ = builder.AddEdge("finish", graph.END)
	compiled, err := builder.Compile()
	if err != nil {
		t.Fatal(err)
	}
	result, err := compiled.Invoke(context.Background(), toolAgentState{Calls: []prebuilt.ToolCall{{ID: "1", Name: "command"}}}, graph.RunConfig{})
	if err != nil || result.Total != 5 {
		t.Fatalf("result=%+v err=%v", result, err)
	}
}

func TestToolNodeCombinesMessagesAndCommands(t *testing.T) {
	messageTool := prebuilt.ToolFunc[toolAgentState, toolAgentDelta]{ToolName: "message", Run: func(context.Context, prebuilt.ToolCall, prebuilt.ToolRuntime[toolAgentState]) (prebuilt.ToolResult[toolAgentDelta], error) {
		return prebuilt.TextResult[toolAgentDelta]("done"), nil
	}}
	commandTool := prebuilt.ToolFunc[toolAgentState, toolAgentDelta]{ToolName: "command", Run: func(context.Context, prebuilt.ToolCall, prebuilt.ToolRuntime[toolAgentState]) (prebuilt.ToolResult[toolAgentDelta], error) {
		return prebuilt.CommandResult(graph.UpdateAndGoto(toolAgentDelta{Add: 2}, "finish")), nil
	}}
	adapter := toolAdapter()
	adapter.Combine = func(_ context.Context, _ toolAgentState, messages []prebuilt.ToolMessage, commands []graph.Command[toolAgentDelta]) (graph.Command[toolAgentDelta], error) {
		combined := toolAgentDelta{Messages: append([]prebuilt.ToolMessage(nil), messages...)}
		var destinations []graph.NodeID
		for _, command := range commands {
			if command.HasUpdate {
				combined.Add += command.Update.Add
				combined.Messages = append(combined.Messages, command.Update.Messages...)
			}
			destinations = append(destinations, command.Goto...)
		}
		return graph.UpdateAndGoto(combined, destinations...), nil
	}
	node, _ := prebuilt.NewToolNode(
		[]prebuilt.Tool[toolAgentState, toolAgentDelta]{messageTool, commandTool}, adapter, prebuilt.ToolNodeConfig{},
	)
	builder := graph.NewStateGraph(toolAgentReducer)
	_ = builder.AddNode("tools", node.Node())
	_ = builder.AddNode("finish", func(context.Context, toolAgentState, graph.Runtime) (graph.Command[toolAgentDelta], error) {
		return graph.Update(toolAgentDelta{Add: 3}), nil
	})
	_ = builder.AddEdge(graph.START, "tools")
	_ = builder.AddEdge("tools", graph.END)
	_ = builder.AddCommandDestinations("tools", "finish")
	_ = builder.AddEdge("finish", graph.END)
	compiled, err := builder.Compile()
	if err != nil {
		t.Fatal(err)
	}
	result, err := compiled.Invoke(context.Background(), toolAgentState{Calls: []prebuilt.ToolCall{
		{ID: "m", Name: "message"}, {ID: "c", Name: "command"},
	}}, graph.RunConfig{})
	if err != nil || result.Total != 5 || len(result.Messages) != 1 || result.Messages[0].ToolCallID != "m" {
		t.Fatalf("result=%+v err=%v", result, err)
	}
}

func TestToolParentCommandTargetsContainingGraph(t *testing.T) {
	parentTool := prebuilt.ToolFunc[toolAgentState, toolAgentDelta]{ToolName: "parent", Run: func(context.Context, prebuilt.ToolCall, prebuilt.ToolRuntime[toolAgentState]) (prebuilt.ToolResult[toolAgentDelta], error) {
		return prebuilt.CommandResult(graph.ParentUpdateAndGoto(toolAgentDelta{Add: 2}, "parent_finish")), nil
	}}
	node, _ := prebuilt.NewToolNode([]prebuilt.Tool[toolAgentState, toolAgentDelta]{parentTool}, toolAdapter(), prebuilt.ToolNodeConfig{})
	childBuilder := graph.NewStateGraph(toolAgentReducer)
	_ = childBuilder.AddNode("tools", node.Node())
	_ = childBuilder.AddEdge(graph.START, "tools")
	_ = childBuilder.AddEdge("tools", graph.END)
	child, err := childBuilder.Compile()
	if err != nil {
		t.Fatal(err)
	}
	parentBuilder := graph.NewStateGraph(toolAgentReducer)
	_ = graph.AddSubgraph(parentBuilder, "child", child, graph.SubgraphAdapter[toolAgentState, toolAgentDelta, toolAgentState, toolAgentDelta]{
		Input: func(_ context.Context, state toolAgentState) (toolAgentState, error) { return state, nil },
		Output: func(context.Context, toolAgentState, toolAgentState) (graph.Command[toolAgentDelta], error) {
			return graph.NoCommand[toolAgentDelta](), nil
		},
	})
	_ = parentBuilder.AddNode("parent_finish", func(context.Context, toolAgentState, graph.Runtime) (graph.Command[toolAgentDelta], error) {
		return graph.Update(toolAgentDelta{Add: 3}), nil
	})
	_ = parentBuilder.AddEdge(graph.START, "child")
	_ = parentBuilder.AddEdge("child", graph.END)
	_ = parentBuilder.AddCommandDestinations("child", "parent_finish")
	_ = parentBuilder.AddEdge("parent_finish", graph.END)
	parent, err := parentBuilder.Compile()
	if err != nil {
		t.Fatal(err)
	}
	result, err := parent.Invoke(context.Background(), toolAgentState{Calls: []prebuilt.ToolCall{{ID: "p", Name: "parent"}}}, graph.RunConfig{})
	if err != nil || result.Total != 5 {
		t.Fatalf("result=%+v err=%v", result, err)
	}
}

func TestToolNodeStreamsToolMessages(t *testing.T) {
	tool := prebuilt.ToolFunc[toolAgentState, toolAgentDelta]{ToolName: "echo", Run: func(context.Context, prebuilt.ToolCall, prebuilt.ToolRuntime[toolAgentState]) (prebuilt.ToolResult[toolAgentDelta], error) {
		return prebuilt.TextResult[toolAgentDelta]("echoed"), nil
	}}
	node, _ := prebuilt.NewToolNode([]prebuilt.Tool[toolAgentState, toolAgentDelta]{tool}, toolAdapter(), prebuilt.ToolNodeConfig{StreamToolResults: true})
	compiled := toolGraph(t, node)
	var message prebuilt.ToolMessage
	var metadata map[string]any
	for event := range compiled.StreamWithOptions(context.Background(), toolAgentState{Calls: []prebuilt.ToolCall{{ID: "call", Name: "echo"}}}, graph.RunConfig{}, graph.StreamOptions{Modes: []graph.StreamMode{graph.StreamMessages}}) {
		if event.Message != nil {
			message, _ = event.Message.Message.(prebuilt.ToolMessage)
			metadata = event.Message.Metadata
		}
	}
	if message.ToolCallID != "call" || metadata["tool_name"] != "echo" || metadata["langgraph_node"] != "tools" {
		t.Fatalf("message=%+v metadata=%v", message, metadata)
	}
}

func TestToolInterruptResumesWithoutBecomingErrorMessage(t *testing.T) {
	approval := prebuilt.ToolFunc[toolAgentState, toolAgentDelta]{ToolName: "approval", Run: func(_ context.Context, _ prebuilt.ToolCall, runtime prebuilt.ToolRuntime[toolAgentState]) (prebuilt.ToolResult[toolAgentDelta], error) {
		answer, err := graph.AwaitResume[string](runtime.Graph, "approve tool?")
		if err != nil {
			return prebuilt.ToolResult[toolAgentDelta]{}, err
		}
		return prebuilt.TextResult[toolAgentDelta](answer), nil
	}}
	node, _ := prebuilt.NewToolNode([]prebuilt.Tool[toolAgentState, toolAgentDelta]{approval}, toolAdapter(), prebuilt.ToolNodeConfig{HandleToolErrors: true})
	saver := checkpointmemory.NewSaver()
	compiled := toolGraph(t, node, graph.WithPersistence(graph.PersistenceConfig[toolAgentState, toolAgentDelta]{
		Saver:      saver,
		StateCodec: checkpoint.MustJSONCodec[toolAgentState]("tests.tool-state", 1),
		DeltaCodec: checkpoint.MustJSONCodec[toolAgentDelta]("tests.tool-delta", 1),
	}))
	config := graph.RunConfig{ThreadID: "tool-interrupt"}
	if _, err := compiled.Invoke(context.Background(), toolAgentState{Calls: []prebuilt.ToolCall{{ID: "approve", Name: "approval"}}}, config); !errors.Is(err, graph.ErrGraphInterrupt) {
		t.Fatalf("initial err=%v", err)
	}
	command, _ := graph.Resume("approved")
	result, err := compiled.Resume(context.Background(), config, command)
	if err != nil || len(result.Messages) != 1 || result.Messages[0].Content != "approved" || result.Messages[0].Status != prebuilt.ToolStatusSuccess {
		t.Fatalf("result=%+v err=%v", result, err)
	}
}

func TestParallelToolInterruptsResumeByID(t *testing.T) {
	ready := make(chan struct{}, 4)
	release := make(chan struct{})
	var oneCalls atomic.Int32
	var twoCalls atomic.Int32
	approval := prebuilt.ToolFunc[toolAgentState, toolAgentDelta]{ToolName: "approval", Run: func(_ context.Context, call prebuilt.ToolCall, runtime prebuilt.ToolRuntime[toolAgentState]) (prebuilt.ToolResult[toolAgentDelta], error) {
		ready <- struct{}{}
		<-release
		attempt := oneCalls.Add(1)
		if call.ID == "two" {
			attempt = twoCalls.Add(1)
			oneCalls.Add(-1)
		}
		// Reverse arrival order on replay to prove IDs remain associated with
		// call order rather than goroutine scheduling.
		if attempt == 1 && call.ID == "two" || attempt > 1 && call.ID == "one" {
			time.Sleep(2 * time.Millisecond)
		}
		answer, err := graph.AwaitResume[string](runtime.Graph, call.ID+"?")
		if err != nil {
			return prebuilt.ToolResult[toolAgentDelta]{}, err
		}
		return prebuilt.TextResult[toolAgentDelta](answer), nil
	}}
	node, _ := prebuilt.NewToolNode([]prebuilt.Tool[toolAgentState, toolAgentDelta]{approval}, toolAdapter(), prebuilt.ToolNodeConfig{MaxConcurrency: 2})
	saver := checkpointmemory.NewSaver()
	compiled := toolGraph(t, node, graph.WithPersistence(graph.PersistenceConfig[toolAgentState, toolAgentDelta]{
		Saver:      saver,
		StateCodec: checkpoint.MustJSONCodec[toolAgentState]("tests.parallel-tool-state", 1),
		DeltaCodec: checkpoint.MustJSONCodec[toolAgentDelta]("tests.parallel-tool-delta", 1),
	}))
	config := graph.RunConfig{ThreadID: "parallel-tool-interrupt"}
	initialDone := make(chan error, 1)
	go func() {
		_, err := compiled.Invoke(context.Background(), toolAgentState{Calls: []prebuilt.ToolCall{
			{ID: "one", Name: "approval"}, {ID: "two", Name: "approval"},
		}}, config)
		initialDone <- err
	}()
	<-ready
	<-ready
	close(release)
	if err := <-initialDone; !errors.Is(err, graph.ErrGraphInterrupt) {
		t.Fatalf("initial err=%v", err)
	}
	paused, err := compiled.GetState(context.Background(), config)
	if err != nil || len(paused.Interrupts) != 2 {
		t.Fatalf("paused interrupts=%+v err=%v", paused.Interrupts, err)
	}
	resumeValues := make(map[string]any, 2)
	for _, interrupt := range paused.Interrupts {
		prompt, decodeErr := graph.DecodeInterrupt[string](interrupt)
		if decodeErr != nil {
			t.Fatal(decodeErr)
		}
		resumeValues[interrupt.ID] = "answer:" + prompt
	}
	command, err := graph.ResumeByID(resumeValues)
	if err != nil {
		t.Fatal(err)
	}
	result, err := compiled.Resume(context.Background(), config, command)
	if err != nil || len(result.Messages) != 2 || result.Messages[0].Content != "answer:one?" || result.Messages[1].Content != "answer:two?" {
		t.Fatalf("result=%+v err=%v", result, err)
	}
}

func TestParallelToolsEachResumeSequentialInterruptsByID(t *testing.T) {
	approval := prebuilt.ToolFunc[toolAgentState, toolAgentDelta]{ToolName: "approval", Run: func(_ context.Context, call prebuilt.ToolCall, runtime prebuilt.ToolRuntime[toolAgentState]) (prebuilt.ToolResult[toolAgentDelta], error) {
		first, err := graph.AwaitResume[string](runtime.Graph, call.ID+":first")
		if err != nil {
			return prebuilt.ToolResult[toolAgentDelta]{}, err
		}
		second, err := graph.AwaitResume[string](runtime.Graph, call.ID+":second")
		if err != nil {
			return prebuilt.ToolResult[toolAgentDelta]{}, err
		}
		return prebuilt.TextResult[toolAgentDelta](first + "+" + second), nil
	}}
	node, err := prebuilt.NewToolNode(
		[]prebuilt.Tool[toolAgentState, toolAgentDelta]{approval},
		toolAdapter(),
		prebuilt.ToolNodeConfig{MaxConcurrency: 2},
	)
	if err != nil {
		t.Fatal(err)
	}
	compiled := toolGraph(t, node, graph.WithPersistence(graph.PersistenceConfig[toolAgentState, toolAgentDelta]{
		Saver:      checkpointmemory.NewSaver(),
		StateCodec: checkpoint.MustJSONCodec[toolAgentState]("tests.sequential-tool-state", 1),
		DeltaCodec: checkpoint.MustJSONCodec[toolAgentDelta]("tests.sequential-tool-delta", 1),
	}))
	config := graph.RunConfig{ThreadID: "parallel-sequential-tool-interrupts"}
	if _, err := compiled.Invoke(context.Background(), toolAgentState{Calls: []prebuilt.ToolCall{
		{ID: "one", Name: "approval"}, {ID: "two", Name: "approval"},
	}}, config); !errors.Is(err, graph.ErrGraphInterrupt) {
		t.Fatalf("initial err=%v", err)
	}

	pending := func(want ...string) map[string]graph.Interrupt {
		t.Helper()
		paused, stateErr := compiled.GetState(context.Background(), config)
		if stateErr != nil {
			t.Fatal(stateErr)
		}
		got := make(map[string]graph.Interrupt, len(paused.Interrupts))
		for _, interrupt := range paused.Interrupts {
			prompt, decodeErr := graph.DecodeInterrupt[string](interrupt)
			if decodeErr != nil {
				t.Fatal(decodeErr)
			}
			got[prompt] = interrupt
		}
		if len(got) != len(want) {
			t.Fatalf("interrupt prompts=%v want=%v", got, want)
		}
		for _, prompt := range want {
			if _, ok := got[prompt]; !ok {
				t.Fatalf("interrupt prompts=%v want=%v", got, want)
			}
		}
		return got
	}
	resumeSome := func(interrupts map[string]graph.Interrupt, prompts ...string) {
		t.Helper()
		values := make(map[string]any, len(prompts))
		for _, prompt := range prompts {
			values[interrupts[prompt].ID] = "answer:" + prompt
		}
		command, commandErr := graph.ResumeByID(values)
		if commandErr != nil {
			t.Fatal(commandErr)
		}
		if _, resumeErr := compiled.Resume(context.Background(), config, command); !errors.Is(resumeErr, graph.ErrGraphInterrupt) {
			t.Fatalf("partial resume err=%v", resumeErr)
		}
	}

	first := pending("one:first", "two:first")
	resumeSome(first, "one:first")
	mixed := pending("two:first", "one:second")
	resumeSome(mixed, "two:first")
	second := pending("one:second", "two:second")
	secondValues := map[string]any{
		second["one:second"].ID: "answer:one:second",
		second["two:second"].ID: "answer:two:second",
	}
	secondResume, err := graph.ResumeByID(secondValues)
	if err != nil {
		t.Fatal(err)
	}
	result, err := compiled.Resume(context.Background(), config, secondResume)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Messages) != 2 ||
		result.Messages[0].Content != "answer:one:first+answer:one:second" ||
		result.Messages[1].Content != "answer:two:first+answer:two:second" {
		t.Fatalf("result=%+v", result)
	}
}

func TestToolNodeValidationAndToolsCondition(t *testing.T) {
	tool := prebuilt.ToolFunc[toolAgentState, toolAgentDelta]{ToolName: "echo", Run: func(context.Context, prebuilt.ToolCall, prebuilt.ToolRuntime[toolAgentState]) (prebuilt.ToolResult[toolAgentDelta], error) {
		return prebuilt.TextResult[toolAgentDelta]("ok"), nil
	}}
	if _, err := prebuilt.NewToolNode([]prebuilt.Tool[toolAgentState, toolAgentDelta]{tool, tool}, toolAdapter(), prebuilt.ToolNodeConfig{}); err == nil {
		t.Fatal("duplicate tool was accepted")
	}
	node, _ := prebuilt.NewToolNode([]prebuilt.Tool[toolAgentState, toolAgentDelta]{tool}, toolAdapter(), prebuilt.ToolNodeConfig{})
	_, err := toolGraph(t, node).Invoke(context.Background(), toolAgentState{Calls: []prebuilt.ToolCall{{ID: "same", Name: "echo"}, {ID: "same", Name: "echo"}}}, graph.RunConfig{})
	if err == nil {
		t.Fatal("duplicate tool call ID was accepted")
	}
	if got := prebuilt.ToolsCondition([]prebuilt.ToolCall{{ID: "1"}}, "tools"); got != "tools" {
		t.Fatalf("tools condition=%q", got)
	}
	if got := prebuilt.ToolsCondition(nil, "tools"); got != graph.END {
		t.Fatalf("empty tools condition=%q", got)
	}
}

func TestToolValueResultUsesStableJSON(t *testing.T) {
	result, err := prebuilt.ValueResult[toolAgentDelta](map[string]int{"value": 3})
	if err != nil || result.Message == nil || result.Message.Content != `{"value":3}` {
		t.Fatalf("result=%+v err=%v", result, err)
	}
	if !reflect.DeepEqual(prebuilt.LastAssistantToolCalls([]prebuilt.AssistantMessage{{ToolCalls: []prebuilt.ToolCall{{ID: "1", Name: "x"}}}}), []prebuilt.ToolCall{{ID: "1", Name: "x"}}) {
		t.Fatal("assistant call extraction mismatch")
	}
}
