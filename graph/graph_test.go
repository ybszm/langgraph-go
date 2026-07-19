package graph_test

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ybszm/langgraph-go/graph"
)

type testState struct {
	Total int
	Path  []string
}

type testDelta struct {
	Add   int
	Label string
}

func testReducer(
	ctx context.Context,
	current testState,
	updates []testDelta,
) (testState, error) {
	if err := ctx.Err(); err != nil {
		return current, err
	}
	next := testState{
		Total: current.Total,
		Path:  append([]string(nil), current.Path...),
	}
	for _, update := range updates {
		next.Total += update.Add
		if update.Label != "" {
			next.Path = append(next.Path, update.Label)
		}
	}
	return next, nil
}

func addNode(
	t *testing.T,
	builder *graph.StateGraph[testState, testDelta],
	id graph.NodeID,
	node graph.Node[testState, testDelta],
) {
	t.Helper()
	if err := builder.AddNode(id, node); err != nil {
		t.Fatalf("AddNode(%q): %v", id, err)
	}
}

func addEdge(
	t *testing.T,
	builder *graph.StateGraph[testState, testDelta],
	from graph.NodeID,
	to graph.NodeID,
) {
	t.Helper()
	if err := builder.AddEdge(from, to); err != nil {
		t.Fatalf("AddEdge(%q, %q): %v", from, to, err)
	}
}

func compileGraph(
	t *testing.T,
	builder *graph.StateGraph[testState, testDelta],
) *graph.CompiledGraph[testState, testDelta] {
	t.Helper()
	compiled, err := builder.Compile()
	if err != nil {
		t.Fatalf("Compile(): %v", err)
	}
	return compiled
}

func TestInvokeSequentialGraph(t *testing.T) {
	builder := graph.NewStateGraph(testReducer)
	addNode(t, builder, "first", func(
		_ context.Context,
		_ testState,
		_ graph.Runtime,
	) (graph.Command[testDelta], error) {
		return graph.Update(testDelta{Add: 1, Label: "first"}), nil
	})
	addNode(t, builder, "second", func(
		_ context.Context,
		state testState,
		_ graph.Runtime,
	) (graph.Command[testDelta], error) {
		if state.Total != 1 {
			return graph.NoCommand[testDelta](), fmt.Errorf(
				"second observed total %d, want 1",
				state.Total,
			)
		}
		return graph.Update(testDelta{Add: 2, Label: "second"}), nil
	})
	addEdge(t, builder, graph.START, "first")
	addEdge(t, builder, "first", "second")
	addEdge(t, builder, "second", graph.END)

	result, err := compileGraph(t, builder).Invoke(
		context.Background(),
		testState{},
		graph.RunConfig{},
	)
	if err != nil {
		t.Fatalf("Invoke(): %v", err)
	}
	if result.Total != 3 {
		t.Fatalf("total = %d, want 3", result.Total)
	}
	if want := []string{"first", "second"}; !reflect.DeepEqual(result.Path, want) {
		t.Fatalf("path = %#v, want %#v", result.Path, want)
	}
}

func TestParallelSuperStepUsesDeterministicTaskOrder(t *testing.T) {
	builder := graph.NewStateGraph(testReducer)
	started := make(chan string, 2)
	release := make(chan struct{})

	makeNode := func(label string) graph.Node[testState, testDelta] {
		return func(
			ctx context.Context,
			_ testState,
			_ graph.Runtime,
		) (graph.Command[testDelta], error) {
			started <- label
			select {
			case <-release:
				return graph.Update(testDelta{Label: label}), nil
			case <-ctx.Done():
				return graph.NoCommand[testDelta](), ctx.Err()
			}
		}
	}

	addNode(t, builder, "slow", makeNode("slow"))
	addNode(t, builder, "fast", makeNode("fast"))
	addEdge(t, builder, graph.START, "slow")
	addEdge(t, builder, graph.START, "fast")
	addEdge(t, builder, "slow", graph.END)
	addEdge(t, builder, "fast", graph.END)
	compiled := compileGraph(t, builder)

	type invokeResult struct {
		state testState
		err   error
	}
	done := make(chan invokeResult, 1)
	go func() {
		state, err := compiled.Invoke(context.Background(), testState{}, graph.RunConfig{})
		done <- invokeResult{state: state, err: err}
	}()

	for range 2 {
		select {
		case <-started:
		case <-time.After(time.Second):
			t.Fatal("parallel nodes did not start together")
		}
	}
	close(release)

	result := <-done
	if result.err != nil {
		t.Fatalf("Invoke(): %v", result.err)
	}
	if want := []string{"slow", "fast"}; !reflect.DeepEqual(result.state.Path, want) {
		t.Fatalf(
			"reducer order = %#v, want declared task order %#v",
			result.state.Path,
			want,
		)
	}
}

func TestConditionalLoopRoutesUsingReducedState(t *testing.T) {
	type loopState struct{ Count int }
	type loopDelta struct{ Increment int }

	reducer := func(
		_ context.Context,
		state loopState,
		updates []loopDelta,
	) (loopState, error) {
		for _, update := range updates {
			state.Count += update.Increment
		}
		return state, nil
	}
	builder := graph.NewStateGraph(reducer)
	if err := builder.AddNode("increment", func(
		_ context.Context,
		_ loopState,
		_ graph.Runtime,
	) (graph.Command[loopDelta], error) {
		return graph.Update(loopDelta{Increment: 1}), nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := builder.AddEdge(graph.START, "increment"); err != nil {
		t.Fatal(err)
	}
	if err := builder.AddConditionalEdges(
		"increment",
		func(_ context.Context, state loopState) ([]graph.NodeID, error) {
			if state.Count < 3 {
				return []graph.NodeID{"increment"}, nil
			}
			return []graph.NodeID{graph.END}, nil
		},
		"increment",
		graph.END,
	); err != nil {
		t.Fatal(err)
	}

	compiled, err := builder.Compile()
	if err != nil {
		t.Fatalf("Compile(): %v", err)
	}
	result, err := compiled.Invoke(context.Background(), loopState{}, graph.RunConfig{})
	if err != nil {
		t.Fatalf("Invoke(): %v", err)
	}
	if result.Count != 3 {
		t.Fatalf("count = %d, want 3", result.Count)
	}
}

func TestRecursionLimit(t *testing.T) {
	builder := graph.NewStateGraph(testReducer)
	addNode(t, builder, "loop", func(
		_ context.Context,
		_ testState,
		_ graph.Runtime,
	) (graph.Command[testDelta], error) {
		return graph.Update(testDelta{Add: 1}), nil
	})
	addEdge(t, builder, graph.START, "loop")
	addEdge(t, builder, "loop", "loop")
	addEdge(t, builder, "loop", graph.END)

	result, err := compileGraph(t, builder).Invoke(
		context.Background(),
		testState{},
		graph.RunConfig{RecursionLimit: 3},
	)
	if !errors.Is(err, graph.ErrRecursionLimit) {
		t.Fatalf("error = %v, want ErrRecursionLimit", err)
	}
	var recursion *graph.RecursionError
	if !errors.As(err, &recursion) || recursion.Limit != 3 || recursion.Step != 3 || recursion.Code() != "GRAPH_RECURSION_LIMIT" {
		t.Fatalf("recursion error = %#v", err)
	}
	if !strings.Contains(err.Error(), "Recursion limit of 3 reached without hitting a stop condition") ||
		!strings.Contains(err.Error(), "/errors/GRAPH_RECURSION_LIMIT") {
		t.Fatalf("recursion message = %q", err.Error())
	}
	if result.Total != 3 {
		t.Fatalf("committed total = %d, want 3", result.Total)
	}
}

func TestCancellationPropagatesThroughNodeError(t *testing.T) {
	builder := graph.NewStateGraph(testReducer)
	started := make(chan struct{})
	addNode(t, builder, "wait", func(
		ctx context.Context,
		_ testState,
		_ graph.Runtime,
	) (graph.Command[testDelta], error) {
		close(started)
		<-ctx.Done()
		return graph.NoCommand[testDelta](), ctx.Err()
	})
	addEdge(t, builder, graph.START, "wait")
	addEdge(t, builder, "wait", graph.END)
	compiled := compileGraph(t, builder)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := compiled.Invoke(ctx, testState{}, graph.RunConfig{})
		done <- err
	}()
	<-started
	cancel()

	err := <-done
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v, want context.Canceled", err)
	}
	var nodeErr *graph.NodeExecutionError
	if !errors.As(err, &nodeErr) {
		t.Fatalf("error = %T, want *NodeExecutionError", err)
	}
}

func TestNodePanicBecomesStructuredError(t *testing.T) {
	builder := graph.NewStateGraph(testReducer)
	addNode(t, builder, "panic", func(
		_ context.Context,
		_ testState,
		_ graph.Runtime,
	) (graph.Command[testDelta], error) {
		panic("boom")
	})
	addEdge(t, builder, graph.START, "panic")
	addEdge(t, builder, "panic", graph.END)

	_, err := compileGraph(t, builder).Invoke(
		context.Background(),
		testState{},
		graph.RunConfig{},
	)
	var panicErr *graph.NodePanicError
	if !errors.As(err, &panicErr) {
		t.Fatalf("error = %v, want NodePanicError", err)
	}
	if panicErr.Value != "boom" || len(panicErr.Stack) == 0 {
		t.Fatalf("unexpected panic detail: %#v", panicErr)
	}
}

func TestStreamEmitsInitialUpdatesValuesAndDone(t *testing.T) {
	builder := graph.NewStateGraph(testReducer)
	addNode(t, builder, "only", func(
		_ context.Context,
		_ testState,
		_ graph.Runtime,
	) (graph.Command[testDelta], error) {
		return graph.Update(testDelta{Add: 4, Label: "only"}), nil
	})
	addEdge(t, builder, graph.START, "only")
	addEdge(t, builder, "only", graph.END)

	events := compileGraph(t, builder).Stream(
		context.Background(),
		testState{},
		graph.RunConfig{},
	)
	var modes []graph.StreamMode
	var final testState
	for event := range events {
		if event.Err != nil {
			t.Fatalf("stream error: %v", event.Err)
		}
		modes = append(modes, event.Mode)
		if event.Mode == graph.StreamDone {
			final = event.State
		}
	}

	wantModes := []graph.StreamMode{
		graph.StreamValues,
		graph.StreamUpdates,
		graph.StreamValues,
		graph.StreamDone,
	}
	if !reflect.DeepEqual(modes, wantModes) {
		t.Fatalf("modes = %#v, want %#v", modes, wantModes)
	}
	if final.Total != 4 {
		t.Fatalf("final total = %d, want 4", final.Total)
	}
}

func TestCompileRejectsInvalidGraphs(t *testing.T) {
	node := func(
		_ context.Context,
		_ testState,
		_ graph.Runtime,
	) (graph.Command[testDelta], error) {
		return graph.NoCommand[testDelta](), nil
	}

	t.Run("duplicate node", func(t *testing.T) {
		builder := graph.NewStateGraph(testReducer)
		if err := builder.AddNode("node", node); err != nil {
			t.Fatal(err)
		}
		err := builder.AddNode("node", node)
		if !errors.Is(err, graph.ErrDuplicateNode) {
			t.Fatalf("error = %v, want ErrDuplicateNode", err)
		}
	})

	t.Run("unknown edge target", func(t *testing.T) {
		builder := graph.NewStateGraph(testReducer)
		addNode(t, builder, "known", node)
		addEdge(t, builder, graph.START, "known")
		addEdge(t, builder, "known", "missing")
		_, err := builder.Compile()
		if !errors.Is(err, graph.ErrUnknownNode) {
			t.Fatalf("error = %v, want ErrUnknownNode", err)
		}
	})

	t.Run("unreachable node", func(t *testing.T) {
		builder := graph.NewStateGraph(testReducer)
		addNode(t, builder, "reachable", node)
		addNode(t, builder, "orphan", node)
		addEdge(t, builder, graph.START, "reachable")
		addEdge(t, builder, "reachable", graph.END)
		_, err := builder.Compile()
		if !errors.Is(err, graph.ErrInvalidGraph) {
			t.Fatalf("error = %v, want ErrInvalidGraph", err)
		}
	})

	t.Run("end unreachable", func(t *testing.T) {
		builder := graph.NewStateGraph(testReducer)
		addNode(t, builder, "loop", node)
		addEdge(t, builder, graph.START, "loop")
		addEdge(t, builder, "loop", "loop")
		_, err := builder.Compile()
		if !errors.Is(err, graph.ErrInvalidGraph) {
			t.Fatalf("error = %v, want ErrInvalidGraph", err)
		}
	})
}

func TestRouterRejectsUndeclaredDestination(t *testing.T) {
	builder := graph.NewStateGraph(testReducer)
	addNode(t, builder, "route", func(
		_ context.Context,
		_ testState,
		_ graph.Runtime,
	) (graph.Command[testDelta], error) {
		return graph.NoCommand[testDelta](), nil
	})
	addEdge(t, builder, graph.START, "route")
	if err := builder.AddConditionalEdges(
		"route",
		func(context.Context, testState) ([]graph.NodeID, error) {
			return []graph.NodeID{"undeclared"}, nil
		},
		graph.END,
	); err != nil {
		t.Fatal(err)
	}

	_, err := compileGraph(t, builder).Invoke(
		context.Background(),
		testState{},
		graph.RunConfig{},
	)
	if !errors.Is(err, graph.ErrUnknownNode) {
		t.Fatalf("error = %v, want ErrUnknownNode", err)
	}
}

func TestExplicitGotoOverridesDeclaredEdges(t *testing.T) {
	builder := graph.NewStateGraph(testReducer)
	addNode(t, builder, "route", func(
		_ context.Context,
		_ testState,
		_ graph.Runtime,
	) (graph.Command[testDelta], error) {
		return graph.UpdateAndGoto(testDelta{Label: "route"}, "target"), nil
	})
	addNode(t, builder, "skipped", func(
		_ context.Context,
		_ testState,
		_ graph.Runtime,
	) (graph.Command[testDelta], error) {
		return graph.Update(testDelta{Label: "skipped"}), nil
	})
	addNode(t, builder, "target", func(
		_ context.Context,
		_ testState,
		_ graph.Runtime,
	) (graph.Command[testDelta], error) {
		return graph.Update(testDelta{Label: "target"}), nil
	})
	addEdge(t, builder, graph.START, "route")
	addEdge(t, builder, "route", "skipped")
	addEdge(t, builder, "skipped", graph.END)
	if err := builder.AddCommandDestinations("route", "target"); err != nil {
		t.Fatal(err)
	}
	addEdge(t, builder, "target", graph.END)

	result, err := compileGraph(t, builder).Invoke(
		context.Background(),
		testState{},
		graph.RunConfig{},
	)
	if err != nil {
		t.Fatalf("Invoke(): %v", err)
	}
	if want := []string{"route", "target"}; !reflect.DeepEqual(result.Path, want) {
		t.Fatalf("path = %#v, want %#v", result.Path, want)
	}
}

func TestStateClonerIsolatesParallelNodes(t *testing.T) {
	type mapState struct{ Values map[string]int }
	type mapDelta struct{}

	builder := graph.NewStateGraph(func(
		_ context.Context,
		state mapState,
		_ []mapDelta,
	) (mapState, error) {
		return state, nil
	})
	builder.SetStateCloner(func(state mapState) (mapState, error) {
		clone := mapState{Values: make(map[string]int, len(state.Values))}
		for key, value := range state.Values {
			clone.Values[key] = value
		}
		return clone, nil
	})

	var calls atomic.Int32
	makeNode := func(key string) graph.Node[mapState, mapDelta] {
		return func(
			_ context.Context,
			state mapState,
			_ graph.Runtime,
		) (graph.Command[mapDelta], error) {
			state.Values[key] = 1
			calls.Add(1)
			return graph.NoCommand[mapDelta](), nil
		}
	}
	if err := builder.AddNode("a", makeNode("a")); err != nil {
		t.Fatal(err)
	}
	if err := builder.AddNode("b", makeNode("b")); err != nil {
		t.Fatal(err)
	}
	for _, edge := range [][2]graph.NodeID{
		{graph.START, "a"},
		{graph.START, "b"},
		{"a", graph.END},
		{"b", graph.END},
	} {
		if err := builder.AddEdge(edge[0], edge[1]); err != nil {
			t.Fatal(err)
		}
	}
	compiled, err := builder.Compile()
	if err != nil {
		t.Fatal(err)
	}
	input := mapState{Values: map[string]int{"original": 1}}
	if _, err := compiled.Invoke(context.Background(), input, graph.RunConfig{}); err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 2 {
		t.Fatalf("calls = %d, want 2", calls.Load())
	}
	if want := map[string]int{"original": 1}; !reflect.DeepEqual(input.Values, want) {
		t.Fatalf("input was mutated: %#v", input.Values)
	}
}

func TestCompiledGraphIsIndependentFromBuilderMutation(t *testing.T) {
	builder := graph.NewStateGraph(testReducer)
	addNode(t, builder, "original", func(
		_ context.Context,
		_ testState,
		_ graph.Runtime,
	) (graph.Command[testDelta], error) {
		return graph.Update(testDelta{Label: "original"}), nil
	})
	addEdge(t, builder, graph.START, "original")
	addEdge(t, builder, "original", graph.END)
	compiled := compileGraph(t, builder)

	addNode(t, builder, "later", func(
		_ context.Context,
		_ testState,
		_ graph.Runtime,
	) (graph.Command[testDelta], error) {
		return graph.Update(testDelta{Label: "later"}), nil
	})
	addEdge(t, builder, "original", "later")

	result, err := compiled.Invoke(context.Background(), testState{}, graph.RunConfig{})
	if err != nil {
		t.Fatal(err)
	}
	if want := []string{"original"}; !reflect.DeepEqual(result.Path, want) {
		t.Fatalf("path = %#v, want %#v", result.Path, want)
	}
	if want := []graph.NodeID{"original"}; !reflect.DeepEqual(compiled.Nodes(), want) {
		t.Fatalf("compiled nodes = %#v, want %#v", compiled.Nodes(), want)
	}
}

func TestCommandOnlyPathCanReachEnd(t *testing.T) {
	builder := graph.NewStateGraph(testReducer)
	addNode(t, builder, "finish", func(
		_ context.Context,
		_ testState,
		_ graph.Runtime,
	) (graph.Command[testDelta], error) {
		return graph.UpdateAndGoto(testDelta{Add: 1}, graph.END), nil
	})
	addEdge(t, builder, graph.START, "finish")
	if err := builder.AddCommandDestinations("finish", graph.END); err != nil {
		t.Fatal(err)
	}

	compiled := compileGraph(t, builder)
	result, err := compiled.Invoke(context.Background(), testState{}, graph.RunConfig{})
	if err != nil {
		t.Fatal(err)
	}
	if result.Total != 1 {
		t.Fatalf("total = %d, want 1", result.Total)
	}
	if want := []graph.NodeID{graph.END}; !reflect.DeepEqual(
		compiled.CommandDestinations("finish"),
		want,
	) {
		t.Fatalf("command destinations do not match declaration")
	}
}

func TestStreamWithNilContextReturnsErrorEvent(t *testing.T) {
	builder := graph.NewStateGraph(testReducer)
	addNode(t, builder, "only", func(
		_ context.Context,
		_ testState,
		_ graph.Runtime,
	) (graph.Command[testDelta], error) {
		return graph.NoCommand[testDelta](), nil
	})
	addEdge(t, builder, graph.START, "only")
	addEdge(t, builder, "only", graph.END)

	events := compileGraph(t, builder).Stream(nil, testState{}, graph.RunConfig{})
	event, ok := <-events
	if !ok {
		t.Fatal("stream closed without an error event")
	}
	if event.Mode != graph.StreamError || !errors.Is(event.Err, graph.ErrInvalidRunConfig) {
		t.Fatalf("event = %#v, want invalid-run-config error", event)
	}
	if _, open := <-events; open {
		t.Fatal("stream emitted more than one event")
	}
}
