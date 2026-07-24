package compat_test

import (
	"context"
	"errors"
	"testing"

	"github.com/ybszm/langgraph-go/checkpoint"
	"github.com/ybszm/langgraph-go/checkpoint/memory"
	"github.com/ybszm/langgraph-go/compat"
	"github.com/ybszm/langgraph-go/graph"
)

func init() {
	compat.Register(compat.Scenario{
		ID:          "graph.basic_linear",
		Area:        "Typed StateGraph",
		Status:      compat.StatusSupported,
		Description: "START → node → END invoke reduces state",
		Run:         testBasicLinear,
	})
	compat.Register(compat.Scenario{
		ID:          "graph.allow_unreachable",
		Area:        "Typed StateGraph",
		Status:      compat.StatusPartial,
		Description: "WithAllowUnreachableNodes compiles offline helper nodes",
		Run:         testAllowUnreachable,
	})
	compat.Register(compat.Scenario{
		ID:          "graph.reject_unreachable_default",
		Area:        "Typed StateGraph",
		Status:      compat.StatusSupported,
		Description: "Default compile rejects unreachable nodes (stricter than Python)",
		Run:         testRejectUnreachable,
	})
	compat.Register(compat.Scenario{
		ID:          "graph.deferred_after_finish",
		Area:        "Typed StateGraph",
		Status:      compat.StatusSupported,
		Description: "Deferred node observes updates from ordinary branches before it runs",
		Run:         testDeferredAfterFinish,
	})
	compat.Register(compat.Scenario{
		ID:          "persistence.durability_modes",
		Area:        "Pregel runtime",
		Status:      compat.StatusSupported,
		Description: "Sync and async retain step history while exit publishes only the final boundary",
		Run:         testDurabilityModes,
	})
}

func TestCompatibilityHarness(t *testing.T) {
	compat.RunAll(t)
}

type counterState struct{ N int }
type counterDelta struct{ Add int }

func counterReduce(_ context.Context, state counterState, updates []counterDelta) (counterState, error) {
	for _, update := range updates {
		state.N += update.Add
	}
	return state, nil
}

func testBasicLinear(t *testing.T) {
	t.Helper()
	builder := graph.NewStateGraph(counterReduce)
	if err := builder.AddNode("inc", func(context.Context, counterState, graph.Runtime) (graph.Command[counterDelta], error) {
		return graph.Update(counterDelta{Add: 1}), nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := builder.AddEdge(graph.START, "inc"); err != nil {
		t.Fatal(err)
	}
	if err := builder.AddEdge("inc", graph.END); err != nil {
		t.Fatal(err)
	}
	compiled, err := builder.Compile()
	if err != nil {
		t.Fatal(err)
	}
	out, err := compiled.Invoke(context.Background(), counterState{}, graph.RunConfig{})
	if err != nil {
		t.Fatal(err)
	}
	if out.N != 1 {
		t.Fatalf("N=%d", out.N)
	}
}

func testAllowUnreachable(t *testing.T) {
	t.Helper()
	builder := graph.NewStateGraph(counterReduce)
	if err := builder.AddNode("online", func(context.Context, counterState, graph.Runtime) (graph.Command[counterDelta], error) {
		return graph.Update(counterDelta{Add: 2}), nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := builder.AddNode("offline_helper", func(context.Context, counterState, graph.Runtime) (graph.Command[counterDelta], error) {
		return graph.Update(counterDelta{Add: 99}), nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := builder.AddEdge(graph.START, "online"); err != nil {
		t.Fatal(err)
	}
	if err := builder.AddEdge("online", graph.END); err != nil {
		t.Fatal(err)
	}
	compiled, err := builder.Compile(graph.WithAllowUnreachableNodes[counterState, counterDelta]())
	if err != nil {
		t.Fatal(err)
	}
	out, err := compiled.Invoke(context.Background(), counterState{}, graph.RunConfig{})
	if err != nil {
		t.Fatal(err)
	}
	if out.N != 2 {
		t.Fatalf("N=%d want 2 (offline not scheduled)", out.N)
	}
}

func testRejectUnreachable(t *testing.T) {
	t.Helper()
	builder := graph.NewStateGraph(counterReduce)
	if err := builder.AddNode("online", func(context.Context, counterState, graph.Runtime) (graph.Command[counterDelta], error) {
		return graph.NoCommand[counterDelta](), nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := builder.AddNode("orphan", func(context.Context, counterState, graph.Runtime) (graph.Command[counterDelta], error) {
		return graph.NoCommand[counterDelta](), nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := builder.AddEdge(graph.START, "online"); err != nil {
		t.Fatal(err)
	}
	if err := builder.AddEdge("online", graph.END); err != nil {
		t.Fatal(err)
	}
	_, err := builder.Compile()
	if err == nil || !errors.Is(err, graph.ErrInvalidGraph) {
		t.Fatalf("expected unreachable compile error, got %v", err)
	}
}

func testDeferredAfterFinish(t *testing.T) {
	t.Helper()
	builder := graph.NewStateGraph(counterReduce)
	for _, item := range []struct {
		node graph.NodeID
		add  int
	}{
		{node: "root", add: 1},
		{node: "early", add: 10},
		{node: "other", add: 20},
		{node: "tail", add: 100},
	} {
		item := item
		if err := builder.AddNode(item.node, func(context.Context, counterState, graph.Runtime) (graph.Command[counterDelta], error) {
			return graph.Update(counterDelta{Add: item.add}), nil
		}); err != nil {
			t.Fatal(err)
		}
	}
	if err := builder.AddNode("final", func(_ context.Context, state counterState, _ graph.Runtime) (graph.Command[counterDelta], error) {
		return graph.Update(counterDelta{Add: state.N}), nil
	}, graph.WithDeferred()); err != nil {
		t.Fatal(err)
	}
	for _, edge := range [][2]graph.NodeID{
		{graph.START, "root"},
		{"root", "early"},
		{"root", "other"},
		{"early", "final"},
		{"early", "tail"},
		{"other", graph.END},
		{"tail", graph.END},
		{"final", graph.END},
	} {
		if err := builder.AddEdge(edge[0], edge[1]); err != nil {
			t.Fatal(err)
		}
	}
	compiled, err := builder.Compile()
	if err != nil {
		t.Fatal(err)
	}
	out, err := compiled.Invoke(context.Background(), counterState{}, graph.RunConfig{})
	if err != nil {
		t.Fatal(err)
	}
	if out.N != 262 {
		t.Fatalf("N=%d, want deferred final to observe 131 and produce 262", out.N)
	}
}

func testDurabilityModes(t *testing.T) {
	t.Helper()
	for _, test := range []struct {
		mode        graph.Durability
		checkpoints int
	}{
		{mode: graph.DurabilitySync, checkpoints: 2},
		{mode: graph.DurabilityAsync, checkpoints: 2},
		{mode: graph.DurabilityExit, checkpoints: 1},
	} {
		t.Run(string(test.mode), func(t *testing.T) {
			saver := memory.NewSaver()
			builder := graph.NewStateGraph(counterReduce)
			if err := builder.AddNode("inc", func(context.Context, counterState, graph.Runtime) (graph.Command[counterDelta], error) {
				return graph.Update(counterDelta{Add: 1}), nil
			}); err != nil {
				t.Fatal(err)
			}
			if err := builder.AddEdge(graph.START, "inc"); err != nil {
				t.Fatal(err)
			}
			if err := builder.AddEdge("inc", graph.END); err != nil {
				t.Fatal(err)
			}
			compiled, err := builder.Compile(graph.WithPersistence(
				graph.PersistenceConfig[counterState, counterDelta]{
					Saver:      saver,
					StateCodec: checkpoint.MustJSONCodec[counterState]("compat/counter-state", 1),
					DeltaCodec: checkpoint.MustJSONCodec[counterDelta]("compat/counter-delta", 1),
				},
			))
			if err != nil {
				t.Fatal(err)
			}
			config := graph.RunConfig{
				ThreadID:   "compat-durability-" + string(test.mode),
				Durability: test.mode,
			}
			out, err := compiled.Invoke(context.Background(), counterState{}, config)
			if err != nil {
				t.Fatal(err)
			}
			if out.N != 1 {
				t.Fatalf("N=%d", out.N)
			}
			history, err := compiled.GetStateHistory(
				context.Background(), config, graph.StateHistoryOptions{},
			)
			if err != nil {
				t.Fatal(err)
			}
			if len(history) != test.checkpoints {
				t.Fatalf("history=%d, want %d", len(history), test.checkpoints)
			}
		})
	}
}
