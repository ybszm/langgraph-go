package graph_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/ybszm/langgraph-go/checkpoint"
	"github.com/ybszm/langgraph-go/checkpoint/memory"
	"github.com/ybszm/langgraph-go/graph"
)

type observedSaver struct {
	checkpoint.Saver
	calls atomic.Int32
}

func (s *observedSaver) GetTuple(ctx context.Context, config checkpoint.Config) (checkpoint.Tuple, bool, error) {
	s.calls.Add(1)
	return s.Saver.GetTuple(ctx, config)
}

func (s *observedSaver) Put(
	ctx context.Context,
	parent checkpoint.Config,
	value checkpoint.Checkpoint,
	metadata checkpoint.Metadata,
	versions map[string]string,
) (checkpoint.Config, error) {
	s.calls.Add(1)
	return s.Saver.Put(ctx, parent, value, metadata, versions)
}

func (s *observedSaver) PutWrites(
	ctx context.Context,
	config checkpoint.Config,
	writes []checkpoint.PendingWrite,
) error {
	s.calls.Add(1)
	return s.Saver.PutWrites(ctx, config, writes)
}

func TestWithDeferredIsRecorded(t *testing.T) {
	builder := graph.NewStateGraph(testReducer)
	if err := builder.AddNode("ready", func(context.Context, testState, graph.Runtime) (graph.Command[testDelta], error) {
		return graph.Update(testDelta{Add: 1}), nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := builder.AddNode("later", func(context.Context, testState, graph.Runtime) (graph.Command[testDelta], error) {
		return graph.Update(testDelta{Add: 2}), nil
	}, graph.WithDeferred()); err != nil {
		t.Fatal(err)
	}
	if err := builder.AddEdge(graph.START, "ready"); err != nil {
		t.Fatal(err)
	}
	if err := builder.AddEdge("ready", "later"); err != nil {
		t.Fatal(err)
	}
	if err := builder.AddEdge("later", graph.END); err != nil {
		t.Fatal(err)
	}
	compiled, err := builder.Compile()
	if err != nil {
		t.Fatal(err)
	}
	if compiled.IsDeferred("later") != true || compiled.IsDeferred("ready") {
		t.Fatalf("deferred flags wrong")
	}
	out, err := compiled.Invoke(context.Background(), testState{}, graph.RunConfig{})
	if err != nil {
		t.Fatal(err)
	}
	if out.Total != 3 {
		t.Fatalf("total=%d", out.Total)
	}
}

func TestDeferredNodeRunsAfterOrdinaryBranchesFinish(t *testing.T) {
	builder := graph.NewStateGraph(testReducer)
	addNode(t, builder, "root", func(context.Context, testState, graph.Runtime) (graph.Command[testDelta], error) {
		return graph.Update(testDelta{Add: 1, Label: "root"}), nil
	})
	addNode(t, builder, "early", func(context.Context, testState, graph.Runtime) (graph.Command[testDelta], error) {
		return graph.Update(testDelta{Add: 10, Label: "early"}), nil
	})
	addNode(t, builder, "late", func(context.Context, testState, graph.Runtime) (graph.Command[testDelta], error) {
		return graph.Update(testDelta{Add: 20, Label: "late"}), nil
	})
	addNode(t, builder, "tail", func(context.Context, testState, graph.Runtime) (graph.Command[testDelta], error) {
		return graph.Update(testDelta{Add: 100, Label: "tail"}), nil
	})
	if err := builder.AddNode("final", func(_ context.Context, state testState, _ graph.Runtime) (graph.Command[testDelta], error) {
		return graph.Update(testDelta{Add: state.Total, Label: "final"}), nil
	}, graph.WithDeferred()); err != nil {
		t.Fatal(err)
	}
	addEdge(t, builder, graph.START, "root")
	addEdge(t, builder, "root", "early")
	addEdge(t, builder, "root", "late")
	addEdge(t, builder, "early", "final")
	addEdge(t, builder, "early", "tail")
	addEdge(t, builder, "late", graph.END)
	addEdge(t, builder, "tail", graph.END)
	addEdge(t, builder, "final", graph.END)

	result, err := compileGraph(t, builder).Invoke(context.Background(), testState{}, graph.RunConfig{})
	if err != nil {
		t.Fatal(err)
	}
	if result.Total != 262 || result.Path[len(result.Path)-1] != "final" {
		t.Fatalf("result = %#v, want deferred final after tail", result)
	}
}

func TestDeferredNodeCollapsesTriggersAcrossSuperSteps(t *testing.T) {
	builder := graph.NewStateGraph(testReducer)
	for _, item := range []struct {
		node graph.NodeID
		add  int
	}{
		{node: "root", add: 1},
		{node: "a", add: 10},
		{node: "b", add: 20},
		{node: "c", add: 100},
	} {
		item := item
		addNode(t, builder, item.node, func(context.Context, testState, graph.Runtime) (graph.Command[testDelta], error) {
			return graph.Update(testDelta{Add: item.add, Label: string(item.node)}), nil
		})
	}
	var calls atomic.Int32
	if err := builder.AddNode("final", func(_ context.Context, state testState, _ graph.Runtime) (graph.Command[testDelta], error) {
		calls.Add(1)
		return graph.Update(testDelta{Add: state.Total, Label: "final"}), nil
	}, graph.WithDeferred()); err != nil {
		t.Fatal(err)
	}
	addEdge(t, builder, graph.START, "root")
	addEdge(t, builder, "root", "a")
	addEdge(t, builder, "root", "b")
	addEdge(t, builder, "a", "final")
	addEdge(t, builder, "b", "c")
	addEdge(t, builder, "c", "final")
	addEdge(t, builder, "final", graph.END)

	result, err := compileGraph(t, builder).Invoke(context.Background(), testState{}, graph.RunConfig{})
	if err != nil {
		t.Fatal(err)
	}
	if result.Total != 262 || calls.Load() != 1 {
		t.Fatalf("result = %#v calls = %d, want one final execution", result, calls.Load())
	}
}

func TestDeferredWaitingEdgeRunsAfterRemainingWork(t *testing.T) {
	builder := graph.NewStateGraph(testReducer)
	for _, item := range []struct {
		node graph.NodeID
		add  int
	}{
		{node: "left", add: 1},
		{node: "middle", add: 10},
		{node: "right", add: 100},
		{node: "tail", add: 1000},
	} {
		item := item
		addNode(t, builder, item.node, func(context.Context, testState, graph.Runtime) (graph.Command[testDelta], error) {
			return graph.Update(testDelta{Add: item.add, Label: string(item.node)}), nil
		})
	}
	if err := builder.AddNode("final", func(_ context.Context, state testState, _ graph.Runtime) (graph.Command[testDelta], error) {
		return graph.Update(testDelta{Add: state.Total, Label: "final"}), nil
	}, graph.WithDeferred()); err != nil {
		t.Fatal(err)
	}
	addEdge(t, builder, graph.START, "left")
	addEdge(t, builder, graph.START, "middle")
	addEdge(t, builder, "middle", "right")
	if err := builder.AddWaitingEdge([]graph.NodeID{"left", "right"}, "final"); err != nil {
		t.Fatal(err)
	}
	addEdge(t, builder, "right", "tail")
	addEdge(t, builder, "tail", graph.END)
	addEdge(t, builder, "final", graph.END)

	result, err := compileGraph(t, builder).Invoke(context.Background(), testState{}, graph.RunConfig{})
	if err != nil {
		t.Fatal(err)
	}
	if result.Total != 2222 || result.Path[len(result.Path)-1] != "final" {
		t.Fatalf("result = %#v, want deferred waiting target after tail", result)
	}
}

func TestDeferredMarkerDoesNotDelayDynamicSendTasks(t *testing.T) {
	builder := graph.NewStateGraph(testReducer)
	addNode(t, builder, "start", func(context.Context, testState, graph.Runtime) (graph.Command[testDelta], error) {
		return graph.Command[testDelta]{Sends: []graph.TaskSend{
			graph.SendTo("final", testState{}),
			graph.SendTo("tail", testState{}),
		}}, nil
	})
	if err := builder.AddNode("final", func(context.Context, testState, graph.Runtime) (graph.Command[testDelta], error) {
		return graph.Update(testDelta{Add: 1, Label: "final"}), nil
	}, graph.WithDeferred()); err != nil {
		t.Fatal(err)
	}
	addNode(t, builder, "tail", func(context.Context, testState, graph.Runtime) (graph.Command[testDelta], error) {
		return graph.Update(testDelta{Add: 10, Label: "tail"}), nil
	})
	addEdge(t, builder, graph.START, "start")
	if err := builder.AddCommandDestinations("start", "final", "tail"); err != nil {
		t.Fatal(err)
	}
	addEdge(t, builder, "final", graph.END)
	addEdge(t, builder, "tail", graph.END)

	result, err := compileGraph(t, builder).Invoke(context.Background(), testState{}, graph.RunConfig{})
	if err != nil {
		t.Fatal(err)
	}
	if result.Total != 11 || len(result.Path) != 2 ||
		result.Path[0] != "final" || result.Path[1] != "tail" {
		t.Fatalf("result = %#v, Send tasks must retain immediate PUSH semantics", result)
	}
}

func TestDeferredTriggerSurvivesPersistentInterrupt(t *testing.T) {
	builder := graph.NewStateGraph(testReducer)
	addNode(t, builder, "root", func(context.Context, testState, graph.Runtime) (graph.Command[testDelta], error) {
		return graph.Update(testDelta{Add: 1, Label: "root"}), nil
	})
	addNode(t, builder, "trigger", func(context.Context, testState, graph.Runtime) (graph.Command[testDelta], error) {
		return graph.Update(testDelta{Add: 10, Label: "trigger"}), nil
	})
	addNode(t, builder, "tail", func(context.Context, testState, graph.Runtime) (graph.Command[testDelta], error) {
		return graph.Update(testDelta{Add: 100, Label: "tail"}), nil
	})
	if err := builder.AddNode("final", func(_ context.Context, state testState, _ graph.Runtime) (graph.Command[testDelta], error) {
		return graph.Update(testDelta{Add: state.Total, Label: "final"}), nil
	}, graph.WithDeferred()); err != nil {
		t.Fatal(err)
	}
	addEdge(t, builder, graph.START, "root")
	addEdge(t, builder, "root", "trigger")
	addEdge(t, builder, "trigger", "final")
	addEdge(t, builder, "trigger", "tail")
	addEdge(t, builder, "tail", graph.END)
	addEdge(t, builder, "final", graph.END)

	compiled, err := builder.Compile(
		graph.WithPersistence(graph.PersistenceConfig[testState, testDelta]{
			Saver:      memory.NewSaver(),
			StateCodec: checkpoint.MustJSONCodec[testState]("tests.deferred-state", 1),
			DeltaCodec: checkpoint.MustJSONCodec[testDelta]("tests.deferred-delta", 1),
		}),
		graph.WithInterruptBefore[testState, testDelta]("tail"),
	)
	if err != nil {
		t.Fatal(err)
	}
	config := graph.RunConfig{ThreadID: "deferred-interrupt"}
	if _, err := compiled.Invoke(context.Background(), testState{}, config); !errors.Is(err, graph.ErrGraphInterrupt) {
		t.Fatalf("Invoke() err = %v, want graph interrupt", err)
	}
	snapshot, err := compiled.GetState(context.Background(), config)
	if err != nil {
		t.Fatal(err)
	}
	if len(snapshot.Next) != 1 || snapshot.Next[0] != "tail" {
		t.Fatalf("paused Next = %v, deferred node must remain parked", snapshot.Next)
	}
	result, err := compiled.Resume(context.Background(), config, graph.Continue())
	if err != nil {
		t.Fatal(err)
	}
	if result.Total != 222 || result.Path[len(result.Path)-1] != "final" {
		t.Fatalf("result = %#v, deferred trigger was not restored", result)
	}
}

func TestDurabilityRejectsUnknownMode(t *testing.T) {
	builder := graph.NewStateGraph(testReducer)
	if err := builder.AddNode("n", func(context.Context, testState, graph.Runtime) (graph.Command[testDelta], error) {
		return graph.NoCommand[testDelta](), nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := builder.AddEdge(graph.START, "n"); err != nil {
		t.Fatal(err)
	}
	if err := builder.AddEdge("n", graph.END); err != nil {
		t.Fatal(err)
	}
	compiled, err := builder.Compile()
	if err != nil {
		t.Fatal(err)
	}
	_, err = compiled.Invoke(context.Background(), testState{}, graph.RunConfig{Durability: graph.Durability("future")})
	if !errors.Is(err, graph.ErrInvalidRunConfig) {
		t.Fatalf("err=%v", err)
	}
}

func TestUnknownDurabilityHasNoPersistenceSideEffects(t *testing.T) {
	saver := &observedSaver{Saver: memory.NewSaver()}
	builder := graph.NewStateGraph(testReducer)
	addNode(t, builder, "n", func(context.Context, testState, graph.Runtime) (graph.Command[testDelta], error) {
		return graph.Update(testDelta{Add: 1}), nil
	})
	addEdge(t, builder, graph.START, "n")
	addEdge(t, builder, "n", graph.END)
	compiled := compilePersistentGraph(t, builder, saver)

	_, err := compiled.Invoke(context.Background(), testState{}, graph.RunConfig{
		ThreadID: "unknown-durability", Durability: graph.Durability("future"),
	})
	if !errors.Is(err, graph.ErrInvalidRunConfig) {
		t.Fatalf("err=%v", err)
	}
	if saver.calls.Load() != 0 {
		t.Fatalf("unknown durability made %d saver calls", saver.calls.Load())
	}
}
