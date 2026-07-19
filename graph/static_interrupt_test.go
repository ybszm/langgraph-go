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

func TestInterruptBeforePausesWholeStepAndContinueDoesNotNeedValue(t *testing.T) {
	builder := graph.NewStateGraph(customReducer)
	var aCalls, bCalls atomic.Int32
	_ = builder.AddNode("a", func(_ context.Context, _ customState, _ graph.Runtime) (graph.Command[customDelta], error) {
		aCalls.Add(1)
		return graph.Update(customDelta{Add: 1}), nil
	})
	_ = builder.AddNode("b", func(_ context.Context, _ customState, _ graph.Runtime) (graph.Command[customDelta], error) {
		bCalls.Add(1)
		return graph.Update(customDelta{Add: 2}), nil
	})
	_ = builder.AddEdge(graph.START, "a")
	_ = builder.AddEdge(graph.START, "b")
	_ = builder.AddEdge("a", graph.END)
	_ = builder.AddEdge("b", graph.END)
	saver := memory.NewSaver()
	compiled, err := builder.Compile(
		graph.WithPersistence(graph.PersistenceConfig[customState, customDelta]{
			Saver:      saver,
			StateCodec: checkpoint.MustJSONCodec[customState]("tests.static-state", 1),
			DeltaCodec: checkpoint.MustJSONCodec[customDelta]("tests.static-delta", 1),
		}),
		graph.WithInterruptBefore[customState, customDelta]("a"),
	)
	if err != nil {
		t.Fatal(err)
	}
	config := graph.RunConfig{ThreadID: "static-before"}
	state, err := compiled.Invoke(context.Background(), customState{}, config)
	if !errors.Is(err, graph.ErrGraphInterrupt) || state.Count != 0 || aCalls.Load() != 0 || bCalls.Load() != 0 {
		t.Fatalf("pause state=%+v calls=%d/%d err=%v", state, aCalls.Load(), bCalls.Load(), err)
	}
	snapshot, err := compiled.GetState(context.Background(), config)
	if err != nil || len(snapshot.Next) != 2 || len(snapshot.Interrupts) != 0 {
		t.Fatalf("static snapshot=%+v err=%v", snapshot, err)
	}
	final, err := compiled.Resume(context.Background(), config, graph.Continue())
	if err != nil || final.Count != 3 || aCalls.Load() != 1 || bCalls.Load() != 1 {
		t.Fatalf("continued=%+v calls=%d/%d err=%v", final, aCalls.Load(), bCalls.Load(), err)
	}
}

func TestInterruptBeforeReFiresOnHistoricalReplay(t *testing.T) {
	builder := graph.NewStateGraph(customReducer)
	_ = builder.AddNode("node", func(_ context.Context, _ customState, _ graph.Runtime) (graph.Command[customDelta], error) {
		return graph.Update(customDelta{Add: 1}), nil
	})
	_ = builder.AddEdge(graph.START, "node")
	_ = builder.AddEdge("node", graph.END)
	compiled, err := builder.Compile(
		graph.WithPersistence(graph.PersistenceConfig[customState, customDelta]{
			Saver:      memory.NewSaver(),
			StateCodec: checkpoint.MustJSONCodec[customState]("tests.static-replay-state", 1),
			DeltaCodec: checkpoint.MustJSONCodec[customDelta]("tests.static-replay-delta", 1),
		}),
		graph.WithInterruptBefore[customState, customDelta]("node"),
	)
	if err != nil {
		t.Fatal(err)
	}
	config := graph.RunConfig{ThreadID: "static-replay"}
	_, _ = compiled.Invoke(context.Background(), customState{}, config)
	paused, _ := compiled.GetState(context.Background(), config)
	if _, err := compiled.Resume(context.Background(), config, graph.Continue()); err != nil {
		t.Fatal(err)
	}
	_, err = compiled.Invoke(context.Background(), customState{}, graph.RunConfig{
		ThreadID: config.ThreadID, CheckpointID: paused.Config.CheckpointID,
	})
	if !errors.Is(err, graph.ErrGraphInterrupt) {
		t.Fatalf("historical static interrupt err=%v", err)
	}
}

func TestInterruptAfterCommitsResultAndDoesNotRerunNode(t *testing.T) {
	builder := graph.NewStateGraph(customReducer)
	var aCalls, bCalls atomic.Int32
	_ = builder.AddNode("a", func(_ context.Context, _ customState, _ graph.Runtime) (graph.Command[customDelta], error) {
		aCalls.Add(1)
		return graph.Update(customDelta{Add: 1}), nil
	})
	_ = builder.AddNode("b", func(_ context.Context, _ customState, _ graph.Runtime) (graph.Command[customDelta], error) {
		bCalls.Add(1)
		return graph.Update(customDelta{Add: 2}), nil
	})
	_ = builder.AddEdge(graph.START, "a")
	_ = builder.AddEdge("a", "b")
	_ = builder.AddEdge("b", graph.END)
	compiled, err := builder.Compile(
		graph.WithPersistence(graph.PersistenceConfig[customState, customDelta]{
			Saver:      memory.NewSaver(),
			StateCodec: checkpoint.MustJSONCodec[customState]("tests.static-after-state", 1),
			DeltaCodec: checkpoint.MustJSONCodec[customDelta]("tests.static-after-delta", 1),
		}),
		graph.WithInterruptAfter[customState, customDelta]("a"),
	)
	if err != nil {
		t.Fatal(err)
	}
	config := graph.RunConfig{ThreadID: "static-after"}
	paused, err := compiled.Invoke(context.Background(), customState{}, config)
	if !errors.Is(err, graph.ErrGraphInterrupt) || paused.Count != 1 || aCalls.Load() != 1 || bCalls.Load() != 0 {
		t.Fatalf("paused=%+v calls=%d/%d err=%v", paused, aCalls.Load(), bCalls.Load(), err)
	}
	snapshot, _ := compiled.GetState(context.Background(), config)
	if snapshot.Values.Count != 1 || len(snapshot.Next) != 1 || snapshot.Next[0] != "b" {
		t.Fatalf("after snapshot=%+v", snapshot)
	}
	final, err := compiled.Resume(context.Background(), config, graph.Continue())
	if err != nil || final.Count != 3 || aCalls.Load() != 1 || bCalls.Load() != 1 {
		t.Fatalf("final=%+v calls=%d/%d err=%v", final, aCalls.Load(), bCalls.Load(), err)
	}
}

func TestInterruptBeforeCompileValidation(t *testing.T) {
	builder := graph.NewStateGraph(customReducer)
	_ = builder.AddNode("node", func(context.Context, customState, graph.Runtime) (graph.Command[customDelta], error) {
		return graph.NoCommand[customDelta](), nil
	})
	_ = builder.AddEdge(graph.START, "node")
	_ = builder.AddEdge("node", graph.END)
	if _, err := builder.Compile(graph.WithInterruptBefore[customState, customDelta]("node")); !errors.Is(err, graph.ErrInvalidGraph) {
		t.Fatalf("nonpersistent static interrupt err=%v", err)
	}
	if _, err := builder.Compile(
		graph.WithPersistence(graph.PersistenceConfig[customState, customDelta]{
			Saver:      memory.NewSaver(),
			StateCodec: checkpoint.MustJSONCodec[customState]("tests.static-validation-state", 1),
			DeltaCodec: checkpoint.MustJSONCodec[customDelta]("tests.static-validation-delta", 1),
		}),
		graph.WithInterruptBefore[customState, customDelta]("missing"),
	); !errors.Is(err, graph.ErrUnknownNode) {
		t.Fatalf("unknown static interrupt node err=%v", err)
	}
}
