package graph_test

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/ybszm/langgraph-go/checkpoint"
	"github.com/ybszm/langgraph-go/checkpoint/memory"
	"github.com/ybszm/langgraph-go/graph"
)

func TestWaitingEdgeJoinsParallelSources(t *testing.T) {
	builder := graph.NewStateGraph(testReducer)
	for _, id := range []graph.NodeID{"left", "right"} {
		id := id
		addNode(t, builder, id, func(context.Context, testState, graph.Runtime) (graph.Command[testDelta], error) {
			return graph.Update(testDelta{Add: 1, Label: string(id)}), nil
		})
	}
	addNode(t, builder, "join", func(_ context.Context, state testState, _ graph.Runtime) (graph.Command[testDelta], error) {
		if state.Total != 2 {
			return graph.NoCommand[testDelta](), errors.New("join ran before both sources")
		}
		return graph.Update(testDelta{Label: "join"}), nil
	})
	addEdge(t, builder, graph.START, "left")
	addEdge(t, builder, graph.START, "right")
	if err := builder.AddWaitingEdge([]graph.NodeID{"left", "right"}, "join"); err != nil {
		t.Fatal(err)
	}
	addEdge(t, builder, "join", graph.END)
	result, err := compileGraph(t, builder).Invoke(context.Background(), testState{}, graph.RunConfig{})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(result.Path, []string{"left", "right", "join"}) {
		t.Fatalf("path=%#v", result.Path)
	}
}

func TestWaitingEdgePersistsPartialBarrierAndReplays(t *testing.T) {
	saver := memory.NewSaver()
	builder := graph.NewStateGraph(testReducer)
	for _, id := range []graph.NodeID{"fast", "middle", "slow", "join"} {
		id := id
		addNode(t, builder, id, func(context.Context, testState, graph.Runtime) (graph.Command[testDelta], error) {
			return graph.Update(testDelta{Label: string(id)}), nil
		})
	}
	addEdge(t, builder, graph.START, "fast")
	addEdge(t, builder, graph.START, "middle")
	addEdge(t, builder, "middle", "slow")
	if err := builder.AddWaitingEdge([]graph.NodeID{"fast", "slow"}, "join"); err != nil {
		t.Fatal(err)
	}
	addEdge(t, builder, "join", graph.END)
	compiled := compilePersistentGraph(t, builder, saver)
	config := graph.RunConfig{ThreadID: "waiting-persistence"}
	result, err := compiled.Invoke(context.Background(), testState{}, config)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(result.Path, []string{"fast", "middle", "slow", "join"}) {
		t.Fatalf("path=%#v", result.Path)
	}
	history, err := saver.List(context.Background(), checkpoint.ListOptions{Config: &checkpoint.Config{ThreadID: config.ThreadID}})
	if err != nil {
		t.Fatal(err)
	}
	var partial checkpoint.Tuple
	for _, tuple := range history {
		if tuple.Checkpoint.Step == 0 {
			partial = tuple
			break
		}
	}
	if !reflect.DeepEqual(partial.Checkpoint.Waiting["waiting:0"], []string{"fast"}) {
		t.Fatalf("partial waiting=%#v", partial.Checkpoint.Waiting)
	}
	replayed, err := compiled.Invoke(context.Background(), testState{}, graph.RunConfig{
		ThreadID: config.ThreadID, CheckpointID: partial.Config.CheckpointID,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(replayed.Path, result.Path) {
		t.Fatalf("replayed=%#v original=%#v", replayed, result)
	}
}

func TestWaitingEdgeResetsAcrossLoop(t *testing.T) {
	builder := graph.NewStateGraph(testReducer)
	for _, id := range []graph.NodeID{"a", "b"} {
		id := id
		addNode(t, builder, id, func(context.Context, testState, graph.Runtime) (graph.Command[testDelta], error) {
			return graph.Update(testDelta{Label: string(id)}), nil
		})
	}
	addNode(t, builder, "join", func(context.Context, testState, graph.Runtime) (graph.Command[testDelta], error) {
		return graph.Update(testDelta{Add: 1, Label: "join"}), nil
	})
	addEdge(t, builder, graph.START, "a")
	addEdge(t, builder, graph.START, "b")
	if err := builder.AddWaitingEdge([]graph.NodeID{"a", "b"}, "join"); err != nil {
		t.Fatal(err)
	}
	if err := builder.AddConditionalEdges("join", func(_ context.Context, state testState) ([]graph.NodeID, error) {
		if state.Total < 2 {
			return []graph.NodeID{"a", "b"}, nil
		}
		return []graph.NodeID{graph.END}, nil
	}, "a", "b", graph.END); err != nil {
		t.Fatal(err)
	}
	result, err := compileGraph(t, builder).Invoke(context.Background(), testState{}, graph.RunConfig{})
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"a", "b", "join", "a", "b", "join"}
	if !reflect.DeepEqual(result.Path, want) {
		t.Fatalf("path=%#v want=%#v", result.Path, want)
	}
}

func TestWaitingEdgeValidation(t *testing.T) {
	builder := graph.NewStateGraph(testReducer)
	if err := builder.AddWaitingEdge([]graph.NodeID{"a"}, "target"); !errors.Is(err, graph.ErrInvalidGraph) {
		t.Fatalf("error=%v", err)
	}
	if err := builder.AddWaitingEdge([]graph.NodeID{"a", "a"}, "target"); !errors.Is(err, graph.ErrInvalidGraph) {
		t.Fatalf("error=%v", err)
	}
}
