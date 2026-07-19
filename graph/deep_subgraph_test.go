package graph_test

import (
	"context"
	"errors"
	"testing"

	"github.com/ybszm/langgraph-go/checkpoint"
	"github.com/ybszm/langgraph-go/checkpoint/memory"
	"github.com/ybszm/langgraph-go/graph"
)

func wrapCustomSubgraph(t *testing.T, id graph.NodeID, child *graph.CompiledGraph[customState, customDelta]) *graph.CompiledGraph[customState, customDelta] {
	t.Helper()
	builder := graph.NewStateGraph(customReducer)
	if err := graph.AddSubgraph(builder, id, child, graph.SubgraphAdapter[customState, customDelta, customState, customDelta]{
		Input: func(_ context.Context, state customState) (customState, error) { return state, nil },
		Output: func(_ context.Context, _ customState, childState customState) (graph.Command[customDelta], error) {
			return graph.Update(customDelta{Add: childState.Count}), nil
		},
		StateCodec: checkpoint.MustJSONCodec[customState]("tests.deep-state", 1),
		DeltaCodec: checkpoint.MustJSONCodec[customDelta]("tests.deep-delta", 1),
	}); err != nil {
		t.Fatal(err)
	}
	_ = builder.AddEdge(graph.START, id)
	_ = builder.AddEdge(id, graph.END)
	compiled, err := builder.Compile()
	if err != nil {
		t.Fatal(err)
	}
	return compiled
}

func TestDeepNestedInterruptStateAndRecursiveResume(t *testing.T) {
	leafBuilder := graph.NewStateGraph(customReducer)
	_ = leafBuilder.AddNode("ask", func(_ context.Context, _ customState, runtime graph.Runtime) (graph.Command[customDelta], error) {
		answer, err := graph.AwaitResume[int](runtime, "deep answer?")
		if err != nil {
			return graph.Command[customDelta]{}, err
		}
		return graph.Update(customDelta{Add: answer}), nil
	})
	_ = leafBuilder.AddEdge(graph.START, "ask")
	_ = leafBuilder.AddEdge("ask", graph.END)
	leaf, err := leafBuilder.Compile()
	if err != nil {
		t.Fatal(err)
	}
	middle := wrapCustomSubgraph(t, "leaf_graph", leaf)
	outer := wrapCustomSubgraph(t, "middle_graph", middle)

	// Recompile the outermost wrapper with persistence; its embedded middle and
	// leaf graphs inherit the same saver with nested namespaces.
	rootBuilder := graph.NewStateGraph(customReducer)
	if err := graph.AddSubgraph(rootBuilder, "outer_graph", outer, graph.SubgraphAdapter[customState, customDelta, customState, customDelta]{
		Input: func(_ context.Context, state customState) (customState, error) { return state, nil },
		Output: func(_ context.Context, _ customState, child customState) (graph.Command[customDelta], error) {
			return graph.Update(customDelta{Add: child.Count}), nil
		},
		StateCodec: checkpoint.MustJSONCodec[customState]("tests.deep-state", 1),
		DeltaCodec: checkpoint.MustJSONCodec[customDelta]("tests.deep-delta", 1),
	}); err != nil {
		t.Fatal(err)
	}
	_ = rootBuilder.AddEdge(graph.START, "outer_graph")
	_ = rootBuilder.AddEdge("outer_graph", graph.END)
	saver := memory.NewSaver()
	root, err := rootBuilder.Compile(graph.WithPersistence(graph.PersistenceConfig[customState, customDelta]{
		Saver:      saver,
		StateCodec: checkpoint.MustJSONCodec[customState]("tests.deep-state", 1),
		DeltaCodec: checkpoint.MustJSONCodec[customDelta]("tests.deep-delta", 1),
	}))
	if err != nil {
		t.Fatal(err)
	}
	config := graph.RunConfig{ThreadID: "deep-interrupt"}
	if _, err := root.Invoke(context.Background(), customState{}, config); !errors.Is(err, graph.ErrGraphInterrupt) {
		t.Fatalf("Invoke err=%v", err)
	}
	snapshot, err := root.GetState(context.Background(), config, graph.WithSubgraphs())
	if err != nil {
		t.Fatal(err)
	}
	level1 := snapshot.Tasks[0].State
	if level1 == nil || len(level1.Tasks) != 1 || level1.Tasks[0].State == nil {
		t.Fatalf("missing first nested state: %+v", level1)
	}
	level2 := level1.Tasks[0].State
	if len(level2.Tasks) != 1 || level2.Tasks[0].State == nil {
		t.Fatalf("missing second nested state: %+v", level2)
	}
	leafState := level2.Tasks[0].State
	if len(leafState.Interrupts) != 1 || len(leafState.Tasks) != 1 {
		t.Fatalf("leaf state=%+v", leafState)
	}
	boundOuter, err := graph.InheritedSubgraph[customState, customDelta, customState, customDelta](root, "outer_graph")
	if err != nil {
		t.Fatal(err)
	}
	boundMiddle, err := graph.InheritedSubgraph[customState, customDelta, customState, customDelta](boundOuter, "middle_graph")
	if err != nil {
		t.Fatal(err)
	}
	boundLeaf, err := graph.InheritedSubgraph[customState, customDelta, customState, customDelta](boundMiddle, "leaf_graph")
	if err != nil {
		t.Fatal(err)
	}
	updatedLeaf, err := graph.BulkUpdateSubgraphState[customState, customDelta, customState, customDelta](
		context.Background(), boundMiddle, "leaf_graph", leafState.Config,
		[][]graph.StateUpdate[customDelta]{{{AsNode: "ask", Delta: customDelta{Add: 2}}}},
	)
	if err != nil || updatedLeaf.Values.Count != 2 || updatedLeaf.ParentConfig == nil ||
		updatedLeaf.ParentConfig.CheckpointID != leafState.Config.CheckpointID {
		t.Fatalf("deep bulk update=%+v err=%v", updatedLeaf, err)
	}
	directResume, _ := graph.Resume(5)
	direct, err := boundLeaf.ResumeFork(context.Background(), graph.RunConfig{
		ThreadID: config.ThreadID, CheckpointNamespace: leafState.Config.Namespace,
		CheckpointID: leafState.Config.CheckpointID,
	}, directResume)
	if err != nil || direct.Count != 5 {
		t.Fatalf("direct leaf Resume final=%+v err=%v", direct, err)
	}
	unchanged, err := root.GetState(context.Background(), config, graph.WithSubgraphs())
	if err != nil || unchanged.Tasks[0].State.Tasks[0].State.Tasks[0].State.Config.CheckpointID != leafState.Config.CheckpointID {
		t.Fatalf("direct child continuation mutated parent=%+v err=%v", unchanged, err)
	}
	pausedConfig := snapshot.Config
	resume, _ := graph.Resume(7)
	final, err := root.Resume(context.Background(), config, resume)
	if err != nil || final.Count != 7 {
		t.Fatalf("recursive Resume final=%+v err=%v", final, err)
	}
	historicalResume, _ := graph.Resume(9)
	historical, err := root.Resume(context.Background(), graph.RunConfig{
		ThreadID: config.ThreadID, CheckpointID: pausedConfig.CheckpointID,
	}, historicalResume)
	if err != nil || historical.Count != 9 {
		t.Fatalf("recursive historical Resume final=%+v err=%v", historical, err)
	}
}
