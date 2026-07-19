package graph_test

import (
	"context"
	"errors"
	"testing"

	"github.com/wahanbo/langgraph-go/checkpoint"
	"github.com/wahanbo/langgraph-go/checkpoint/memory"
	"github.com/wahanbo/langgraph-go/graph"
)

func bulkGraph(t *testing.T) (*graph.CompiledGraph[customState, customDelta], *memory.Saver) {
	t.Helper()
	builder := graph.NewStateGraph(customReducer)
	for _, id := range []graph.NodeID{"a", "b"} {
		id := id
		if err := builder.AddNode(id, func(_ context.Context, _ customState, _ graph.Runtime) (graph.Command[customDelta], error) {
			return graph.NoCommand[customDelta](), nil
		}); err != nil {
			t.Fatal(err)
		}
		_ = builder.AddEdge(graph.START, id)
		_ = builder.AddEdge(id, graph.END)
	}
	saver := memory.NewSaver()
	compiled, err := builder.Compile(graph.WithPersistence(graph.PersistenceConfig[customState, customDelta]{
		Saver:      saver,
		StateCodec: checkpoint.MustJSONCodec[customState]("tests.bulk-state", 1),
		DeltaCodec: checkpoint.MustJSONCodec[customDelta]("tests.bulk-delta", 1),
	}))
	if err != nil {
		t.Fatal(err)
	}
	return compiled, saver
}

func TestBulkUpdateStateCreatesOneCheckpointPerSuperstep(t *testing.T) {
	compiled, _ := bulkGraph(t)
	config := graph.RunConfig{ThreadID: "bulk"}
	latest, err := compiled.BulkUpdateState(context.Background(), config, [][]graph.StateUpdate[customDelta]{
		{{Delta: customDelta{Add: 1}, AsNode: "a", TaskID: "first"}},
		{
			{Delta: customDelta{Add: 2}, AsNode: "a"},
			{Delta: customDelta{Add: 3}, AsNode: "b"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := compiled.GetState(context.Background(), config)
	if err != nil || snapshot.Values.Count != 6 || snapshot.Config != latest {
		t.Fatalf("snapshot=%+v latest=%+v err=%v", snapshot, latest, err)
	}
	history, err := compiled.GetStateHistory(context.Background(), config, graph.StateHistoryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if len(history) != 2 || history[0].Metadata["bulk"] != true || history[1].Metadata["bulk"] != true {
		t.Fatalf("history=%+v", history)
	}
	if history[0].ParentConfig == nil || history[0].ParentConfig.CheckpointID != history[1].Config.CheckpointID {
		t.Fatalf("bulk checkpoints are not a parent chain: %+v", history)
	}
}

func TestBulkUpdateStateValidatesAllNodesBeforeWriting(t *testing.T) {
	compiled, _ := bulkGraph(t)
	config := graph.RunConfig{ThreadID: "bulk-invalid"}
	_, err := compiled.BulkUpdateState(context.Background(), config, [][]graph.StateUpdate[customDelta]{
		{{Delta: customDelta{Add: 1}, AsNode: "a"}},
		{{Delta: customDelta{Add: 2}, AsNode: "missing"}},
	})
	if !errors.Is(err, graph.ErrInvalidStateUpdate) || !errors.Is(err, graph.ErrUnknownNode) {
		t.Fatalf("invalid bulk err=%v", err)
	}
	history, historyErr := compiled.GetStateHistory(context.Background(), config, graph.StateHistoryOptions{})
	if historyErr != nil || len(history) != 0 {
		t.Fatalf("prevalidation wrote checkpoints: len=%d err=%v", len(history), historyErr)
	}
	for _, supersteps := range [][][]graph.StateUpdate[customDelta]{nil, {nil}} {
		if _, err := compiled.BulkUpdateState(context.Background(), config, supersteps); !errors.Is(err, graph.ErrInvalidStateUpdate) {
			t.Fatalf("empty supersteps=%v err=%v", supersteps, err)
		}
	}
}

func TestBulkUpdateStateBranchesFromHistoricalCheckpoint(t *testing.T) {
	compiled, _ := bulkGraph(t)
	config := graph.RunConfig{ThreadID: "bulk-branch"}
	_, err := compiled.BulkUpdateState(context.Background(), config, [][]graph.StateUpdate[customDelta]{
		{{Delta: customDelta{Add: 1}, AsNode: "a"}},
		{{Delta: customDelta{Add: 2}, AsNode: "b"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	history, _ := compiled.GetStateHistory(context.Background(), config, graph.StateHistoryOptions{})
	base := history[1]
	branched, err := compiled.BulkUpdateState(context.Background(), graph.RunConfig{
		ThreadID: config.ThreadID, CheckpointID: base.Config.CheckpointID,
	}, [][]graph.StateUpdate[customDelta]{{{Delta: customDelta{Add: 10}, AsNode: "a"}}})
	if err != nil {
		t.Fatal(err)
	}
	latest, err := compiled.GetState(context.Background(), config)
	if err != nil || latest.Config != branched || latest.Values.Count != 11 {
		t.Fatalf("branched latest=%+v config=%+v err=%v", latest, branched, err)
	}
	if latest.ParentConfig == nil || latest.ParentConfig.CheckpointID != base.Config.CheckpointID {
		t.Fatalf("branch parent=%+v want=%+v", latest.ParentConfig, base.Config)
	}
}
