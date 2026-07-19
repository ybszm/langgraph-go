package graph_test

import (
	"context"
	"errors"
	"testing"

	"github.com/ybszm/langgraph-go/checkpoint"
	"github.com/ybszm/langgraph-go/checkpoint/memory"
	"github.com/ybszm/langgraph-go/graph"
)

func TestExactHistoricalResumeForksAndReplacesOldAnswer(t *testing.T) {
	builder := graph.NewStateGraph(customReducer)
	_ = builder.AddNode("ask", func(_ context.Context, _ customState, runtime graph.Runtime) (graph.Command[customDelta], error) {
		answer, err := graph.AwaitResume[int](runtime, "number?")
		if err != nil {
			return graph.Command[customDelta]{}, err
		}
		return graph.Update(customDelta{Add: answer}), nil
	})
	_ = builder.AddEdge(graph.START, "ask")
	_ = builder.AddEdge("ask", graph.END)
	saver := memory.NewSaver()
	compiled, err := builder.Compile(graph.WithPersistence(graph.PersistenceConfig[customState, customDelta]{
		Saver:      saver,
		StateCodec: checkpoint.MustJSONCodec[customState]("tests.historical-resume-state", 1),
		DeltaCodec: checkpoint.MustJSONCodec[customDelta]("tests.historical-resume-delta", 1),
	}))
	if err != nil {
		t.Fatal(err)
	}
	config := graph.RunConfig{ThreadID: "historical-resume"}
	if _, err := compiled.Invoke(context.Background(), customState{}, config); !errors.Is(err, graph.ErrGraphInterrupt) {
		t.Fatalf("initial interrupt err=%v", err)
	}
	paused, err := compiled.GetState(context.Background(), config)
	if err != nil || len(paused.Interrupts) != 1 {
		t.Fatalf("paused=%+v err=%v", paused, err)
	}
	resumeOne, _ := graph.Resume(1)
	first, err := compiled.Resume(context.Background(), config, resumeOne)
	if err != nil || first.Count != 1 {
		t.Fatalf("first branch=%+v err=%v", first, err)
	}

	resumeTwo, _ := graph.Resume(2)
	second, err := compiled.Resume(context.Background(), graph.RunConfig{
		ThreadID: config.ThreadID, CheckpointID: paused.Config.CheckpointID,
	}, resumeTwo)
	if err != nil || second.Count != 2 {
		t.Fatalf("historical branch=%+v err=%v", second, err)
	}
	history, err := compiled.GetStateHistory(context.Background(), config, graph.StateHistoryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	foundFork := false
	for _, snapshot := range history {
		if snapshot.Metadata["resume_fork"] == true {
			foundFork = true
			if snapshot.ParentConfig == nil || snapshot.ParentConfig.CheckpointID != paused.Config.CheckpointID {
				t.Fatalf("resume fork parent=%+v want=%+v", snapshot.ParentConfig, paused.Config)
			}
		}
	}
	if !foundFork {
		t.Fatal("resume fork checkpoint not found")
	}
}

func TestExactHistoricalResumeWithoutInterruptDoesNotWriteFork(t *testing.T) {
	compiled, _ := bulkGraph(t)
	config := graph.RunConfig{ThreadID: "historical-no-interrupt"}
	_, err := compiled.BulkUpdateState(context.Background(), config, [][]graph.StateUpdate[customDelta]{{{
		Delta: customDelta{Add: 1}, AsNode: "a",
	}}})
	if err != nil {
		t.Fatal(err)
	}
	before, _ := compiled.GetStateHistory(context.Background(), config, graph.StateHistoryOptions{})
	resume, _ := graph.Resume(2)
	_, err = compiled.Resume(context.Background(), graph.RunConfig{
		ThreadID: config.ThreadID, CheckpointID: before[0].Config.CheckpointID,
	}, resume)
	if !errors.Is(err, graph.ErrInvalidResume) {
		t.Fatalf("resume without interrupt err=%v", err)
	}
	after, _ := compiled.GetStateHistory(context.Background(), config, graph.StateHistoryOptions{})
	if len(after) != len(before) {
		t.Fatalf("invalid resume wrote fork: before=%d after=%d", len(before), len(after))
	}
}

func TestExactHistoricalNestedSubgraphResumeForksChildBranch(t *testing.T) {
	compiled, saver := subgraphFixture(t)
	ctx := context.Background()
	config := graph.RunConfig{ThreadID: "historical-nested-resume"}
	if _, err := compiled.Invoke(ctx, subParentState{Input: "seed"}, config); !errors.Is(err, graph.ErrGraphInterrupt) {
		t.Fatalf("initial Invoke() err=%v", err)
	}
	paused, err := compiled.GetState(ctx, config, graph.WithSubgraphs())
	if err != nil || len(paused.Tasks) != 1 || paused.Tasks[0].State == nil {
		t.Fatalf("paused=%+v err=%v", paused, err)
	}
	pausedConfig := paused.Config
	originalChildConfig := paused.Tasks[0].State.Config

	oldResume, err := graph.Resume("old")
	if err != nil {
		t.Fatal(err)
	}
	oldResult, err := compiled.Resume(ctx, config, oldResume)
	if err != nil || oldResult.Result != "seed:prepared:old" {
		t.Fatalf("old branch=%+v err=%v", oldResult, err)
	}
	historicalView, err := compiled.GetState(ctx, graph.RunConfig{
		ThreadID: config.ThreadID, CheckpointID: pausedConfig.CheckpointID,
	}, graph.WithSubgraphs())
	if err != nil || len(historicalView.Tasks) != 1 || historicalView.Tasks[0].State == nil ||
		historicalView.Tasks[0].State.Config.CheckpointID != originalChildConfig.CheckpointID {
		t.Fatalf("historical nested view=%+v err=%v", historicalView.Tasks, err)
	}
	originalChildHead, found, err := saver.GetTuple(ctx, checkpoint.Config{
		ThreadID: config.ThreadID, Namespace: originalChildConfig.Namespace,
	})
	if err != nil || !found {
		t.Fatalf("original child head found=%v err=%v", found, err)
	}

	newResume, err := graph.Resume("new")
	if err != nil {
		t.Fatal(err)
	}
	newResult, err := compiled.Resume(ctx, graph.RunConfig{
		ThreadID: config.ThreadID, CheckpointID: pausedConfig.CheckpointID,
	}, newResume)
	if err != nil || newResult.Result != "seed:prepared:new" {
		t.Fatalf("new branch=%+v err=%v", newResult, err)
	}
	unchangedChildHead, found, err := saver.GetTuple(ctx, checkpoint.Config{
		ThreadID: config.ThreadID, Namespace: originalChildConfig.Namespace,
	})
	if err != nil || !found || unchangedChildHead.Config.CheckpointID != originalChildHead.Config.CheckpointID {
		t.Fatalf("original child branch changed from %q to %q (found=%v err=%v)",
			originalChildHead.Config.CheckpointID, unchangedChildHead.Config.CheckpointID, found, err)
	}

	history, err := compiled.GetStateHistory(ctx, config, graph.StateHistoryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	foundFork := false
	for _, item := range history {
		source, _ := item.Metadata["source"].(string)
		if source == string(checkpoint.SourceFork) && item.ParentConfig != nil &&
			item.ParentConfig.CheckpointID == pausedConfig.CheckpointID {
			foundFork = true
			break
		}
	}
	if !foundFork {
		t.Fatal("parent history has no fork rooted at the historical interrupt checkpoint")
	}
	all, err := saver.List(ctx, checkpoint.ListOptions{
		Config:        &checkpoint.Config{ThreadID: config.ThreadID},
		AllNamespaces: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	childNamespaces := make(map[string]struct{})
	for _, tuple := range all {
		if tuple.Config.Namespace != "" {
			childNamespaces[tuple.Config.Namespace] = struct{}{}
		}
	}
	if len(childNamespaces) < 2 {
		t.Fatalf("child namespaces=%v; historical branch reused the original child", childNamespaces)
	}
}
