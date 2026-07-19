package graph_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/wahanbo/langgraph-go/checkpoint"
	"github.com/wahanbo/langgraph-go/checkpoint/memory"
	"github.com/wahanbo/langgraph-go/graph"
)

type subParentState struct{ Input, Result string }
type subParentDelta struct{ Result string }
type subChildState struct{ Text string }
type subChildDelta struct{ Append string }

func subgraphFixture(t *testing.T) (*graph.CompiledGraph[subParentState, subParentDelta], *memory.Saver) {
	t.Helper()
	childBuilder := graph.NewStateGraph(func(_ context.Context, state subChildState, updates []subChildDelta) (subChildState, error) {
		for _, update := range updates {
			state.Text += update.Append
		}
		return state, nil
	})
	if err := childBuilder.AddNode("prepare", func(_ context.Context, _ subChildState, runtime graph.Runtime) (graph.Command[subChildDelta], error) {
		if runtime.ThreadID == "" || runtime.CheckpointNamespace == "" || runtime.CheckpointID == "" {
			return graph.Command[subChildDelta]{}, errors.New("child runtime has no durable coordinates")
		}
		return graph.Update(subChildDelta{Append: ":prepared"}), nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := childBuilder.AddNode("ask", func(_ context.Context, _ subChildState, runtime graph.Runtime) (graph.Command[subChildDelta], error) {
		answer, err := graph.AwaitResume[string](runtime, "continue?")
		if err != nil {
			return graph.Command[subChildDelta]{}, err
		}
		return graph.Update(subChildDelta{Append: ":" + answer}), nil
	}); err != nil {
		t.Fatal(err)
	}
	for _, edge := range [][2]graph.NodeID{{graph.START, "prepare"}, {"prepare", "ask"}, {"ask", graph.END}} {
		if err := childBuilder.AddEdge(edge[0], edge[1]); err != nil {
			t.Fatal(err)
		}
	}
	child, err := childBuilder.Compile()
	if err != nil {
		t.Fatal(err)
	}

	parentBuilder := graph.NewStateGraph(func(_ context.Context, state subParentState, updates []subParentDelta) (subParentState, error) {
		for _, update := range updates {
			state.Result = update.Result
		}
		return state, nil
	})
	err = graph.AddSubgraph(parentBuilder, "child", child, graph.SubgraphAdapter[subParentState, subParentDelta, subChildState, subChildDelta]{
		Input: func(_ context.Context, parent subParentState) (subChildState, error) {
			return subChildState{Text: parent.Input}, nil
		},
		Output: func(_ context.Context, _ subParentState, child subChildState) (graph.Command[subParentDelta], error) {
			return graph.Update(subParentDelta{Result: child.Text}), nil
		},
		StateCodec: checkpoint.MustJSONCodec[subChildState]("tests.sub-child-state", 1),
		DeltaCodec: checkpoint.MustJSONCodec[subChildDelta]("tests.sub-child-delta", 1),
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := parentBuilder.AddEdge(graph.START, "child"); err != nil {
		t.Fatal(err)
	}
	if err := parentBuilder.AddEdge("child", graph.END); err != nil {
		t.Fatal(err)
	}
	saver := memory.NewSaver()
	parent, err := parentBuilder.Compile(graph.WithPersistence(graph.PersistenceConfig[subParentState, subParentDelta]{
		Saver:      saver,
		StateCodec: checkpoint.MustJSONCodec[subParentState]("tests.sub-parent-state", 1),
		DeltaCodec: checkpoint.MustJSONCodec[subParentDelta]("tests.sub-parent-delta", 1),
	}))
	if err != nil {
		t.Fatal(err)
	}
	return parent, saver
}

func TestSubgraphInheritsPersistenceInterruptsAndNestedState(t *testing.T) {
	compiled, _ := subgraphFixture(t)
	config := graph.RunConfig{ThreadID: "subgraph-thread"}
	state, err := compiled.Invoke(context.Background(), subParentState{Input: "seed"}, config)
	if !errors.Is(err, graph.ErrGraphInterrupt) || state.Result != "" {
		t.Fatalf("Invoke() state=%+v err=%v", state, err)
	}

	snapshot, err := compiled.GetState(context.Background(), config, graph.WithSubgraphs())
	if err != nil {
		t.Fatal(err)
	}
	if len(snapshot.Tasks) != 1 || snapshot.Tasks[0].State == nil {
		t.Fatalf("nested task state = %+v", snapshot.Tasks)
	}
	if !strings.Contains(snapshot.Tasks[0].State.Config.Namespace, "child:") || len(snapshot.Tasks[0].State.Interrupts) != 1 {
		t.Fatalf("nested snapshot = %+v", snapshot.Tasks[0].State)
	}
	if snapshot.Interrupts[0].Namespace != snapshot.Tasks[0].State.Config.Namespace {
		t.Fatalf("interrupt namespace=%q child=%q", snapshot.Interrupts[0].Namespace, snapshot.Tasks[0].State.Config.Namespace)
	}

	resume, err := graph.Resume("approved")
	if err != nil {
		t.Fatal(err)
	}
	completed, err := compiled.Resume(context.Background(), config, resume)
	if err != nil {
		t.Fatal(err)
	}
	if completed.Result != "seed:prepared:approved" {
		t.Fatalf("completed=%+v", completed)
	}
}

func TestBulkUpdateSubgraphStateDelegatesTypedSupersteps(t *testing.T) {
	compiled, _ := subgraphFixture(t)
	config := graph.RunConfig{ThreadID: "subgraph-bulk-thread"}
	if _, err := compiled.Invoke(context.Background(), subParentState{Input: "seed"}, config); !errors.Is(err, graph.ErrGraphInterrupt) {
		t.Fatalf("Invoke() err=%v", err)
	}
	parent, err := compiled.GetState(context.Background(), config, graph.WithSubgraphs())
	if err != nil || len(parent.Tasks) != 1 || parent.Tasks[0].State == nil {
		t.Fatalf("parent=%+v err=%v", parent, err)
	}
	childCoordinate := parent.Tasks[0].State.Config
	updated, err := graph.BulkUpdateSubgraphState[subParentState, subParentDelta, subChildState, subChildDelta](
		context.Background(), compiled, "child", childCoordinate,
		[][]graph.StateUpdate[subChildDelta]{{{AsNode: "prepare", Delta: subChildDelta{Append: ":bulk"}}}},
	)
	if err != nil {
		t.Fatal(err)
	}
	if updated.Values.Text != "seed:prepared:bulk" || len(updated.Next) != 1 || updated.Next[0] != "ask" ||
		updated.Config.CheckpointID == childCoordinate.CheckpointID || updated.ParentConfig == nil ||
		updated.ParentConfig.CheckpointID != childCoordinate.CheckpointID {
		t.Fatalf("updated=%+v", updated)
	}
	// The parent remains an immutable pointer to the original interrupted child
	// branch until an explicit parent resume/fork operation selects otherwise.
	again, err := compiled.GetState(context.Background(), config, graph.WithSubgraphs())
	if err != nil || again.Tasks[0].State.Config.CheckpointID != childCoordinate.CheckpointID {
		t.Fatalf("parent changed=%+v err=%v", again, err)
	}
}

func TestSubgraphReplayUsesFreshChildNamespace(t *testing.T) {
	compiled, saver := subgraphFixture(t)
	config := graph.RunConfig{ThreadID: "subgraph-replay"}
	_, _ = compiled.Invoke(context.Background(), subParentState{Input: "seed"}, config)
	first, err := compiled.GetState(context.Background(), config, graph.WithSubgraphs())
	if err != nil {
		t.Fatal(err)
	}
	firstNamespace := first.Tasks[0].State.Config.Namespace
	resume, _ := graph.Resume("one")
	if _, err := compiled.Resume(context.Background(), config, resume); err != nil {
		t.Fatal(err)
	}

	history, err := compiled.GetStateHistory(context.Background(), config, graph.StateHistoryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	var before checkpoint.Config
	for _, item := range history {
		if len(item.Next) == 1 && item.Next[0] == "child" {
			before = item.Config
			break
		}
	}
	if before.CheckpointID == "" {
		t.Fatal("parent checkpoint before child not found")
	}
	_, err = compiled.Invoke(context.Background(), subParentState{}, graph.RunConfig{ThreadID: config.ThreadID, CheckpointID: before.CheckpointID})
	if !errors.Is(err, graph.ErrGraphInterrupt) {
		t.Fatalf("replay err=%v", err)
	}
	replayed, err := compiled.GetState(context.Background(), config, graph.WithSubgraphs())
	if err != nil {
		t.Fatal(err)
	}
	if replayed.Tasks[0].State.Config.Namespace == firstNamespace {
		t.Fatalf("replay reused child namespace %q", firstNamespace)
	}

	tuples, err := saver.List(context.Background(), checkpoint.ListOptions{Config: &checkpoint.Config{ThreadID: config.ThreadID}, AllNamespaces: true})
	if err != nil || len(tuples) < 2 {
		t.Fatalf("all namespace history len=%d err=%v", len(tuples), err)
	}
}

func TestSubgraphStreamCarriesNamespaceAndTypedPayload(t *testing.T) {
	compiled, _ := subgraphFixture(t)
	events := compiled.Stream(context.Background(), subParentState{Input: "seed"}, graph.RunConfig{
		ThreadID: "subgraph-stream", StreamSubgraphs: true,
	})
	seenChild := false
	for event := range events {
		if len(event.Namespace) > 0 {
			seenChild = true
			if event.Subgraph == nil {
				t.Fatal("child event has no type-erased payload")
			}
		}
	}
	if !seenChild {
		t.Fatal("no namespaced child stream event")
	}
}
