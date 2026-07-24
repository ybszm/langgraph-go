package graph_test

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/ybszm/langgraph-go/checkpoint"
	"github.com/ybszm/langgraph-go/checkpoint/memory"
	"github.com/ybszm/langgraph-go/graph"
)

type retainedChildState struct{ Values []string }
type retainedChildDelta struct{ Values []string }
type retainedParentState struct {
	Input  string
	Result []string
}
type retainedParentDelta struct{ Result []string }

func retainedFixture(t *testing.T) (*graph.CompiledGraph[retainedParentState, retainedParentDelta], *memory.Saver) {
	return retainedFixtureWithLoop(t, false)
}

func retainedFixtureWithLoop(t *testing.T, loop bool) (*graph.CompiledGraph[retainedParentState, retainedParentDelta], *memory.Saver) {
	t.Helper()
	childBuilder := graph.NewStateGraph(func(_ context.Context, state retainedChildState, updates []retainedChildDelta) (retainedChildState, error) {
		for _, update := range updates {
			state.Values = append(state.Values, update.Values...)
		}
		return state, nil
	})
	_ = childBuilder.AddNode("process", func(_ context.Context, _ retainedChildState, _ graph.Runtime) (graph.Command[retainedChildDelta], error) {
		return graph.Update(retainedChildDelta{Values: []string{"processed"}}), nil
	})
	_ = childBuilder.AddEdge(graph.START, "process")
	_ = childBuilder.AddEdge("process", graph.END)
	child, err := childBuilder.Compile()
	if err != nil {
		t.Fatal(err)
	}
	parentBuilder := graph.NewStateGraph(func(_ context.Context, state retainedParentState, updates []retainedParentDelta) (retainedParentState, error) {
		for _, update := range updates {
			state.Result = append([]string(nil), update.Result...)
		}
		return state, nil
	})
	err = graph.AddSubgraph(parentBuilder, "retained", child, graph.SubgraphAdapter[retainedParentState, retainedParentDelta, retainedChildState, retainedChildDelta]{
		Input: func(_ context.Context, parent retainedParentState) (retainedChildState, error) {
			return retainedChildState{Values: []string{parent.Input}}, nil
		},
		Output: func(_ context.Context, _ retainedParentState, child retainedChildState) (graph.Command[retainedParentDelta], error) {
			return graph.Update(retainedParentDelta{Result: append([]string(nil), child.Values...)}), nil
		},
		StateCodec: checkpoint.MustJSONCodec[retainedChildState]("tests.retained-child-state", 1),
		DeltaCodec: checkpoint.MustJSONCodec[retainedChildDelta]("tests.retained-child-delta", 1),
		Stateful:   true,
		MergeInput: func(_ context.Context, previous, input retainedChildState) (retainedChildState, error) {
			return retainedChildState{Values: append(append([]string(nil), previous.Values...), input.Values...)}, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	_ = parentBuilder.AddEdge(graph.START, "retained")
	if loop {
		if err := parentBuilder.AddConditionalEdges(
			"retained",
			func(_ context.Context, state retainedParentState) ([]graph.NodeID, error) {
				if len(state.Result) < 4 {
					return []graph.NodeID{"retained"}, nil
				}
				return []graph.NodeID{graph.END}, nil
			},
			"retained", graph.END,
		); err != nil {
			t.Fatal(err)
		}
	} else {
		_ = parentBuilder.AddEdge("retained", graph.END)
	}
	saver := memory.NewSaver()
	parent, err := parentBuilder.Compile(graph.WithPersistence(graph.PersistenceConfig[retainedParentState, retainedParentDelta]{
		Saver:      saver,
		StateCodec: checkpoint.MustJSONCodec[retainedParentState]("tests.retained-parent-state", 1),
		DeltaCodec: checkpoint.MustJSONCodec[retainedParentDelta]("tests.retained-parent-delta", 1),
	}))
	if err != nil {
		t.Fatal(err)
	}
	return parent, saver
}

func TestDurabilityModesRetainStatefulSubgraphAcrossSameRun(t *testing.T) {
	for _, durability := range []graph.Durability{graph.DurabilityAsync, graph.DurabilityExit} {
		t.Run(string(durability), func(t *testing.T) {
			compiled, _ := retainedFixtureWithLoop(t, true)
			result, err := compiled.Invoke(
				context.Background(),
				retainedParentState{Input: "one"},
				graph.RunConfig{
					ThreadID:   "retained-same-run-" + string(durability),
					Durability: durability, RecursionLimit: 10,
				},
			)
			want := []string{"one", "processed", "one", "processed"}
			if err != nil || !equalStrings(result.Result, want) {
				t.Fatalf("result=%+v want=%v err=%v", result, want, err)
			}
		})
	}
}

func TestNewRunStatefulSubgraphRetainsPreviousState(t *testing.T) {
	compiled, saver := retainedFixture(t)
	config := graph.RunConfig{ThreadID: "retained"}
	first, err := compiled.Invoke(context.Background(), retainedParentState{Input: "one"}, config)
	if err != nil || len(first.Result) != 2 {
		t.Fatalf("first=%+v err=%v", first, err)
	}
	second, err := compiled.Invoke(context.Background(), retainedParentState{Input: "two"}, graph.RunConfig{
		ThreadID: config.ThreadID, NewRun: true,
	})
	want := []string{"one", "processed", "two", "processed"}
	if err != nil || !equalStrings(second.Result, want) {
		t.Fatalf("second=%+v want=%v err=%v", second, want, err)
	}
	tuples, err := saver.List(context.Background(), checkpoint.ListOptions{
		Config: &checkpoint.Config{ThreadID: config.ThreadID}, AllNamespaces: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	childNamespaces := map[string]struct{}{}
	for _, tuple := range tuples {
		if tuple.Config.Namespace != "" {
			childNamespaces[tuple.Config.Namespace] = struct{}{}
		}
	}
	if len(childNamespaces) != 1 {
		t.Fatalf("stateful child used unstable namespaces: %v", childNamespaces)
	}
}

func TestStatefulSubgraphReplayCannotReadFutureChildState(t *testing.T) {
	compiled, _ := retainedFixture(t)
	config := graph.RunConfig{ThreadID: "retained-replay"}
	_, _ = compiled.Invoke(context.Background(), retainedParentState{Input: "one"}, config)
	_, err := compiled.Invoke(context.Background(), retainedParentState{Input: "two"}, graph.RunConfig{ThreadID: config.ThreadID, NewRun: true})
	if err != nil {
		t.Fatal(err)
	}
	history, err := compiled.GetStateHistory(context.Background(), config, graph.StateHistoryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	var beforeSecond checkpoint.Config
	for _, snapshot := range history {
		if len(snapshot.Next) == 1 && snapshot.Next[0] == "retained" && snapshot.Values.Input == "two" {
			beforeSecond = snapshot.Config
			break
		}
	}
	if beforeSecond.CheckpointID == "" {
		t.Fatal("checkpoint before second child invocation not found")
	}
	replayed, err := compiled.Invoke(context.Background(), retainedParentState{}, graph.RunConfig{
		ThreadID: config.ThreadID, CheckpointID: beforeSecond.CheckpointID,
	})
	want := []string{"one", "processed", "two", "processed"}
	if err != nil || !equalStrings(replayed.Result, want) {
		t.Fatalf("replayed=%+v want=%v err=%v", replayed, want, err)
	}
}

func TestStatefulSubgraphInterruptResumeAcrossNewRuns(t *testing.T) {
	childBuilder := graph.NewStateGraph(func(_ context.Context, state retainedChildState, updates []retainedChildDelta) (retainedChildState, error) {
		for _, update := range updates {
			state.Values = append(state.Values, update.Values...)
		}
		return state, nil
	})
	_ = childBuilder.AddNode("ask", func(_ context.Context, _ retainedChildState, runtime graph.Runtime) (graph.Command[retainedChildDelta], error) {
		answer, err := graph.AwaitResume[string](runtime, "answer?")
		if err != nil {
			return graph.Command[retainedChildDelta]{}, err
		}
		return graph.Update(retainedChildDelta{Values: []string{answer}}), nil
	})
	_ = childBuilder.AddEdge(graph.START, "ask")
	_ = childBuilder.AddEdge("ask", graph.END)
	child, _ := childBuilder.Compile()
	parentBuilder := graph.NewStateGraph(func(_ context.Context, state retainedParentState, updates []retainedParentDelta) (retainedParentState, error) {
		for _, update := range updates {
			state.Result = append([]string(nil), update.Result...)
		}
		return state, nil
	})
	if err := graph.AddSubgraph(parentBuilder, "retained", child, graph.SubgraphAdapter[retainedParentState, retainedParentDelta, retainedChildState, retainedChildDelta]{
		Input: func(_ context.Context, parent retainedParentState) (retainedChildState, error) {
			return retainedChildState{Values: []string{parent.Input}}, nil
		},
		Output: func(_ context.Context, _ retainedParentState, child retainedChildState) (graph.Command[retainedParentDelta], error) {
			return graph.Update(retainedParentDelta{Result: append([]string(nil), child.Values...)}), nil
		},
		StateCodec: checkpoint.MustJSONCodec[retainedChildState]("tests.retained-interrupt-state", 1),
		DeltaCodec: checkpoint.MustJSONCodec[retainedChildDelta]("tests.retained-interrupt-delta", 1),
		Stateful:   true,
		MergeInput: func(_ context.Context, previous, input retainedChildState) (retainedChildState, error) {
			return retainedChildState{Values: append(append([]string(nil), previous.Values...), input.Values...)}, nil
		},
	}); err != nil {
		t.Fatal(err)
	}
	_ = parentBuilder.AddEdge(graph.START, "retained")
	_ = parentBuilder.AddEdge("retained", graph.END)
	saver := memory.NewSaver()
	parent, err := parentBuilder.Compile(graph.WithPersistence(graph.PersistenceConfig[retainedParentState, retainedParentDelta]{
		Saver:      saver,
		StateCodec: checkpoint.MustJSONCodec[retainedParentState]("tests.retained-interrupt-parent-state", 1),
		DeltaCodec: checkpoint.MustJSONCodec[retainedParentDelta]("tests.retained-interrupt-parent-delta", 1),
	}))
	if err != nil {
		t.Fatal(err)
	}
	config := graph.RunConfig{ThreadID: "retained-interrupt"}
	if _, err := parent.Invoke(context.Background(), retainedParentState{Input: "one"}, config); !errors.Is(err, graph.ErrGraphInterrupt) {
		t.Fatalf("first interrupt err=%v", err)
	}
	firstPaused, err := parent.GetState(context.Background(), config)
	if err != nil {
		t.Fatal(err)
	}
	resumeA, _ := graph.Resume("a")
	first, err := parent.Resume(context.Background(), config, resumeA)
	if err != nil || !equalStrings(first.Result, []string{"one", "a"}) {
		t.Fatalf("first result=%+v err=%v", first, err)
	}
	if _, err := parent.Invoke(context.Background(), retainedParentState{Input: "two"}, graph.RunConfig{ThreadID: config.ThreadID, NewRun: true}); !errors.Is(err, graph.ErrGraphInterrupt) {
		t.Fatalf("second interrupt err=%v", err)
	}
	resumeB, _ := graph.Resume("b")
	second, err := parent.Resume(context.Background(), config, resumeB)
	if err != nil || !equalStrings(second.Result, []string{"one", "a", "two", "b"}) {
		t.Fatalf("second result=%+v err=%v", second, err)
	}
	resumeHistorical, _ := graph.Resume("historical")
	historical, err := parent.Resume(context.Background(), graph.RunConfig{
		ThreadID: config.ThreadID, CheckpointID: firstPaused.Config.CheckpointID,
	}, resumeHistorical)
	if err != nil || !equalStrings(historical.Result, []string{"one", "historical"}) {
		t.Fatalf("historical result=%+v err=%v", historical, err)
	}
}

type keyedRetainedCall struct {
	Key   string
	Value string
}

type keyedRetainedParentState struct {
	Calls   []keyedRetainedCall
	Key     string
	Value   string
	Results map[string][]string
}

type keyedRetainedParentDelta struct {
	Key    string
	Values []string
}

func TestParallelSendsToStatefulSubgraphUseIndependentInstanceKeys(t *testing.T) {
	childBuilder := graph.NewStateGraph(func(_ context.Context, state retainedChildState, updates []retainedChildDelta) (retainedChildState, error) {
		for _, update := range updates {
			state.Values = append(state.Values, update.Values...)
		}
		return state, nil
	})
	_ = childBuilder.AddNode("process", func(_ context.Context, _ retainedChildState, _ graph.Runtime) (graph.Command[retainedChildDelta], error) {
		return graph.Update(retainedChildDelta{Values: []string{"processed"}}), nil
	})
	_ = childBuilder.AddEdge(graph.START, "process")
	_ = childBuilder.AddEdge("process", graph.END)
	child, err := childBuilder.Compile()
	if err != nil {
		t.Fatal(err)
	}

	parentBuilder := graph.NewStateGraph(func(_ context.Context, state keyedRetainedParentState, updates []keyedRetainedParentDelta) (keyedRetainedParentState, error) {
		if state.Results == nil {
			state.Results = make(map[string][]string)
		}
		for _, update := range updates {
			state.Results[update.Key] = append([]string(nil), update.Values...)
		}
		return state, nil
	})
	_ = parentBuilder.AddNode("dispatch", func(_ context.Context, state keyedRetainedParentState, _ graph.Runtime) (graph.Command[keyedRetainedParentDelta], error) {
		sends := make([]graph.TaskSend, len(state.Calls))
		for index, call := range state.Calls {
			sends[index] = graph.SendTo("retained", keyedRetainedParentState{Key: call.Key, Value: call.Value})
		}
		return graph.Dispatch[keyedRetainedParentDelta](sends...), nil
	})
	err = graph.AddSubgraph(parentBuilder, "retained", child, graph.SubgraphAdapter[keyedRetainedParentState, keyedRetainedParentDelta, retainedChildState, retainedChildDelta]{
		Input: func(_ context.Context, parent keyedRetainedParentState) (retainedChildState, error) {
			return retainedChildState{Values: []string{parent.Value}}, nil
		},
		Output: func(_ context.Context, parent keyedRetainedParentState, child retainedChildState) (graph.Command[keyedRetainedParentDelta], error) {
			return graph.Update(keyedRetainedParentDelta{Key: parent.Key, Values: append([]string(nil), child.Values...)}), nil
		},
		StateCodec: checkpoint.MustJSONCodec[retainedChildState]("tests.keyed-retained-child-state", 1),
		DeltaCodec: checkpoint.MustJSONCodec[retainedChildDelta]("tests.keyed-retained-child-delta", 1),
		Stateful:   true,
		InstanceKey: func(_ context.Context, parent keyedRetainedParentState, _ graph.Runtime) (string, error) {
			return parent.Key, nil
		},
		MergeInput: func(_ context.Context, previous, input retainedChildState) (retainedChildState, error) {
			return retainedChildState{Values: append(append([]string(nil), previous.Values...), input.Values...)}, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	_ = parentBuilder.AddEdge(graph.START, "dispatch")
	_ = parentBuilder.AddCommandDestinations("dispatch", "retained")
	_ = parentBuilder.AddEdge("retained", graph.END)
	saver := memory.NewSaver()
	parent, err := parentBuilder.Compile(graph.WithPersistence(graph.PersistenceConfig[keyedRetainedParentState, keyedRetainedParentDelta]{
		Saver:      saver,
		StateCodec: checkpoint.MustJSONCodec[keyedRetainedParentState]("tests.keyed-retained-parent-state", 1),
		DeltaCodec: checkpoint.MustJSONCodec[keyedRetainedParentDelta]("tests.keyed-retained-parent-delta", 1),
	}))
	if err != nil {
		t.Fatal(err)
	}

	config := graph.RunConfig{ThreadID: "parallel-keyed-retained", MaxConcurrency: 2}
	first, err := parent.Invoke(context.Background(), keyedRetainedParentState{Calls: []keyedRetainedCall{
		{Key: "alpha|north", Value: "a1"}, {Key: "beta/south", Value: "b1"},
	}}, config)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(first.Results["alpha|north"], []string{"a1", "processed"}) ||
		!reflect.DeepEqual(first.Results["beta/south"], []string{"b1", "processed"}) {
		t.Fatalf("first results=%v", first.Results)
	}

	second, err := parent.Invoke(context.Background(), keyedRetainedParentState{Calls: []keyedRetainedCall{
		{Key: "alpha|north", Value: "a2"}, {Key: "beta/south", Value: "b2"},
	}}, graph.RunConfig{ThreadID: config.ThreadID, NewRun: true, MaxConcurrency: 2})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(second.Results["alpha|north"], []string{"a1", "processed", "a2", "processed"}) ||
		!reflect.DeepEqual(second.Results["beta/south"], []string{"b1", "processed", "b2", "processed"}) {
		t.Fatalf("second results=%v", second.Results)
	}

	tuples, err := saver.List(context.Background(), checkpoint.ListOptions{
		Config: &checkpoint.Config{ThreadID: config.ThreadID}, AllNamespaces: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	namespaces := map[string]struct{}{}
	for _, tuple := range tuples {
		if tuple.Config.Namespace != "" {
			namespaces[tuple.Config.Namespace] = struct{}{}
		}
	}
	if len(namespaces) != 2 {
		t.Fatalf("keyed stateful namespaces=%v", namespaces)
	}
	for namespace := range namespaces {
		if !strings.HasPrefix(namespace, "retained:instance:") || strings.Contains(namespace, "|") {
			t.Fatalf("unsafe or unexpected instance namespace %q", namespace)
		}
	}

	_, err = parent.Invoke(context.Background(), keyedRetainedParentState{Calls: []keyedRetainedCall{
		{Key: "", Value: "invalid"},
	}}, graph.RunConfig{ThreadID: config.ThreadID, NewRun: true})
	if err == nil || !strings.Contains(err.Error(), "instance key is empty") {
		t.Fatalf("empty InstanceKey err=%v", err)
	}
}

func TestSubgraphInstanceKeyRequiresStatefulMode(t *testing.T) {
	childBuilder := graph.NewStateGraph(func(_ context.Context, state retainedChildState, _ []retainedChildDelta) (retainedChildState, error) {
		return state, nil
	})
	_ = childBuilder.AddNode("done", func(_ context.Context, _ retainedChildState, _ graph.Runtime) (graph.Command[retainedChildDelta], error) {
		return graph.Update(retainedChildDelta{}), nil
	})
	_ = childBuilder.AddEdge(graph.START, "done")
	_ = childBuilder.AddEdge("done", graph.END)
	child, err := childBuilder.Compile()
	if err != nil {
		t.Fatal(err)
	}
	parentBuilder := graph.NewStateGraph(func(_ context.Context, state retainedParentState, _ []retainedParentDelta) (retainedParentState, error) {
		return state, nil
	})
	err = graph.AddSubgraph(parentBuilder, "child", child, graph.SubgraphAdapter[retainedParentState, retainedParentDelta, retainedChildState, retainedChildDelta]{
		Input: func(_ context.Context, _ retainedParentState) (retainedChildState, error) {
			return retainedChildState{}, nil
		},
		Output: func(_ context.Context, _ retainedParentState, _ retainedChildState) (graph.Command[retainedParentDelta], error) {
			return graph.Update(retainedParentDelta{}), nil
		},
		InstanceKey: func(_ context.Context, _ retainedParentState, _ graph.Runtime) (string, error) {
			return "key", nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	_ = parentBuilder.AddEdge(graph.START, "child")
	_ = parentBuilder.AddEdge("child", graph.END)
	_, err = parentBuilder.Compile()
	if err == nil || !strings.Contains(err.Error(), "InstanceKey requires Stateful") {
		t.Fatalf("Compile() err=%v", err)
	}
}

func equalStrings(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}
