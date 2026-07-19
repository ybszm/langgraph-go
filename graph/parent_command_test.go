package graph_test

import (
	"context"
	"errors"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"github.com/wahanbo/langgraph-go/checkpoint"
	"github.com/wahanbo/langgraph-go/checkpoint/memory"
	"github.com/wahanbo/langgraph-go/graph"
)

func TestSubgraphParentCommandUpdatesAndRoutesClosestParent(t *testing.T) {
	childBuilder := graph.NewStateGraph(customReducer)
	_ = childBuilder.AddNode("jump", func(context.Context, customState, graph.Runtime) (graph.Command[customDelta], error) {
		return graph.ParentUpdateAndGoto(customDelta{Add: 2}, "parent_second"), nil
	})
	_ = childBuilder.AddEdge(graph.START, "jump")
	_ = childBuilder.AddEdge("jump", graph.END)
	child, err := childBuilder.Compile()
	if err != nil {
		t.Fatal(err)
	}

	parentBuilder := graph.NewStateGraph(customReducer)
	outputCalled := false
	if err := graph.AddSubgraph(parentBuilder, "child", child, graph.SubgraphAdapter[customState, customDelta, customState, customDelta]{
		Input: func(_ context.Context, state customState) (customState, error) { return state, nil },
		Output: func(context.Context, customState, customState) (graph.Command[customDelta], error) {
			outputCalled = true
			return graph.NoCommand[customDelta](), nil
		},
	}); err != nil {
		t.Fatal(err)
	}
	_ = parentBuilder.AddNode("parent_second", func(context.Context, customState, graph.Runtime) (graph.Command[customDelta], error) {
		return graph.Update(customDelta{Add: 3}), nil
	})
	_ = parentBuilder.AddEdge(graph.START, "child")
	_ = parentBuilder.AddEdge("child", graph.END)
	_ = parentBuilder.AddCommandDestinations("child", "parent_second")
	_ = parentBuilder.AddEdge("parent_second", graph.END)
	parent, err := parentBuilder.Compile()
	if err != nil {
		t.Fatal(err)
	}
	result, err := parent.Invoke(context.Background(), customState{}, graph.RunConfig{})
	if err != nil || result.Count != 5 {
		t.Fatalf("result=%+v err=%v", result, err)
	}
	if outputCalled {
		t.Fatal("normal subgraph Output adapter ran after a parent command")
	}
}

type parentCommandParentState struct{ Total int }
type parentCommandParentDelta struct{ Add int }
type parentCommandChildState struct{ Text string }
type parentCommandChildDelta struct{ Text string }

func TestHeterogeneousSubgraphMapsParentCommand(t *testing.T) {
	childBuilder := graph.NewStateGraph(func(_ context.Context, state parentCommandChildState, updates []parentCommandChildDelta) (parentCommandChildState, error) {
		for _, update := range updates {
			state.Text += update.Text
		}
		return state, nil
	})
	_ = childBuilder.AddNode("jump", func(context.Context, parentCommandChildState, graph.Runtime) (graph.Command[parentCommandChildDelta], error) {
		return graph.ParentUpdateAndGoto(parentCommandChildDelta{Text: "abcd"}, "finish"), nil
	})
	_ = childBuilder.AddEdge(graph.START, "jump")
	_ = childBuilder.AddEdge("jump", graph.END)
	child, _ := childBuilder.Compile()

	parentBuilder := graph.NewStateGraph(func(_ context.Context, state parentCommandParentState, updates []parentCommandParentDelta) (parentCommandParentState, error) {
		for _, update := range updates {
			state.Total += update.Add
		}
		return state, nil
	})
	err := graph.AddSubgraph(parentBuilder, "child", child, graph.SubgraphAdapter[parentCommandParentState, parentCommandParentDelta, parentCommandChildState, parentCommandChildDelta]{
		Input: func(context.Context, parentCommandParentState) (parentCommandChildState, error) {
			return parentCommandChildState{}, nil
		},
		Output: func(context.Context, parentCommandParentState, parentCommandChildState) (graph.Command[parentCommandParentDelta], error) {
			return graph.NoCommand[parentCommandParentDelta](), nil
		},
		Parent: func(_ context.Context, _ parentCommandParentState, child graph.Command[parentCommandChildDelta]) (graph.Command[parentCommandParentDelta], error) {
			return graph.UpdateAndGoto(parentCommandParentDelta{Add: len(child.Update.Text)}, child.Goto...), nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	_ = parentBuilder.AddNode("finish", func(context.Context, parentCommandParentState, graph.Runtime) (graph.Command[parentCommandParentDelta], error) {
		return graph.Update(parentCommandParentDelta{Add: 1}), nil
	})
	_ = parentBuilder.AddEdge(graph.START, "child")
	_ = parentBuilder.AddEdge("child", graph.END)
	_ = parentBuilder.AddCommandDestinations("child", "finish")
	_ = parentBuilder.AddEdge("finish", graph.END)
	parent, err := parentBuilder.Compile()
	if err != nil {
		t.Fatal(err)
	}
	result, err := parent.Invoke(context.Background(), parentCommandParentState{}, graph.RunConfig{})
	if err != nil || result.Total != 5 {
		t.Fatalf("result=%+v err=%v", result, err)
	}
}

func TestDeepParentCommandTargetsImmediateParent(t *testing.T) {
	leafBuilder := graph.NewStateGraph(customReducer)
	_ = leafBuilder.AddNode("leaf", func(context.Context, customState, graph.Runtime) (graph.Command[customDelta], error) {
		return graph.ParentUpdateAndGoto(customDelta{Add: 1}, "middle_after"), nil
	})
	_ = leafBuilder.AddEdge(graph.START, "leaf")
	_ = leafBuilder.AddEdge("leaf", graph.END)
	leaf, _ := leafBuilder.Compile()

	middleBuilder := graph.NewStateGraph(customReducer)
	_ = graph.AddSubgraph(middleBuilder, "inner", leaf, graph.SubgraphAdapter[customState, customDelta, customState, customDelta]{
		Input: func(_ context.Context, state customState) (customState, error) { return state, nil },
		Output: func(context.Context, customState, customState) (graph.Command[customDelta], error) {
			return graph.NoCommand[customDelta](), nil
		},
	})
	_ = middleBuilder.AddNode("middle_after", func(context.Context, customState, graph.Runtime) (graph.Command[customDelta], error) {
		return graph.Update(customDelta{Add: 2}), nil
	})
	_ = middleBuilder.AddEdge(graph.START, "inner")
	_ = middleBuilder.AddEdge("inner", graph.END)
	_ = middleBuilder.AddCommandDestinations("inner", "middle_after")
	_ = middleBuilder.AddEdge("middle_after", graph.END)
	middle, err := middleBuilder.Compile()
	if err != nil {
		t.Fatal(err)
	}

	rootBuilder := graph.NewStateGraph(customReducer)
	_ = graph.AddSubgraph(rootBuilder, "middle", middle, graph.SubgraphAdapter[customState, customDelta, customState, customDelta]{
		Input: func(_ context.Context, state customState) (customState, error) { return state, nil },
		Output: func(_ context.Context, _ customState, state customState) (graph.Command[customDelta], error) {
			return graph.Update(customDelta{Add: state.Count}), nil
		},
	})
	_ = rootBuilder.AddEdge(graph.START, "middle")
	_ = rootBuilder.AddEdge("middle", graph.END)
	root, err := rootBuilder.Compile()
	if err != nil {
		t.Fatal(err)
	}
	result, err := root.Invoke(context.Background(), customState{}, graph.RunConfig{})
	if err != nil || result.Count != 3 {
		t.Fatalf("result=%+v err=%v", result, err)
	}
}

func TestTopLevelParentCommandBubblesWithoutRetry(t *testing.T) {
	builder := graph.NewStateGraph(customReducer)
	var calls atomic.Int32
	_ = builder.AddNode("jump", func(context.Context, customState, graph.Runtime) (graph.Command[customDelta], error) {
		calls.Add(1)
		return graph.ParentGoto[customDelta]("outside"), nil
	}, graph.WithRetryPolicies(graph.RetryPolicy{
		InitialInterval: time.Nanosecond,
		MaxInterval:     time.Nanosecond,
		MaxAttempts:     3,
		RetryOn:         func(error) bool { return true },
	}))
	_ = builder.AddEdge(graph.START, "jump")
	_ = builder.AddEdge("jump", graph.END)
	compiled, err := builder.Compile()
	if err != nil {
		t.Fatal(err)
	}
	_, err = compiled.Invoke(context.Background(), customState{}, graph.RunConfig{})
	if !errors.Is(err, graph.ErrParentCommand) || calls.Load() != 1 {
		t.Fatalf("err=%v calls=%d", err, calls.Load())
	}
	command, ok := graph.AsParentCommand[customDelta](err)
	if !ok || !reflect.DeepEqual(command.Goto, []graph.NodeID{"outside"}) {
		t.Fatalf("command=%+v ok=%v", command, ok)
	}
}

func TestPersistentParentCommandIsCommittedAsParentTaskResult(t *testing.T) {
	childBuilder := graph.NewStateGraph(customReducer)
	_ = childBuilder.AddNode("jump", func(context.Context, customState, graph.Runtime) (graph.Command[customDelta], error) {
		return graph.ParentUpdateAndGoto(customDelta{Add: 2}, "finish"), nil
	})
	_ = childBuilder.AddEdge(graph.START, "jump")
	_ = childBuilder.AddEdge("jump", graph.END)
	child, _ := childBuilder.Compile()
	parentBuilder := graph.NewStateGraph(customReducer)
	_ = graph.AddSubgraph(parentBuilder, "child", child, graph.SubgraphAdapter[customState, customDelta, customState, customDelta]{
		Input: func(_ context.Context, state customState) (customState, error) { return state, nil },
		Output: func(context.Context, customState, customState) (graph.Command[customDelta], error) {
			return graph.NoCommand[customDelta](), nil
		},
		StateCodec: checkpoint.MustJSONCodec[customState]("tests.parent-command-child-state", 1),
		DeltaCodec: checkpoint.MustJSONCodec[customDelta]("tests.parent-command-child-delta", 1),
	})
	_ = parentBuilder.AddNode("finish", func(context.Context, customState, graph.Runtime) (graph.Command[customDelta], error) {
		return graph.Update(customDelta{Add: 3}), nil
	})
	_ = parentBuilder.AddEdge(graph.START, "child")
	_ = parentBuilder.AddEdge("child", graph.END)
	_ = parentBuilder.AddCommandDestinations("child", "finish")
	_ = parentBuilder.AddEdge("finish", graph.END)
	parent, err := parentBuilder.Compile(graph.WithPersistence(graph.PersistenceConfig[customState, customDelta]{
		Saver:      memory.NewSaver(),
		StateCodec: checkpoint.MustJSONCodec[customState]("tests.parent-command-parent-state", 1),
		DeltaCodec: checkpoint.MustJSONCodec[customDelta]("tests.parent-command-parent-delta", 1),
	}))
	if err != nil {
		t.Fatal(err)
	}
	config := graph.RunConfig{ThreadID: "parent-command-persistence"}
	result, err := parent.Invoke(context.Background(), customState{}, config)
	if err != nil || result.Count != 5 {
		t.Fatalf("result=%+v err=%v", result, err)
	}
	history, err := parent.GetStateHistory(context.Background(), config, graph.StateHistoryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	var routed bool
	for _, snapshot := range history {
		if len(snapshot.Next) == 1 && snapshot.Next[0] == "finish" && snapshot.Values.Count == 2 {
			routed = true
		}
	}
	if !routed {
		t.Fatalf("history did not commit parent update/routing: %+v", history)
	}
}
