package graph_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/ybszm/langgraph-go/graph"
)

type typedNodeInput struct{ Count int }
type typedNodeOutput struct{ Increment int }

func TestAddTypedNodeProjectsNodeSpecificInputAndPublishesSchema(t *testing.T) {
	builder := graph.NewStateGraph(customReducer)
	if err := graph.AddTypedNode(builder, "typed",
		func(_ context.Context, state customState) (typedNodeInput, error) {
			return typedNodeInput{Count: state.Count}, nil
		},
		func(_ context.Context, input typedNodeInput, _ graph.Runtime) (graph.Command[customDelta], error) {
			return graph.Update(customDelta{Add: input.Count + 1}), nil
		},
	); err != nil {
		t.Fatal(err)
	}
	if err := builder.AddEdge(graph.START, "typed"); err != nil {
		t.Fatal(err)
	}
	if err := builder.AddEdge("typed", graph.END); err != nil {
		t.Fatal(err)
	}
	compiled, err := builder.Compile()
	if err != nil {
		t.Fatal(err)
	}
	result, err := compiled.Invoke(context.Background(), customState{Count: 2}, graph.RunConfig{})
	if err != nil || result.Count != 5 {
		t.Fatalf("result=%+v err=%v", result, err)
	}
	view := compiled.Inspect()
	if view.StateSchema == "" || view.DeltaSchema == "" || len(view.Nodes) != 3 ||
		view.Nodes[1].InputSchema != "graph_test.typedNodeInput" || view.Nodes[1].OutputSchema != "graph_test.customDelta" {
		t.Fatalf("inspection=%+v", view)
	}
}

func TestAddTypedNodeWithOutputMapsNodeSpecificOutputAndPublishesSchema(t *testing.T) {
	builder := graph.NewStateGraph(customReducer)
	if err := graph.AddTypedNodeWithOutput(builder, "typed-output",
		func(_ context.Context, state customState) (typedNodeInput, error) {
			return typedNodeInput{Count: state.Count}, nil
		},
		func(_ context.Context, input typedNodeInput, _ graph.Runtime) (graph.Command[typedNodeOutput], error) {
			return graph.Update(typedNodeOutput{Increment: input.Count + 2}), nil
		},
		func(_ context.Context, output typedNodeOutput) (customDelta, error) {
			return customDelta{Add: output.Increment}, nil
		},
	); err != nil {
		t.Fatal(err)
	}
	_ = builder.AddEdge(graph.START, "typed-output")
	_ = builder.AddEdge("typed-output", graph.END)
	compiled, err := builder.Compile()
	if err != nil {
		t.Fatal(err)
	}
	result, err := compiled.Invoke(context.Background(), customState{Count: 3}, graph.RunConfig{})
	if err != nil || result.Count != 8 {
		t.Fatalf("result=%+v err=%v", result, err)
	}
	node := compiled.Inspect().Nodes[1]
	if node.InputSchema != "graph_test.typedNodeInput" || node.OutputSchema != "graph_test.typedNodeOutput" {
		t.Fatalf("node=%+v", node)
	}
}

func TestAddTypedNodeWithOutputClassifiesMapperError(t *testing.T) {
	want := errors.New("typed output failed")
	builder := graph.NewStateGraph(customReducer)
	if err := graph.AddTypedNodeWithOutput(builder, "typed-output",
		func(context.Context, customState) (typedNodeInput, error) { return typedNodeInput{}, nil },
		func(context.Context, typedNodeInput, graph.Runtime) (graph.Command[typedNodeOutput], error) {
			return graph.Update(typedNodeOutput{Increment: 1}), nil
		},
		func(context.Context, typedNodeOutput) (customDelta, error) { return customDelta{}, want },
	); err != nil {
		t.Fatal(err)
	}
	_ = builder.AddEdge(graph.START, "typed-output")
	_ = builder.AddEdge("typed-output", graph.END)
	compiled, err := builder.Compile()
	if err != nil {
		t.Fatal(err)
	}
	_, err = compiled.Invoke(context.Background(), customState{}, graph.RunConfig{})
	if !errors.Is(err, graph.ErrNodeOutputSchema) || !errors.Is(err, want) {
		t.Fatalf("Invoke() err=%v", err)
	}
}

func TestAddTypedNodeWithOutputRetriesMapperInsideAttempt(t *testing.T) {
	retryErr := errors.New("typed output retry")
	builder := graph.NewStateGraph(customReducer)
	mappings := 0
	if err := graph.AddTypedNodeWithOutput(builder, "typed-output",
		func(context.Context, customState) (typedNodeInput, error) { return typedNodeInput{}, nil },
		func(context.Context, typedNodeInput, graph.Runtime) (graph.Command[typedNodeOutput], error) {
			return graph.Update(typedNodeOutput{Increment: 1}), nil
		},
		func(context.Context, typedNodeOutput) (customDelta, error) {
			mappings++
			if mappings == 1 {
				return customDelta{}, retryErr
			}
			return customDelta{Add: 1}, nil
		},
		graph.WithRetryPolicies(graph.RetryPolicy{
			MaxAttempts: 2, InitialInterval: time.Nanosecond,
			RetryOn: func(err error) bool { return errors.Is(err, retryErr) },
		}),
	); err != nil {
		t.Fatal(err)
	}
	_ = builder.AddEdge(graph.START, "typed-output")
	_ = builder.AddEdge("typed-output", graph.END)
	compiled, err := builder.Compile()
	if err != nil {
		t.Fatal(err)
	}
	result, err := compiled.Invoke(context.Background(), customState{}, graph.RunConfig{})
	if err != nil || result.Count != 1 || mappings != 2 {
		t.Fatalf("result=%+v mappings=%d err=%v", result, mappings, err)
	}
}

func TestAddTypedNodeClassifiesProjectionErrorsBeforeNodeBody(t *testing.T) {
	projectionErr := errors.New("typed node input failed")
	builder := graph.NewStateGraph(customReducer)
	called := false
	if err := graph.AddTypedNode(builder, "typed",
		func(context.Context, customState) (typedNodeInput, error) { return typedNodeInput{}, projectionErr },
		func(context.Context, typedNodeInput, graph.Runtime) (graph.Command[customDelta], error) {
			called = true
			return graph.NoCommand[customDelta](), nil
		},
	); err != nil {
		t.Fatal(err)
	}
	_ = builder.AddEdge(graph.START, "typed")
	_ = builder.AddEdge("typed", graph.END)
	compiled, err := builder.Compile()
	if err != nil {
		t.Fatal(err)
	}
	_, err = compiled.Invoke(context.Background(), customState{}, graph.RunConfig{})
	if !errors.Is(err, graph.ErrNodeInputSchema) || !errors.Is(err, projectionErr) || called {
		t.Fatalf("Invoke() err=%v called=%v", err, called)
	}
}

func TestAddTypedNodeReprojectsInputForRetryAttempts(t *testing.T) {
	retryErr := errors.New("typed retry")
	builder := graph.NewStateGraph(customReducer)
	projections := 0
	if err := graph.AddTypedNode(builder, "typed",
		func(_ context.Context, state customState) (typedNodeInput, error) {
			projections++
			return typedNodeInput{Count: state.Count}, nil
		},
		func(_ context.Context, _ typedNodeInput, runtime graph.Runtime) (graph.Command[customDelta], error) {
			if runtime.Attempt == 1 {
				return graph.NoCommand[customDelta](), retryErr
			}
			return graph.NoCommand[customDelta](), nil
		},
		graph.WithRetryPolicies(graph.RetryPolicy{
			MaxAttempts: 2, InitialInterval: time.Nanosecond,
			RetryOn: func(err error) bool { return errors.Is(err, retryErr) },
		}),
	); err != nil {
		t.Fatal(err)
	}
	_ = builder.AddEdge(graph.START, "typed")
	_ = builder.AddEdge("typed", graph.END)
	compiled, err := builder.Compile()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := compiled.Invoke(context.Background(), customState{}, graph.RunConfig{}); err != nil {
		t.Fatal(err)
	}
	if projections != 2 {
		t.Fatalf("input projections=%d want=2", projections)
	}
}
