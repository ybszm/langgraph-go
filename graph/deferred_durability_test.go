package graph_test

import (
	"context"
	"errors"
	"testing"

	"github.com/ybszm/langgraph-go/graph"
)

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

func TestDurabilityUnsupported(t *testing.T) {
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
	_, err = compiled.Invoke(context.Background(), testState{}, graph.RunConfig{Durability: graph.DurabilityAsync})
	if !errors.Is(err, graph.ErrUnsupportedDurability) {
		t.Fatalf("err=%v", err)
	}
}
