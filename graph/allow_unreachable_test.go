package graph_test

import (
	"context"
	"errors"
	"testing"

	"github.com/ybszm/langgraph-go/graph"
)

func TestWithAllowUnreachableNodes(t *testing.T) {
	builder := graph.NewStateGraph(testReducer)
	if err := builder.AddNode("a", func(context.Context, testState, graph.Runtime) (graph.Command[testDelta], error) {
		return graph.Update(testDelta{Add: 1}), nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := builder.AddNode("orphan", func(context.Context, testState, graph.Runtime) (graph.Command[testDelta], error) {
		return graph.Update(testDelta{Add: 9}), nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := builder.AddEdge(graph.START, "a"); err != nil {
		t.Fatal(err)
	}
	if err := builder.AddEdge("a", graph.END); err != nil {
		t.Fatal(err)
	}
	if _, err := builder.Compile(); err == nil || !errors.Is(err, graph.ErrInvalidGraph) {
		t.Fatalf("expected default reject, got %v", err)
	}
	compiled, err := builder.Compile(graph.WithAllowUnreachableNodes[testState, testDelta]())
	if err != nil {
		t.Fatal(err)
	}
	out, err := compiled.Invoke(context.Background(), testState{}, graph.RunConfig{})
	if err != nil {
		t.Fatal(err)
	}
	if out.Total != 1 {
		t.Fatalf("total=%d", out.Total)
	}
}
