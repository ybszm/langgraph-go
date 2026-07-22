package compat_test

import (
	"context"
	"errors"
	"testing"

	"github.com/ybszm/langgraph-go/compat"
	"github.com/ybszm/langgraph-go/graph"
)

func init() {
	compat.Register(compat.Scenario{
		ID:          "graph.basic_linear",
		Area:        "Typed StateGraph",
		Status:      compat.StatusSupported,
		Description: "START → node → END invoke reduces state",
		Run:         testBasicLinear,
	})
	compat.Register(compat.Scenario{
		ID:          "graph.allow_unreachable",
		Area:        "Typed StateGraph",
		Status:      compat.StatusPartial,
		Description: "WithAllowUnreachableNodes compiles offline helper nodes",
		Run:         testAllowUnreachable,
	})
	compat.Register(compat.Scenario{
		ID:          "graph.reject_unreachable_default",
		Area:        "Typed StateGraph",
		Status:      compat.StatusSupported,
		Description: "Default compile rejects unreachable nodes (stricter than Python)",
		Run:         testRejectUnreachable,
	})
}

func TestCompatibilityHarness(t *testing.T) {
	compat.RunAll(t)
}

type counterState struct{ N int }
type counterDelta struct{ Add int }

func counterReduce(_ context.Context, state counterState, updates []counterDelta) (counterState, error) {
	for _, update := range updates {
		state.N += update.Add
	}
	return state, nil
}

func testBasicLinear(t *testing.T) {
	t.Helper()
	builder := graph.NewStateGraph(counterReduce)
	if err := builder.AddNode("inc", func(context.Context, counterState, graph.Runtime) (graph.Command[counterDelta], error) {
		return graph.Update(counterDelta{Add: 1}), nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := builder.AddEdge(graph.START, "inc"); err != nil {
		t.Fatal(err)
	}
	if err := builder.AddEdge("inc", graph.END); err != nil {
		t.Fatal(err)
	}
	compiled, err := builder.Compile()
	if err != nil {
		t.Fatal(err)
	}
	out, err := compiled.Invoke(context.Background(), counterState{}, graph.RunConfig{})
	if err != nil {
		t.Fatal(err)
	}
	if out.N != 1 {
		t.Fatalf("N=%d", out.N)
	}
}

func testAllowUnreachable(t *testing.T) {
	t.Helper()
	builder := graph.NewStateGraph(counterReduce)
	if err := builder.AddNode("online", func(context.Context, counterState, graph.Runtime) (graph.Command[counterDelta], error) {
		return graph.Update(counterDelta{Add: 2}), nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := builder.AddNode("offline_helper", func(context.Context, counterState, graph.Runtime) (graph.Command[counterDelta], error) {
		return graph.Update(counterDelta{Add: 99}), nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := builder.AddEdge(graph.START, "online"); err != nil {
		t.Fatal(err)
	}
	if err := builder.AddEdge("online", graph.END); err != nil {
		t.Fatal(err)
	}
	compiled, err := builder.Compile(graph.WithAllowUnreachableNodes[counterState, counterDelta]())
	if err != nil {
		t.Fatal(err)
	}
	out, err := compiled.Invoke(context.Background(), counterState{}, graph.RunConfig{})
	if err != nil {
		t.Fatal(err)
	}
	if out.N != 2 {
		t.Fatalf("N=%d want 2 (offline not scheduled)", out.N)
	}
}

func testRejectUnreachable(t *testing.T) {
	t.Helper()
	builder := graph.NewStateGraph(counterReduce)
	if err := builder.AddNode("online", func(context.Context, counterState, graph.Runtime) (graph.Command[counterDelta], error) {
		return graph.NoCommand[counterDelta](), nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := builder.AddNode("orphan", func(context.Context, counterState, graph.Runtime) (graph.Command[counterDelta], error) {
		return graph.NoCommand[counterDelta](), nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := builder.AddEdge(graph.START, "online"); err != nil {
		t.Fatal(err)
	}
	if err := builder.AddEdge("online", graph.END); err != nil {
		t.Fatal(err)
	}
	_, err := builder.Compile()
	if err == nil || !errors.Is(err, graph.ErrInvalidGraph) {
		t.Fatalf("expected unreachable compile error, got %v", err)
	}
}
