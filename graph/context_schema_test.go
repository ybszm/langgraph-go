package graph_test

import (
	"context"
	"errors"
	"testing"
	"unicode/utf8"

	"github.com/wahanbo/langgraph-go/graph"
)

type compileContext struct{ Prefix string }
type incompatibleContext struct{ Value int }

func TestContextSchemaValidatesRunBeforeNodeExecution(t *testing.T) {
	builder := graph.NewStateGraph(customReducer)
	called := false
	if err := builder.AddNode("node", func(_ context.Context, _ customState, runtime graph.Runtime) (graph.Command[customDelta], error) {
		called = true
		dependencies, err := graph.RuntimeContext[*compileContext](runtime)
		if err != nil {
			return graph.NoCommand[customDelta](), err
		}
		return graph.Update(customDelta{Add: utf8.RuneCountInString(dependencies.Prefix)}), nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := builder.AddEdge(graph.START, "node"); err != nil {
		t.Fatal(err)
	}
	if err := builder.AddEdge("node", graph.END); err != nil {
		t.Fatal(err)
	}
	compiled, err := builder.Compile(graph.WithContextSchema[customState, customDelta, *compileContext]())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := compiled.Invoke(context.Background(), customState{}, graph.RunConfig{}); !errors.Is(err, graph.ErrRuntimeContextType) || called {
		t.Fatalf("missing context err=%v called=%v", err, called)
	}
	if _, err := compiled.Invoke(context.Background(), customState{}, graph.RunConfig{Context: &incompatibleContext{}}); !errors.Is(err, graph.ErrRuntimeContextType) || called {
		t.Fatalf("wrong context err=%v called=%v", err, called)
	}
	result, err := compiled.Invoke(context.Background(), customState{}, graph.RunConfig{Context: &compileContext{Prefix: "ok"}})
	if err != nil || result.Count != 2 || !called {
		t.Fatalf("result=%+v err=%v called=%v", result, err, called)
	}
}

func TestContextSchemaRejectsDuplicateConfiguration(t *testing.T) {
	builder := graph.NewStateGraph(customReducer)
	if err := builder.AddNode("node", func(context.Context, customState, graph.Runtime) (graph.Command[customDelta], error) {
		return graph.NoCommand[customDelta](), nil
	}); err != nil {
		t.Fatal(err)
	}
	_ = builder.AddEdge(graph.START, "node")
	_ = builder.AddEdge("node", graph.END)
	_, err := builder.Compile(
		graph.WithContextSchema[customState, customDelta, *compileContext](),
		graph.WithContextSchema[customState, customDelta, *compileContext](),
	)
	if !errors.Is(err, graph.ErrInvalidGraph) {
		t.Fatalf("Compile() err=%v", err)
	}
}

func TestContextSchemaValidatesSubgraphCompatibilityRecursively(t *testing.T) {
	childBuilder := graph.NewStateGraph(customReducer)
	if err := childBuilder.AddNode("leaf", func(context.Context, customState, graph.Runtime) (graph.Command[customDelta], error) {
		return graph.NoCommand[customDelta](), nil
	}); err != nil {
		t.Fatal(err)
	}
	_ = childBuilder.AddEdge(graph.START, "leaf")
	_ = childBuilder.AddEdge("leaf", graph.END)
	child, err := childBuilder.Compile(graph.WithContextSchema[customState, customDelta, *compileContext]())
	if err != nil {
		t.Fatal(err)
	}
	newParent := func() *graph.StateGraph[customState, customDelta] {
		parent := graph.NewStateGraph(customReducer)
		if err := graph.AddSubgraph(parent, "child", child, graph.SubgraphAdapter[customState, customDelta, customState, customDelta]{
			Input: func(context.Context, customState) (customState, error) { return customState{}, nil },
			Output: func(context.Context, customState, customState) (graph.Command[customDelta], error) {
				return graph.NoCommand[customDelta](), nil
			},
		}); err != nil {
			t.Fatal(err)
		}
		_ = parent.AddEdge(graph.START, "child")
		_ = parent.AddEdge("child", graph.END)
		return parent
	}
	if _, err := newParent().Compile(); !errors.Is(err, graph.ErrSubgraphValidation) {
		t.Fatalf("missing parent schema err=%v", err)
	}
	if _, err := newParent().Compile(graph.WithContextSchema[customState, customDelta, *incompatibleContext]()); !errors.Is(err, graph.ErrSubgraphValidation) {
		t.Fatalf("incompatible schema err=%v", err)
	}
	compiled, err := newParent().Compile(graph.WithContextSchema[customState, customDelta, *compileContext]())
	if err != nil {
		t.Fatalf("compatible schema err=%v", err)
	}
	if _, err := compiled.Invoke(context.Background(), customState{}, graph.RunConfig{Context: &compileContext{}}); err != nil {
		t.Fatalf("Invoke() err=%v", err)
	}
}
