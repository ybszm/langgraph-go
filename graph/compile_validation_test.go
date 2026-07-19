package graph_test

import (
	"context"
	"errors"
	"testing"

	"github.com/ybszm/langgraph-go/checkpoint"
	checkpointmemory "github.com/ybszm/langgraph-go/checkpoint/memory"
	"github.com/ybszm/langgraph-go/graph"
)

func validationChild(t *testing.T, options ...graph.CompileOption[customState, customDelta]) *graph.CompiledGraph[customState, customDelta] {
	t.Helper()
	builder := graph.NewStateGraph(customReducer)
	if err := builder.AddNode("leaf", func(context.Context, customState, graph.Runtime) (graph.Command[customDelta], error) {
		return graph.NoCommand[customDelta](), nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := builder.AddEdge(graph.START, "leaf"); err != nil {
		t.Fatal(err)
	}
	if err := builder.AddEdge("leaf", graph.END); err != nil {
		t.Fatal(err)
	}
	child, err := builder.Compile(options...)
	if err != nil {
		t.Fatal(err)
	}
	return child
}

func validationParent(
	t *testing.T,
	child *graph.CompiledGraph[customState, customDelta],
	adapter graph.SubgraphAdapter[customState, customDelta, customState, customDelta],
) *graph.StateGraph[customState, customDelta] {
	t.Helper()
	builder := graph.NewStateGraph(customReducer)
	if adapter.Input == nil {
		adapter.Input = func(context.Context, customState) (customState, error) { return customState{}, nil }
	}
	if adapter.Output == nil {
		adapter.Output = func(context.Context, customState, customState) (graph.Command[customDelta], error) {
			return graph.NoCommand[customDelta](), nil
		}
	}
	if err := graph.AddSubgraph(builder, "child", child, adapter); err != nil {
		t.Fatal(err)
	}
	if err := builder.AddEdge(graph.START, "child"); err != nil {
		t.Fatal(err)
	}
	if err := builder.AddEdge("child", graph.END); err != nil {
		t.Fatal(err)
	}
	return builder
}

func validationPersistence() graph.CompileOption[customState, customDelta] {
	return graph.WithPersistence(graph.PersistenceConfig[customState, customDelta]{
		Saver:      checkpointmemory.NewSaver(),
		StateCodec: checkpoint.MustJSONCodec[customState]("tests/validation-state", 1),
		DeltaCodec: checkpoint.MustJSONCodec[customDelta]("tests/validation-delta", 1),
	})
}

func TestCompileRejectsInvalidStatefulSubgraphConfiguration(t *testing.T) {
	child := validationChild(t)
	t.Run("missing parent persistence", func(t *testing.T) {
		builder := validationParent(t, child, graph.SubgraphAdapter[customState, customDelta, customState, customDelta]{
			Stateful: true,
			MergeInput: func(context.Context, customState, customState) (customState, error) {
				return customState{}, nil
			},
		})
		_, err := builder.Compile()
		if !errors.Is(err, graph.ErrInvalidGraph) || !errors.Is(err, graph.ErrSubgraphValidation) {
			t.Fatalf("Compile() err=%v", err)
		}
	})

	t.Run("missing merge input", func(t *testing.T) {
		builder := validationParent(t, child, graph.SubgraphAdapter[customState, customDelta, customState, customDelta]{
			Stateful:   true,
			StateCodec: checkpoint.MustJSONCodec[customState]("tests/stateful-child-state", 1),
			DeltaCodec: checkpoint.MustJSONCodec[customDelta]("tests/stateful-child-delta", 1),
		})
		_, err := builder.Compile(validationPersistence())
		if !errors.Is(err, graph.ErrInvalidGraph) || !errors.Is(err, graph.ErrSubgraphValidation) {
			t.Fatalf("Compile() err=%v", err)
		}
	})
}

func TestCompileRejectsMissingInheritedSubgraphCodecs(t *testing.T) {
	builder := validationParent(t, validationChild(t), graph.SubgraphAdapter[customState, customDelta, customState, customDelta]{})
	_, err := builder.Compile(validationPersistence())
	if !errors.Is(err, graph.ErrInvalidGraph) || !errors.Is(err, graph.ErrSubgraphValidation) {
		t.Fatalf("Compile() err=%v", err)
	}
}

func TestCompilePropagatesNestedStaticInterruptPersistenceRequirement(t *testing.T) {
	interrupting := validationChild(t,
		validationPersistence(),
		graph.WithInterruptBefore[customState, customDelta]("leaf"),
	)
	middle := validationParent(t, interrupting, graph.SubgraphAdapter[customState, customDelta, customState, customDelta]{})
	middleCompiled, err := middle.Compile(validationPersistence())
	if err != nil {
		t.Fatal(err)
	}
	root := validationParent(t, middleCompiled, graph.SubgraphAdapter[customState, customDelta, customState, customDelta]{})
	_, err = root.Compile()
	if !errors.Is(err, graph.ErrInvalidGraph) || !errors.Is(err, graph.ErrSubgraphValidation) {
		t.Fatalf("Compile() err=%v", err)
	}
}

func TestCompileAcceptsRecursivelyValidPersistentSubgraphs(t *testing.T) {
	child := validationChild(t)
	adapter := graph.SubgraphAdapter[customState, customDelta, customState, customDelta]{
		StateCodec: checkpoint.MustJSONCodec[customState]("tests/valid-child-state", 1),
		DeltaCodec: checkpoint.MustJSONCodec[customDelta]("tests/valid-child-delta", 1),
	}
	builder := validationParent(t, child, adapter)
	if _, err := builder.Compile(validationPersistence()); err != nil {
		t.Fatalf("Compile() err=%v", err)
	}
}

func TestCompileRejectsDeclaredDynamicInterruptWithoutPersistence(t *testing.T) {
	builder := graph.NewStateGraph(customReducer)
	if err := builder.AddNode("interrupting", func(context.Context, customState, graph.Runtime) (graph.Command[customDelta], error) {
		return graph.NoCommand[customDelta](), nil
	}, graph.WithDynamicInterrupts()); err != nil {
		t.Fatal(err)
	}
	_ = builder.AddEdge(graph.START, "interrupting")
	_ = builder.AddEdge("interrupting", graph.END)
	_, err := builder.Compile()
	if !errors.Is(err, graph.ErrInvalidGraph) || !errors.Is(err, graph.ErrCheckpointerRequired) {
		t.Fatalf("Compile() err=%v", err)
	}
	compiled, err := builder.Compile(validationPersistence())
	if err != nil {
		t.Fatal(err)
	}
	if nodes := compiled.Inspect().Nodes; !nodes[1].DynamicInterrupt {
		t.Fatalf("inspection nodes=%+v", nodes)
	}
}

func TestCompilePropagatesDeclaredDynamicInterruptRequirement(t *testing.T) {
	childBuilder := graph.NewStateGraph(customReducer)
	if err := childBuilder.AddNode("leaf", func(context.Context, customState, graph.Runtime) (graph.Command[customDelta], error) {
		return graph.NoCommand[customDelta](), nil
	}, graph.WithDynamicInterrupts()); err != nil {
		t.Fatal(err)
	}
	_ = childBuilder.AddEdge(graph.START, "leaf")
	_ = childBuilder.AddEdge("leaf", graph.END)
	child, err := childBuilder.Compile(validationPersistence())
	if err != nil {
		t.Fatal(err)
	}
	parent := validationParent(t, child, graph.SubgraphAdapter[customState, customDelta, customState, customDelta]{})
	_, err = parent.Compile()
	if !errors.Is(err, graph.ErrInvalidGraph) || !errors.Is(err, graph.ErrSubgraphValidation) {
		t.Fatalf("Compile() err=%v", err)
	}
}
