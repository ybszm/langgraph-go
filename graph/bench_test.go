package graph_test

import (
	"context"
	"testing"

	"github.com/ybszm/langgraph-go/graph"
)

func BenchmarkLinearInvoke(b *testing.B) {
	builder := graph.NewStateGraph(testReducer)
	_ = builder.AddNode("n", func(context.Context, testState, graph.Runtime) (graph.Command[testDelta], error) {
		return graph.Update(testDelta{Add: 1}), nil
	})
	_ = builder.AddEdge(graph.START, "n")
	_ = builder.AddEdge("n", graph.END)
	compiled, err := builder.Compile()
	if err != nil {
		b.Fatal(err)
	}
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := compiled.Invoke(ctx, testState{}, graph.RunConfig{}); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkFanOutThree(b *testing.B) {
	builder := graph.NewStateGraph(testReducer)
	for _, id := range []graph.NodeID{"a", "b", "c"} {
		name := id
		_ = builder.AddNode(name, func(context.Context, testState, graph.Runtime) (graph.Command[testDelta], error) {
			return graph.Update(testDelta{Add: 1, Label: string(name)}), nil
		})
	}
	_ = builder.AddEdge(graph.START, "a")
	_ = builder.AddEdge(graph.START, "b")
	_ = builder.AddEdge(graph.START, "c")
	_ = builder.AddEdge("a", graph.END)
	_ = builder.AddEdge("b", graph.END)
	_ = builder.AddEdge("c", graph.END)
	compiled, err := builder.Compile()
	if err != nil {
		b.Fatal(err)
	}
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := compiled.Invoke(ctx, testState{}, graph.RunConfig{}); err != nil {
			b.Fatal(err)
		}
	}
}
