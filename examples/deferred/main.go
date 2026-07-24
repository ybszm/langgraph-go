package main

import (
	"context"
	"fmt"

	"github.com/ybszm/langgraph-go/graph"
)

type state struct {
	Count int
	Path  []string
}

type delta struct {
	Add   int
	Label string
}

func main() {
	builder := graph.NewStateGraph(reduce)
	must(builder.AddNode("root", func(context.Context, state, graph.Runtime) (graph.Command[delta], error) {
		return graph.Update(delta{Add: 1, Label: "root"}), nil
	}))
	must(builder.AddNode("tail", func(context.Context, state, graph.Runtime) (graph.Command[delta], error) {
		return graph.Update(delta{Add: 10, Label: "tail"}), nil
	}))
	must(builder.AddNode("final", func(_ context.Context, current state, _ graph.Runtime) (graph.Command[delta], error) {
		return graph.Update(delta{Add: current.Count, Label: "final"}), nil
	}, graph.WithDeferred()))
	must(builder.AddEdge(graph.START, "root"))
	must(builder.AddEdge("root", "final"))
	must(builder.AddEdge("root", "tail"))
	must(builder.AddEdge("tail", graph.END))
	must(builder.AddEdge("final", graph.END))

	compiled, err := builder.Compile()
	must(err)
	result, err := compiled.Invoke(context.Background(), state{}, graph.RunConfig{})
	must(err)
	fmt.Printf("count=%d path=%v\n", result.Count, result.Path)
	// count=22 path=[root tail final]
}

func reduce(_ context.Context, current state, updates []delta) (state, error) {
	current.Path = append([]string(nil), current.Path...)
	for _, update := range updates {
		current.Count += update.Add
		current.Path = append(current.Path, update.Label)
	}
	return current, nil
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}
