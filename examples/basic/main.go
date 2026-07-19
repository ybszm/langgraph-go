package main

import (
	"context"
	"fmt"
	"log"

	"github.com/wahanbo/langgraph-go/graph"
)

type state struct {
	Count int
}

type delta struct {
	Increment int
}

func main() {
	builder := graph.NewStateGraph(func(
		_ context.Context,
		current state,
		updates []delta,
	) (state, error) {
		for _, update := range updates {
			current.Count += update.Increment
		}
		return current, nil
	})

	err := builder.AddNode("increment", func(
		_ context.Context,
		_ state,
		_ graph.Runtime,
	) (graph.Command[delta], error) {
		return graph.Update(delta{Increment: 1}), nil
	})
	if err != nil {
		log.Fatal(err)
	}
	if err := builder.AddEdge(graph.START, "increment"); err != nil {
		log.Fatal(err)
	}
	if err := builder.AddEdge("increment", graph.END); err != nil {
		log.Fatal(err)
	}

	compiled, err := builder.Compile()
	if err != nil {
		log.Fatal(err)
	}
	result, err := compiled.Invoke(context.Background(), state{}, graph.RunConfig{})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(result.Count)
}
