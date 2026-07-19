package main

import (
	"context"
	"fmt"

	"github.com/wahanbo/langgraph-go/graph"
)

type state struct{ Count int }
type delta struct{ Add int }

func main() {
	b := graph.NewStateGraph(func(_ context.Context, s state, updates []delta) (state, error) {
		for _, update := range updates {
			s.Count += update.Add
		}
		return s, nil
	})
	must(b.AddNode("increment", func(context.Context, state, graph.Runtime) (graph.Command[delta], error) {
		return graph.Update(delta{Add: 1}), nil
	}))
	must(b.AddEdge(graph.START, "increment"))
	must(b.AddConditionalEdges("increment", func(_ context.Context, s state) ([]graph.NodeID, error) {
		if s.Count < 3 {
			return []graph.NodeID{"increment"}, nil
		}
		return []graph.NodeID{graph.END}, nil
	}, "increment", graph.END))
	compiled, err := b.Compile()
	must(err)
	result, err := compiled.Invoke(context.Background(), state{}, graph.RunConfig{})
	must(err)
	fmt.Println(result.Count) // 3
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}
