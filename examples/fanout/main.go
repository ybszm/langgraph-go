package main

import (
	"context"
	"fmt"

	"github.com/ybszm/langgraph-go/graph"
)

type state struct{ Path []string }
type delta struct{ Label string }

func main() {
	b := graph.NewStateGraph(func(_ context.Context, s state, updates []delta) (state, error) {
		s.Path = append([]string(nil), s.Path...)
		for _, update := range updates {
			s.Path = append(s.Path, update.Label)
		}
		return s, nil
	})
	for _, name := range []string{"left", "right"} {
		name := name
		must(b.AddNode(graph.NodeID(name), func(context.Context, state, graph.Runtime) (graph.Command[delta], error) {
			return graph.Update(delta{Label: name}), nil
		}))
		must(b.AddEdge(graph.START, graph.NodeID(name)))
		must(b.AddEdge(graph.NodeID(name), graph.END))
	}
	compiled, err := b.Compile()
	must(err)
	result, err := compiled.Invoke(context.Background(), state{}, graph.RunConfig{})
	must(err)
	fmt.Println(result.Path) // [left right], deterministic declaration order
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}
