package main

import (
	"context"
	"fmt"

	"github.com/ybszm/langgraph-go/graph"
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
	must(b.AddNode("work", func(_ context.Context, _ state, runtime graph.Runtime) (graph.Command[delta], error) {
		if err := runtime.WriteCustom(map[string]any{"phase": "working"}); err != nil {
			return graph.NoCommand[delta](), err
		}
		return graph.Update(delta{Add: 2}), nil
	}))
	must(b.AddEdge(graph.START, "work"))
	must(b.AddEdge("work", graph.END))
	compiled, err := b.Compile()
	must(err)
	for event := range compiled.Stream(context.Background(), state{}, graph.RunConfig{}) {
		must(event.Err)
		fmt.Printf("%s count=%d\n", event.Mode, event.State.Count)
	}
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}
