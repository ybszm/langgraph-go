package main

import (
	"context"
	"fmt"

	"github.com/ybszm/langgraph-go/checkpoint"
	checkpointmemory "github.com/ybszm/langgraph-go/checkpoint/memory"
	"github.com/ybszm/langgraph-go/graph"
)

type state struct{ Count int }
type delta struct{ Add int }

func main() {
	b := graph.NewStateGraph(reduce)
	must(b.AddNode("increment", func(context.Context, state, graph.Runtime) (graph.Command[delta], error) {
		return graph.Update(delta{Add: 1}), nil
	}))
	must(b.AddEdge(graph.START, "increment"))
	must(b.AddEdge("increment", graph.END))
	compiled, err := b.Compile(graph.WithPersistence(graph.PersistenceConfig[state, delta]{
		Saver:      checkpointmemory.NewSaver(),
		StateCodec: checkpoint.MustJSONCodec[state]("example.state", 1),
		DeltaCodec: checkpoint.MustJSONCodec[delta]("example.delta", 1),
	}))
	must(err)
	config := graph.RunConfig{ThreadID: "demo-thread"}
	result, err := compiled.Invoke(context.Background(), state{}, config)
	must(err)
	history, err := compiled.GetStateHistory(context.Background(), config, graph.StateHistoryOptions{})
	must(err)
	fmt.Printf("count=%d checkpoints=%d\n", result.Count, len(history))
}

func reduce(_ context.Context, s state, updates []delta) (state, error) {
	for _, u := range updates {
		s.Count += u.Add
	}
	return s, nil
}
func must(err error) {
	if err != nil {
		panic(err)
	}
}
