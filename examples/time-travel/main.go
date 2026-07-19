package main

import (
	"context"
	"fmt"

	"github.com/wahanbo/langgraph-go/checkpoint"
	checkpointmemory "github.com/wahanbo/langgraph-go/checkpoint/memory"
	"github.com/wahanbo/langgraph-go/graph"
)

type state struct{ Count int }
type delta struct{ Add int }

func main() {
	b := graph.NewStateGraph(func(_ context.Context, s state, updates []delta) (state, error) {
		for _, u := range updates {
			s.Count += u.Add
		}
		return s, nil
	})
	must(b.AddNode("first", add(1)))
	must(b.AddNode("second", add(2)))
	must(b.AddEdge(graph.START, "first"))
	must(b.AddEdge("first", "second"))
	must(b.AddEdge("second", graph.END))
	compiled, err := b.Compile(graph.WithPersistence(graph.PersistenceConfig[state, delta]{
		Saver: checkpointmemory.NewSaver(), StateCodec: checkpoint.MustJSONCodec[state]("travel.state", 1), DeltaCodec: checkpoint.MustJSONCodec[delta]("travel.delta", 1),
	}))
	must(err)
	config := graph.RunConfig{ThreadID: "travel-thread"}
	_, err = compiled.Invoke(context.Background(), state{}, config)
	must(err)
	history, err := compiled.GetStateHistory(context.Background(), config, graph.StateHistoryOptions{})
	must(err)
	input := history[len(history)-1].Config
	branch, err := compiled.UpdateState(context.Background(), graph.RunConfig{ThreadID: config.ThreadID, CheckpointID: input.CheckpointID}, graph.StateUpdate[delta]{AsNode: "first", Delta: delta{Add: 10}})
	must(err)
	snapshot, err := compiled.GetState(context.Background(), graph.RunConfig{ThreadID: config.ThreadID, CheckpointID: branch.CheckpointID})
	must(err)
	fmt.Printf("branched count=%d next=%v\n", snapshot.Values.Count, snapshot.Next)
}

func add(n int) graph.Node[state, delta] {
	return func(context.Context, state, graph.Runtime) (graph.Command[delta], error) {
		return graph.Update(delta{Add: n}), nil
	}
}
func must(err error) {
	if err != nil {
		panic(err)
	}
}
