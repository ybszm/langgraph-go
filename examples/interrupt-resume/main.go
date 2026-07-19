package main

import (
	"context"
	"errors"
	"fmt"

	"github.com/ybszm/langgraph-go/checkpoint"
	checkpointmemory "github.com/ybszm/langgraph-go/checkpoint/memory"
	"github.com/ybszm/langgraph-go/graph"
)

type state struct{ Approval string }
type delta struct{ Approval string }

func main() {
	b := graph.NewStateGraph(func(_ context.Context, s state, updates []delta) (state, error) {
		for _, update := range updates {
			s.Approval = update.Approval
		}
		return s, nil
	})
	must(b.AddNode("approval", func(_ context.Context, _ state, runtime graph.Runtime) (graph.Command[delta], error) {
		answer, err := graph.AwaitResume[string](runtime, "approve deployment?")
		if err != nil {
			return graph.NoCommand[delta](), err
		}
		return graph.Update(delta{Approval: answer}), nil
	}, graph.WithDynamicInterrupts()))
	must(b.AddEdge(graph.START, "approval"))
	must(b.AddEdge("approval", graph.END))
	compiled, err := b.Compile(graph.WithPersistence(graph.PersistenceConfig[state, delta]{
		Saver: checkpointmemory.NewSaver(), StateCodec: checkpoint.MustJSONCodec[state]("approval.state", 1), DeltaCodec: checkpoint.MustJSONCodec[delta]("approval.delta", 1),
	}))
	must(err)
	config := graph.RunConfig{ThreadID: "approval-thread"}
	_, err = compiled.Invoke(context.Background(), state{}, config)
	if !errors.Is(err, graph.ErrGraphInterrupt) {
		panic(err)
	}
	resume, err := graph.Resume("approved")
	must(err)
	result, err := compiled.Resume(context.Background(), config, resume)
	must(err)
	fmt.Println(result.Approval)
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}
