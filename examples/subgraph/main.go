package main

import (
	"context"
	"fmt"

	"github.com/wahanbo/langgraph-go/graph"
)

type parentState struct{ Input, Output string }
type parentDelta struct{ Output string }
type childState struct{ Text string }
type childDelta struct{ Suffix string }

func main() {
	childBuilder := graph.NewStateGraph(func(_ context.Context, s childState, updates []childDelta) (childState, error) {
		for _, u := range updates {
			s.Text += u.Suffix
		}
		return s, nil
	})
	must(childBuilder.AddNode("decorate", func(context.Context, childState, graph.Runtime) (graph.Command[childDelta], error) {
		return graph.Update(childDelta{Suffix: " from child"}), nil
	}))
	must(childBuilder.AddEdge(graph.START, "decorate"))
	must(childBuilder.AddEdge("decorate", graph.END))
	child, err := childBuilder.Compile()
	must(err)
	parentBuilder := graph.NewStateGraph(func(_ context.Context, s parentState, updates []parentDelta) (parentState, error) {
		for _, u := range updates {
			s.Output = u.Output
		}
		return s, nil
	})
	must(graph.AddSubgraph(parentBuilder, "child", child, graph.SubgraphAdapter[parentState, parentDelta, childState, childDelta]{
		Input: func(_ context.Context, s parentState) (childState, error) { return childState{Text: s.Input}, nil },
		Output: func(_ context.Context, _ parentState, s childState) (graph.Command[parentDelta], error) {
			return graph.Update(parentDelta{Output: s.Text}), nil
		},
	}))
	must(parentBuilder.AddEdge(graph.START, "child"))
	must(parentBuilder.AddEdge("child", graph.END))
	parent, err := parentBuilder.Compile()
	must(err)
	result, err := parent.Invoke(context.Background(), parentState{Input: "hello"}, graph.RunConfig{})
	must(err)
	fmt.Println(result.Output)
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}
