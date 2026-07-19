package graph_test

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/wahanbo/langgraph-go/graph"
)

type namedBranchState struct{ Trace []string }
type namedBranchDelta struct{ Value string }

func namedBranchReducer(_ context.Context, state namedBranchState, updates []namedBranchDelta) (namedBranchState, error) {
	for _, update := range updates {
		state.Trace = append(state.Trace, update.Value)
	}
	return state, nil
}

func TestNamedConditionalBranchesMergeInDeclarationOrder(t *testing.T) {
	builder := graph.NewStateGraph(namedBranchReducer)
	for _, node := range []graph.NodeID{"source", "alpha-node", "beta-node"} {
		id := node
		if err := builder.AddNode(id, func(context.Context, namedBranchState, graph.Runtime) (graph.Command[namedBranchDelta], error) {
			if id == "source" {
				return graph.NoCommand[namedBranchDelta](), nil
			}
			return graph.Update(namedBranchDelta{Value: string(id)}), nil
		}); err != nil {
			t.Fatal(err)
		}
	}
	_ = builder.AddEdge(graph.START, "source")
	if err := builder.AddNamedConditionalEdges("source", "alpha", func(context.Context, namedBranchState) ([]graph.NodeID, error) {
		return []graph.NodeID{"alpha-node"}, nil
	}, "alpha-node"); err != nil {
		t.Fatal(err)
	}
	if err := builder.AddNamedConditionalEdges("source", "beta", func(context.Context, namedBranchState) ([]graph.NodeID, error) {
		return []graph.NodeID{"beta-node"}, nil
	}, "beta-node"); err != nil {
		t.Fatal(err)
	}
	_ = builder.AddEdge("alpha-node", graph.END)
	_ = builder.AddEdge("beta-node", graph.END)
	compiled, err := builder.Compile()
	if err != nil {
		t.Fatal(err)
	}
	state, err := compiled.Invoke(context.Background(), namedBranchState{}, graph.RunConfig{})
	if err != nil || !reflect.DeepEqual(state.Trace, []string{"alpha-node", "beta-node"}) {
		t.Fatalf("state=%+v err=%v", state, err)
	}
	branches := map[string]graph.NodeID{}
	for _, edge := range compiled.Inspect().Edges {
		if edge.Kind == graph.GraphEdgeConditional {
			branches[edge.Branch] = edge.Target
		}
	}
	if !reflect.DeepEqual(branches, map[string]graph.NodeID{"alpha": "alpha-node", "beta": "beta-node"}) {
		t.Fatalf("branches=%v", branches)
	}
}

func TestNamedConditionalBranchRejectsDuplicateNameAndReportsFailingBranch(t *testing.T) {
	builder := graph.NewStateGraph(namedBranchReducer)
	_ = builder.AddNode("source", func(context.Context, namedBranchState, graph.Runtime) (graph.Command[namedBranchDelta], error) {
		return graph.NoCommand[namedBranchDelta](), nil
	})
	_ = builder.AddNode("target", func(context.Context, namedBranchState, graph.Runtime) (graph.Command[namedBranchDelta], error) {
		return graph.NoCommand[namedBranchDelta](), nil
	})
	_ = builder.AddEdge(graph.START, "source")
	want := errors.New("branch failed")
	if err := builder.AddNamedConditionalEdges("source", "decision", func(context.Context, namedBranchState) ([]graph.NodeID, error) {
		return nil, want
	}, "target"); err != nil {
		t.Fatal(err)
	}
	if err := builder.AddNamedConditionalEdges("source", "decision", func(context.Context, namedBranchState) ([]graph.NodeID, error) {
		return []graph.NodeID{"target"}, nil
	}, "target"); !errors.Is(err, graph.ErrInvalidGraph) {
		t.Fatalf("duplicate err=%v", err)
	}
	_ = builder.AddEdge("target", graph.END)
	compiled, err := builder.Compile()
	if err != nil {
		t.Fatal(err)
	}
	_, err = compiled.Invoke(context.Background(), namedBranchState{}, graph.RunConfig{})
	var routerErr *graph.RouterError
	if !errors.As(err, &routerErr) || routerErr.Branch != "decision" || !errors.Is(err, want) || !strings.Contains(err.Error(), "decision") {
		t.Fatalf("Invoke() err=%v", err)
	}
}
