package distributed_test

import (
	"context"
	"testing"

	"github.com/wahanbo/langgraph-go/backend/distributed"
	"github.com/wahanbo/langgraph-go/graph"
)

func TestCompiledRouteResolverUsesGraphRoutingAndEncodesSendInput(t *testing.T) {
	builder := graph.NewStateGraph[nodeState, nodeDelta](func(_ context.Context, state nodeState, updates []nodeDelta) (nodeState, error) {
		for _, update := range updates {
			state.Value += update.Add
		}
		return state, nil
	})
	for _, node := range []graph.NodeID{"a", "b", "c"} {
		_ = builder.AddNode(node, func(context.Context, nodeState, graph.Runtime) (graph.Command[nodeDelta], error) {
			return graph.Command[nodeDelta]{}, nil
		})
	}
	_ = builder.AddEdge(graph.START, "a")
	_ = builder.AddEdge("a", "b")
	_ = builder.AddEdge("b", graph.END)
	_ = builder.AddEdge("c", graph.END)
	_ = builder.AddCommandDestinations("a", "c")
	compiled, _ := builder.Compile()
	resolver, err := distributed.NewCompiledRouteResolver(compiled, checkpointJSONCodec[nodeState]{})
	if err != nil {
		t.Fatal(err)
	}
	plan, err := resolver(context.Background(), distributed.StepInput[nodeState, nodeDelta]{
		State: nodeState{Value: 1}, Step: 2, TaskIDs: []string{"task-a"}, Nodes: []graph.NodeID{"a"},
		Commands: []graph.Command[nodeDelta]{{Sends: []graph.TaskSend{graph.SendTo("c", nodeState{Value: 9})}}},
	})
	if err != nil || len(plan.Next) != 2 || plan.Next[0].Name != "b" || plan.Next[1].Name != "c" || plan.Next[1].Input == nil {
		t.Fatalf("plan=%+v err=%v", plan, err)
	}
	decoded, err := (checkpointJSONCodec[nodeState]{}).Decode(*plan.Next[1].Input)
	if err != nil || decoded.Value != 9 {
		t.Fatalf("decoded=%+v err=%v", decoded, err)
	}
}
