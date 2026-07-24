package graph_test

import (
	"context"
	"testing"

	"github.com/ybszm/langgraph-go/graph"
)

func TestResolveTasksExposesCompiledRoutingWithoutExecution(t *testing.T) {
	builder := graph.NewStateGraph[testState, testDelta](testReducer)
	for _, node := range []graph.NodeID{"a", "b", "c"} {
		_ = builder.AddNode(node, func(context.Context, testState, graph.Runtime) (graph.Command[testDelta], error) {
			return graph.Command[testDelta]{}, nil
		})
	}
	_ = builder.AddEdge(graph.START, "a")
	_ = builder.AddEdge("a", "b")
	_ = builder.AddEdge("b", graph.END)
	_ = builder.AddEdge("c", graph.END)
	_ = builder.AddCommandDestinations("a", "c")
	compiled, err := builder.Compile()
	if err != nil {
		t.Fatal(err)
	}
	plan, err := compiled.ResolveTasks(context.Background(), 2, testState{}, []graph.CompletedTask[testDelta]{
		{Node: "a", TaskID: "task-a", Command: graph.Command[testDelta]{Sends: []graph.TaskSend{graph.SendTo("c", testState{Total: 7})}}},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Tasks) != 2 || plan.Tasks[0].Node != "b" || plan.Tasks[0].ID != "step:3:task:0:node:b" || plan.Tasks[0].HasInput ||
		plan.Tasks[1].Node != "c" || plan.Tasks[1].ID != "step:3:task:1:node:c" || !plan.Tasks[1].HasInput || plan.Tasks[1].Input.Total != 7 {
		t.Fatalf("plan=%+v", plan)
	}
}

func TestResolveTasksRejectsParentCommandAtRootAdapter(t *testing.T) {
	builder := graph.NewStateGraph[testState, testDelta](testReducer)
	_ = builder.AddNode("a", func(context.Context, testState, graph.Runtime) (graph.Command[testDelta], error) {
		return graph.Command[testDelta]{}, nil
	})
	_ = builder.AddEdge(graph.START, "a")
	_ = builder.AddEdge("a", graph.END)
	compiled, err := builder.Compile()
	if err != nil {
		t.Fatal(err)
	}
	_, err = compiled.ResolveTasks(context.Background(), 0, testState{}, []graph.CompletedTask[testDelta]{
		{Node: "a", TaskID: "task", Command: graph.ToParent(graph.Update(testDelta{Add: 1}))},
	}, nil)
	if err == nil {
		t.Fatal("expected parent command error")
	}
}

func TestResolveTasksCarriesDeferredStateUntilOrdinaryWorkDrains(t *testing.T) {
	builder := graph.NewStateGraph[testState, testDelta](testReducer)
	for _, node := range []graph.NodeID{"early", "tail"} {
		if err := builder.AddNode(node, func(context.Context, testState, graph.Runtime) (graph.Command[testDelta], error) {
			return graph.NoCommand[testDelta](), nil
		}); err != nil {
			t.Fatal(err)
		}
	}
	if err := builder.AddNode("final", func(context.Context, testState, graph.Runtime) (graph.Command[testDelta], error) {
		return graph.NoCommand[testDelta](), nil
	}, graph.WithDeferred()); err != nil {
		t.Fatal(err)
	}
	for _, edge := range [][2]graph.NodeID{
		{graph.START, "early"},
		{"early", "final"},
		{"early", "tail"},
		{"tail", graph.END},
		{"final", graph.END},
	} {
		if err := builder.AddEdge(edge[0], edge[1]); err != nil {
			t.Fatal(err)
		}
	}
	compiled, err := builder.Compile()
	if err != nil {
		t.Fatal(err)
	}

	first, err := compiled.ResolveTasks(context.Background(), 0, testState{}, []graph.CompletedTask[testDelta]{
		{Node: "early", TaskID: "task-early", Command: graph.NoCommand[testDelta]()},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(first.Tasks) != 1 || first.Tasks[0].Node != "tail" ||
		len(first.Waiting["__deferred__:final"]) != 1 {
		t.Fatalf("first plan=%+v", first)
	}
	second, err := compiled.ResolveTasks(context.Background(), 1, testState{}, []graph.CompletedTask[testDelta]{
		{Node: "tail", TaskID: first.Tasks[0].ID, Command: graph.NoCommand[testDelta]()},
	}, first.Waiting)
	if err != nil {
		t.Fatal(err)
	}
	if len(second.Tasks) != 1 || second.Tasks[0].Node != "final" || len(second.Waiting) != 0 {
		t.Fatalf("second plan=%+v", second)
	}
}
