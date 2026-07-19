package graph_test

import (
	"context"
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	"github.com/wahanbo/langgraph-go/graph"
)

type inspectionSchemaState struct {
	Name   string                 `json:"name" jsonschema:"description=display name"`
	Count  int                    `json:"count,omitempty"`
	Hidden string                 `json:"-"`
	Next   *inspectionSchemaState `json:"next,omitempty"`
}

type inspectionSchemaDelta struct {
	Add int `json:"add"`
}

func TestInspectReturnsDeterministicDeclaredTopology(t *testing.T) {
	builder := graph.NewStateGraph(customReducer)
	for _, name := range []graph.NodeID{"e", "d", "c", "b", "a"} {
		_ = builder.AddNode(name, func(context.Context, customState, graph.Runtime) (graph.Command[customDelta], error) {
			return graph.NoCommand[customDelta](), nil
		})
	}
	_ = builder.AddEdge(graph.START, "a")
	_ = builder.AddConditionalEdges("a", func(context.Context, customState) ([]graph.NodeID, error) { return []graph.NodeID{"b"}, nil }, "c", "b")
	_ = builder.AddCommandDestinations("a", "d")
	_ = builder.AddSendEdges("a", func(context.Context, customState) ([]graph.Send[customState], error) { return nil, nil }, "e")
	_ = builder.AddWaitingEdge([]graph.NodeID{"b", "c"}, "d")
	for _, name := range []graph.NodeID{"b", "c", "d", "e"} {
		_ = builder.AddEdge(name, graph.END)
	}
	compiled, err := builder.Compile()
	if err != nil {
		t.Fatal(err)
	}
	view := compiled.Inspect()
	wantNodes := []graph.GraphNodeInfo{
		{ID: graph.START, Virtual: true},
		{ID: "a"}, {ID: "b"}, {ID: "c"}, {ID: "d"}, {ID: "e"},
		{ID: graph.END, Virtual: true},
	}
	if !reflect.DeepEqual(view.Nodes, wantNodes) {
		t.Fatalf("nodes=%+v", view.Nodes)
	}
	wantEdges := []graph.GraphEdgeInfo{
		{Sources: []graph.NodeID{graph.START}, Target: "a", Kind: graph.GraphEdgeStatic},
		{Sources: []graph.NodeID{"a"}, Target: "b", Kind: graph.GraphEdgeConditional, Branch: "condition"},
		{Sources: []graph.NodeID{"a"}, Target: "c", Kind: graph.GraphEdgeConditional, Branch: "condition"},
		{Sources: []graph.NodeID{"a"}, Target: "d", Kind: graph.GraphEdgeCommand},
		{Sources: []graph.NodeID{"a"}, Target: "e", Kind: graph.GraphEdgeSend},
		{Sources: []graph.NodeID{"b"}, Target: graph.END, Kind: graph.GraphEdgeStatic},
		{Sources: []graph.NodeID{"b", "c"}, Target: "d", Kind: graph.GraphEdgeWaiting},
		{Sources: []graph.NodeID{"c"}, Target: graph.END, Kind: graph.GraphEdgeStatic},
		{Sources: []graph.NodeID{"d"}, Target: graph.END, Kind: graph.GraphEdgeStatic},
		{Sources: []graph.NodeID{"e"}, Target: graph.END, Kind: graph.GraphEdgeStatic},
	}
	if !reflect.DeepEqual(view.Edges, wantEdges) {
		t.Fatalf("edges=%+v", view.Edges)
	}
	view.Edges[0].Sources[0] = "mutated"
	if reflect.DeepEqual(compiled.Inspect().Edges, view.Edges) {
		t.Fatal("inspection edges alias compiled topology")
	}
}

func TestInspectPublishesDetachedJSONSchemaFieldMetadata(t *testing.T) {
	builder := graph.NewStateGraph(func(_ context.Context, state inspectionSchemaState, updates []inspectionSchemaDelta) (inspectionSchemaState, error) {
		for _, update := range updates {
			state.Count += update.Add
		}
		return state, nil
	})
	if err := graph.AddTypedNodeWithOutput(builder, "typed",
		func(context.Context, inspectionSchemaState) (typedNodeInput, error) { return typedNodeInput{}, nil },
		func(context.Context, typedNodeInput, graph.Runtime) (graph.Command[typedNodeOutput], error) {
			return graph.NoCommand[typedNodeOutput](), nil
		},
		func(context.Context, typedNodeOutput) (inspectionSchemaDelta, error) {
			return inspectionSchemaDelta{}, nil
		},
	); err != nil {
		t.Fatal(err)
	}
	_ = builder.AddEdge(graph.START, "typed")
	_ = builder.AddEdge("typed", graph.END)
	compiled, err := builder.Compile()
	if err != nil {
		t.Fatal(err)
	}
	view := compiled.Inspect()
	var schema map[string]any
	if err := json.Unmarshal(view.StateJSONSchema, &schema); err != nil {
		t.Fatal(err)
	}
	properties, ok := schema["properties"].(map[string]any)
	if !ok || properties["name"] == nil || properties["count"] == nil || properties["next"] == nil || properties["Hidden"] != nil {
		t.Fatalf("state schema=%s", view.StateJSONSchema)
	}
	name := properties["name"].(map[string]any)
	if name["description"] != "display name" || !reflect.DeepEqual(schema["required"], []any{"name"}) {
		t.Fatalf("state schema=%s", view.StateJSONSchema)
	}
	if len(view.Nodes[1].InputJSONSchema) == 0 || len(view.Nodes[1].OutputJSONSchema) == 0 || len(view.DeltaJSONSchema) == 0 {
		t.Fatalf("inspection=%+v", view)
	}
	view.StateJSONSchema[0] = '!'
	if compiled.Inspect().StateJSONSchema[0] == '!' {
		t.Fatal("JSON schema aliases a prior inspection")
	}
}

func TestInspectMarksSubgraphWithoutFlatteningInternals(t *testing.T) {
	childBuilder := graph.NewStateGraph(customReducer)
	_ = childBuilder.AddNode("leaf", func(context.Context, customState, graph.Runtime) (graph.Command[customDelta], error) {
		return graph.NoCommand[customDelta](), nil
	})
	_ = childBuilder.AddEdge(graph.START, "leaf")
	_ = childBuilder.AddEdge("leaf", graph.END)
	child, err := childBuilder.Compile()
	if err != nil {
		t.Fatal(err)
	}
	parentBuilder := graph.NewStateGraph(customReducer)
	err = graph.AddSubgraph(parentBuilder, "child", child, graph.SubgraphAdapter[customState, customDelta, customState, customDelta]{
		Input: func(context.Context, customState) (customState, error) { return customState{}, nil },
		Output: func(context.Context, customState, customState) (graph.Command[customDelta], error) {
			return graph.NoCommand[customDelta](), nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	_ = parentBuilder.AddEdge(graph.START, "child")
	_ = parentBuilder.AddEdge("child", graph.END)
	parent, err := parentBuilder.Compile()
	if err != nil {
		t.Fatal(err)
	}
	view := parent.Inspect()
	if len(view.Nodes) != 3 || view.Nodes[1].ID != "child" || !view.Nodes[1].IsSubgraph {
		t.Fatalf("nodes=%+v", view.Nodes)
	}
}

func TestInspectRecursiveReturnsDetachedChildTopology(t *testing.T) {
	childBuilder := graph.NewStateGraph(customReducer)
	_ = childBuilder.AddNode("leaf", func(context.Context, customState, graph.Runtime) (graph.Command[customDelta], error) {
		return graph.NoCommand[customDelta](), nil
	})
	_ = childBuilder.AddEdge(graph.START, "leaf")
	_ = childBuilder.AddEdge("leaf", graph.END)
	child, _ := childBuilder.Compile()
	parentBuilder := graph.NewStateGraph(customReducer)
	if err := graph.AddSubgraph(parentBuilder, "child", child, graph.SubgraphAdapter[customState, customDelta, customState, customDelta]{
		Input: func(context.Context, customState) (customState, error) { return customState{}, nil },
		Output: func(context.Context, customState, customState) (graph.Command[customDelta], error) {
			return graph.NoCommand[customDelta](), nil
		},
	}); err != nil {
		t.Fatal(err)
	}
	_ = parentBuilder.AddEdge(graph.START, "child")
	_ = parentBuilder.AddEdge("child", graph.END)
	parent, _ := parentBuilder.Compile()
	view := parent.InspectRecursive()
	if len(view.Subgraphs) != 1 || view.Subgraphs[0].Node != "child" || len(view.Subgraphs[0].Graph.Nodes) != 3 || view.Subgraphs[0].Graph.Nodes[1].ID != "leaf" {
		t.Fatalf("recursive view=%+v", view)
	}
	view.Subgraphs[0].Graph.Nodes[1].ID = "mutated"
	if parent.InspectRecursive().Subgraphs[0].Graph.Nodes[1].ID != "leaf" {
		t.Fatal("recursive inspection aliases compiled child topology")
	}
}

func TestMermaidRenderingIsStableEscapedAndIncludesWaitingAndSubgraphs(t *testing.T) {
	childBuilder := graph.NewStateGraph(customReducer)
	_ = childBuilder.AddNode("leaf\"line", func(context.Context, customState, graph.Runtime) (graph.Command[customDelta], error) {
		return graph.NoCommand[customDelta](), nil
	})
	_ = childBuilder.AddEdge(graph.START, "leaf\"line")
	_ = childBuilder.AddEdge("leaf\"line", graph.END)
	child, _ := childBuilder.Compile()
	parentBuilder := graph.NewStateGraph(customReducer)
	for _, node := range []graph.NodeID{"a", "b", "join"} {
		_ = parentBuilder.AddNode(node, func(context.Context, customState, graph.Runtime) (graph.Command[customDelta], error) {
			return graph.NoCommand[customDelta](), nil
		})
	}
	_ = graph.AddSubgraph(parentBuilder, "child", child, graph.SubgraphAdapter[customState, customDelta, customState, customDelta]{
		Input: func(context.Context, customState) (customState, error) { return customState{}, nil },
		Output: func(context.Context, customState, customState) (graph.Command[customDelta], error) {
			return graph.NoCommand[customDelta](), nil
		},
	})
	_ = parentBuilder.AddEdge(graph.START, "a")
	_ = parentBuilder.AddEdge(graph.START, "b")
	_ = parentBuilder.AddWaitingEdge([]graph.NodeID{"a", "b"}, "join")
	_ = parentBuilder.AddEdge("join", "child")
	_ = parentBuilder.AddEdge("child", graph.END)
	parent, err := parentBuilder.Compile()
	if err != nil {
		t.Fatal(err)
	}
	want := parent.InspectRecursive().Mermaid()
	for range 50 {
		if got := parent.InspectRecursive().Mermaid(); got != want {
			t.Fatalf("unstable Mermaid:\n%s\n---\n%s", want, got)
		}
	}
	for _, fragment := range []string{"flowchart TD", "waiting", "subgraph", "child", `leaf\"line`, "contains"} {
		if !strings.Contains(want, fragment) {
			t.Fatalf("Mermaid missing %q:\n%s", fragment, want)
		}
	}
}
