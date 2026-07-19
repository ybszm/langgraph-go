package graph_test

import (
	"context"
	"testing"
	"time"

	"github.com/wahanbo/langgraph-go/graph"
)

type customState struct{ Count int }
type customDelta struct{ Add int }

func customReducer(_ context.Context, state customState, updates []customDelta) (customState, error) {
	for _, update := range updates {
		state.Count += update.Add
	}
	return state, nil
}

func TestRuntimeCustomStreamPreservesNodeEmissionOrder(t *testing.T) {
	builder := graph.NewStateGraph(customReducer)
	if err := builder.AddNode("writer", func(_ context.Context, _ customState, runtime graph.Runtime) (graph.Command[customDelta], error) {
		if err := runtime.WriteCustom("first"); err != nil {
			return graph.Command[customDelta]{}, err
		}
		if err := runtime.WriteCustom(map[string]any{"sequence": 2}); err != nil {
			return graph.Command[customDelta]{}, err
		}
		return graph.Update(customDelta{Add: 1}), nil
	}); err != nil {
		t.Fatal(err)
	}
	_ = builder.AddEdge(graph.START, "writer")
	_ = builder.AddEdge("writer", graph.END)
	compiled, err := builder.Compile()
	if err != nil {
		t.Fatal(err)
	}

	custom := make([]any, 0, 2)
	for event := range compiled.Stream(context.Background(), customState{}, graph.RunConfig{}) {
		if event.Mode == graph.StreamCustom {
			custom = append(custom, event.Custom)
		}
	}
	if len(custom) != 2 || custom[0] != "first" || custom[1].(map[string]any)["sequence"] != 2 {
		t.Fatalf("custom events=%#v", custom)
	}
	// The writer is intentionally a no-op outside Stream.
	if final, err := compiled.Invoke(context.Background(), customState{}, graph.RunConfig{}); err != nil || final.Count != 1 {
		t.Fatalf("Invoke final=%+v err=%v", final, err)
	}
}

func TestStreamWithOptionsFiltersModes(t *testing.T) {
	builder := graph.NewStateGraph(customReducer)
	_ = builder.AddNode("writer", func(_ context.Context, _ customState, runtime graph.Runtime) (graph.Command[customDelta], error) {
		if err := runtime.WriteCustom("only-this-mode"); err != nil {
			return graph.Command[customDelta]{}, err
		}
		return graph.Update(customDelta{Add: 1}), nil
	})
	_ = builder.AddEdge(graph.START, "writer")
	_ = builder.AddEdge("writer", graph.END)
	compiled, err := builder.Compile()
	if err != nil {
		t.Fatal(err)
	}
	events := compiled.StreamWithOptions(context.Background(), customState{}, graph.RunConfig{}, graph.StreamOptions{
		Modes: []graph.StreamMode{graph.StreamCustom}, Buffer: 4,
	})
	var collected []graph.StreamEvent[customState, customDelta]
	for event := range events {
		collected = append(collected, event)
	}
	if len(collected) != 1 || collected[0].Mode != graph.StreamCustom || collected[0].Custom != "only-this-mode" {
		t.Fatalf("filtered events=%+v", collected)
	}
}

func TestStreamWithOptionsRejectsUnknownModeAndNegativeBuffer(t *testing.T) {
	builder := graph.NewStateGraph(customReducer)
	_ = builder.AddNode("node", func(_ context.Context, _ customState, _ graph.Runtime) (graph.Command[customDelta], error) {
		return graph.NoCommand[customDelta](), nil
	})
	_ = builder.AddEdge(graph.START, "node")
	_ = builder.AddEdge("node", graph.END)
	compiled, err := builder.Compile()
	if err != nil {
		t.Fatal(err)
	}
	for _, options := range []graph.StreamOptions{
		{Modes: []graph.StreamMode{"unsupported"}},
		{Buffer: -1},
	} {
		events := compiled.StreamWithOptions(context.Background(), customState{}, graph.RunConfig{}, options)
		event, ok := <-events
		if !ok || event.Mode != graph.StreamError || event.Err == nil {
			t.Fatalf("invalid options event=%+v ok=%v", event, ok)
		}
		if _, extra := <-events; extra {
			t.Fatal("invalid stream options emitted more than one event")
		}
	}
}

func TestCustomStreamCancellationUnblocksWriter(t *testing.T) {
	builder := graph.NewStateGraph(customReducer)
	_ = builder.AddNode("writer", func(_ context.Context, _ customState, runtime graph.Runtime) (graph.Command[customDelta], error) {
		for index := 0; index < 1_000; index++ {
			if err := runtime.WriteCustom(index); err != nil {
				return graph.Command[customDelta]{}, err
			}
		}
		return graph.NoCommand[customDelta](), nil
	})
	_ = builder.AddEdge(graph.START, "writer")
	_ = builder.AddEdge("writer", graph.END)
	compiled, err := builder.Compile()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	events := compiled.Stream(ctx, customState{}, graph.RunConfig{})
	<-events // initial values event
	cancel()
	done := make(chan struct{})
	go func() {
		for range events {
		}
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("custom stream writer remained blocked after cancellation")
	}
}

func TestSubgraphCustomStreamCarriesNamespace(t *testing.T) {
	childBuilder := graph.NewStateGraph(customReducer)
	_ = childBuilder.AddNode("child_writer", func(_ context.Context, _ customState, runtime graph.Runtime) (graph.Command[customDelta], error) {
		return graph.NoCommand[customDelta](), runtime.WriteCustom("from-child")
	})
	_ = childBuilder.AddEdge(graph.START, "child_writer")
	_ = childBuilder.AddEdge("child_writer", graph.END)
	child, err := childBuilder.Compile()
	if err != nil {
		t.Fatal(err)
	}
	parentBuilder := graph.NewStateGraph(customReducer)
	err = graph.AddSubgraph(parentBuilder, "child", child, graph.SubgraphAdapter[customState, customDelta, customState, customDelta]{
		Input: func(_ context.Context, state customState) (customState, error) { return state, nil },
		Output: func(_ context.Context, _ customState, _ customState) (graph.Command[customDelta], error) {
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
	found := false
	for event := range parent.StreamWithOptions(context.Background(), customState{}, graph.RunConfig{}, graph.StreamOptions{
		Modes: []graph.StreamMode{graph.StreamCustom}, Subgraphs: true,
	}) {
		if event.Mode != graph.StreamCustom || event.Subgraph == nil {
			continue
		}
		found = true
		if len(event.Namespace) != 1 || event.Subgraph.Custom != "from-child" {
			t.Fatalf("namespaced custom event=%+v", event)
		}
	}
	if !found {
		t.Fatal("child custom event not observed")
	}
}
