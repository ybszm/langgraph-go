package graph_test

import (
	"context"
	"errors"
	"testing"

	"github.com/wahanbo/langgraph-go/checkpoint"
	checkpointmemory "github.com/wahanbo/langgraph-go/checkpoint/memory"
	"github.com/wahanbo/langgraph-go/graph"
)

type schemaInput struct{ Seed int }
type schemaState struct {
	Total   int
	Private string
}
type schemaDelta struct{ Add int }
type schemaOutput struct{ Total int }

func schemaReducer(_ context.Context, state schemaState, updates []schemaDelta) (schemaState, error) {
	for _, update := range updates {
		state.Total += update.Add
	}
	return state, nil
}

func TestSchemaGraphSeparatesPublicInputInternalStateAndOutput(t *testing.T) {
	builder := graph.NewStateGraph(schemaReducer)
	if err := builder.AddNode("node", func(context.Context, schemaState, graph.Runtime) (graph.Command[schemaDelta], error) {
		return graph.Update(schemaDelta{Add: 2}), nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := builder.AddEdge(graph.START, "node"); err != nil {
		t.Fatal(err)
	}
	if err := builder.AddEdge("node", graph.END); err != nil {
		t.Fatal(err)
	}
	compiled, err := graph.CompileSchemaGraph(builder, graph.SchemaAdapter[schemaInput, schemaState, schemaOutput]{
		Input: func(_ context.Context, input schemaInput) (schemaState, error) {
			return schemaState{Total: input.Seed, Private: "internal"}, nil
		},
		Output: func(_ context.Context, state schemaState) (schemaOutput, error) {
			return schemaOutput{Total: state.Total}, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	result, err := compiled.Invoke(context.Background(), schemaInput{Seed: 3}, graph.RunConfig{})
	if err != nil || result != (schemaOutput{Total: 5}) {
		t.Fatalf("Invoke() result=%+v err=%v", result, err)
	}
}

func TestSchemaGraphResumeMapsDurableInternalStateToPublicOutput(t *testing.T) {
	builder := graph.NewStateGraph(schemaReducer)
	if err := builder.AddNode("human", func(_ context.Context, _ schemaState, runtime graph.Runtime) (graph.Command[schemaDelta], error) {
		_, err := graph.AwaitResume[string](runtime, "continue?")
		if err != nil {
			return graph.NoCommand[schemaDelta](), err
		}
		return graph.Update(schemaDelta{Add: 4}), nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := builder.AddEdge(graph.START, "human"); err != nil {
		t.Fatal(err)
	}
	if err := builder.AddEdge("human", graph.END); err != nil {
		t.Fatal(err)
	}
	saver := checkpointmemory.NewSaver()
	compiled, err := graph.CompileSchemaGraph(
		builder,
		graph.SchemaAdapter[schemaInput, schemaState, schemaOutput]{
			Input: func(_ context.Context, input schemaInput) (schemaState, error) {
				return schemaState{Total: input.Seed, Private: "durable-only"}, nil
			},
			Output: func(_ context.Context, state schemaState) (schemaOutput, error) {
				return schemaOutput{Total: state.Total}, nil
			},
		},
		graph.WithPersistence(graph.PersistenceConfig[schemaState, schemaDelta]{
			Saver:      saver,
			StateCodec: checkpoint.MustJSONCodec[schemaState]("tests/schema-state", 1),
			DeltaCodec: checkpoint.MustJSONCodec[schemaDelta]("tests/schema-delta", 1),
		}),
	)
	if err != nil {
		t.Fatal(err)
	}
	config := graph.RunConfig{ThreadID: "schema-resume"}
	if _, err := compiled.Invoke(context.Background(), schemaInput{Seed: 6}, config); !errors.Is(err, graph.ErrGraphInterrupt) {
		t.Fatalf("Invoke() err=%v", err)
	}
	command, _ := graph.Resume("yes")
	result, err := compiled.Resume(context.Background(), config, command)
	if err != nil || result != (schemaOutput{Total: 10}) {
		t.Fatalf("Resume() result=%+v err=%v", result, err)
	}
	snapshot, err := compiled.Graph().GetState(context.Background(), config)
	if err != nil || snapshot.Values != (schemaState{Total: 10, Private: "durable-only"}) {
		t.Fatalf("internal snapshot=%+v err=%v", snapshot.Values, err)
	}
}

func TestSchemaGraphClassifiesAdapterErrors(t *testing.T) {
	inputErr := errors.New("bad public input")
	outputErr := errors.New("bad public output")
	newBuilder := func() *graph.StateGraph[schemaState, schemaDelta] {
		builder := graph.NewStateGraph(schemaReducer)
		if err := builder.AddNode("node", func(context.Context, schemaState, graph.Runtime) (graph.Command[schemaDelta], error) {
			return graph.Update(schemaDelta{Add: 1}), nil
		}); err != nil {
			t.Fatal(err)
		}
		if err := builder.AddEdge(graph.START, "node"); err != nil {
			t.Fatal(err)
		}
		if err := builder.AddEdge("node", graph.END); err != nil {
			t.Fatal(err)
		}
		return builder
	}

	inputGraph, err := graph.CompileSchemaGraph(newBuilder(), graph.SchemaAdapter[schemaInput, schemaState, schemaOutput]{
		Input:  func(context.Context, schemaInput) (schemaState, error) { return schemaState{}, inputErr },
		Output: func(context.Context, schemaState) (schemaOutput, error) { return schemaOutput{}, nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := inputGraph.Invoke(context.Background(), schemaInput{}, graph.RunConfig{}); !errors.Is(err, graph.ErrSchemaAdapter) || !errors.Is(err, inputErr) {
		t.Fatalf("input error=%v", err)
	}

	outputGraph, err := graph.CompileSchemaGraph(newBuilder(), graph.SchemaAdapter[schemaInput, schemaState, schemaOutput]{
		Input:  func(context.Context, schemaInput) (schemaState, error) { return schemaState{}, nil },
		Output: func(context.Context, schemaState) (schemaOutput, error) { return schemaOutput{}, outputErr },
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := outputGraph.Invoke(context.Background(), schemaInput{}, graph.RunConfig{}); !errors.Is(err, graph.ErrSchemaAdapter) || !errors.Is(err, outputErr) {
		t.Fatalf("output error=%v", err)
	}

	if _, err := graph.CompileSchemaGraph(newBuilder(), graph.SchemaAdapter[schemaInput, schemaState, schemaOutput]{}); !errors.Is(err, graph.ErrInvalidGraph) {
		t.Fatalf("nil adapter compile error=%v", err)
	}
}

func TestSchemaGraphProjectsStateBearingStreamEvents(t *testing.T) {
	builder := graph.NewStateGraph(schemaReducer)
	if err := builder.AddNode("node", func(context.Context, schemaState, graph.Runtime) (graph.Command[schemaDelta], error) {
		return graph.Update(schemaDelta{Add: 2}), nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := builder.AddEdge(graph.START, "node"); err != nil {
		t.Fatal(err)
	}
	if err := builder.AddEdge("node", graph.END); err != nil {
		t.Fatal(err)
	}
	compiled, err := graph.CompileSchemaGraph(builder, graph.SchemaAdapter[schemaInput, schemaState, schemaOutput]{
		Input: func(_ context.Context, input schemaInput) (schemaState, error) {
			return schemaState{Total: input.Seed, Private: "hidden"}, nil
		},
		Output: func(_ context.Context, state schemaState) (schemaOutput, error) {
			return schemaOutput{Total: state.Total}, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	var values []schemaOutput
	for event := range compiled.StreamWithOptions(context.Background(), schemaInput{Seed: 3}, graph.RunConfig{}, graph.StreamOptions{
		Modes: []graph.StreamMode{graph.StreamValues, graph.StreamDone},
	}) {
		if event.Err != nil {
			t.Fatal(event.Err)
		}
		if event.Mode == graph.StreamValues || event.Mode == graph.StreamDone {
			values = append(values, event.State)
		}
	}
	if len(values) != 3 || values[0] != (schemaOutput{Total: 3}) || values[len(values)-1] != (schemaOutput{Total: 5}) {
		t.Fatalf("stream values=%+v", values)
	}
}
