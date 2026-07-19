package graph_test

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/wahanbo/langgraph-go/checkpoint"
	"github.com/wahanbo/langgraph-go/checkpoint/memory"
	"github.com/wahanbo/langgraph-go/graph"
)

type runDependencies struct {
	UserID string
	Secret string
}

func TestTypedRuntimeContextSurvivesRetriesAndIsNotCheckpointed(t *testing.T) {
	dependencies := &runDependencies{UserID: "alice", Secret: "never-persist-this-secret"}
	builder := graph.NewStateGraph(customReducer)
	seen := make([]*runDependencies, 0, 2)
	if err := builder.AddNode("node", func(_ context.Context, _ customState, runtime graph.Runtime) (graph.Command[customDelta], error) {
		value, err := graph.RuntimeContext[*runDependencies](runtime)
		if err != nil {
			return graph.Command[customDelta]{}, err
		}
		seen = append(seen, value)
		if runtime.Attempt == 1 {
			return graph.Command[customDelta]{}, errors.New("retry once")
		}
		return graph.Update(customDelta{Add: 1}), nil
	}, graph.WithRetryPolicies(graph.RetryPolicy{InitialInterval: time.Nanosecond, MaxAttempts: 2, RetryOn: func(error) bool { return true }})); err != nil {
		t.Fatal(err)
	}
	_ = builder.AddEdge(graph.START, "node")
	_ = builder.AddEdge("node", graph.END)
	saver := memory.NewSaver()
	compiled, err := builder.Compile(graph.WithPersistence(graph.PersistenceConfig[customState, customDelta]{
		Saver:      saver,
		StateCodec: checkpoint.MustJSONCodec[customState]("tests.context-state", 1),
		DeltaCodec: checkpoint.MustJSONCodec[customDelta]("tests.context-delta", 1),
	}))
	if err != nil {
		t.Fatal(err)
	}
	result, err := compiled.Invoke(context.Background(), customState{}, graph.RunConfig{
		ThreadID: "context-thread", Context: dependencies,
	})
	if err != nil || result.Count != 1 {
		t.Fatalf("result=%+v err=%v", result, err)
	}
	if len(seen) != 2 || seen[0] != dependencies || seen[1] != dependencies {
		t.Fatalf("retry contexts=%p %p want=%p", seen[0], seen[1], dependencies)
	}
	tuples, err := saver.List(context.Background(), checkpoint.ListOptions{Config: &checkpoint.Config{ThreadID: "context-thread"}})
	if err != nil {
		t.Fatal(err)
	}
	encoded, _ := json.Marshal(tuples)
	if strings.Contains(string(encoded), dependencies.Secret) {
		t.Fatal("run context leaked into durable checkpoint data")
	}
}

func TestRuntimeContextTypeMismatch(t *testing.T) {
	_, err := graph.RuntimeContext[runDependencies](graph.Runtime{Context: "wrong"})
	if !errors.Is(err, graph.ErrRuntimeContextType) {
		t.Fatalf("type mismatch err=%v", err)
	}
}

func TestSubgraphInheritsRuntimeContext(t *testing.T) {
	dependencies := &runDependencies{UserID: "nested"}
	childBuilder := graph.NewStateGraph(customReducer)
	_ = childBuilder.AddNode("child", func(_ context.Context, _ customState, runtime graph.Runtime) (graph.Command[customDelta], error) {
		value, err := graph.RuntimeContext[*runDependencies](runtime)
		if err != nil {
			return graph.Command[customDelta]{}, err
		}
		if value != dependencies {
			return graph.Command[customDelta]{}, errors.New("subgraph received a different context")
		}
		return graph.NoCommand[customDelta](), nil
	})
	_ = childBuilder.AddEdge(graph.START, "child")
	_ = childBuilder.AddEdge("child", graph.END)
	child, _ := childBuilder.Compile()
	parentBuilder := graph.NewStateGraph(customReducer)
	if err := graph.AddSubgraph(parentBuilder, "sub", child, graph.SubgraphAdapter[customState, customDelta, customState, customDelta]{
		Input: func(_ context.Context, state customState) (customState, error) { return state, nil },
		Output: func(_ context.Context, _ customState, _ customState) (graph.Command[customDelta], error) {
			return graph.NoCommand[customDelta](), nil
		},
	}); err != nil {
		t.Fatal(err)
	}
	_ = parentBuilder.AddEdge(graph.START, "sub")
	_ = parentBuilder.AddEdge("sub", graph.END)
	parent, err := parentBuilder.Compile()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := parent.Invoke(context.Background(), customState{}, graph.RunConfig{Context: dependencies}); err != nil {
		t.Fatal(err)
	}
}
