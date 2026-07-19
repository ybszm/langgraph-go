package graph_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	cachememory "github.com/wahanbo/langgraph-go/cache/memory"
	"github.com/wahanbo/langgraph-go/checkpoint"
	"github.com/wahanbo/langgraph-go/graph"
)

func TestDefaultNodeTimeoutAppliesAtCompileAndExplicitWins(t *testing.T) {
	t.Run("late default applies", func(t *testing.T) {
		builder := graph.NewStateGraph(customReducer)
		_ = builder.AddNode("node", func(ctx context.Context, _ customState, _ graph.Runtime) (graph.Command[customDelta], error) {
			<-ctx.Done()
			return graph.NoCommand[customDelta](), ctx.Err()
		})
		_ = builder.AddEdge(graph.START, "node")
		_ = builder.AddEdge("node", graph.END)
		if err := builder.SetDefaultNodeTimeout(graph.NodeTimeoutPolicy{RunTimeout: 10 * time.Millisecond}); err != nil {
			t.Fatal(err)
		}
		compiled, err := builder.Compile()
		if err != nil {
			t.Fatal(err)
		}
		if _, err := compiled.Invoke(context.Background(), customState{}, graph.RunConfig{}); !errors.Is(err, graph.ErrNodeTimeout) {
			t.Fatalf("error=%v", err)
		}
	})

	t.Run("explicit wins", func(t *testing.T) {
		builder := graph.NewStateGraph(customReducer)
		_ = builder.AddNode("node", func(ctx context.Context, _ customState, _ graph.Runtime) (graph.Command[customDelta], error) {
			select {
			case <-time.After(15 * time.Millisecond):
				return graph.Update(customDelta{Add: 1}), nil
			case <-ctx.Done():
				return graph.NoCommand[customDelta](), ctx.Err()
			}
		}, graph.WithNodeTimeout(graph.NodeTimeoutPolicy{RunTimeout: 50 * time.Millisecond}))
		_ = builder.AddEdge(graph.START, "node")
		_ = builder.AddEdge("node", graph.END)
		if err := builder.SetDefaultNodeTimeout(graph.NodeTimeoutPolicy{RunTimeout: 5 * time.Millisecond}); err != nil {
			t.Fatal(err)
		}
		compiled, err := builder.Compile()
		if err != nil {
			t.Fatal(err)
		}
		result, err := compiled.Invoke(context.Background(), customState{}, graph.RunConfig{})
		if err != nil || result.Count != 1 {
			t.Fatalf("result=%+v error=%v", result, err)
		}
	})

	builder := graph.NewStateGraph(customReducer)
	if err := builder.SetDefaultNodeTimeout(graph.NodeTimeoutPolicy{IdleTimeout: -1}); !errors.Is(err, graph.ErrInvalidGraph) {
		t.Fatalf("negative default error=%v", err)
	}
}

func TestDefaultCachePolicyAppliesToPreviouslyRegisteredNode(t *testing.T) {
	builder := graph.NewStateGraph(testReducer)
	var calls atomic.Int32
	addNode(t, builder, "cached", func(context.Context, testState, graph.Runtime) (graph.Command[testDelta], error) {
		calls.Add(1)
		return graph.Update(testDelta{Add: 1, Label: "cached"}), nil
	})
	addEdge(t, builder, graph.START, "cached")
	addEdge(t, builder, "cached", graph.END)
	if err := builder.SetDefaultCachePolicy(graph.CachePolicy[testState]{}); err != nil {
		t.Fatal(err)
	}
	compiled, err := builder.Compile(graph.WithTaskCache[testState, testDelta](graph.TaskCacheConfig[testDelta]{
		Store: cachememory.New(), Namespace: "defaults", DeltaCodec: checkpoint.MustJSONCodec[testDelta]("tests.default-cache", 1),
	}))
	if err != nil {
		t.Fatal(err)
	}
	for range 2 {
		result, err := compiled.Invoke(context.Background(), testState{}, graph.RunConfig{})
		if err != nil || result.Total != 1 {
			t.Fatalf("result=%+v error=%v", result, err)
		}
	}
	if calls.Load() != 1 {
		t.Fatalf("node calls=%d", calls.Load())
	}
	if err := builder.SetDefaultCachePolicy(graph.CachePolicy[testState]{TTL: -1}); !errors.Is(err, graph.ErrInvalidGraph) {
		t.Fatalf("negative default cache error=%v", err)
	}
}

func TestExplicitCachePolicyOverridesDefault(t *testing.T) {
	builder := graph.NewStateGraph(testReducer)
	if err := builder.AddNode("cached", func(context.Context, testState, graph.Runtime) (graph.Command[testDelta], error) {
		return graph.Update(testDelta{Add: 1}), nil
	}, graph.WithCachePolicy(graph.CachePolicy[testState]{KeyFunc: func(testState) (string, error) {
		return "explicit", nil
	}})); err != nil {
		t.Fatal(err)
	}
	addEdge(t, builder, graph.START, "cached")
	addEdge(t, builder, "cached", graph.END)
	want := errors.New("default policy used")
	if err := builder.SetDefaultCachePolicy(graph.CachePolicy[testState]{KeyFunc: func(testState) (string, error) {
		return "", want
	}}); err != nil {
		t.Fatal(err)
	}
	compiled, err := builder.Compile(graph.WithTaskCache[testState, testDelta](graph.TaskCacheConfig[testDelta]{
		Store: cachememory.New(), Namespace: "explicit", DeltaCodec: checkpoint.MustJSONCodec[testDelta]("tests.explicit-cache", 1),
	}))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := compiled.Invoke(context.Background(), testState{}, graph.RunConfig{}); err != nil {
		t.Fatalf("Invoke error=%v", err)
	}
}
