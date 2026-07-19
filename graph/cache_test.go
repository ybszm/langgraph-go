package graph_test

import (
	"context"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"

	cachepkg "github.com/wahanbo/langgraph-go/cache"
	cachememory "github.com/wahanbo/langgraph-go/cache/memory"
	"github.com/wahanbo/langgraph-go/checkpoint"
	"github.com/wahanbo/langgraph-go/graph"
)

type countingCache struct {
	inner      cachepkg.Store
	mu         sync.Mutex
	getBatches []int
	setBatches []int
}

func (c *countingCache) Get(ctx context.Context, keys []cachepkg.Key) (map[cachepkg.Key][]byte, error) {
	c.mu.Lock()
	c.getBatches = append(c.getBatches, len(keys))
	c.mu.Unlock()
	return c.inner.Get(ctx, keys)
}
func (c *countingCache) Set(ctx context.Context, items map[cachepkg.Key]cachepkg.Item) error {
	c.mu.Lock()
	c.setBatches = append(c.setBatches, len(items))
	c.mu.Unlock()
	return c.inner.Set(ctx, items)
}
func (c *countingCache) Clear(ctx context.Context, namespaces []string) error {
	return c.inner.Clear(ctx, namespaces)
}

func TestTaskCacheReusesCommandAndMarksUpdate(t *testing.T) {
	store := cachememory.New()
	builder := graph.NewStateGraph(testReducer)
	var calls atomic.Int32
	err := builder.AddNode("cached", func(
		context.Context,
		testState,
		graph.Runtime,
	) (graph.Command[testDelta], error) {
		calls.Add(1)
		return graph.Update(testDelta{Add: 3, Label: "cached"}), nil
	}, graph.WithCachePolicy(graph.CachePolicy[testState]{}))
	if err != nil {
		t.Fatal(err)
	}
	addEdge(t, builder, graph.START, "cached")
	addEdge(t, builder, "cached", graph.END)
	compiled, err := builder.Compile(graph.WithTaskCache[testState, testDelta](
		graph.TaskCacheConfig[testDelta]{
			Store: store, Namespace: "cache-test",
			DeltaCodec: checkpoint.MustJSONCodec[testDelta]("cache/delta", 1),
		},
	))
	if err != nil {
		t.Fatal(err)
	}
	first, err := compiled.Invoke(context.Background(), testState{}, graph.RunConfig{})
	if err != nil || first.Total != 3 {
		t.Fatalf("first=%#v err=%v", first, err)
	}
	var cachedFlag bool
	var second testState
	for event := range compiled.Stream(context.Background(), testState{}, graph.RunConfig{}) {
		if event.Err != nil {
			t.Fatal(event.Err)
		}
		if event.Mode == graph.StreamUpdates {
			cachedFlag = len(event.Updates) == 1 && event.Updates[0].Cached
		}
		if event.Mode == graph.StreamDone {
			second = event.State
		}
	}
	if !cachedFlag || !reflect.DeepEqual(second, first) || calls.Load() != 1 {
		t.Fatalf("cached=%v first=%#v second=%#v calls=%d", cachedFlag, first, second, calls.Load())
	}

	third, err := compiled.Invoke(context.Background(), testState{Total: 10}, graph.RunConfig{})
	if err != nil || third.Total != 13 || calls.Load() != 2 {
		t.Fatalf("different input third=%#v calls=%d err=%v", third, calls.Load(), err)
	}
}

func TestTaskCachePreservesExplicitGoto(t *testing.T) {
	store := cachememory.New()
	builder := graph.NewStateGraph(testReducer)
	var routeCalls atomic.Int32
	if err := builder.AddNode("route", func(
		context.Context,
		testState,
		graph.Runtime,
	) (graph.Command[testDelta], error) {
		routeCalls.Add(1)
		return graph.UpdateAndGoto(testDelta{Label: "route"}, "target"), nil
	}, graph.WithCachePolicy(graph.CachePolicy[testState]{})); err != nil {
		t.Fatal(err)
	}
	addNode(t, builder, "skipped", func(context.Context, testState, graph.Runtime) (graph.Command[testDelta], error) {
		return graph.Update(testDelta{Label: "skipped"}), nil
	})
	addNode(t, builder, "target", func(context.Context, testState, graph.Runtime) (graph.Command[testDelta], error) {
		return graph.Update(testDelta{Label: "target"}), nil
	})
	addEdge(t, builder, graph.START, "route")
	addEdge(t, builder, "route", "skipped")
	addEdge(t, builder, "skipped", graph.END)
	if err := builder.AddCommandDestinations("route", "target"); err != nil {
		t.Fatal(err)
	}
	addEdge(t, builder, "target", graph.END)
	compiled, err := builder.Compile(graph.WithTaskCache[testState, testDelta](graph.TaskCacheConfig[testDelta]{
		Store: store, Namespace: "goto", DeltaCodec: checkpoint.MustJSONCodec[testDelta]("goto/delta", 1),
	}))
	if err != nil {
		t.Fatal(err)
	}
	for range 2 {
		result, invokeErr := compiled.Invoke(context.Background(), testState{}, graph.RunConfig{})
		if invokeErr != nil || !reflect.DeepEqual(result.Path, []string{"route", "target"}) {
			t.Fatalf("result=%#v err=%v", result, invokeErr)
		}
	}
	if routeCalls.Load() != 1 {
		t.Fatalf("route calls=%d", routeCalls.Load())
	}
}

func TestTaskCacheBatchesSuperstepLookupAndUpdate(t *testing.T) {
	store := &countingCache{inner: cachememory.New()}
	builder := graph.NewStateGraph(testReducer)
	var calls atomic.Int32
	for _, id := range []graph.NodeID{"a", "b"} {
		id := id
		if err := builder.AddNode(id, func(context.Context, testState, graph.Runtime) (graph.Command[testDelta], error) {
			calls.Add(1)
			return graph.Update(testDelta{Add: 1, Label: string(id)}), nil
		}, graph.WithCachePolicy(graph.CachePolicy[testState]{})); err != nil {
			t.Fatal(err)
		}
		addEdge(t, builder, graph.START, id)
		addEdge(t, builder, id, graph.END)
	}
	compiled, err := builder.Compile(graph.WithTaskCache[testState, testDelta](graph.TaskCacheConfig[testDelta]{
		Store: store, Namespace: "batch", DeltaCodec: checkpoint.MustJSONCodec[testDelta]("batch-cache/delta", 1),
	}))
	if err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if _, err := compiled.Invoke(context.Background(), testState{}, graph.RunConfig{}); err != nil {
			t.Fatal(err)
		}
	}
	store.mu.Lock()
	gets, sets := append([]int(nil), store.getBatches...), append([]int(nil), store.setBatches...)
	store.mu.Unlock()
	if calls.Load() != 2 || !reflect.DeepEqual(gets, []int{2, 2}) || !reflect.DeepEqual(sets, []int{2}) {
		t.Fatalf("calls=%d gets=%v sets=%v", calls.Load(), gets, sets)
	}
}
