package sqlite_test

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ybszm/langgraph-go/cache"
	cachesqlite "github.com/ybszm/langgraph-go/cache/sqlite"
	"github.com/ybszm/langgraph-go/checkpoint"
	"github.com/ybszm/langgraph-go/graph"
)

type graphCacheState struct {
	Input  string
	Output string
}

type graphCacheDelta struct {
	Output string
}

func TestStoreSharesDurableValuesAcrossHandlesAndEvictsTTL(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	path := filepath.Join(t.TempDir(), "cache.db")
	options := cachesqlite.Options{Clock: func() time.Time { return now }}
	first, err := cachesqlite.Open(context.Background(), path, options)
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	second, err := cachesqlite.Open(context.Background(), path, options)
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	expiring := cache.Key{Namespace: "worker", Key: "expiring"}
	stable := cache.Key{Namespace: "worker", Key: "stable"}
	other := cache.Key{Namespace: "other", Key: "stable"}
	if err := first.Set(context.Background(), map[cache.Key]cache.Item{
		expiring: {Data: []byte("short"), TTL: time.Second},
		stable:   {Data: []byte("stable")},
		other:    {Data: []byte("other")},
	}); err != nil {
		t.Fatal(err)
	}
	values, err := second.Get(context.Background(), []cache.Key{expiring, stable, other, stable})
	if err != nil || string(values[expiring]) != "short" || string(values[stable]) != "stable" {
		t.Fatalf("values=%v err=%v", values, err)
	}
	values[stable][0] = 'X'
	detached, _ := first.Get(context.Background(), []cache.Key{stable})
	if string(detached[stable]) != "stable" {
		t.Fatal("Get exposed mutable database bytes")
	}
	now = now.Add(time.Second)
	expired, err := second.Get(context.Background(), []cache.Key{expiring, stable})
	if err != nil || len(expired) != 1 || string(expired[stable]) != "stable" {
		t.Fatalf("expired=%v err=%v", expired, err)
	}
	if err := second.Clear(context.Background(), []string{"worker"}); err != nil {
		t.Fatal(err)
	}
	remaining, _ := first.Get(context.Background(), []cache.Key{stable, other})
	if len(remaining) != 1 || string(remaining[other]) != "other" {
		t.Fatalf("remaining=%v", remaining)
	}
}

func TestSetValidationIsAtomicAndIndependentHandlesWriteConcurrently(t *testing.T) {
	path := filepath.Join(t.TempDir(), "concurrent.db")
	first, _ := cachesqlite.Open(context.Background(), path, cachesqlite.Options{})
	defer first.Close()
	second, _ := cachesqlite.Open(context.Background(), path, cachesqlite.Options{})
	defer second.Close()
	good := cache.Key{Namespace: "atomic", Key: "good"}
	bad := cache.Key{Namespace: "atomic", Key: "bad"}
	if err := first.Set(context.Background(), map[cache.Key]cache.Item{
		good: {Data: []byte("must-not-commit")},
		bad:  {Data: []byte("bad"), TTL: -time.Second},
	}); err == nil {
		t.Fatal("negative TTL was accepted")
	}
	values, _ := second.Get(context.Background(), []cache.Key{good, bad})
	if len(values) != 0 {
		t.Fatalf("partial validation write=%v", values)
	}
	var wait sync.WaitGroup
	for index := 0; index < 20; index++ {
		wait.Add(1)
		go func(index int) {
			defer wait.Done()
			store := first
			if index%2 == 1 {
				store = second
			}
			key := cache.Key{Namespace: "parallel", Key: string(rune('a' + index))}
			if err := store.Set(context.Background(), map[cache.Key]cache.Item{
				key: {Data: []byte{byte(index)}},
			}); err != nil {
				t.Errorf("Set(%d): %v", index, err)
			}
		}(index)
	}
	wait.Wait()
	keys := make([]cache.Key, 20)
	for index := range keys {
		keys[index] = cache.Key{Namespace: "parallel", Key: string(rune('a' + index))}
	}
	values, err := first.Get(context.Background(), keys)
	if err != nil || len(values) != len(keys) {
		t.Fatalf("parallel values=%d err=%v", len(values), err)
	}
}

func TestSQLiteCacheSubprocessWriter(t *testing.T) {
	if os.Getenv("LANGGRAPH_CACHE_HELPER") != "1" {
		return
	}
	store, err := cachesqlite.Open(context.Background(), os.Getenv("LANGGRAPH_CACHE_PATH"), cachesqlite.Options{})
	if err != nil {
		os.Exit(2)
	}
	if err := store.Set(context.Background(), map[cache.Key]cache.Item{
		{Namespace: "process", Key: "result"}: {Data: []byte("committed")},
	}); err != nil {
		os.Exit(3)
	}
	// Deliberately skip Close to model abrupt worker termination after commit.
	os.Exit(0)
}

func TestCommittedValueSurvivesAbruptWriterProcessExit(t *testing.T) {
	path := filepath.Join(t.TempDir(), "process.db")
	command := exec.Command(os.Args[0], "-test.run=^TestSQLiteCacheSubprocessWriter$")
	command.Env = append(os.Environ(),
		"LANGGRAPH_CACHE_HELPER=1",
		"LANGGRAPH_CACHE_PATH="+path,
	)
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("helper: %v output=%s", err, output)
	}
	store, err := cachesqlite.Open(context.Background(), path, cachesqlite.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	key := cache.Key{Namespace: "process", Key: "result"}
	values, err := store.Get(context.Background(), []cache.Key{key})
	if err != nil || string(values[key]) != "committed" {
		t.Fatalf("values=%v err=%v", values, err)
	}
}

func TestIndependentCompiledGraphsReuseSQLiteTaskResult(t *testing.T) {
	path := filepath.Join(t.TempDir(), "graph-cache.db")
	firstStore, _ := cachesqlite.Open(context.Background(), path, cachesqlite.Options{})
	defer firstStore.Close()
	secondStore, _ := cachesqlite.Open(context.Background(), path, cachesqlite.Options{})
	defer secondStore.Close()
	var calls atomic.Int32
	compile := func(store cache.Store) *graph.CompiledGraph[graphCacheState, graphCacheDelta] {
		builder := graph.NewStateGraph(func(_ context.Context, state graphCacheState, updates []graphCacheDelta) (graphCacheState, error) {
			for _, update := range updates {
				state.Output = update.Output
			}
			return state, nil
		})
		if err := builder.AddNode("cached", func(_ context.Context, state graphCacheState, _ graph.Runtime) (graph.Command[graphCacheDelta], error) {
			calls.Add(1)
			return graph.Update(graphCacheDelta{Output: "computed:" + state.Input}), nil
		}, graph.WithCachePolicy(graph.CachePolicy[graphCacheState]{})); err != nil {
			t.Fatal(err)
		}
		_ = builder.AddEdge(graph.START, "cached")
		_ = builder.AddEdge("cached", graph.END)
		compiled, err := builder.Compile(graph.WithTaskCache[graphCacheState, graphCacheDelta](
			graph.TaskCacheConfig[graphCacheDelta]{
				Store: store, Namespace: "cross-process-graph",
				DeltaCodec: checkpoint.MustJSONCodec[graphCacheDelta]("tests.sqlite-cache-delta", 1),
			},
		))
		if err != nil {
			t.Fatal(err)
		}
		return compiled
	}
	first := compile(firstStore)
	second := compile(secondStore)
	input := graphCacheState{Input: "same"}
	firstResult, err := first.Invoke(context.Background(), input, graph.RunConfig{})
	if err != nil {
		t.Fatal(err)
	}
	secondResult, err := second.Invoke(context.Background(), input, graph.RunConfig{})
	if err != nil {
		t.Fatal(err)
	}
	if firstResult.Output != "computed:same" || secondResult.Output != firstResult.Output || calls.Load() != 1 {
		t.Fatalf("first=%+v second=%+v calls=%d", firstResult, secondResult, calls.Load())
	}
}
