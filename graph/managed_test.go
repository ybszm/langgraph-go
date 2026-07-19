package graph_test

import (
	"context"
	"errors"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	cachememory "github.com/wahanbo/langgraph-go/cache/memory"
	"github.com/wahanbo/langgraph-go/checkpoint"
	checkpointmemory "github.com/wahanbo/langgraph-go/checkpoint/memory"
	"github.com/wahanbo/langgraph-go/graph"
	"github.com/wahanbo/langgraph-go/managed"
)

type managedState struct {
	Total     int
	Remaining int
	Last      bool
}

type managedDelta struct{ Add int }

func managedReducer(_ context.Context, current managedState, updates []managedDelta) (managedState, error) {
	for _, update := range updates {
		current.Total += update.Add
	}
	return current, nil
}

func managedProjector(_ context.Context, state managedState, scope managed.Scope) (managedState, error) {
	state.Remaining = managed.RemainingSteps(scope)
	state.Last = managed.IsLastStep(scope)
	return state, nil
}

func TestManagedValuesAreProjectedPerStepAndExcludedFromCheckpoint(t *testing.T) {
	builder := graph.NewStateGraph(managedReducer)
	builder.SetManagedValues(managedProjector)
	var observed []managedState
	for _, node := range []graph.NodeID{"a", "b", "c"} {
		node := node
		if err := builder.AddNode(node, func(_ context.Context, state managedState, _ graph.Runtime) (graph.Command[managedDelta], error) {
			observed = append(observed, state)
			return graph.Update(managedDelta{Add: 1}), nil
		}); err != nil {
			t.Fatal(err)
		}
	}
	for _, edge := range [][2]graph.NodeID{{graph.START, "a"}, {"a", "b"}, {"b", "c"}, {"c", graph.END}} {
		if err := builder.AddEdge(edge[0], edge[1]); err != nil {
			t.Fatal(err)
		}
	}
	saver := checkpointmemory.NewSaver()
	compiled, err := builder.Compile(graph.WithPersistence(graph.PersistenceConfig[managedState, managedDelta]{
		Saver:      saver,
		StateCodec: checkpoint.MustJSONCodec[managedState]("tests/managed-state", 1),
		DeltaCodec: checkpoint.MustJSONCodec[managedDelta]("tests/managed-delta", 1),
	}))
	if err != nil {
		t.Fatal(err)
	}
	config := graph.RunConfig{ThreadID: "managed-values", RecursionLimit: 3}
	result, err := compiled.Invoke(context.Background(), managedState{}, config)
	if err != nil || result != (managedState{Total: 3}) {
		t.Fatalf("Invoke() result=%+v err=%v", result, err)
	}
	want := []managedState{
		{Remaining: 3, Last: false},
		{Total: 1, Remaining: 2, Last: false},
		{Total: 2, Remaining: 1, Last: true},
	}
	if !reflect.DeepEqual(observed, want) {
		t.Fatalf("observed=%+v want=%+v", observed, want)
	}
	snapshot, err := compiled.GetState(context.Background(), config)
	if err != nil || snapshot.Values != (managedState{Total: 3}) {
		t.Fatalf("checkpoint state=%+v err=%v", snapshot.Values, err)
	}
}

func TestManagedValuesParticipateInCacheKeysWithoutEnteringState(t *testing.T) {
	builder := graph.NewStateGraph(managedReducer)
	builder.SetManagedValues(managedProjector)
	var calls atomic.Int32
	if err := builder.AddNode("loop", func(_ context.Context, state managedState, _ graph.Runtime) (graph.Command[managedDelta], error) {
		calls.Add(1)
		if state.Last {
			return graph.Goto[managedDelta](), nil
		}
		return graph.Goto[managedDelta]("loop"), nil
	}, graph.WithCachePolicy(graph.CachePolicy[managedState]{})); err != nil {
		t.Fatal(err)
	}
	if err := builder.AddEdge(graph.START, "loop"); err != nil {
		t.Fatal(err)
	}
	if err := builder.AddEdge("loop", graph.END); err != nil {
		t.Fatal(err)
	}
	if err := builder.AddCommandDestinations("loop", "loop"); err != nil {
		t.Fatal(err)
	}
	compiled, err := builder.Compile(graph.WithTaskCache[managedState, managedDelta](graph.TaskCacheConfig[managedDelta]{
		Store: cachememory.New(), Namespace: "managed-cache",
		DeltaCodec: checkpoint.MustJSONCodec[managedDelta]("tests/managed-cache-delta", 1),
	}))
	if err != nil {
		t.Fatal(err)
	}
	for range 2 {
		result, invokeErr := compiled.Invoke(context.Background(), managedState{}, graph.RunConfig{RecursionLimit: 3})
		if invokeErr != nil || result != (managedState{}) {
			t.Fatalf("Invoke() result=%+v err=%v", result, invokeErr)
		}
	}
	if calls.Load() != 3 {
		t.Fatalf("node calls=%d want=3 distinct managed cache keys", calls.Load())
	}
}

func TestManagedValuesAreVisibleToConditionalRouters(t *testing.T) {
	builder := graph.NewStateGraph(managedReducer)
	builder.SetManagedValues(managedProjector)
	if err := builder.AddNode("loop", func(_ context.Context, _ managedState, _ graph.Runtime) (graph.Command[managedDelta], error) {
		return graph.Update(managedDelta{Add: 1}), nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := builder.AddEdge(graph.START, "loop"); err != nil {
		t.Fatal(err)
	}
	var routed []managedState
	if err := builder.AddConditionalEdges("loop", func(_ context.Context, state managedState) ([]graph.NodeID, error) {
		routed = append(routed, state)
		if state.Last {
			return []graph.NodeID{graph.END}, nil
		}
		return []graph.NodeID{"loop"}, nil
	}, "loop", graph.END); err != nil {
		t.Fatal(err)
	}
	result, err := mustCompileManaged(t, builder).Invoke(context.Background(), managedState{}, graph.RunConfig{RecursionLimit: 3})
	if err != nil || result != (managedState{Total: 3}) {
		t.Fatalf("Invoke() result=%+v err=%v", result, err)
	}
	want := []managedState{
		{Total: 1, Remaining: 3},
		{Total: 2, Remaining: 2},
		{Total: 3, Remaining: 1, Last: true},
	}
	if !reflect.DeepEqual(routed, want) {
		t.Fatalf("router states=%+v want=%+v", routed, want)
	}
}

func TestManagedValuesRemainStableAcrossRetryAndRecomputeOnResume(t *testing.T) {
	t.Run("retry", func(t *testing.T) {
		retryErr := errors.New("retry managed node")
		builder := graph.NewStateGraph(managedReducer)
		builder.SetManagedValues(managedProjector)
		var observed []int
		if err := builder.AddNode("node", func(_ context.Context, state managedState, runtime graph.Runtime) (graph.Command[managedDelta], error) {
			observed = append(observed, state.Remaining)
			if runtime.Attempt == 1 {
				return graph.NoCommand[managedDelta](), retryErr
			}
			return graph.NoCommand[managedDelta](), nil
		}, graph.WithRetryPolicies(graph.RetryPolicy{
			MaxAttempts: 2, InitialInterval: time.Nanosecond,
			RetryOn: func(err error) bool { return errors.Is(err, retryErr) },
		})); err != nil {
			t.Fatal(err)
		}
		if err := builder.AddEdge(graph.START, "node"); err != nil {
			t.Fatal(err)
		}
		if err := builder.AddEdge("node", graph.END); err != nil {
			t.Fatal(err)
		}
		if _, err := mustCompileManaged(t, builder).Invoke(context.Background(), managedState{}, graph.RunConfig{RecursionLimit: 1}); err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(observed, []int{1, 1}) {
			t.Fatalf("retry managed values=%v", observed)
		}
	})

	t.Run("resume", func(t *testing.T) {
		builder := graph.NewStateGraph(managedReducer)
		builder.SetManagedValues(managedProjector)
		var observed []int
		if err := builder.AddNode("human", func(_ context.Context, state managedState, runtime graph.Runtime) (graph.Command[managedDelta], error) {
			observed = append(observed, state.Remaining)
			_, err := graph.AwaitResume[string](runtime, "continue?")
			if err != nil {
				return graph.NoCommand[managedDelta](), err
			}
			return graph.Update(managedDelta{Add: 1}), nil
		}); err != nil {
			t.Fatal(err)
		}
		if err := builder.AddEdge(graph.START, "human"); err != nil {
			t.Fatal(err)
		}
		if err := builder.AddEdge("human", graph.END); err != nil {
			t.Fatal(err)
		}
		compiled, err := builder.Compile(graph.WithPersistence(graph.PersistenceConfig[managedState, managedDelta]{
			Saver:      checkpointmemory.NewSaver(),
			StateCodec: checkpoint.MustJSONCodec[managedState]("tests/managed-resume-state", 1),
			DeltaCodec: checkpoint.MustJSONCodec[managedDelta]("tests/managed-resume-delta", 1),
		}))
		if err != nil {
			t.Fatal(err)
		}
		config := graph.RunConfig{ThreadID: "managed-resume", RecursionLimit: 2}
		if _, err := compiled.Invoke(context.Background(), managedState{}, config); !errors.Is(err, graph.ErrGraphInterrupt) {
			t.Fatalf("Invoke() err=%v", err)
		}
		resume, _ := graph.Resume("yes")
		result, err := compiled.Resume(context.Background(), config, resume)
		if err != nil || result != (managedState{Total: 1}) {
			t.Fatalf("Resume() result=%+v err=%v", result, err)
		}
		if !reflect.DeepEqual(observed, []int{2, 2}) {
			t.Fatalf("resume managed values=%v", observed)
		}
	})
}

func TestManagedValueProjectionFailureStopsBeforeNode(t *testing.T) {
	projectionErr := errors.New("managed projection failed")
	builder := graph.NewStateGraph(managedReducer)
	builder.SetManagedValues(func(context.Context, managedState, managed.Scope) (managedState, error) {
		return managedState{}, projectionErr
	})
	var calls atomic.Int32
	if err := builder.AddNode("node", func(context.Context, managedState, graph.Runtime) (graph.Command[managedDelta], error) {
		calls.Add(1)
		return graph.NoCommand[managedDelta](), nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := builder.AddEdge(graph.START, "node"); err != nil {
		t.Fatal(err)
	}
	if err := builder.AddEdge("node", graph.END); err != nil {
		t.Fatal(err)
	}
	compiled := mustCompileManaged(t, builder)
	_, err := compiled.Invoke(context.Background(), managedState{}, graph.RunConfig{})
	if !errors.Is(err, graph.ErrManagedValue) || !errors.Is(err, projectionErr) || calls.Load() != 0 {
		t.Fatalf("Invoke() err=%v calls=%d", err, calls.Load())
	}
}

func mustCompileManaged(t *testing.T, builder *graph.StateGraph[managedState, managedDelta]) *graph.CompiledGraph[managedState, managedDelta] {
	t.Helper()
	compiled, err := builder.Compile()
	if err != nil {
		t.Fatal(err)
	}
	return compiled
}
