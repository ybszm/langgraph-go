package graph_test

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/wahanbo/langgraph-go/graph"
)

func TestNodeRetryBackoffAndRuntimeAttempt(t *testing.T) {
	builder := graph.NewStateGraph(testReducer)
	var calls atomic.Int32
	var attempts []int
	var firstTimes []time.Time
	var delays []time.Duration
	var mu sync.Mutex
	policy := graph.RetryPolicy{
		InitialInterval: time.Millisecond,
		BackoffFactor:   2,
		MaxInterval:     time.Second,
		MaxAttempts:     3,
		EnableJitter:    true,
		Jitter: func(base time.Duration) time.Duration {
			mu.Lock()
			delays = append(delays, base)
			mu.Unlock()
			return 0
		},
		RetryOn: func(err error) bool { return errors.Is(err, errRetryable) },
	}
	err := builder.AddNode("flaky", func(
		_ context.Context,
		_ testState,
		runtime graph.Runtime,
	) (graph.Command[testDelta], error) {
		calls.Add(1)
		attempts = append(attempts, runtime.Attempt)
		firstTimes = append(firstTimes, runtime.FirstAttemptTime)
		if runtime.Attempt < 3 {
			return graph.NoCommand[testDelta](), errRetryable
		}
		return graph.Update(testDelta{Add: 1}), nil
	}, graph.WithRetryPolicies(policy))
	if err != nil {
		t.Fatal(err)
	}
	addEdge(t, builder, graph.START, "flaky")
	addEdge(t, builder, "flaky", graph.END)

	result, err := compileGraph(t, builder).Invoke(context.Background(), testState{}, graph.RunConfig{})
	if err != nil || result.Total != 1 {
		t.Fatalf("result=%#v err=%v", result, err)
	}
	if calls.Load() != 3 || !reflect.DeepEqual(attempts, []int{1, 2, 3}) {
		t.Fatalf("calls=%d attempts=%#v", calls.Load(), attempts)
	}
	if firstTimes[0].IsZero() || firstTimes[0] != firstTimes[1] || firstTimes[1] != firstTimes[2] {
		t.Fatalf("first attempt timestamps changed: %#v", firstTimes)
	}
	if !reflect.DeepEqual(delays, []time.Duration{time.Millisecond, 2 * time.Millisecond}) {
		t.Fatalf("delays=%#v", delays)
	}
}

var errRetryable = errors.New("retryable")

func TestRetryPredicateAndMaxAttempts(t *testing.T) {
	t.Run("predicate mismatch", func(t *testing.T) {
		builder := graph.NewStateGraph(testReducer)
		var calls atomic.Int32
		err := builder.AddNode("node", func(
			context.Context,
			testState,
			graph.Runtime,
		) (graph.Command[testDelta], error) {
			calls.Add(1)
			return graph.NoCommand[testDelta](), errors.New("not selected")
		}, graph.WithRetryPolicies(graph.RetryPolicy{
			MaxAttempts: 5,
			RetryOn:     func(err error) bool { return errors.Is(err, errRetryable) },
		}))
		if err != nil {
			t.Fatal(err)
		}
		addEdge(t, builder, graph.START, "node")
		addEdge(t, builder, "node", graph.END)
		_, invokeErr := compileGraph(t, builder).Invoke(context.Background(), testState{}, graph.RunConfig{})
		if invokeErr == nil || calls.Load() != 1 {
			t.Fatalf("error=%v calls=%d", invokeErr, calls.Load())
		}
	})

	t.Run("max attempts includes first", func(t *testing.T) {
		builder := graph.NewStateGraph(testReducer)
		var calls atomic.Int32
		err := builder.AddNode("node", func(
			context.Context,
			testState,
			graph.Runtime,
		) (graph.Command[testDelta], error) {
			calls.Add(1)
			return graph.NoCommand[testDelta](), errRetryable
		}, graph.WithRetryPolicies(graph.RetryPolicy{
			MaxAttempts:     2,
			InitialInterval: time.Nanosecond,
			MaxInterval:     time.Nanosecond,
			RetryOn:         func(error) bool { return true },
		}))
		if err != nil {
			t.Fatal(err)
		}
		addEdge(t, builder, graph.START, "node")
		addEdge(t, builder, "node", graph.END)
		_, invokeErr := compileGraph(t, builder).Invoke(context.Background(), testState{}, graph.RunConfig{})
		if !errors.Is(invokeErr, errRetryable) || calls.Load() != 2 {
			t.Fatalf("error=%v calls=%d", invokeErr, calls.Load())
		}
	})
}

func TestRetryBackoffHonorsContextCancellation(t *testing.T) {
	builder := graph.NewStateGraph(testReducer)
	started := make(chan struct{})
	var calls atomic.Int32
	err := builder.AddNode("node", func(
		context.Context,
		testState,
		graph.Runtime,
	) (graph.Command[testDelta], error) {
		if calls.Add(1) == 1 {
			close(started)
		}
		return graph.NoCommand[testDelta](), errRetryable
	}, graph.WithRetryPolicies(graph.RetryPolicy{
		MaxAttempts:     3,
		InitialInterval: time.Hour,
		MaxInterval:     time.Hour,
		RetryOn:         func(error) bool { return true },
	}))
	if err != nil {
		t.Fatal(err)
	}
	addEdge(t, builder, graph.START, "node")
	addEdge(t, builder, "node", graph.END)
	compiled := compileGraph(t, builder)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, invokeErr := compiled.Invoke(ctx, testState{}, graph.RunConfig{})
		done <- invokeErr
	}()
	<-started
	cancel()
	select {
	case invokeErr := <-done:
		if !errors.Is(invokeErr, context.Canceled) || calls.Load() != 1 {
			t.Fatalf("error=%v calls=%d", invokeErr, calls.Load())
		}
	case <-time.After(time.Second):
		t.Fatal("retry backoff ignored cancellation")
	}
}

func TestDefaultRetryPolicyAndPerNodeOverride(t *testing.T) {
	builder := graph.NewStateGraph(testReducer)
	if err := builder.SetDefaultRetryPolicies(graph.RetryPolicy{
		MaxAttempts:     2,
		InitialInterval: time.Nanosecond,
		MaxInterval:     time.Nanosecond,
		RetryOn:         func(error) bool { return true },
	}); err != nil {
		t.Fatal(err)
	}
	var inheritedCalls atomic.Int32
	if err := builder.AddNode("inherited", func(
		context.Context,
		testState,
		graph.Runtime,
	) (graph.Command[testDelta], error) {
		if inheritedCalls.Add(1) == 1 {
			return graph.NoCommand[testDelta](), errRetryable
		}
		return graph.Update(testDelta{Add: 1}), nil
	}); err != nil {
		t.Fatal(err)
	}
	var overrideCalls atomic.Int32
	if err := builder.AddNode("override", func(
		context.Context,
		testState,
		graph.Runtime,
	) (graph.Command[testDelta], error) {
		overrideCalls.Add(1)
		return graph.Update(testDelta{Add: 1}), nil
	}, graph.WithRetryPolicies(graph.RetryPolicy{MaxAttempts: 1})); err != nil {
		t.Fatal(err)
	}
	addEdge(t, builder, graph.START, "inherited")
	addEdge(t, builder, "inherited", "override")
	addEdge(t, builder, "override", graph.END)
	result, err := compileGraph(t, builder).Invoke(context.Background(), testState{}, graph.RunConfig{})
	if err != nil || result.Total != 2 || inheritedCalls.Load() != 2 || overrideCalls.Load() != 1 {
		t.Fatalf("result=%#v err=%v inherited=%d override=%d", result, err, inheritedCalls.Load(), overrideCalls.Load())
	}
}

func TestRetryPolicyValidation(t *testing.T) {
	builder := graph.NewStateGraph(testReducer)
	node := func(context.Context, testState, graph.Runtime) (graph.Command[testDelta], error) {
		return graph.NoCommand[testDelta](), nil
	}
	err := builder.AddNode("invalid", node, graph.WithRetryPolicies(graph.RetryPolicy{
		MaxAttempts: -1,
	}))
	if !errors.Is(err, graph.ErrInvalidGraph) {
		t.Fatalf("error=%v", err)
	}
}
