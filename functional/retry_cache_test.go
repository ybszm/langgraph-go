package functional_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	cachememory "github.com/wahanbo/langgraph-go/cache/memory"
	"github.com/wahanbo/langgraph-go/checkpoint"
	"github.com/wahanbo/langgraph-go/functional"
	"github.com/wahanbo/langgraph-go/graph"
)

func TestTaskRetryPolicyEventuallySucceeds(t *testing.T) {
	var attempts atomic.Int32
	transient := errors.New("transient")
	task, err := functional.NewTask("retry", func(context.Context, int) (int, error) {
		if attempts.Add(1) < 3 {
			return 0, transient
		}
		return 42, nil
	}, functional.TaskOptions[int, int]{RetryPolicies: []graph.RetryPolicy{{
		InitialInterval: time.Millisecond,
		MaxInterval:     time.Millisecond,
		BackoffFactor:   1,
		MaxAttempts:     3,
		RetryOn:         func(err error) bool { return errors.Is(err, transient) },
	}}})
	if err != nil {
		t.Fatal(err)
	}
	entry, _ := functional.NewEntrypoint("entry", func(ctx context.Context, input int) (int, error) {
		return task.Call(ctx, input).Await(ctx)
	}, functional.EntrypointOptions{})
	result, err := entry.Invoke(context.Background(), 1)
	if err != nil || result != 42 || attempts.Load() != 3 {
		t.Fatalf("result=%d attempts=%d error=%v", result, attempts.Load(), err)
	}
}

func TestTaskRetryWaitHonorsCancellation(t *testing.T) {
	var attempts atomic.Int32
	task, err := functional.NewTask("retry", func(context.Context, int) (int, error) {
		attempts.Add(1)
		return 0, errors.New("again")
	}, functional.TaskOptions[int, int]{RetryPolicies: []graph.RetryPolicy{{
		InitialInterval: time.Hour, MaxInterval: time.Hour, BackoffFactor: 1, MaxAttempts: 3,
	}}})
	if err != nil {
		t.Fatal(err)
	}
	entry, _ := functional.NewEntrypoint("entry", func(ctx context.Context, input int) (int, error) {
		return task.Call(ctx, input).Await(ctx)
	}, functional.EntrypointOptions{})
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	_, err = entry.Invoke(ctx, 1)
	if !errors.Is(err, context.DeadlineExceeded) || attempts.Load() != 1 {
		t.Fatalf("attempts=%d error=%v", attempts.Load(), err)
	}
}

func TestTaskCacheHitAndClear(t *testing.T) {
	store := cachememory.New()
	codec := checkpoint.MustJSONCodec[int]("functional-result", 1)
	var calls atomic.Int32
	task, err := functional.NewTask("cached", func(context.Context, int) (int, error) {
		calls.Add(1)
		return 10, nil
	}, functional.TaskOptions[int, int]{Cache: &functional.TaskCachePolicy[int, int]{
		Store: store, Namespace: "tests", Codec: codec, TTL: time.Minute,
	}})
	if err != nil {
		t.Fatal(err)
	}
	entry, _ := functional.NewEntrypoint("entry", func(ctx context.Context, input int) (int, error) {
		return task.Call(ctx, input).Await(ctx)
	}, functional.EntrypointOptions{})
	for range 2 {
		if result, err := entry.Invoke(context.Background(), 1); err != nil || result != 10 {
			t.Fatalf("result=%d error=%v", result, err)
		}
	}
	if calls.Load() != 1 {
		t.Fatalf("calls=%d", calls.Load())
	}
	if err := task.ClearCache(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := entry.Invoke(context.Background(), 1); err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 2 {
		t.Fatalf("calls after clear=%d", calls.Load())
	}
}

func TestTaskCacheCollapsesConcurrentSameKeyMisses(t *testing.T) {
	store := cachememory.New()
	codec := checkpoint.MustJSONCodec[int]("functional-singleflight", 1)
	var calls atomic.Int32
	task, err := functional.NewTask("cached", func(context.Context, int) (int, error) {
		calls.Add(1)
		time.Sleep(10 * time.Millisecond)
		return 7, nil
	}, functional.TaskOptions[int, int]{Cache: &functional.TaskCachePolicy[int, int]{
		Store: store, Namespace: "tests", Codec: codec,
	}})
	if err != nil {
		t.Fatal(err)
	}
	entry, _ := functional.NewEntrypoint("entry", func(ctx context.Context, input int) (int, error) {
		first := task.Call(ctx, input)
		second := task.Call(ctx, input)
		a, err := first.Await(ctx)
		if err != nil {
			return 0, err
		}
		b, err := second.Await(ctx)
		return a + b, err
	}, functional.EntrypointOptions{MaxConcurrency: 2})
	result, err := entry.Invoke(context.Background(), 1)
	if err != nil || result != 14 || calls.Load() != 1 {
		t.Fatalf("result=%d calls=%d error=%v", result, calls.Load(), err)
	}
}

func TestTaskOptionsValidation(t *testing.T) {
	_, err := functional.NewTask("bad-retry", func(context.Context, int) (int, error) { return 0, nil }, functional.TaskOptions[int, int]{RetryPolicies: []graph.RetryPolicy{{MaxAttempts: -1}}})
	if err == nil {
		t.Fatal("expected retry validation error")
	}
	_, err = functional.NewTask("bad-cache", func(context.Context, int) (int, error) { return 0, nil }, functional.TaskOptions[int, int]{Cache: &functional.TaskCachePolicy[int, int]{}})
	if err == nil {
		t.Fatal("expected cache validation error")
	}
}
