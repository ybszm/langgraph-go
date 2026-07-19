package functional_test

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ybszm/langgraph-go/functional"
)

func TestTasksRunConcurrentlyAndFuturesPreserveCallOrder(t *testing.T) {
	started := make(chan struct{})
	var once sync.Once
	var active atomic.Int32
	task, err := functional.NewTask("double", func(ctx context.Context, input int) (int, error) {
		if active.Add(1) == 2 {
			once.Do(func() { close(started) })
		}
		defer active.Add(-1)
		select {
		case <-started:
		case <-ctx.Done():
			return 0, ctx.Err()
		}
		if input == 1 {
			time.Sleep(10 * time.Millisecond)
		}
		return input * 2, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	entry, err := functional.NewEntrypoint("map", func(ctx context.Context, inputs []int) ([]int, error) {
		futures := make([]*functional.Future[int], len(inputs))
		for index, input := range inputs {
			futures[index] = task.Call(ctx, input)
		}
		results := make([]int, len(futures))
		for index, future := range futures {
			value, err := future.Await(ctx)
			if err != nil {
				return nil, err
			}
			results[index] = value
		}
		return results, nil
	}, functional.EntrypointOptions{MaxConcurrency: 2})
	if err != nil {
		t.Fatal(err)
	}
	result, err := entry.Invoke(context.Background(), []int{1, 2})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(result, []int{2, 4}) {
		t.Fatalf("result=%v", result)
	}
}

func TestTaskOutsideEntrypointReturnsCompletedErrorFuture(t *testing.T) {
	task, _ := functional.NewTask("identity", func(_ context.Context, input int) (int, error) { return input, nil })
	_, err := task.Call(context.Background(), 1).Await(context.Background())
	if !errors.Is(err, functional.ErrTaskOutsideEntrypoint) {
		t.Fatalf("error=%v", err)
	}
}

func TestEntrypointWaitsForUnawaitedTasks(t *testing.T) {
	finished := make(chan struct{})
	task, _ := functional.NewTask("background", func(ctx context.Context, _ struct{}) (struct{}, error) {
		select {
		case <-time.After(10 * time.Millisecond):
			close(finished)
			return struct{}{}, nil
		case <-ctx.Done():
			return struct{}{}, ctx.Err()
		}
	})
	entry, _ := functional.NewEntrypoint("entry", func(ctx context.Context, input int) (int, error) {
		task.Call(ctx, struct{}{})
		return input, nil
	}, functional.EntrypointOptions{})
	if _, err := entry.Invoke(context.Background(), 7); err != nil {
		t.Fatal(err)
	}
	select {
	case <-finished:
	default:
		t.Fatal("entrypoint returned before unawaited task completed")
	}
}

func TestTaskErrorCancelsPeersAndPreservesContext(t *testing.T) {
	peerStarted := make(chan struct{})
	peerCanceled := make(chan struct{})
	peer, _ := functional.NewTask("peer", func(ctx context.Context, _ struct{}) (struct{}, error) {
		close(peerStarted)
		<-ctx.Done()
		close(peerCanceled)
		return struct{}{}, ctx.Err()
	})
	boom := errors.New("boom")
	failing, _ := functional.NewTask("failing", func(context.Context, struct{}) (struct{}, error) {
		return struct{}{}, boom
	})
	entry, _ := functional.NewEntrypoint("entry", func(ctx context.Context, _ struct{}) (struct{}, error) {
		peer.Call(ctx, struct{}{})
		<-peerStarted
		_, err := failing.Call(ctx, struct{}{}).Await(ctx)
		return struct{}{}, err
	}, functional.EntrypointOptions{MaxConcurrency: 2})
	_, err := entry.Invoke(context.Background(), struct{}{})
	var taskErr *functional.TaskError
	if !errors.Is(err, boom) || !errors.As(err, &taskErr) || taskErr.Name != "failing" {
		t.Fatalf("error=%v", err)
	}
	select {
	case <-peerCanceled:
	case <-time.After(time.Second):
		t.Fatal("peer task was not canceled")
	}
}

func TestTaskPanicAndContextCancellation(t *testing.T) {
	panicking, _ := functional.NewTask("panic-task", func(context.Context, int) (int, error) {
		panic("bad")
	})
	entry, _ := functional.NewEntrypoint("panic-entry", func(ctx context.Context, input int) (int, error) {
		return panicking.Call(ctx, input).Await(ctx)
	}, functional.EntrypointOptions{})
	_, err := entry.Invoke(context.Background(), 1)
	var panicErr *functional.TaskPanicError
	if !errors.As(err, &panicErr) || len(panicErr.Stack) == 0 {
		t.Fatalf("error=%v", err)
	}

	blocking, _ := functional.NewTask("blocking", func(ctx context.Context, input int) (int, error) {
		<-ctx.Done()
		return 0, ctx.Err()
	})
	cancelEntry, _ := functional.NewEntrypoint("cancel-entry", func(ctx context.Context, input int) (int, error) {
		return blocking.Call(ctx, input).Await(ctx)
	}, functional.EntrypointOptions{})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = cancelEntry.Invoke(ctx, 1)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error=%v", err)
	}
}

func TestFunctionalConstructorsValidateInputs(t *testing.T) {
	if _, err := functional.NewTask[int, int]("", func(context.Context, int) (int, error) { return 0, nil }); err == nil {
		t.Fatal("expected empty task name error")
	}
	if _, err := functional.NewEntrypoint[int, int]("entry", nil, functional.EntrypointOptions{}); err == nil {
		t.Fatal("expected nil entrypoint function error")
	}
	if _, err := functional.NewEntrypoint("entry", func(context.Context, int) (int, error) { return 0, nil }, functional.EntrypointOptions{MaxConcurrency: -1}); err == nil {
		t.Fatal("expected invalid concurrency error")
	}
}
