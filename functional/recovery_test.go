package functional_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/wahanbo/langgraph-go/checkpoint"
	checkpointmemory "github.com/wahanbo/langgraph-go/checkpoint/memory"
	"github.com/wahanbo/langgraph-go/functional"
)

func TestDurableEntrypointRecoversSuccessfulTaskAfterPeerFailure(t *testing.T) {
	saver := checkpointmemory.NewSaver()
	resultCodec := checkpoint.MustJSONCodec[int]("functional-task-int", 1)
	var stableCalls atomic.Int32
	stable, err := functional.NewTask("stable", func(context.Context, int) (int, error) {
		stableCalls.Add(1)
		return 10, nil
	}, functional.TaskOptions[int, int]{Persistence: &functional.TaskPersistencePolicy[int, int]{Codec: resultCodec}})
	if err != nil {
		t.Fatal(err)
	}
	boom := errors.New("boom")
	var flakyCalls atomic.Int32
	flaky, err := functional.NewTask("flaky", func(context.Context, int) (int, error) {
		if flakyCalls.Add(1) == 1 {
			return 0, boom
		}
		return 20, nil
	}, functional.TaskOptions[int, int]{Persistence: &functional.TaskPersistencePolicy[int, int]{Codec: resultCodec}})
	if err != nil {
		t.Fatal(err)
	}
	entry, err := functional.NewDurableEntrypoint(
		"workflow",
		func(ctx context.Context, input int, _ *int) (functional.Final[int, int], error) {
			a, err := stable.Call(ctx, input).Await(ctx)
			if err != nil {
				return functional.Final[int, int]{}, err
			}
			b, err := flaky.Call(ctx, input).Await(ctx)
			return functional.Final[int, int]{Value: a + b, Save: a + b}, err
		},
		functional.DurableEntrypointConfig[int]{
			Saver: saver, Codec: checkpoint.MustJSONCodec[int]("functional-save", 1), EnableTaskRecovery: true,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	run := functional.DurableRunConfig{ThreadID: "thread"}
	if _, err := entry.Invoke(context.Background(), 1, run); !errors.Is(err, boom) {
		t.Fatalf("first error=%v", err)
	}
	interrupted, found, err := saver.GetTuple(context.Background(), checkpoint.Config{ThreadID: "thread"})
	if err != nil || !found || len(interrupted.PendingWrites) != 1 || interrupted.PendingWrites[0].TaskID != "task:stable:0" {
		t.Fatalf("interrupted=%+v found=%v error=%v", interrupted, found, err)
	}
	result, err := entry.Invoke(context.Background(), 1, run)
	if err != nil || result != 30 {
		t.Fatalf("result=%d error=%v", result, err)
	}
	if stableCalls.Load() != 1 || flakyCalls.Load() != 2 {
		t.Fatalf("stable=%d flaky=%d", stableCalls.Load(), flakyCalls.Load())
	}
	history, err := saver.List(context.Background(), checkpoint.ListOptions{Config: &checkpoint.Config{ThreadID: "thread"}})
	if err != nil || len(history) != 2 || history[1].Metadata["status"] != "running" || history[0].Metadata["status"] != "complete" {
		t.Fatalf("history=%+v error=%v", history, err)
	}
	if len(history[1].PendingWrites) != 2 {
		t.Fatalf("pending writes=%+v", history[1].PendingWrites)
	}
}

func TestDurableTaskRecoveryDoesNotReuseDifferentInput(t *testing.T) {
	saver := checkpointmemory.NewSaver()
	codec := checkpoint.MustJSONCodec[int]("functional-task-input", 1)
	var calls atomic.Int32
	task, _ := functional.NewTask("task", func(context.Context, int) (int, error) {
		calls.Add(1)
		return 10, nil
	}, functional.TaskOptions[int, int]{Persistence: &functional.TaskPersistencePolicy[int, int]{Codec: codec}})
	boom := errors.New("stop after task")
	var first atomic.Bool
	first.Store(true)
	entry, err := functional.NewDurableEntrypoint(
		"workflow",
		func(ctx context.Context, input int, _ *int) (functional.Final[int, int], error) {
			value, err := task.Call(ctx, input).Await(ctx)
			if err != nil {
				return functional.Final[int, int]{}, err
			}
			if first.Swap(false) {
				return functional.Final[int, int]{}, boom
			}
			return functional.Final[int, int]{Value: value, Save: value}, nil
		},
		functional.DurableEntrypointConfig[int]{Saver: saver, Codec: checkpoint.MustJSONCodec[int]("functional-save-input", 1), EnableTaskRecovery: true},
	)
	if err != nil {
		t.Fatal(err)
	}
	run := functional.DurableRunConfig{ThreadID: "thread"}
	_, _ = entry.Invoke(context.Background(), 1, run)
	if _, err := entry.Invoke(context.Background(), 2, run); err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 2 {
		t.Fatalf("calls=%d", calls.Load())
	}
}

func TestTaskPersistenceOptionsValidation(t *testing.T) {
	_, err := functional.NewTask("task", func(context.Context, int) (int, error) { return 0, nil }, functional.TaskOptions[int, int]{Persistence: &functional.TaskPersistencePolicy[int, int]{}})
	if err == nil {
		t.Fatal("expected persistence codec validation error")
	}
}
