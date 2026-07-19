package functional_test

import (
	"context"
	"testing"

	"github.com/ybszm/langgraph-go/checkpoint"
	checkpointmemory "github.com/ybszm/langgraph-go/checkpoint/memory"
	"github.com/ybszm/langgraph-go/functional"
)

func TestDurableEntrypointHistoricalCheckpointFork(t *testing.T) {
	saver := checkpointmemory.NewSaver()
	entry, err := functional.NewDurableEntrypoint(
		"counter",
		func(_ context.Context, input int, previous *int) (functional.Final[int, int], error) {
			prior := 0
			if previous != nil {
				prior = *previous
			}
			return functional.Final[int, int]{Value: prior, Save: prior + input}, nil
		},
		functional.DurableEntrypointConfig[int]{Saver: saver, Codec: checkpoint.MustJSONCodec[int]("time-travel", 1)},
	)
	if err != nil {
		t.Fatal(err)
	}
	run := functional.DurableRunConfig{ThreadID: "thread"}
	if value, err := entry.Invoke(context.Background(), 1, run); err != nil || value != 0 {
		t.Fatalf("first=%d error=%v", value, err)
	}
	first, found, err := saver.GetTuple(context.Background(), checkpoint.Config{ThreadID: "thread"})
	if err != nil || !found {
		t.Fatal(err)
	}
	if value, err := entry.Invoke(context.Background(), 2, run); err != nil || value != 1 {
		t.Fatalf("second=%d error=%v", value, err)
	}
	forkRun := functional.DurableRunConfig{ThreadID: "thread", CheckpointID: first.Config.CheckpointID}
	if value, err := entry.Invoke(context.Background(), 10, forkRun); err != nil || value != 1 {
		t.Fatalf("fork=%d error=%v", value, err)
	}
	latest, found, err := saver.GetTuple(context.Background(), checkpoint.Config{ThreadID: "thread"})
	if err != nil || !found || latest.ParentConfig == nil || latest.ParentConfig.CheckpointID != first.Config.CheckpointID {
		t.Fatalf("latest=%+v found=%v error=%v", latest, found, err)
	}
	if value, err := entry.Invoke(context.Background(), 1, run); err != nil || value != 11 {
		t.Fatalf("continued fork=%d error=%v", value, err)
	}
}

func TestDurableEntrypointUnknownHistoricalCheckpoint(t *testing.T) {
	entry, err := functional.NewDurableEntrypoint(
		"entry",
		func(context.Context, int, *int) (functional.Final[int, int], error) {
			return functional.Final[int, int]{}, nil
		},
		functional.DurableEntrypointConfig[int]{Saver: checkpointmemory.NewSaver(), Codec: checkpoint.MustJSONCodec[int]("unknown-history", 1)},
	)
	if err != nil {
		t.Fatal(err)
	}
	_, err = entry.Invoke(context.Background(), 1, functional.DurableRunConfig{ThreadID: "thread", CheckpointID: "missing"})
	if err == nil {
		t.Fatal("expected missing historical checkpoint error")
	}
}
