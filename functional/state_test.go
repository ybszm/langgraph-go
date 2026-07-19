package functional_test

import (
	"context"
	"testing"

	"github.com/wahanbo/langgraph-go/checkpoint"
	checkpointmemory "github.com/wahanbo/langgraph-go/checkpoint/memory"
	"github.com/wahanbo/langgraph-go/functional"
)

func TestDurableEntrypointGetStateAndHistory(t *testing.T) {
	entry, err := functional.NewDurableEntrypoint(
		"counter",
		func(_ context.Context, input int, previous *int) (functional.Final[int, int], error) {
			prior := 0
			if previous != nil {
				prior = *previous
			}
			return functional.Final[int, int]{Value: prior, Save: prior + input}, nil
		},
		functional.DurableEntrypointConfig[int]{Saver: checkpointmemory.NewSaver(), Codec: checkpoint.MustJSONCodec[int]("state-inspection", 1)},
	)
	if err != nil {
		t.Fatal(err)
	}
	run := functional.DurableRunConfig{ThreadID: "thread"}
	_, _ = entry.Invoke(context.Background(), 1, run)
	first, found, err := entry.GetState(context.Background(), run)
	if err != nil || !found || first.Previous == nil || *first.Previous != 1 || first.Status != "complete" {
		t.Fatalf("first=%+v found=%v error=%v", first, found, err)
	}
	_, _ = entry.Invoke(context.Background(), 2, run)
	latest, found, err := entry.GetState(context.Background(), run)
	if err != nil || !found || latest.Previous == nil || *latest.Previous != 3 {
		t.Fatalf("latest=%+v found=%v error=%v", latest, found, err)
	}
	exact, found, err := entry.GetState(context.Background(), functional.DurableRunConfig{ThreadID: "thread", CheckpointID: first.Config.CheckpointID})
	if err != nil || !found || exact.Previous == nil || *exact.Previous != 1 {
		t.Fatalf("exact=%+v found=%v error=%v", exact, found, err)
	}
	history, err := entry.GetStateHistory(context.Background(), "thread", 1)
	if err != nil || len(history) != 1 || history[0].Config.CheckpointID != latest.Config.CheckpointID {
		t.Fatalf("history=%+v error=%v", history, err)
	}
}

func TestDurableEntrypointGetStateMissing(t *testing.T) {
	entry, _ := functional.NewDurableEntrypoint(
		"entry",
		func(context.Context, int, *int) (functional.Final[int, int], error) {
			return functional.Final[int, int]{}, nil
		},
		functional.DurableEntrypointConfig[int]{Saver: checkpointmemory.NewSaver(), Codec: checkpoint.MustJSONCodec[int]("state-missing", 1)},
	)
	_, found, err := entry.GetState(context.Background(), functional.DurableRunConfig{ThreadID: "thread"})
	if err != nil || found {
		t.Fatalf("found=%v error=%v", found, err)
	}
}
