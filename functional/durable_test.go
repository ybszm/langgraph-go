package functional_test

import (
	"context"
	"errors"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/wahanbo/langgraph-go/checkpoint"
	checkpointmemory "github.com/wahanbo/langgraph-go/checkpoint/memory"
	"github.com/wahanbo/langgraph-go/functional"
)

func TestDurableEntrypointPreviousAndFinalSave(t *testing.T) {
	saver := checkpointmemory.NewSaver()
	codec := checkpoint.MustJSONCodec[int]("functional-previous", 1)
	ids := 0
	entry, err := functional.NewDurableEntrypoint(
		"counter",
		func(_ context.Context, input int, previous *int) (functional.Final[string, int], error) {
			prior := 0
			if previous != nil {
				prior = *previous
			}
			return functional.Final[string, int]{Value: string(rune('0' + prior)), Save: prior + input}, nil
		},
		functional.DurableEntrypointConfig[int]{
			Saver: saver, Codec: codec, Namespace: "workflow",
			Clock: checkpoint.ClockFunc(func() time.Time { return time.Unix(int64(ids+1), 0).UTC() }),
			IDGenerator: checkpoint.IDGeneratorFunc(func(time.Time) (string, error) {
				ids++
				return string(rune('a' + ids - 1)), nil
			}),
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	first, err := entry.Invoke(context.Background(), 3, functional.DurableRunConfig{ThreadID: "thread-1"})
	if err != nil || first != "0" {
		t.Fatalf("first=%q error=%v", first, err)
	}
	second, err := entry.Invoke(context.Background(), 2, functional.DurableRunConfig{ThreadID: "thread-1"})
	if err != nil || second != "3" {
		t.Fatalf("second=%q error=%v", second, err)
	}
	other, err := entry.Invoke(context.Background(), 1, functional.DurableRunConfig{ThreadID: "thread-2"})
	if err != nil || other != "0" {
		t.Fatalf("other=%q error=%v", other, err)
	}

	history, err := saver.List(context.Background(), checkpoint.ListOptions{Config: &checkpoint.Config{ThreadID: "thread-1", Namespace: "workflow"}})
	if err != nil || len(history) != 2 {
		t.Fatalf("history=%+v error=%v", history, err)
	}
	if history[0].ParentConfig == nil || history[0].ParentConfig.CheckpointID != history[1].Config.CheckpointID {
		t.Fatalf("parent chain=%+v", history)
	}
}

func TestDurableEntrypointFailureDoesNotAdvancePrevious(t *testing.T) {
	saver := checkpointmemory.NewSaver()
	codec := checkpoint.MustJSONCodec[int]("functional-failure", 1)
	boom := errors.New("boom")
	entry, err := functional.NewDurableEntrypoint(
		"workflow",
		func(_ context.Context, input int, previous *int) (functional.Final[int, int], error) {
			prior := 0
			if previous != nil {
				prior = *previous
			}
			if input < 0 {
				return functional.Final[int, int]{}, boom
			}
			return functional.Final[int, int]{Value: prior, Save: prior + input}, nil
		},
		functional.DurableEntrypointConfig[int]{Saver: saver, Codec: codec},
	)
	if err != nil {
		t.Fatal(err)
	}
	config := functional.DurableRunConfig{ThreadID: "thread"}
	if _, err := entry.Invoke(context.Background(), 2, config); err != nil {
		t.Fatal(err)
	}
	if _, err := entry.Invoke(context.Background(), -1, config); !errors.Is(err, boom) {
		t.Fatalf("error=%v", err)
	}
	value, err := entry.Invoke(context.Background(), 1, config)
	if err != nil || value != 2 {
		t.Fatalf("value=%d error=%v", value, err)
	}
	history, _ := saver.List(context.Background(), checkpoint.ListOptions{Config: &checkpoint.Config{ThreadID: "thread"}})
	if len(history) != 2 {
		t.Fatalf("history length=%d", len(history))
	}
}

func TestDurableEntrypointSerializesSameThread(t *testing.T) {
	saver := checkpointmemory.NewSaver()
	entry, err := functional.NewDurableEntrypoint(
		"counter",
		func(_ context.Context, input int, previous *int) (functional.Final[int, int], error) {
			prior := 0
			if previous != nil {
				prior = *previous
			}
			time.Sleep(time.Millisecond)
			return functional.Final[int, int]{Value: prior, Save: prior + input}, nil
		},
		functional.DurableEntrypointConfig[int]{Saver: saver, Codec: checkpoint.MustJSONCodec[int]("serialized", 1)},
	)
	if err != nil {
		t.Fatal(err)
	}
	const invocations = 20
	results := make([]int, invocations)
	errorsFound := make([]error, invocations)
	var group sync.WaitGroup
	for index := range invocations {
		group.Add(1)
		go func() {
			defer group.Done()
			results[index], errorsFound[index] = entry.Invoke(context.Background(), 1, functional.DurableRunConfig{ThreadID: "thread"})
		}()
	}
	group.Wait()
	for _, err := range errorsFound {
		if err != nil {
			t.Fatal(err)
		}
	}
	sort.Ints(results)
	for index, result := range results {
		if result != index {
			t.Fatalf("results=%v", results)
		}
	}
	history, _ := saver.List(context.Background(), checkpoint.ListOptions{Config: &checkpoint.Config{ThreadID: "thread"}})
	if len(history) != invocations {
		t.Fatalf("history length=%d", len(history))
	}
}

func TestDurableEntrypointValidationAndCancellation(t *testing.T) {
	codec := checkpoint.MustJSONCodec[int]("functional-validation", 1)
	if _, err := functional.NewDurableEntrypoint[int, int, int]("entry", nil, functional.DurableEntrypointConfig[int]{}); err == nil {
		t.Fatal("expected invalid durable entrypoint")
	}
	entry, err := functional.NewDurableEntrypoint(
		"entry",
		func(ctx context.Context, input int, _ *int) (functional.Final[int, int], error) {
			<-ctx.Done()
			return functional.Final[int, int]{}, ctx.Err()
		},
		functional.DurableEntrypointConfig[int]{Saver: checkpointmemory.NewSaver(), Codec: codec},
	)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = entry.Invoke(ctx, 1, functional.DurableRunConfig{ThreadID: "thread"})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error=%v", err)
	}
	if _, err := entry.Invoke(context.Background(), 1, functional.DurableRunConfig{}); !errors.Is(err, checkpoint.ErrInvalidConfig) {
		t.Fatalf("error=%v", err)
	}
}

type legacyFunctionalState struct {
	Count int `json:"count"`
}

type currentFunctionalState struct {
	Count int    `json:"count"`
	Label string `json:"label"`
}

func TestDurableEntrypointMigratesCompletedPreviousStateIntoDescendant(t *testing.T) {
	saver := checkpointmemory.NewSaver()
	legacyCodec := checkpoint.MustJSONCodec[legacyFunctionalState]("tests.functional-state", 1)
	legacy, err := functional.NewDurableEntrypoint(
		"counter",
		func(_ context.Context, input int, previous *legacyFunctionalState) (functional.Final[int, legacyFunctionalState], error) {
			value := legacyFunctionalState{}
			if previous != nil {
				value = *previous
			}
			value.Count += input
			return functional.Final[int, legacyFunctionalState]{Value: value.Count, Save: value}, nil
		},
		functional.DurableEntrypointConfig[legacyFunctionalState]{
			Saver: saver, Codec: legacyCodec,
			Migration: &functional.DurableMigrationPolicy[legacyFunctionalState]{Revision: "v1"},
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	config := functional.DurableRunConfig{ThreadID: "migration-complete"}
	if value, err := legacy.Invoke(context.Background(), 3, config); err != nil || value != 3 {
		t.Fatalf("legacy value=%d err=%v", value, err)
	}

	currentCodec := checkpoint.MustJSONCodec[currentFunctionalState]("tests.functional-state", 2)
	migrationCalls := 0
	current, err := functional.NewDurableEntrypoint(
		"counter",
		func(_ context.Context, input int, previous *currentFunctionalState) (functional.Final[int, currentFunctionalState], error) {
			value := currentFunctionalState{Label: "new"}
			if previous != nil {
				value = *previous
			}
			value.Count += input
			return functional.Final[int, currentFunctionalState]{Value: value.Count, Save: value}, nil
		},
		functional.DurableEntrypointConfig[currentFunctionalState]{
			Saver: saver, Codec: currentCodec,
			Migration: &functional.DurableMigrationPolicy[currentFunctionalState]{
				Revision: "v2",
				MigratePrevious: func(_ context.Context, from string, encoded checkpoint.EncodedValue) (currentFunctionalState, error) {
					migrationCalls++
					if from != "v1" {
						return currentFunctionalState{}, errors.New("unexpected source revision")
					}
					old, err := legacyCodec.Decode(encoded)
					return currentFunctionalState{Count: old.Count, Label: "migrated"}, err
				},
			},
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if value, err := current.Invoke(context.Background(), 2, config); err != nil || value != 5 {
		t.Fatalf("current value=%d err=%v", value, err)
	}
	if migrationCalls != 1 {
		t.Fatalf("migration calls=%d", migrationCalls)
	}
	history, err := saver.List(context.Background(), checkpoint.ListOptions{Config: &checkpoint.Config{ThreadID: config.ThreadID}})
	if err != nil || len(history) != 2 {
		t.Fatalf("history=%+v err=%v", history, err)
	}
	versions := make(map[string]int, len(history))
	for _, tuple := range history {
		revision, _ := tuple.Metadata["functional_revision"].(string)
		versions[revision] = tuple.Checkpoint.Values["__functional_previous__"].Version
	}
	if versions["v2"] != 2 || versions["v1"] != 1 {
		t.Fatalf("persisted versions=%v history=%+v", versions, history)
	}
}

func TestDurableEntrypointRejectsRevisionChangeForRunningAttempt(t *testing.T) {
	saver := checkpointmemory.NewSaver()
	old, err := functional.NewDurableEntrypoint(
		"approval",
		func(ctx context.Context, _ int, _ *int) (functional.Final[int, int], error) {
			_, err := functional.AwaitResume[string](ctx, "approve?")
			return functional.Final[int, int]{}, err
		},
		functional.DurableEntrypointConfig[int]{
			Saver: saver, Codec: checkpoint.MustJSONCodec[int]("tests.revision-running", 1), EnableTaskRecovery: true,
			Migration: &functional.DurableMigrationPolicy[int]{Revision: "v1"},
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	config := functional.DurableRunConfig{ThreadID: "migration-running"}
	if _, err := old.Invoke(context.Background(), 1, config); !errors.Is(err, functional.ErrInterrupted) {
		t.Fatalf("old invoke err=%v", err)
	}
	migrated := false
	current, err := functional.NewDurableEntrypoint(
		"approval",
		func(_ context.Context, _ int, _ *int) (functional.Final[int, int], error) {
			return functional.Final[int, int]{}, nil
		},
		functional.DurableEntrypointConfig[int]{
			Saver: saver, Codec: checkpoint.MustJSONCodec[int]("tests.revision-running", 2), EnableTaskRecovery: true,
			Migration: &functional.DurableMigrationPolicy[int]{
				Revision: "v2",
				MigratePrevious: func(context.Context, string, checkpoint.EncodedValue) (int, error) {
					migrated = true
					return 0, nil
				},
			},
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	tuple, found, err := saver.GetTuple(context.Background(), checkpoint.Config{ThreadID: config.ThreadID})
	if err != nil || !found || len(tuple.PendingWrites) != 1 {
		t.Fatalf("running tuple=%+v found=%v err=%v", tuple, found, err)
	}
	interruptID := tuple.PendingWrites[0].TaskID
	before := checkpoint.CloneEncodedValue(tuple.PendingWrites[0].Value)
	if _, err := current.Resume(context.Background(), 1, config, map[string]any{interruptID: "approved"}); !errors.Is(err, functional.ErrIncompatibleRevision) {
		t.Fatalf("current resume err=%v", err)
	}
	afterTuple, _, err := saver.GetTuple(context.Background(), checkpoint.Config{ThreadID: config.ThreadID})
	if err != nil || len(afterTuple.PendingWrites) != 1 || string(afterTuple.PendingWrites[0].Value.Data) != string(before.Data) {
		t.Fatalf("revision rejection mutated pending write: before=%+v after=%+v err=%v", before, afterTuple.PendingWrites, err)
	}
	if _, err := current.Invoke(context.Background(), 1, config); !errors.Is(err, functional.ErrIncompatibleRevision) {
		t.Fatalf("current invoke err=%v", err)
	}
	if migrated {
		t.Fatal("running attempt migration callback must not run")
	}
}
