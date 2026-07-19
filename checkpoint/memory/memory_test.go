package memory_test

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/wahanbo/langgraph-go/checkpoint"
	"github.com/wahanbo/langgraph-go/checkpoint/memory"
	"github.com/wahanbo/langgraph-go/checkpoint/savertest"
)

var fixedTime = time.Date(2026, 7, 18, 12, 0, 0, 0, time.UTC)

func TestSaverContract(t *testing.T) {
	savertest.Run(t, func(t *testing.T) checkpoint.Saver {
		return memory.NewSaver()
	})
}

func encoded(text string) checkpoint.EncodedValue {
	return checkpoint.EncodedValue{
		Type:    "tests.string",
		Version: 1,
		Data:    []byte(text),
	}
}

func makeCheckpoint(id string, step int, values map[string]checkpoint.EncodedValue) checkpoint.Checkpoint {
	versions := make(map[string]string, len(values))
	updated := make([]string, 0, len(values))
	for channel := range values {
		versions[channel] = id + ":" + channel
		updated = append(updated, channel)
	}
	return checkpoint.Checkpoint{
		Version:         checkpoint.CurrentVersion,
		ID:              id,
		Timestamp:       fixedTime.Add(time.Duration(step+1) * time.Second),
		Step:            step,
		Values:          values,
		ChannelVersions: versions,
		VersionsSeen:    map[string]map[string]string{},
		UpdatedChannels: updated,
		Next: []checkpoint.Task{{
			ID:   fmt.Sprintf("task-%d", step+1),
			Name: "node",
		}},
	}
}

func put(
	t *testing.T,
	saver checkpoint.Saver,
	parent checkpoint.Config,
	value checkpoint.Checkpoint,
	metadata checkpoint.Metadata,
) checkpoint.Config {
	t.Helper()
	stored, err := saver.Put(
		context.Background(),
		parent,
		value,
		metadata,
		value.ChannelVersions,
	)
	if err != nil {
		t.Fatalf("Put(): %v", err)
	}
	return stored
}

func TestPutGetLatestAndParentRoundTrip(t *testing.T) {
	saver := memory.NewSaver()
	root := checkpoint.Config{ThreadID: "thread", Namespace: ""}
	first := makeCheckpoint("0001", -1, map[string]checkpoint.EncodedValue{"state": encoded("one")})
	firstConfig := put(t, saver, root, first, checkpoint.Metadata{
		"source": string(checkpoint.SourceInput),
		"step":   -1,
	})
	second := makeCheckpoint("0002", 0, map[string]checkpoint.EncodedValue{"state": encoded("two")})
	secondConfig := put(t, saver, firstConfig, second, checkpoint.Metadata{
		"source": string(checkpoint.SourceLoop),
		"step":   0,
	})

	latest, found, err := saver.GetTuple(context.Background(), root)
	if err != nil || !found {
		t.Fatalf("GetTuple latest: found=%v err=%v", found, err)
	}
	if latest.Config != secondConfig || latest.Checkpoint.ID != "0002" {
		t.Fatalf("latest tuple = %#v", latest)
	}
	if latest.ParentConfig == nil || *latest.ParentConfig != firstConfig {
		t.Fatalf("parent config = %#v, want %#v", latest.ParentConfig, firstConfig)
	}
	if got := string(latest.Checkpoint.Values["state"].Data); got != "two" {
		t.Fatalf("state blob = %q, want two", got)
	}

	exact, found, err := saver.GetTuple(context.Background(), firstConfig)
	if err != nil || !found || exact.Checkpoint.ID != "0001" {
		t.Fatalf("GetTuple exact: tuple=%#v found=%v err=%v", exact, found, err)
	}
}

func TestIncrementalChannelBlobsAndRemoval(t *testing.T) {
	saver := memory.NewSaver()
	root := checkpoint.Config{ThreadID: "thread"}
	cp1 := makeCheckpoint("0001", -1, map[string]checkpoint.EncodedValue{
		"a": encoded("a1"),
	})
	cfg1 := put(t, saver, root, cp1, nil)

	cp2 := makeCheckpoint("0002", 0, map[string]checkpoint.EncodedValue{
		"b": encoded("b1"),
	})
	cp2.ChannelVersions["a"] = cp1.ChannelVersions["a"]
	cfg2, err := saver.Put(
		context.Background(),
		cfg1,
		cp2,
		nil,
		map[string]string{"b": cp2.ChannelVersions["b"]},
	)
	if err != nil {
		t.Fatal(err)
	}
	tuple, found, err := saver.GetTuple(context.Background(), cfg2)
	if err != nil || !found {
		t.Fatalf("GetTuple: found=%v err=%v", found, err)
	}
	if got := string(tuple.Checkpoint.Values["a"].Data); got != "a1" {
		t.Fatalf("reused a = %q", got)
	}
	if got := string(tuple.Checkpoint.Values["b"].Data); got != "b1" {
		t.Fatalf("new b = %q", got)
	}

	cp3 := makeCheckpoint("0003", 1, nil)
	cp3.ChannelVersions = map[string]string{"a": "0003:a"}
	cfg3, err := saver.Put(
		context.Background(),
		cfg2,
		cp3,
		nil,
		map[string]string{"a": "0003:a"},
	)
	if err != nil {
		t.Fatal(err)
	}
	tuple, found, err = saver.GetTuple(context.Background(), cfg3)
	if err != nil || !found {
		t.Fatalf("GetTuple removed: found=%v err=%v", found, err)
	}
	if _, exists := tuple.Checkpoint.Values["a"]; exists {
		t.Fatal("removed channel a was reconstructed")
	}
}

func TestPendingWritesAreOrderedIsolatedAndFirstWriteWins(t *testing.T) {
	saver := memory.NewSaver()
	config := put(
		t,
		saver,
		checkpoint.Config{ThreadID: "thread"},
		makeCheckpoint("0001", -1, nil),
		nil,
	)
	writes := []checkpoint.PendingWrite{
		{TaskID: "task-b", Index: 0, Channel: "state", Value: encoded("b")},
		{TaskID: "task-a", Index: 1, Channel: "route", Value: encoded("route")},
		{TaskID: "task-a", Index: 0, Channel: "state", Value: encoded("a")},
	}
	if err := saver.PutWrites(context.Background(), config, writes); err != nil {
		t.Fatal(err)
	}
	if err := saver.PutWrites(context.Background(), config, []checkpoint.PendingWrite{
		{TaskID: "task-a", Index: 0, Channel: "state", Value: encoded("replacement")},
	}); err != nil {
		t.Fatal(err)
	}

	tuple, found, err := saver.GetTuple(context.Background(), config)
	if err != nil || !found {
		t.Fatalf("GetTuple: found=%v err=%v", found, err)
	}
	if len(tuple.PendingWrites) != 3 {
		t.Fatalf("pending writes = %d, want 3", len(tuple.PendingWrites))
	}
	gotOrder := []string{
		tuple.PendingWrites[0].TaskID + ":" + fmt.Sprint(tuple.PendingWrites[0].Index),
		tuple.PendingWrites[1].TaskID + ":" + fmt.Sprint(tuple.PendingWrites[1].Index),
		tuple.PendingWrites[2].TaskID + ":" + fmt.Sprint(tuple.PendingWrites[2].Index),
	}
	if want := []string{"task-a:0", "task-a:1", "task-b:0"}; !reflect.DeepEqual(gotOrder, want) {
		t.Fatalf("order = %#v, want %#v", gotOrder, want)
	}
	if got := string(tuple.PendingWrites[0].Value.Data); got != "a" {
		t.Fatalf("duplicate write replaced first value with %q", got)
	}
}

func TestListSupportsNamespaceFilterBeforeAndLimit(t *testing.T) {
	saver := memory.NewSaver()
	root := checkpoint.Config{ThreadID: "thread", Namespace: ""}
	inner := checkpoint.Config{ThreadID: "thread", Namespace: "inner"}
	put(t, saver, root, makeCheckpoint("0001", -1, nil), checkpoint.Metadata{"source": "input", "score": 1})
	put(t, saver, root, makeCheckpoint("0002", 0, nil), checkpoint.Metadata{"source": "loop", "score": 2})
	put(t, saver, inner, makeCheckpoint("0003", 0, nil), checkpoint.Metadata{"source": "loop", "score": 3})
	put(t, saver, checkpoint.Config{ThreadID: "other"}, makeCheckpoint("0004", 0, nil), nil)

	rootOnly, err := saver.List(context.Background(), checkpoint.ListOptions{Config: &root})
	if err != nil {
		t.Fatal(err)
	}
	if got := checkpointIDs(rootOnly); !reflect.DeepEqual(got, []string{"0002", "0001"}) {
		t.Fatalf("root IDs = %#v", got)
	}
	allNamespaces, err := saver.List(context.Background(), checkpoint.ListOptions{
		Config:        &root,
		AllNamespaces: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if got := checkpointIDs(allNamespaces); !reflect.DeepEqual(got, []string{"0003", "0002", "0001"}) {
		t.Fatalf("all namespace IDs = %#v", got)
	}
	filtered, err := saver.List(context.Background(), checkpoint.ListOptions{
		Config: &root,
		Filter: checkpoint.Metadata{"source": "loop"},
		Before: &checkpoint.Config{CheckpointID: "0003"},
		Limit:  1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if got := checkpointIDs(filtered); !reflect.DeepEqual(got, []string{"0002"}) {
		t.Fatalf("filtered IDs = %#v", got)
	}
}

func TestSaverReturnsMutationIsolatedCopies(t *testing.T) {
	saver := memory.NewSaver()
	value := makeCheckpoint("0001", -1, map[string]checkpoint.EncodedValue{
		"state": encoded("original"),
	})
	metadata := checkpoint.Metadata{
		"nested": map[string]any{"items": []any{"original"}},
	}
	config := put(t, saver, checkpoint.Config{ThreadID: "thread"}, value, metadata)
	value.Values["state"] = encoded("mutated")
	metadata["nested"].(map[string]any)["items"].([]any)[0] = "mutated"

	first, found, err := saver.GetTuple(context.Background(), config)
	if err != nil || !found {
		t.Fatalf("GetTuple: found=%v err=%v", found, err)
	}
	first.Checkpoint.Values["state"] = encoded("caller-change")
	first.Metadata["nested"].(map[string]any)["items"].([]any)[0] = "caller-change"

	second, found, err := saver.GetTuple(context.Background(), config)
	if err != nil || !found {
		t.Fatalf("GetTuple second: found=%v err=%v", found, err)
	}
	if got := string(second.Checkpoint.Values["state"].Data); got != "original" {
		t.Fatalf("stored state = %q", got)
	}
	gotNested := second.Metadata["nested"].(map[string]any)["items"].([]any)[0]
	if gotNested != "original" {
		t.Fatalf("stored metadata = %#v", second.Metadata)
	}
}

func TestThreadsAndDeleteAreIsolated(t *testing.T) {
	saver := memory.NewSaver()
	put(t, saver, checkpoint.Config{ThreadID: "delete"}, makeCheckpoint("0001", -1, nil), nil)
	put(t, saver, checkpoint.Config{ThreadID: "keep"}, makeCheckpoint("0002", -1, nil), nil)
	if err := saver.DeleteThread(context.Background(), "delete"); err != nil {
		t.Fatal(err)
	}
	if _, found, err := saver.GetTuple(context.Background(), checkpoint.Config{ThreadID: "delete"}); err != nil || found {
		t.Fatalf("deleted thread: found=%v err=%v", found, err)
	}
	if _, found, err := saver.GetTuple(context.Background(), checkpoint.Config{ThreadID: "keep"}); err != nil || !found {
		t.Fatalf("kept thread: found=%v err=%v", found, err)
	}
}

func TestConcurrentPuts(t *testing.T) {
	saver := memory.NewSaver()
	var wait sync.WaitGroup
	for index := range 50 {
		wait.Add(1)
		go func() {
			defer wait.Done()
			id := fmt.Sprintf("%04d", index)
			value := makeCheckpoint(id, index, map[string]checkpoint.EncodedValue{
				"state": encoded(id),
			})
			if _, err := saver.Put(
				context.Background(),
				checkpoint.Config{ThreadID: "thread"},
				value,
				nil,
				value.ChannelVersions,
			); err != nil {
				t.Errorf("Put(%s): %v", id, err)
			}
		}()
	}
	wait.Wait()
	history, err := saver.List(context.Background(), checkpoint.ListOptions{
		Config: &checkpoint.Config{ThreadID: "thread"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(history) != 50 {
		t.Fatalf("history = %d, want 50", len(history))
	}
}

func TestCanceledAndInvalidOperations(t *testing.T) {
	saver := memory.NewSaver()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, _, err := saver.GetTuple(ctx, checkpoint.Config{ThreadID: "thread"}); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled error = %v", err)
	}
	if _, _, err := saver.GetTuple(context.Background(), checkpoint.Config{}); !errors.Is(err, checkpoint.ErrInvalidConfig) {
		t.Fatalf("invalid config error = %v", err)
	}
}

func checkpointIDs(tuples []checkpoint.Tuple) []string {
	result := make([]string, len(tuples))
	for index, tuple := range tuples {
		result[index] = tuple.Checkpoint.ID
	}
	return result
}
