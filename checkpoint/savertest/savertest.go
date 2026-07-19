// Package savertest provides a reusable black-box conformance suite for
// checkpoint.Saver implementations.
package savertest

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/ybszm/langgraph-go/checkpoint"
)

// Factory creates a fresh Saver for one contract subtest. The factory should
// register any required cleanup with t.
type Factory func(t *testing.T) checkpoint.Saver

var contractTime = time.Date(2026, 7, 18, 14, 0, 0, 0, time.UTC)

// Run executes the checkpoint Saver contract against fresh instances from
// factory. Backend packages should expose this as a normal TestSaverContract.
func Run(t *testing.T, factory Factory) {
	t.Helper()
	if factory == nil {
		t.Fatal("savertest: nil Factory")
	}
	t.Run("put_get_history_parent_and_incremental_blobs", func(t *testing.T) {
		saver := factory(t)
		root := checkpoint.Config{ThreadID: "contract-thread", Namespace: "child"}
		first := contractCheckpoint("0001", -1,
			map[string]checkpoint.EncodedValue{"state": contractEncoded("one"), "removed": contractEncoded("present")},
			map[string]string{"state": "state-v1", "removed": "removed-v1"},
		)
		firstConfig := contractPut(t, saver, root, first, checkpoint.Metadata{
			"source": "input", "step": -1, "custom": map[string]any{"ok": true},
		}, first.ChannelVersions)
		second := contractCheckpoint("0002", 0,
			map[string]checkpoint.EncodedValue{"state": contractEncoded("two")},
			map[string]string{"state": "state-v2", "removed": "removed-v2"},
		)
		secondConfig := contractPut(t, saver, firstConfig, second, checkpoint.Metadata{
			"source": "loop", "step": 0,
		}, map[string]string{"state": "state-v2", "removed": "removed-v2"})

		latest, found, err := saver.GetTuple(context.Background(), root)
		if err != nil || !found {
			t.Fatalf("GetTuple(latest): found=%v err=%v", found, err)
		}
		if latest.Config != secondConfig || latest.ParentConfig == nil || *latest.ParentConfig != firstConfig {
			t.Fatalf("latest config=%+v parent=%+v", latest.Config, latest.ParentConfig)
		}
		if string(latest.Checkpoint.Values["state"].Data) != "two" {
			t.Fatalf("latest state=%+v", latest.Checkpoint.Values["state"])
		}
		if _, exists := latest.Checkpoint.Values["removed"]; exists {
			t.Fatal("removed channel was resurrected")
		}
		exact, found, err := saver.GetTuple(context.Background(), firstConfig)
		if err != nil || !found || string(exact.Checkpoint.Values["removed"].Data) != "present" {
			t.Fatalf("GetTuple(exact): tuple=%+v found=%v err=%v", exact, found, err)
		}
		wantMetadata := checkpoint.Metadata{"source": "input", "step": -1, "custom": map[string]any{"ok": true}}
		if !reflect.DeepEqual(exact.Metadata, wantMetadata) {
			t.Fatalf("metadata=%#v want=%#v", exact.Metadata, wantMetadata)
		}
	})

	t.Run("pending_write_conflicts_and_order", func(t *testing.T) {
		saver := factory(t)
		value := contractCheckpoint("0001", -1, map[string]checkpoint.EncodedValue{"state": contractEncoded("one")}, map[string]string{"state": "v1"})
		config := contractPut(t, saver, checkpoint.Config{ThreadID: "writes"}, value, nil, value.ChannelVersions)
		if err := saver.PutWrites(context.Background(), config, []checkpoint.PendingWrite{
			{TaskID: "b", TaskPath: "pull/1", Index: 1, Channel: "result", Value: contractEncoded("b")},
			{TaskID: "a", TaskPath: "pull/0", Index: 0, Channel: "result", Value: contractEncoded("first")},
			{TaskID: "a", TaskPath: "pull/0", Index: -1, Channel: checkpoint.InterruptChannel, Value: contractEncoded("old")},
		}); err != nil {
			t.Fatal(err)
		}
		if err := saver.PutWrites(context.Background(), config, []checkpoint.PendingWrite{
			{TaskID: "a", TaskPath: "changed", Index: 0, Channel: "result", Value: contractEncoded("ignored")},
			{TaskID: "a", TaskPath: "pull/0", Index: -1, Channel: checkpoint.InterruptChannel, Value: contractEncoded("new")},
		}); err != nil {
			t.Fatal(err)
		}
		tuple, found, err := saver.GetTuple(context.Background(), config)
		if err != nil || !found || len(tuple.PendingWrites) != 3 {
			t.Fatalf("writes=%+v found=%v err=%v", tuple.PendingWrites, found, err)
		}
		if got := tuple.PendingWrites; got[0].Index != -1 || string(got[0].Value.Data) != "new" || got[1].TaskID != "a" || string(got[1].Value.Data) != "first" || got[1].TaskPath != "pull/0" || got[2].TaskID != "b" {
			t.Fatalf("pending write contract=%+v", got)
		}
	})

	t.Run("list_filter_before_limit_and_delete", func(t *testing.T) {
		saver := factory(t)
		fixtures := []struct{ thread, namespace, id string }{
			{"one", "", "0001"}, {"one", "child", "0002"}, {"one", "", "0003"}, {"two", "", "0004"},
		}
		for index, fixture := range fixtures {
			value := contractCheckpoint(fixture.id, index, map[string]checkpoint.EncodedValue{"state": contractEncoded(fixture.id)}, map[string]string{"state": fixture.id})
			contractPut(t, saver, checkpoint.Config{ThreadID: fixture.thread, Namespace: fixture.namespace}, value, checkpoint.Metadata{"kind": "contract", "index": index}, value.ChannelVersions)
		}
		before := checkpoint.Config{CheckpointID: "0004"}
		listed, err := saver.List(context.Background(), checkpoint.ListOptions{
			Config: &checkpoint.Config{ThreadID: "one"}, AllNamespaces: true,
			Filter: checkpoint.Metadata{"kind": "contract"}, Before: &before, Limit: 2,
		})
		if err != nil || len(listed) != 2 || listed[0].Checkpoint.ID != "0003" || listed[1].Checkpoint.ID != "0002" {
			t.Fatalf("List()=%+v err=%v", listed, err)
		}
		if err := saver.DeleteThread(context.Background(), "one"); err != nil {
			t.Fatal(err)
		}
		remaining, err := saver.List(context.Background(), checkpoint.ListOptions{})
		if err != nil || len(remaining) != 1 || remaining[0].Config.ThreadID != "two" {
			t.Fatalf("remaining=%+v err=%v", remaining, err)
		}
	})

	t.Run("mutation_isolation", func(t *testing.T) {
		saver := factory(t)
		metadataNested := map[string]any{"value": "original"}
		metadata := checkpoint.Metadata{"nested": metadataNested}
		value := contractCheckpoint("0001", -1, map[string]checkpoint.EncodedValue{"state": contractEncoded("original")}, map[string]string{"state": "v1"})
		config := contractPut(t, saver, checkpoint.Config{ThreadID: "isolation"}, value, metadata, value.ChannelVersions)
		value.Values["state"] = contractEncoded("caller mutation")
		metadataNested["value"] = "caller mutation"
		stored, found, err := saver.GetTuple(context.Background(), config)
		if err != nil || !found {
			t.Fatalf("GetTuple(): found=%v err=%v", found, err)
		}
		if string(stored.Checkpoint.Values["state"].Data) != "original" || stored.Metadata["nested"].(map[string]any)["value"] != "original" {
			t.Fatalf("stored aliases caller: %+v", stored)
		}
		stored.Checkpoint.Values["state"] = contractEncoded("result mutation")
		stored.Metadata["nested"].(map[string]any)["value"] = "result mutation"
		again, _, err := saver.GetTuple(context.Background(), config)
		if err != nil || string(again.Checkpoint.Values["state"].Data) != "original" || again.Metadata["nested"].(map[string]any)["value"] != "original" {
			t.Fatalf("result aliases backend: %+v err=%v", again, err)
		}
	})

	t.Run("invalid_and_canceled_operations", func(t *testing.T) {
		saver := factory(t)
		if _, _, err := saver.GetTuple(context.Background(), checkpoint.Config{}); !errors.Is(err, checkpoint.ErrInvalidConfig) {
			t.Fatalf("invalid config err=%v", err)
		}
		if err := saver.PutWrites(context.Background(), checkpoint.Config{ThreadID: "missing", CheckpointID: "none"}, []checkpoint.PendingWrite{{TaskID: "task", Channel: "value", Value: contractEncoded("x")}}); !errors.Is(err, checkpoint.ErrInvalidConfig) {
			t.Fatalf("missing checkpoint err=%v", err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		if _, err := saver.List(ctx, checkpoint.ListOptions{}); !errors.Is(err, context.Canceled) {
			t.Fatalf("canceled list err=%v", err)
		}
	})

	t.Run("concurrent_threads", func(t *testing.T) {
		saver := factory(t)
		var group sync.WaitGroup
		for worker := 0; worker < 8; worker++ {
			worker := worker
			group.Add(1)
			go func() {
				defer group.Done()
				id := fmt.Sprintf("%04d", worker)
				value := contractCheckpoint(id, worker, map[string]checkpoint.EncodedValue{"state": contractEncoded(id)}, map[string]string{"state": id})
				if _, err := saver.Put(context.Background(), checkpoint.Config{ThreadID: "thread-" + id}, value, checkpoint.Metadata{"worker": worker}, value.ChannelVersions); err != nil {
					t.Errorf("Put(%d): %v", worker, err)
				}
			}()
		}
		group.Wait()
		listed, err := saver.List(context.Background(), checkpoint.ListOptions{})
		if err != nil || len(listed) != 8 {
			t.Fatalf("concurrent list len=%d err=%v", len(listed), err)
		}
	})
}

func contractEncoded(text string) checkpoint.EncodedValue {
	return checkpoint.EncodedValue{Type: "savertest.string", Version: 1, Data: []byte(text)}
}

func contractCheckpoint(id string, step int, values map[string]checkpoint.EncodedValue, versions map[string]string) checkpoint.Checkpoint {
	return checkpoint.Checkpoint{
		Version: checkpoint.CurrentVersion, ID: id,
		Timestamp: contractTime.Add(time.Duration(step+1) * time.Second), Step: step,
		Values: values, ChannelVersions: versions, VersionsSeen: map[string]map[string]string{},
		UpdatedChannels: []string{"state"}, Next: []checkpoint.Task{{ID: "task-" + id, Name: "node"}},
	}
}

func contractPut(t *testing.T, saver checkpoint.Saver, parent checkpoint.Config, value checkpoint.Checkpoint, metadata checkpoint.Metadata, versions map[string]string) checkpoint.Config {
	t.Helper()
	config, err := saver.Put(context.Background(), parent, value, metadata, versions)
	if err != nil {
		t.Fatalf("Put(%s): %v", value.ID, err)
	}
	return config
}
