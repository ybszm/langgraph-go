package retention_test

import (
	"context"
	"testing"
	"time"

	"github.com/ybszm/langgraph-go/checkpoint"
	checkpointmemory "github.com/ybszm/langgraph-go/checkpoint/memory"
	"github.com/ybszm/langgraph-go/checkpoint/retention"
)

func put(t *testing.T, saver *checkpointmemory.Saver, thread, id string, ts time.Time) {
	t.Helper()
	_, err := saver.Put(context.Background(), checkpoint.Config{ThreadID: thread}, checkpoint.Checkpoint{
		Version: checkpoint.CurrentVersion, ID: id, Timestamp: ts, Step: 1,
		Values: map[string]checkpoint.EncodedValue{}, ChannelVersions: map[string]string{},
	}, checkpoint.Metadata{"source": "test"}, map[string]string{})
	if err != nil {
		t.Fatal(err)
	}
}

func TestApplyKeepLatest(t *testing.T) {
	saver := checkpointmemory.NewSaver()
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	put(t, saver, "t1", "c1", base)
	put(t, saver, "t1", "c2", base.Add(time.Minute))
	put(t, saver, "t1", "c3", base.Add(2*time.Minute))
	deleted, removed, err := retention.Apply(context.Background(), saver, retention.Policy{KeepLatest: 1})
	if err != nil {
		t.Fatal(err)
	}
	if deleted != 0 || removed != 2 {
		t.Fatalf("deleted=%d removed=%d", deleted, removed)
	}
	list, err := saver.List(context.Background(), checkpoint.ListOptions{
		Config: &checkpoint.Config{ThreadID: "t1"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(list) != 1 || list[0].Checkpoint.ID != "c3" {
		t.Fatalf("list=%+v", list)
	}
}

func TestApplyMaxThreadAge(t *testing.T) {
	saver := checkpointmemory.NewSaver()
	old := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	put(t, saver, "old", "c1", old)
	put(t, saver, "new", "c1", time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	deleted, _, err := retention.Apply(context.Background(), saver, retention.Policy{
		MaxThreadAge: 24 * time.Hour,
		Now:          func() time.Time { return time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC) },
	})
	if err != nil {
		t.Fatal(err)
	}
	if deleted != 1 {
		t.Fatalf("deleted=%d", deleted)
	}
}
