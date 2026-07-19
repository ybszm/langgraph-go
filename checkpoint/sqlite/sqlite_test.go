package sqlite_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/wahanbo/langgraph-go/checkpoint"
	"github.com/wahanbo/langgraph-go/checkpoint/savertest"
	checkpointsqlite "github.com/wahanbo/langgraph-go/checkpoint/sqlite"
)

var testTime = time.Date(2026, 7, 18, 13, 0, 0, 0, time.UTC)

func TestSaverContract(t *testing.T) {
	savertest.Run(t, func(t *testing.T) checkpoint.Saver {
		saver, err := checkpointsqlite.Open(context.Background(), filepath.Join(t.TempDir(), "contract.db"))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = saver.Close() })
		return saver
	})
}

func encoded(text string) checkpoint.EncodedValue {
	return checkpoint.EncodedValue{Type: "tests.string", Version: 1, Data: []byte(text)}
}

func makeCheckpoint(id string, step int, values map[string]checkpoint.EncodedValue, versions map[string]string) checkpoint.Checkpoint {
	return checkpoint.Checkpoint{
		Version:         checkpoint.CurrentVersion,
		ID:              id,
		Timestamp:       testTime.Add(time.Duration(step+1) * time.Second),
		Step:            step,
		Values:          values,
		ChannelVersions: versions,
		VersionsSeen:    map[string]map[string]string{},
		UpdatedChannels: []string{"state"},
		Next:            []checkpoint.Task{{ID: "task-" + id, Name: "node"}},
	}
}

func put(t *testing.T, saver checkpoint.Saver, parent checkpoint.Config, value checkpoint.Checkpoint, metadata checkpoint.Metadata, newVersions map[string]string) checkpoint.Config {
	t.Helper()
	config, err := saver.Put(context.Background(), parent, value, metadata, newVersions)
	if err != nil {
		t.Fatalf("Put(): %v", err)
	}
	return config
}

func TestPersistenceIncrementalBlobsAndParent(t *testing.T) {
	path := filepath.Join(t.TempDir(), "checkpoints.db")
	saver, err := checkpointsqlite.Open(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	root := checkpoint.Config{ThreadID: "thread", Namespace: "child"}
	first := makeCheckpoint("0001", -1,
		map[string]checkpoint.EncodedValue{"state": encoded("one"), "removed": encoded("present")},
		map[string]string{"state": "v1", "removed": "v1"},
	)
	firstConfig := put(t, saver, root, first, checkpoint.Metadata{"source": "input", "step": -1}, first.ChannelVersions)
	second := makeCheckpoint("0002", 0,
		map[string]checkpoint.EncodedValue{"state": encoded("two")},
		map[string]string{"state": "v2", "removed": "v2"},
	)
	secondConfig := put(t, saver, firstConfig, second, checkpoint.Metadata{"source": "loop", "step": 0}, map[string]string{"state": "v2", "removed": "v2"})
	if err := saver.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := checkpointsqlite.Open(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = reopened.Close() })
	latest, found, err := reopened.GetTuple(context.Background(), root)
	if err != nil || !found {
		t.Fatalf("GetTuple(): found=%v err=%v", found, err)
	}
	if latest.Config != secondConfig || latest.ParentConfig == nil || *latest.ParentConfig != firstConfig {
		t.Fatalf("latest tuple config=%+v parent=%+v", latest.Config, latest.ParentConfig)
	}
	if got := string(latest.Checkpoint.Values["state"].Data); got != "two" {
		t.Fatalf("state=%q", got)
	}
	if _, exists := latest.Checkpoint.Values["removed"]; exists {
		t.Fatal("removed channel was resurrected")
	}
	exact, found, err := reopened.GetTuple(context.Background(), firstConfig)
	if err != nil || !found || string(exact.Checkpoint.Values["removed"].Data) != "present" {
		t.Fatalf("exact first=%+v found=%v err=%v", exact, found, err)
	}
}

func TestPendingWritesOrderingIsolationAndConflictPolicy(t *testing.T) {
	saver, err := checkpointsqlite.Open(context.Background(), filepath.Join(t.TempDir(), "writes.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = saver.Close() })
	root := checkpoint.Config{ThreadID: "thread"}
	value := makeCheckpoint("0001", -1, map[string]checkpoint.EncodedValue{"state": encoded("one")}, map[string]string{"state": "v1"})
	config := put(t, saver, root, value, nil, value.ChannelVersions)
	writes := []checkpoint.PendingWrite{
		{TaskID: "b", TaskPath: "pull/1", Index: 1, Channel: "result", Value: encoded("b1")},
		{TaskID: "a", TaskPath: "pull/0", Index: 0, Channel: "result", Value: encoded("first")},
	}
	if err := saver.PutWrites(context.Background(), config, writes); err != nil {
		t.Fatal(err)
	}
	if err := saver.PutWrites(context.Background(), config, []checkpoint.PendingWrite{
		{TaskID: "a", TaskPath: "changed", Index: 0, Channel: "result", Value: encoded("ignored")},
		{TaskID: "a", TaskPath: "pull/0", Index: -1, Channel: checkpoint.InterruptChannel, Value: encoded("old")},
	}); err != nil {
		t.Fatal(err)
	}
	if err := saver.PutWrites(context.Background(), config, []checkpoint.PendingWrite{
		{TaskID: "a", TaskPath: "pull/0", Index: -1, Channel: checkpoint.InterruptChannel, Value: encoded("new")},
	}); err != nil {
		t.Fatal(err)
	}
	tuple, found, err := saver.GetTuple(context.Background(), config)
	if err != nil || !found {
		t.Fatalf("GetTuple(): found=%v err=%v", found, err)
	}
	if len(tuple.PendingWrites) != 3 {
		t.Fatalf("writes=%+v", tuple.PendingWrites)
	}
	if tuple.PendingWrites[0].TaskID != "a" || tuple.PendingWrites[0].Index != -1 || string(tuple.PendingWrites[0].Value.Data) != "new" {
		t.Fatalf("reserved write=%+v", tuple.PendingWrites[0])
	}
	if tuple.PendingWrites[1].Index != 0 || string(tuple.PendingWrites[1].Value.Data) != "first" || tuple.PendingWrites[1].TaskPath != "pull/0" {
		t.Fatalf("first-write-wins=%+v", tuple.PendingWrites[1])
	}
	if tuple.PendingWrites[2].TaskID != "b" {
		t.Fatalf("deterministic order=%+v", tuple.PendingWrites)
	}
}

func TestListFilterBeforeNamespacesLimitAndDelete(t *testing.T) {
	saver, err := checkpointsqlite.Open(context.Background(), filepath.Join(t.TempDir(), "list.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = saver.Close() })
	fixtures := []struct {
		thread, namespace, id string
		step                  int
	}{
		{"one", "", "0001", 1},
		{"one", "", "0003", 3},
		{"one", "child", "0002", 2},
		{"two", "", "0004", 4},
	}
	for _, fixture := range fixtures {
		value := makeCheckpoint(fixture.id, fixture.step, map[string]checkpoint.EncodedValue{"state": encoded(fixture.id)}, map[string]string{"state": fixture.id})
		put(t, saver, checkpoint.Config{ThreadID: fixture.thread, Namespace: fixture.namespace}, value, checkpoint.Metadata{"kind": "test", "step": fixture.step}, value.ChannelVersions)
	}
	before := checkpoint.Config{CheckpointID: "0004"}
	listed, err := saver.List(context.Background(), checkpoint.ListOptions{
		Config:        &checkpoint.Config{ThreadID: "one"},
		AllNamespaces: true,
		Filter:        checkpoint.Metadata{"kind": "test"},
		Before:        &before,
		Limit:         2,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(listed) != 2 || listed[0].Checkpoint.ID != "0003" || listed[1].Checkpoint.ID != "0002" {
		t.Fatalf("List()=%+v", listed)
	}
	if err := saver.DeleteThread(context.Background(), "one"); err != nil {
		t.Fatal(err)
	}
	remaining, err := saver.List(context.Background(), checkpoint.ListOptions{})
	if err != nil || len(remaining) != 1 || remaining[0].Config.ThreadID != "two" {
		t.Fatalf("remaining=%+v err=%v", remaining, err)
	}
}

func TestPutIsAtomicWhenCheckpointInsertFails(t *testing.T) {
	db, err := sql.Open("sqlite", filepath.Join(t.TempDir(), "atomic.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	saver, err := checkpointsqlite.New(db)
	if err != nil {
		t.Fatal(err)
	}
	if err := saver.Setup(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`CREATE TRIGGER reject_bad BEFORE INSERT ON checkpoints WHEN NEW.checkpoint_id = 'bad' BEGIN SELECT RAISE(ABORT, 'rejected'); END`); err != nil {
		t.Fatal(err)
	}
	value := makeCheckpoint("bad", 0, map[string]checkpoint.EncodedValue{"state": encoded("must rollback")}, map[string]string{"state": "bad-version"})
	_, err = saver.Put(context.Background(), checkpoint.Config{ThreadID: "thread"}, value, nil, value.ChannelVersions)
	if err == nil {
		t.Fatal("Put unexpectedly succeeded")
	}
	var count int
	if err := db.QueryRow(`SELECT count(*) FROM checkpoint_blobs WHERE version = 'bad-version'`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("transaction leaked %d channel blobs", count)
	}
}

func TestSetupMigratesV1WritesWithoutDataLoss(t *testing.T) {
	db, err := sql.Open("sqlite", filepath.Join(t.TempDir(), "migration.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	legacy := []string{
		`CREATE TABLE checkpoint_migrations (version INTEGER PRIMARY KEY)`,
		`INSERT INTO checkpoint_migrations(version) VALUES (1)`,
		`CREATE TABLE checkpoint_writes (
			thread_id TEXT NOT NULL, checkpoint_ns TEXT NOT NULL DEFAULT '',
			checkpoint_id TEXT NOT NULL, task_id TEXT NOT NULL, idx INTEGER NOT NULL,
			channel TEXT NOT NULL, type TEXT NOT NULL, value_version INTEGER NOT NULL,
			value BLOB, PRIMARY KEY (thread_id, checkpoint_ns, checkpoint_id, task_id, idx)
		)`,
		`INSERT INTO checkpoint_writes
			(thread_id, checkpoint_ns, checkpoint_id, task_id, idx, channel, type, value_version, value)
			VALUES ('thread', '', 'checkpoint', 'task', 0, 'result', 'tests.string', 1, x'6f6b')`,
	}
	for _, statement := range legacy {
		if _, err := db.Exec(statement); err != nil {
			t.Fatal(err)
		}
	}
	saver, err := checkpointsqlite.New(db)
	if err != nil {
		t.Fatal(err)
	}
	if err := saver.Setup(context.Background()); err != nil {
		t.Fatalf("Setup() migration: %v", err)
	}

	var taskPath string
	var payload []byte
	if err := db.QueryRow(`SELECT task_path, value FROM checkpoint_writes WHERE task_id = 'task'`).Scan(&taskPath, &payload); err != nil {
		t.Fatalf("read migrated write: %v", err)
	}
	if taskPath != "" || string(payload) != "ok" {
		t.Fatalf("migrated write task_path=%q value=%q", taskPath, payload)
	}
	rows, err := db.Query(`SELECT version FROM checkpoint_migrations ORDER BY version`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var versions []int
	for rows.Next() {
		var version int
		if err := rows.Scan(&version); err != nil {
			t.Fatal(err)
		}
		versions = append(versions, version)
	}
	if fmt.Sprint(versions) != "[1 2]" {
		t.Fatalf("migration versions = %v", versions)
	}
}

func TestSetupRejectsFutureSchemaBeforeMutation(t *testing.T) {
	db, err := sql.Open("sqlite", filepath.Join(t.TempDir(), "future.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if _, err := db.Exec(`CREATE TABLE checkpoint_migrations (version INTEGER PRIMARY KEY); INSERT INTO checkpoint_migrations(version) VALUES (999)`); err != nil {
		t.Fatal(err)
	}
	saver, err := checkpointsqlite.New(db)
	if err != nil {
		t.Fatal(err)
	}
	err = saver.Setup(context.Background())
	if err == nil {
		t.Fatal("Setup() unexpectedly accepted a future schema")
	}
	var count int
	if err := db.QueryRow(`SELECT count(*) FROM sqlite_master WHERE type='table' AND name='checkpoints'`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatal("Setup() mutated a future-version database")
	}
}

func TestConcurrentPutsAndCancellation(t *testing.T) {
	saver, err := checkpointsqlite.Open(context.Background(), filepath.Join(t.TempDir(), "concurrent.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = saver.Close() })
	var group sync.WaitGroup
	for worker := 0; worker < 12; worker++ {
		worker := worker
		group.Add(1)
		go func() {
			defer group.Done()
			id := fmt.Sprintf("%04d", worker)
			value := makeCheckpoint(id, worker, map[string]checkpoint.EncodedValue{"state": encoded(id)}, map[string]string{"state": id})
			if _, err := saver.Put(context.Background(), checkpoint.Config{ThreadID: fmt.Sprintf("thread-%02d", worker)}, value, checkpoint.Metadata{"worker": worker}, value.ChannelVersions); err != nil {
				t.Errorf("Put(%d): %v", worker, err)
			}
		}()
	}
	group.Wait()
	listed, err := saver.List(context.Background(), checkpoint.ListOptions{})
	if err != nil || len(listed) != 12 {
		t.Fatalf("List(): len=%d err=%v", len(listed), err)
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	_, _, err = saver.GetTuple(canceled, checkpoint.Config{ThreadID: "thread-00"})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("GetTuple canceled err=%v", err)
	}
	_, err = checkpointsqlite.Open(canceled, filepath.Join(t.TempDir(), "canceled.db"))
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Open canceled err=%v", err)
	}
}

func TestInvalidInputs(t *testing.T) {
	if _, err := checkpointsqlite.New(nil); !errors.Is(err, checkpoint.ErrInvalidConfig) {
		t.Fatalf("New(nil) err=%v", err)
	}
	if _, err := checkpointsqlite.Open(context.Background(), ""); !errors.Is(err, checkpoint.ErrInvalidConfig) {
		t.Fatalf("Open(empty) err=%v", err)
	}
	saver, err := checkpointsqlite.Open(context.Background(), filepath.Join(t.TempDir(), "invalid.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = saver.Close() })
	_, err = saver.Put(context.Background(), checkpoint.Config{ThreadID: "thread"}, checkpoint.Checkpoint{}, nil, nil)
	if !errors.Is(err, checkpoint.ErrInvalidCheckpoint) {
		t.Fatalf("Put(invalid checkpoint) err=%v", err)
	}
	err = saver.PutWrites(context.Background(), checkpoint.Config{ThreadID: "thread"}, nil)
	if !errors.Is(err, checkpoint.ErrInvalidConfig) {
		t.Fatalf("PutWrites(missing checkpoint ID) err=%v", err)
	}
	_, err = saver.List(context.Background(), checkpoint.ListOptions{Limit: -1})
	if !errors.Is(err, checkpoint.ErrInvalidConfig) {
		t.Fatalf("List(negative limit) err=%v", err)
	}
}
