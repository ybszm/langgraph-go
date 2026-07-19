package distributed_test

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/wahanbo/langgraph-go/backend/distributed"
	"github.com/wahanbo/langgraph-go/graph"
)

func TestPostgresInterruptStoreMultiInstanceResumeAndRetention(t *testing.T) {
	db := distributedPostgresDB(t)
	ctx := context.Background()
	clock := &postgresQueueClock{now: time.Date(2026, 7, 19, 12, 0, 0, 0, time.UTC)}
	left, err := distributed.NewPostgresInterruptStore(db, clock.Now)
	if err != nil {
		t.Fatal(err)
	}
	right, err := distributed.NewPostgresInterruptStore(db, clock.Now)
	if err != nil {
		t.Fatal(err)
	}
	if err = left.Setup(ctx); err != nil {
		t.Fatal(err)
	}
	if err = right.Setup(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err = db.ExecContext(ctx, `TRUNCATE distributed_interrupts`); err != nil {
		t.Fatal(err)
	}
	request := graph.InterruptRequest{ThreadID: "thread", TaskID: "task", CheckpointID: "checkpoint", Index: 0, Interrupt: graph.Interrupt{ID: "interrupt", Namespace: "child", Value: json.RawMessage(`{"question":"continue?"}`)}}
	if _, err = left.Await(ctx, request); !errors.Is(err, graph.ErrGraphInterrupt) {
		t.Fatalf("await err=%v", err)
	}
	listed, err := right.List(ctx, "thread")
	if err != nil || len(listed) != 1 || listed[0].Status != distributed.InterruptPending || string(listed[0].Interrupt.Value) != string(request.Interrupt.Value) {
		t.Fatalf("listed=%+v err=%v", listed, err)
	}
	clock.Advance(time.Minute)
	start := make(chan struct{})
	errs := make([]error, 2)
	var wg sync.WaitGroup
	for i, store := range []*distributed.PostgresInterruptStore{left, right} {
		wg.Add(1)
		go func(i int, store *distributed.PostgresInterruptStore) {
			defer wg.Done()
			<-start
			errs[i] = store.Resume(ctx, "thread", "interrupt", map[string]any{"approved": true})
		}(i, store)
	}
	close(start)
	wg.Wait()
	if errs[0] != nil || errs[1] != nil {
		t.Fatalf("resume errs=%v", errs)
	}
	if err = left.Resume(ctx, "thread", "interrupt", map[string]any{"approved": false}); !errors.Is(err, distributed.ErrInterruptConflict) {
		t.Fatalf("conflict err=%v", err)
	}
	resumed, err := left.Await(ctx, request)
	if err != nil || string(resumed) != `{"approved":true}` {
		t.Fatalf("resumed=%s err=%v", resumed, err)
	}
	pending := graph.InterruptRequest{ThreadID: "thread", TaskID: "task-2", CheckpointID: "checkpoint", Index: 1, Interrupt: graph.Interrupt{ID: "pending", Value: json.RawMessage(`"wait"`)}}
	if _, err = right.Provider()(ctx, pending); !errors.Is(err, graph.ErrGraphInterrupt) {
		t.Fatalf("pending err=%v", err)
	}
	stats, err := left.Stats(ctx)
	if err != nil || stats.Pending != 1 || stats.Resumed != 1 {
		t.Fatalf("stats=%+v err=%v", stats, err)
	}
	clock.Advance(time.Minute)
	count, err := right.Prune(ctx, clock.Now())
	if err != nil || count != 1 {
		t.Fatalf("prune=%d err=%v", count, err)
	}
	listed, err = left.List(ctx, "thread")
	if err != nil || len(listed) != 1 || listed[0].Interrupt.ID != "pending" {
		t.Fatalf("after prune=%+v err=%v", listed, err)
	}
}

func TestPostgresInterruptStorePromptConflictAndMissingResume(t *testing.T) {
	db := distributedPostgresDB(t)
	ctx := context.Background()
	store, _ := distributed.NewPostgresInterruptStore(db, time.Now)
	if err := store.Setup(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `TRUNCATE distributed_interrupts`); err != nil {
		t.Fatal(err)
	}
	request := graph.InterruptRequest{ThreadID: "thread", TaskID: "task", Index: 0, Interrupt: graph.Interrupt{ID: "id", Value: json.RawMessage(`1`)}}
	_, _ = store.Await(ctx, request)
	request.Interrupt.Value = json.RawMessage(`1 `)
	if _, err := store.Await(ctx, request); !errors.Is(err, distributed.ErrInterruptConflict) {
		t.Fatalf("prompt err=%v", err)
	}
	if err := store.Resume(ctx, "thread", "missing", true); !errors.Is(err, distributed.ErrInterruptNotFound) {
		t.Fatalf("missing err=%v", err)
	}
}
