package distributed_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ybszm/langgraph-go/backend/distributed"
)

func TestPostgresLeasedOutboxAtomicCommitAckAndRecovery(t *testing.T) {
	db := distributedPostgresDB(t)
	ctx := context.Background()
	clock := &postgresQueueClock{now: time.Date(2026, 7, 19, 13, 0, 0, 0, time.UTC)}
	var ids atomic.Int64
	options := distributed.QueueOptions{Clock: clock.Now, IDGenerator: func() string { return fmt.Sprintf("outbox-id-%d", ids.Add(1)) }}
	queue, _ := distributed.NewPostgresQueue(db, options, jsonCodec[taskPayload]{})
	outbox, _ := distributed.NewPostgresLeasedOutbox(db, jsonCodec[completion]{}, clock.Now)
	plain, _ := distributed.NewPostgresOutbox(db, jsonCodec[completion]{}, clock.Now)
	if err := queue.Setup(ctx); err != nil {
		t.Fatal(err)
	}
	if err := outbox.Setup(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `TRUNCATE distributed_queue_tasks,distributed_queue_idempotency,distributed_outbox`); err != nil {
		t.Fatal(err)
	}
	if _, err := queue.Enqueue(ctx, distributed.EnqueueRequest[taskPayload]{TaskID: "task", Payload: taskPayload{Value: 2}}); err != nil {
		t.Fatal(err)
	}
	tasks, err := queue.Claim(ctx, "worker", 1, time.Minute)
	if err != nil || len(tasks) != 1 {
		t.Fatalf("tasks=%+v err=%v", tasks, err)
	}
	record := distributed.Completion[completion]{TaskID: tasks[0].ID, Attempt: tasks[0].Attempt, LeaseToken: tasks[0].Lease.Token, WorkerID: tasks[0].Lease.WorkerID, Value: completion{Value: 4}}
	start := make(chan struct{})
	errs := make([]error, 2)
	var wg sync.WaitGroup
	for i := range errs {
		wg.Add(1)
		go func(i int) { defer wg.Done(); <-start; errs[i] = outbox.Commit(ctx, record) }(i)
	}
	close(start)
	wg.Wait()
	if errs[0] != nil || errs[1] != nil {
		t.Fatalf("commit errs=%v", errs)
	}
	got, found, err := outbox.Get(ctx, "task")
	if err != nil || !found || got.Value.Value != 4 || got.WorkerID != "worker" {
		t.Fatalf("got=%+v found=%v err=%v", got, found, err)
	}
	tasks, err = queue.Claim(ctx, "other", 1, time.Minute)
	if err != nil || len(tasks) != 0 {
		t.Fatalf("atomically acked task reclaimed: %+v err=%v", tasks, err)
	}
	conflict := record
	conflict.Value.Value = 5
	if err = outbox.Commit(ctx, conflict); !errors.Is(err, distributed.ErrCompletionConflict) {
		t.Fatalf("conflict err=%v", err)
	}
	if _, err = queue.Enqueue(ctx, distributed.EnqueueRequest[taskPayload]{TaskID: "recover", Payload: taskPayload{Value: 3}}); err != nil {
		t.Fatal(err)
	}
	tasks, err = queue.Claim(ctx, "worker", 1, time.Minute)
	if err != nil || len(tasks) != 1 {
		t.Fatalf("recovery claim=%+v err=%v", tasks, err)
	}
	recovery := distributed.Completion[completion]{TaskID: "recover", Attempt: tasks[0].Attempt, LeaseToken: tasks[0].Lease.Token, WorkerID: "worker", Value: completion{Value: 6}}
	if err = plain.Commit(ctx, recovery); err != nil {
		t.Fatal(err)
	}
	if err = outbox.Commit(ctx, recovery); err != nil {
		t.Fatalf("recover precommitted result and ack: %v", err)
	}
	if err = queue.Ack(ctx, tasks[0].Lease); !errors.Is(err, distributed.ErrTaskNotFound) {
		t.Fatalf("task survived atomic recovery: %v", err)
	}
	if _, err = queue.Enqueue(ctx, distributed.EnqueueRequest[taskPayload]{TaskID: "stale", Payload: taskPayload{Value: 1}}); err != nil {
		t.Fatal(err)
	}
	tasks, _ = queue.Claim(ctx, "owner", 1, time.Minute)
	stale := distributed.Completion[completion]{TaskID: "stale", Attempt: 1, LeaseToken: tasks[0].Lease.Token, WorkerID: "intruder", Value: completion{Value: 1}}
	if err = outbox.Commit(ctx, stale); !errors.Is(err, distributed.ErrLeaseLost) {
		t.Fatalf("stale err=%v", err)
	}
	if _, found, _ = outbox.Get(ctx, "stale"); found {
		t.Fatal("failed lease transaction leaked result")
	}
	stats, err := outbox.Stats(ctx)
	if err != nil || stats.Completions != 2 {
		t.Fatalf("stats=%+v err=%v", stats, err)
	}
	clock.Advance(time.Hour)
	count, err := outbox.Prune(ctx, clock.Now())
	if err != nil || count != 2 {
		t.Fatalf("prune=%d err=%v", count, err)
	}
}
