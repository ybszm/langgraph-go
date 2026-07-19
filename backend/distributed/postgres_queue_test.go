package distributed_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/wahanbo/langgraph-go/backend/distributed"
)

type postgresQueueClock struct {
	mu  sync.RWMutex
	now time.Time
}

func (c *postgresQueueClock) Now() time.Time { c.mu.RLock(); defer c.mu.RUnlock(); return c.now }
func (c *postgresQueueClock) Advance(value time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(value)
	c.mu.Unlock()
}

func TestPostgresQueueMultiWorkerFencingRecoveryAndOperations(t *testing.T) {
	db := distributedPostgresDB(t)
	ctx := context.Background()
	clock := &postgresQueueClock{now: time.Date(2026, 7, 19, 11, 0, 0, 0, time.UTC)}
	var ids atomic.Int64
	options := distributed.QueueOptions{Clock: clock.Now, IDGenerator: func() string { return fmt.Sprintf("generated-%03d", ids.Add(1)) }}
	left, err := distributed.NewPostgresQueue(db, options, jsonCodec[taskPayload]{})
	if err != nil {
		t.Fatal(err)
	}
	right, err := distributed.NewPostgresQueue(db, options, jsonCodec[taskPayload]{})
	if err != nil {
		t.Fatal(err)
	}
	if err = left.Setup(ctx); err != nil {
		t.Fatal(err)
	}
	if err = right.Setup(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err = db.ExecContext(ctx, `TRUNCATE distributed_queue_tasks,distributed_queue_idempotency`); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 30; i++ {
		_, err = left.Enqueue(ctx, distributed.EnqueueRequest[taskPayload]{TaskID: fmt.Sprintf("task-%02d", i), Payload: taskPayload{Value: i}})
		if err != nil {
			t.Fatal(err)
		}
	}
	start := make(chan struct{})
	claimed := make([][]distributed.Task[taskPayload], 2)
	errs := make([]error, 2)
	var wg sync.WaitGroup
	for i, queue := range []*distributed.PostgresQueue[taskPayload]{left, right} {
		wg.Add(1)
		go func(i int, queue *distributed.PostgresQueue[taskPayload]) {
			defer wg.Done()
			<-start
			claimed[i], errs[i] = queue.Claim(ctx, fmt.Sprintf("worker-%d", i), 20, time.Minute)
		}(i, queue)
	}
	close(start)
	wg.Wait()
	if errs[0] != nil || errs[1] != nil {
		t.Fatalf("claim errs=%v", errs)
	}
	seen := map[string]bool{}
	for _, group := range claimed {
		for _, task := range group {
			if seen[task.ID] {
				t.Fatalf("duplicate claim %s", task.ID)
			}
			seen[task.ID] = true
		}
	}
	if len(seen) != 30 {
		t.Fatalf("claimed=%d groups=%d/%d", len(seen), len(claimed[0]), len(claimed[1]))
	}
	first := claimed[0][0]
	if err = right.Heartbeat(ctx, distributed.Lease{TaskID: first.ID, Token: first.Lease.Token, WorkerID: "wrong"}, time.Minute); !errors.Is(err, distributed.ErrLeaseLost) {
		t.Fatalf("wrong heartbeat err=%v", err)
	}
	if err = left.Heartbeat(ctx, first.Lease, 2*time.Minute); err != nil {
		t.Fatal(err)
	}
	if err = left.Nack(ctx, first.Lease, time.Second); err != nil {
		t.Fatal(err)
	}
	if tasks, err := right.Claim(ctx, "worker-retry", 1, time.Minute); err != nil || len(tasks) != 0 {
		t.Fatalf("early retry=%+v err=%v", tasks, err)
	}
	clock.Advance(time.Second)
	retried, err := right.Claim(ctx, "worker-retry", 1, time.Minute)
	if err != nil || len(retried) != 1 || retried[0].ID != first.ID || retried[0].Attempt != 2 {
		t.Fatalf("retried=%+v err=%v", retried, err)
	}
	if err = right.Ack(ctx, retried[0].Lease); err != nil {
		t.Fatal(err)
	}
	for _, group := range claimed {
		for _, task := range group {
			if task.ID == first.ID {
				continue
			}
			queue := left
			if task.Lease.WorkerID == "worker-1" {
				queue = right
			}
			if err = queue.Ack(ctx, task.Lease); err != nil {
				t.Fatalf("ack %s: %v", task.ID, err)
			}
		}
	}
	stats, err := left.Stats(ctx)
	if err != nil || stats.Total != 0 {
		t.Fatalf("stats=%+v err=%v", stats, err)
	}
	stable, err := left.Enqueue(ctx, distributed.EnqueueRequest[taskPayload]{Payload: taskPayload{Value: 99}, IdempotencyKey: "stable"})
	if err != nil {
		t.Fatal(err)
	}
	leased, err := left.Claim(ctx, "worker-stable", 1, time.Second)
	if err != nil || len(leased) != 1 {
		t.Fatalf("leased=%+v err=%v", leased, err)
	}
	if err = left.Ack(ctx, leased[0].Lease); err != nil {
		t.Fatal(err)
	}
	replayed, err := right.Enqueue(ctx, distributed.EnqueueRequest[taskPayload]{Payload: taskPayload{Value: 99}, IdempotencyKey: "stable"})
	if err != nil || replayed != stable {
		t.Fatalf("replayed=%s stable=%s err=%v", replayed, stable, err)
	}
	count, err := right.PruneIdempotency(ctx, clock.Now().Add(time.Second))
	if err != nil || count != 1 {
		t.Fatalf("prune=%d err=%v", count, err)
	}
	replacement, err := right.Enqueue(ctx, distributed.EnqueueRequest[taskPayload]{Payload: taskPayload{Value: 99}, IdempotencyKey: "stable"})
	if err != nil || replacement == stable {
		t.Fatalf("replacement=%s stable=%s err=%v", replacement, stable, err)
	}
	leased, err = left.Claim(ctx, "expiring", 1, time.Second)
	if err != nil || len(leased) != 1 {
		t.Fatalf("expiring=%+v err=%v", leased, err)
	}
	clock.Advance(time.Second)
	released, err := right.ReleaseExpiredLeases(ctx)
	if err != nil || released != 1 {
		t.Fatalf("released=%d err=%v", released, err)
	}
	recovered, err := right.Claim(ctx, "recovery", 1, time.Minute)
	if err != nil || len(recovered) != 1 || recovered[0].Attempt != 2 {
		t.Fatalf("recovered=%+v err=%v", recovered, err)
	}
}

func TestPostgresQueueConcurrentIdempotencyAcrossInstances(t *testing.T) {
	db := distributedPostgresDB(t)
	ctx := context.Background()
	var ids atomic.Int64
	options := distributed.QueueOptions{Clock: time.Now, IDGenerator: func() string { return fmt.Sprintf("id-%d", ids.Add(1)) }}
	left, _ := distributed.NewPostgresQueue(db, options, jsonCodec[taskPayload]{})
	right, _ := distributed.NewPostgresQueue(db, options, jsonCodec[taskPayload]{})
	if err := left.Setup(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `TRUNCATE distributed_queue_tasks,distributed_queue_idempotency`); err != nil {
		t.Fatal(err)
	}
	request := distributed.EnqueueRequest[taskPayload]{Payload: taskPayload{Value: 7}, IdempotencyKey: "same"}
	start := make(chan struct{})
	results := make([]string, 2)
	errs := make([]error, 2)
	var wg sync.WaitGroup
	for i, queue := range []*distributed.PostgresQueue[taskPayload]{left, right} {
		wg.Add(1)
		go func(i int, queue *distributed.PostgresQueue[taskPayload]) {
			defer wg.Done()
			<-start
			results[i], errs[i] = queue.Enqueue(ctx, request)
		}(i, queue)
	}
	close(start)
	wg.Wait()
	if errs[0] != nil || errs[1] != nil || results[0] != results[1] || ids.Load() != 1 {
		t.Fatalf("results=%v errs=%v ids=%d", results, errs, ids.Load())
	}
}
