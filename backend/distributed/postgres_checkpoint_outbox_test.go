package distributed_test

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/wahanbo/langgraph-go/backend/distributed"
	"github.com/wahanbo/langgraph-go/checkpoint"
	checkpointpostgres "github.com/wahanbo/langgraph-go/checkpoint/postgres"
)

func TestPostgresCheckpointOutboxAtomicPendingWriteAndAck(t *testing.T) {
	db := distributedPostgresDB(t)
	ctx := context.Background()
	clock := &postgresQueueClock{now: time.Date(2026, 7, 19, 14, 0, 0, 0, time.UTC)}
	var ids atomic.Int64
	queue, _ := distributed.NewPostgresQueue(db, distributed.QueueOptions{Clock: clock.Now, IDGenerator: func() string { return fmt.Sprintf("checkpoint-token-%d", ids.Add(1)) }}, jsonCodec[taskPayload]{})
	if err := queue.Setup(ctx); err != nil {
		t.Fatal(err)
	}
	saver, err := checkpointpostgres.New(db)
	if err != nil {
		t.Fatal(err)
	}
	if err = saver.Setup(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err = db.ExecContext(ctx, `TRUNCATE distributed_queue_tasks,distributed_queue_idempotency,checkpoint_writes,checkpoints,checkpoint_blobs`); err != nil {
		t.Fatal(err)
	}
	value := checkpoint.Checkpoint{Version: checkpoint.CurrentVersion, ID: "checkpoint", Timestamp: clock.Now(), Values: map[string]checkpoint.EncodedValue{}, ChannelVersions: map[string]string{}, VersionsSeen: map[string]map[string]string{}}
	config, err := saver.Put(ctx, checkpoint.Config{ThreadID: "thread"}, value, checkpoint.Metadata{}, map[string]string{})
	if err != nil {
		t.Fatal(err)
	}
	outbox, err := distributed.NewPostgresCheckpointOutbox(db, checkpointJSONCodec[completion]{}, clock.Now)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = queue.Enqueue(ctx, distributed.EnqueueRequest[taskPayload]{TaskID: "task", Payload: taskPayload{Value: 2}}); err != nil {
		t.Fatal(err)
	}
	tasks, err := queue.Claim(ctx, "worker", 1, time.Minute)
	if err != nil || len(tasks) != 1 {
		t.Fatalf("tasks=%+v err=%v", tasks, err)
	}
	record := distributed.Completion[distributed.CheckpointResult[completion]]{TaskID: "task", Attempt: 1, LeaseToken: tasks[0].Lease.Token, WorkerID: "worker", Value: distributed.CheckpointResult[completion]{Config: config, TaskPath: "0", Index: 0, Channel: "result", Value: completion{Value: 4}}}
	if err = outbox.Commit(ctx, record); err != nil {
		t.Fatal(err)
	}
	tuple, found, err := saver.GetTuple(ctx, config)
	if err != nil || !found || len(tuple.PendingWrites) != 1 || tuple.PendingWrites[0].TaskID != "task" {
		t.Fatalf("tuple=%+v found=%v err=%v", tuple, found, err)
	}
	if err = queue.Ack(ctx, tasks[0].Lease); !errors.Is(err, distributed.ErrTaskNotFound) {
		t.Fatalf("queue task survived: %v", err)
	}
	if err = outbox.Commit(ctx, record); err != nil {
		t.Fatalf("idempotent replay: %v", err)
	}
	different := record
	different.Value.Value.Value = 5
	if err = outbox.Commit(ctx, different); !errors.Is(err, distributed.ErrCompletionConflict) {
		t.Fatalf("conflict err=%v", err)
	}
	if _, err = queue.Enqueue(ctx, distributed.EnqueueRequest[taskPayload]{TaskID: "stale", Payload: taskPayload{Value: 1}}); err != nil {
		t.Fatal(err)
	}
	tasks, _ = queue.Claim(ctx, "owner", 1, time.Minute)
	stale := distributed.Completion[distributed.CheckpointResult[completion]]{TaskID: "stale", Attempt: 1, LeaseToken: tasks[0].Lease.Token, WorkerID: "intruder", Value: distributed.CheckpointResult[completion]{Config: config, Index: 0, Channel: "result", Value: completion{Value: 1}}}
	if err = outbox.Commit(ctx, stale); !errors.Is(err, distributed.ErrLeaseLost) {
		t.Fatalf("stale err=%v", err)
	}
	tuple, _, _ = saver.GetTuple(ctx, config)
	for _, write := range tuple.PendingWrites {
		if write.TaskID == "stale" {
			t.Fatal("failed lease transaction leaked checkpoint write")
		}
	}
	if _, err = queue.Enqueue(ctx, distributed.EnqueueRequest[taskPayload]{TaskID: "recover", Payload: taskPayload{Value: 3}}); err != nil {
		t.Fatal(err)
	}
	tasks, _ = queue.Claim(ctx, "worker", 1, time.Minute)
	encoded, _ := checkpointJSONCodec[completion]{}.Encode(completion{Value: 6})
	if err = saver.PutWrites(ctx, config, []checkpoint.PendingWrite{{TaskID: "recover", Index: 0, Channel: "result", Value: encoded}}); err != nil {
		t.Fatal(err)
	}
	recovery := distributed.Completion[distributed.CheckpointResult[completion]]{TaskID: "recover", Attempt: 1, LeaseToken: tasks[0].Lease.Token, WorkerID: "worker", Value: distributed.CheckpointResult[completion]{Config: config, Index: 0, Channel: "result", Value: completion{Value: 6}}}
	if err = outbox.Commit(ctx, recovery); err != nil {
		t.Fatalf("recover precommitted pending write: %v", err)
	}
	if err = queue.Ack(ctx, tasks[0].Lease); !errors.Is(err, distributed.ErrTaskNotFound) {
		t.Fatalf("recovery task survived: %v", err)
	}
}
