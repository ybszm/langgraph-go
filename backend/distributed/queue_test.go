package distributed_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/wahanbo/langgraph-go/backend/distributed"
)

type taskPayload struct{ Value int }

func TestMemoryQueueLeaseExpiryRedeliveryAndStaleAck(t *testing.T) {
	now := time.Date(2026, 7, 19, 3, 0, 0, 0, time.UTC)
	ids := []string{"task-a", "task-b", "lease-a1", "lease-a2", "lease-b1"}
	index := 0
	queue, err := distributed.NewMemoryQueue[taskPayload](distributed.QueueOptions{
		Clock:       func() time.Time { return now },
		IDGenerator: func() string { value := ids[index]; index++; return value },
	})
	if err != nil {
		t.Fatal(err)
	}
	firstID, _ := queue.Enqueue(context.Background(), distributed.EnqueueRequest[taskPayload]{Payload: taskPayload{Value: 1}})
	secondID, _ := queue.Enqueue(context.Background(), distributed.EnqueueRequest[taskPayload]{Payload: taskPayload{Value: 2}})
	if firstID != "task-a" || secondID != "task-b" {
		t.Fatalf("ids=%q,%q", firstID, secondID)
	}
	claimed, err := queue.Claim(context.Background(), "worker-1", 1, time.Minute)
	if err != nil || len(claimed) != 1 || claimed[0].ID != firstID || claimed[0].Attempt != 1 || claimed[0].Lease.Token != "lease-a1" {
		t.Fatalf("claimed=%+v err=%v", claimed, err)
	}
	oldLease := claimed[0].Lease
	now = now.Add(time.Minute)
	claimed, err = queue.Claim(context.Background(), "worker-2", 1, time.Minute)
	if err != nil || len(claimed) != 1 || claimed[0].ID != firstID || claimed[0].Attempt != 2 || claimed[0].Lease.Token != "lease-a2" {
		t.Fatalf("claimed=%+v err=%v", claimed, err)
	}
	if err := queue.Ack(context.Background(), oldLease); !errors.Is(err, distributed.ErrLeaseLost) {
		t.Fatalf("stale ack err=%v", err)
	}
	if err := queue.Ack(context.Background(), claimed[0].Lease); err != nil {
		t.Fatal(err)
	}
	claimed, _ = queue.Claim(context.Background(), "worker-1", 1, time.Minute)
	if len(claimed) != 1 || claimed[0].ID != secondID {
		t.Fatalf("claimed=%+v", claimed)
	}
}

func TestMemoryQueueHeartbeatAndNack(t *testing.T) {
	now := time.Date(2026, 7, 19, 4, 0, 0, 0, time.UTC)
	counter := 0
	queue, _ := distributed.NewMemoryQueue[taskPayload](distributed.QueueOptions{
		Clock:       func() time.Time { return now },
		IDGenerator: func() string { counter++; return string(rune('0' + counter)) },
	})
	_, _ = queue.Enqueue(context.Background(), distributed.EnqueueRequest[taskPayload]{Payload: taskPayload{Value: 1}})
	claimed, _ := queue.Claim(context.Background(), "worker", 1, time.Minute)
	if err := queue.Heartbeat(context.Background(), claimed[0].Lease, 2*time.Minute); err != nil {
		t.Fatal(err)
	}
	now = now.Add(90 * time.Second)
	other, _ := queue.Claim(context.Background(), "other", 1, time.Minute)
	if len(other) != 0 {
		t.Fatalf("heartbeat lease was redelivered: %+v", other)
	}
	if err := queue.Nack(context.Background(), claimed[0].Lease, time.Minute); err != nil {
		t.Fatal(err)
	}
	other, _ = queue.Claim(context.Background(), "other", 1, time.Minute)
	if len(other) != 0 {
		t.Fatalf("delayed nack was immediately claimable: %+v", other)
	}
	now = now.Add(time.Minute)
	other, _ = queue.Claim(context.Background(), "other", 1, time.Minute)
	if len(other) != 1 || other[0].Attempt != 2 {
		t.Fatalf("claimed=%+v", other)
	}
}
