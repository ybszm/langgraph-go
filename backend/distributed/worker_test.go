package distributed_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/wahanbo/langgraph-go/backend/distributed"
)

type fakeQueue struct {
	mu         sync.Mutex
	task       *distributed.Task[taskPayload]
	heartbeats chan struct{}
	acks       chan struct{}
	nacks      chan struct{}
}

func (*fakeQueue) Enqueue(context.Context, distributed.EnqueueRequest[taskPayload]) (string, error) {
	return "", nil
}
func (q *fakeQueue) Claim(context.Context, string, int, time.Duration) ([]distributed.Task[taskPayload], error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.task == nil {
		return nil, nil
	}
	task := *q.task
	q.task = nil
	return []distributed.Task[taskPayload]{task}, nil
}
func (q *fakeQueue) Ack(context.Context, distributed.Lease) error {
	q.acks <- struct{}{}
	return nil
}
func (q *fakeQueue) Nack(context.Context, distributed.Lease, time.Duration) error {
	q.nacks <- struct{}{}
	return nil
}
func (q *fakeQueue) Heartbeat(context.Context, distributed.Lease, time.Duration) error {
	select {
	case q.heartbeats <- struct{}{}:
	default:
	}
	return nil
}

func newFakeQueue() *fakeQueue {
	return &fakeQueue{
		task:       &distributed.Task[taskPayload]{ID: "task", Payload: taskPayload{Value: 2}, Lease: distributed.Lease{TaskID: "task", Token: "lease", WorkerID: "worker"}},
		heartbeats: make(chan struct{}, 1), acks: make(chan struct{}, 1), nacks: make(chan struct{}, 1),
	}
}

func TestWorkerHeartbeatsAndAcknowledgesSuccessfulTask(t *testing.T) {
	queue := newFakeQueue()
	release := make(chan struct{})
	worker, err := distributed.NewWorker(queue, func(_ context.Context, task distributed.Task[taskPayload]) error {
		if task.Payload.Value != 2 {
			t.Errorf("task=%+v", task)
		}
		<-release
		return nil
	}, distributed.WorkerOptions{
		WorkerID: "worker", LeaseDuration: 50 * time.Millisecond, HeartbeatInterval: 5 * time.Millisecond,
		PollInterval: time.Millisecond, BatchSize: 1, MaxConcurrency: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- worker.Run(ctx) }()
	select {
	case <-queue.heartbeats:
	case <-time.After(time.Second):
		t.Fatal("worker did not heartbeat")
	}
	close(release)
	select {
	case <-queue.acks:
	case <-time.After(time.Second):
		t.Fatal("worker did not ack")
	}
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("err=%v", err)
	}
}

func TestWorkerNacksHandlerFailure(t *testing.T) {
	queue := newFakeQueue()
	worker, _ := distributed.NewWorker(queue, func(context.Context, distributed.Task[taskPayload]) error {
		return errors.New("retry")
	}, distributed.WorkerOptions{
		WorkerID: "worker", LeaseDuration: time.Second, HeartbeatInterval: 100 * time.Millisecond,
		PollInterval: time.Millisecond, NackDelay: time.Second, BatchSize: 1, MaxConcurrency: 1,
	})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- worker.Run(ctx) }()
	select {
	case <-queue.nacks:
	case <-time.After(time.Second):
		t.Fatal("worker did not nack")
	}
	cancel()
	<-done
}
