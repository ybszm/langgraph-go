package distributed_test

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/wahanbo/langgraph-go/backend/distributed"
)

type completion struct{ Value int }

type recordingOutbox struct {
	mu    sync.Mutex
	calls *[]string
	err   error
}

type atomicRecordingOutbox struct{ committed chan struct{} }

func (o *atomicRecordingOutbox) Commit(context.Context, distributed.Completion[completion]) error {
	close(o.committed)
	return nil
}
func (*atomicRecordingOutbox) AcknowledgesLease() bool { return true }

func (o *recordingOutbox) Commit(_ context.Context, value distributed.Completion[completion]) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	*o.calls = append(*o.calls, "commit")
	return o.err
}

type orderedQueue struct {
	*fakeQueue
	mu    sync.Mutex
	calls *[]string
}

func (q *orderedQueue) Ack(ctx context.Context, lease distributed.Lease) error {
	q.mu.Lock()
	*q.calls = append(*q.calls, "ack")
	q.mu.Unlock()
	return q.fakeQueue.Ack(ctx, lease)
}
func (q *orderedQueue) Nack(ctx context.Context, lease distributed.Lease, delay time.Duration) error {
	q.mu.Lock()
	*q.calls = append(*q.calls, "nack")
	q.mu.Unlock()
	return q.fakeQueue.Nack(ctx, lease, delay)
}

func TestOutboxHandlerCommitsBeforeWorkerAck(t *testing.T) {
	var calls []string
	queue := &orderedQueue{fakeQueue: newFakeQueue(), calls: &calls}
	outbox := &recordingOutbox{calls: &calls}
	handler, err := distributed.WithOutbox(func(_ context.Context, task distributed.Task[taskPayload]) (completion, error) {
		return completion{Value: task.Payload.Value * 2}, nil
	}, outbox)
	if err != nil {
		t.Fatal(err)
	}
	worker, _ := distributed.NewWorker[taskPayload](queue, handler, distributed.WorkerOptions{
		WorkerID: "worker", LeaseDuration: time.Second, HeartbeatInterval: 100 * time.Millisecond,
		PollInterval: time.Millisecond, BatchSize: 1, MaxConcurrency: 1,
	})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- worker.Run(ctx) }()
	<-queue.acks
	cancel()
	<-done
	if !reflect.DeepEqual(calls, []string{"commit", "ack"}) {
		t.Fatalf("calls=%v", calls)
	}
}

func TestOutboxFailureNacksWithoutAck(t *testing.T) {
	var calls []string
	queue := &orderedQueue{fakeQueue: newFakeQueue(), calls: &calls}
	handler, _ := distributed.WithOutbox(func(context.Context, distributed.Task[taskPayload]) (completion, error) {
		return completion{}, nil
	}, &recordingOutbox{calls: &calls, err: errors.New("store unavailable")})
	worker, _ := distributed.NewWorker[taskPayload](queue, handler, distributed.WorkerOptions{
		WorkerID: "worker", LeaseDuration: time.Second, HeartbeatInterval: 100 * time.Millisecond,
		PollInterval: time.Millisecond, BatchSize: 1, MaxConcurrency: 1,
	})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- worker.Run(ctx) }()
	<-queue.nacks
	cancel()
	<-done
	if !reflect.DeepEqual(calls, []string{"commit", "nack"}) {
		t.Fatalf("calls=%v", calls)
	}
}

func TestLeaseAcknowledgingOutboxSkipsWorkerAckAndNack(t *testing.T) {
	queue := newFakeQueue()
	outbox := &atomicRecordingOutbox{committed: make(chan struct{})}
	handler, _ := distributed.WithOutbox(func(context.Context, distributed.Task[taskPayload]) (completion, error) {
		return completion{Value: 4}, nil
	}, outbox)
	worker, _ := distributed.NewWorker(queue, handler, distributed.WorkerOptions{WorkerID: "worker", LeaseDuration: time.Second, HeartbeatInterval: 100 * time.Millisecond, PollInterval: time.Millisecond, BatchSize: 1, MaxConcurrency: 1})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- worker.Run(ctx) }()
	select {
	case <-outbox.committed:
	case <-time.After(time.Second):
		t.Fatal("atomic outbox did not commit")
	}
	select {
	case <-queue.acks:
		t.Fatal("worker issued a second ack")
	case <-queue.nacks:
		t.Fatal("worker nacked an atomically completed task")
	case <-time.After(25 * time.Millisecond):
	}
	cancel()
	<-done
}

func TestMemoryOutboxIsIdempotentAndIsolated(t *testing.T) {
	outbox, _ := distributed.NewMemoryOutbox(jsonCodec[map[string][]int]{})
	value := map[string][]int{"values": {1}}
	record := distributed.Completion[map[string][]int]{TaskID: "task", Attempt: 1, LeaseToken: "lease-1", Value: value}
	if err := outbox.Commit(context.Background(), record); err != nil {
		t.Fatal(err)
	}
	value["values"][0] = 9
	replayed := record
	replayed.LeaseToken = "lease-2"
	replayed.Value = map[string][]int{"values": {1}}
	if err := outbox.Commit(context.Background(), replayed); err != nil {
		t.Fatalf("idempotent replay: %v", err)
	}
	got, found, _ := outbox.Get(context.Background(), "task")
	if !found || got.Value["values"][0] != 1 {
		t.Fatalf("got=%+v found=%v", got, found)
	}
	replayed.Value["values"][0] = 2
	if err := outbox.Commit(context.Background(), replayed); !errors.Is(err, distributed.ErrCompletionConflict) {
		t.Fatalf("err=%v", err)
	}
}
