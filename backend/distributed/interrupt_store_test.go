package distributed_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/ybszm/langgraph-go/backend/distributed"
	"github.com/ybszm/langgraph-go/checkpoint"
	"github.com/ybszm/langgraph-go/graph"
)

func TestMemoryInterruptStorePausesThenResumesDistributedNode(t *testing.T) {
	clock := time.Date(2026, 7, 19, 8, 0, 0, 0, time.UTC)
	store, err := distributed.NewMemoryInterruptStore(func() time.Time { return clock })
	if err != nil {
		t.Fatal(err)
	}
	handler, _ := distributed.NewGraphNodeHandler(func(_ context.Context, _ nodeState, runtime graph.Runtime) (graph.Command[nodeDelta], error) {
		value, err := graph.AwaitResume[int](runtime, map[string]string{"question": "number?"})
		if err != nil {
			return graph.Command[nodeDelta]{}, err
		}
		return graph.Update(nodeDelta{Add: value}), nil
	}, distributed.GraphNodeOptions{Resume: store.Provider()})
	task := distributed.Task[distributed.GraphTask[nodeState]]{
		ID: "task", Attempt: 1, Payload: distributed.GraphTask[nodeState]{
			Node: "node", Step: 1, Checkpoint: checkpoint.Config{ThreadID: "thread", Namespace: "ns", CheckpointID: "cp"},
		},
	}
	_, err = handler(context.Background(), task)
	if !errors.Is(err, graph.ErrGraphInterrupt) {
		t.Fatalf("err=%v", err)
	}
	records, err := store.List(context.Background(), "thread")
	if err != nil || len(records) != 1 || records[0].Interrupt.ID == "" || records[0].Status != distributed.InterruptPending || !records[0].CreatedAt.Equal(clock) {
		t.Fatalf("records=%+v err=%v", records, err)
	}
	if err := store.Resume(context.Background(), "thread", records[0].Interrupt.ID, 7); err != nil {
		t.Fatal(err)
	}
	task.Attempt = 2
	result, err := handler(context.Background(), task)
	if err != nil || result.Value.Update.Add != 7 {
		t.Fatalf("result=%+v err=%v", result, err)
	}
}

func TestMemoryInterruptStoreResumeIsIdempotentAndConflictingValueFails(t *testing.T) {
	store, _ := distributed.NewMemoryInterruptStore(time.Now)
	request := graph.InterruptRequest{
		Interrupt: graph.Interrupt{ID: "interrupt", Value: []byte(`"prompt"`)},
		TaskID:    "task", ThreadID: "thread", CheckpointID: "cp",
	}
	_, _ = store.Await(context.Background(), request)
	if err := store.Resume(context.Background(), "thread", "interrupt", map[string]int{"value": 1}); err != nil {
		t.Fatal(err)
	}
	if err := store.Resume(context.Background(), "thread", "interrupt", map[string]int{"value": 1}); err != nil {
		t.Fatalf("idempotent resume: %v", err)
	}
	if err := store.Resume(context.Background(), "thread", "interrupt", map[string]int{"value": 2}); !errors.Is(err, distributed.ErrInterruptConflict) {
		t.Fatalf("err=%v", err)
	}
}
