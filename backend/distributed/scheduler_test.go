package distributed_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/ybszm/langgraph-go/backend/distributed"
	"github.com/ybszm/langgraph-go/checkpoint"
)

func TestSchedulerEnqueuesOnlyIncompleteCheckpointTasksWithStableIDs(t *testing.T) {
	config := checkpoint.Config{ThreadID: "thread", Namespace: "ns", CheckpointID: "cp"}
	stateCodec := checkpointJSONCodec[nodeState]{}
	encodedState, _ := stateCodec.Encode(nodeState{Value: 4})
	saver := &writeSaver{tuple: checkpoint.Tuple{
		Config: config,
		Checkpoint: checkpoint.Checkpoint{Version: checkpoint.CurrentVersion, ID: "cp", Timestamp: time.Now(), Step: 5,
			Values: map[string]checkpoint.EncodedValue{checkpoint.StateChannel: encodedState},
			Next:   []checkpoint.Task{{ID: "task-a", Name: "a"}, {ID: "task-b", Name: "b"}},
		},
		PendingWrites: []checkpoint.PendingWrite{{TaskID: "task-a", Index: 0, Channel: checkpoint.TaskResultChannel}},
	}}
	lease := 0
	queue, _ := distributed.NewCodecMemoryQueue(distributed.QueueOptions{
		Clock:       time.Now,
		IDGenerator: func() string { lease++; return "lease-" + string(rune('0'+lease)) },
	}, jsonCodec[distributed.GraphTask[nodeState]]{})
	scheduler, err := distributed.NewScheduler(saver, stateCodec, queue)
	if err != nil {
		t.Fatal(err)
	}
	ids, err := scheduler.Schedule(context.Background(), config)
	if err != nil || len(ids) != 1 || ids[0] != "task-b" {
		t.Fatalf("ids=%v err=%v", ids, err)
	}
	claimed, _ := queue.Claim(context.Background(), "worker", 1, time.Minute)
	if len(claimed) != 1 || claimed[0].ID != "task-b" || claimed[0].Payload.Node != "b" ||
		claimed[0].Payload.Step != 6 || claimed[0].Payload.State.Value != 4 || claimed[0].Payload.Checkpoint != config {
		t.Fatalf("claimed=%+v", claimed)
	}
	// Retry scheduling reuses the queue idempotency binding and creates no duplicate.
	ids, err = scheduler.Schedule(context.Background(), config)
	if err != nil || len(ids) != 1 || ids[0] != "task-b" {
		t.Fatalf("ids=%v err=%v", ids, err)
	}
	other, _ := queue.Claim(context.Background(), "other", 1, time.Minute)
	if len(other) != 0 {
		t.Fatalf("duplicate tasks=%+v", other)
	}
}

func TestSchedulerUsesTaskLocalInput(t *testing.T) {
	config := checkpoint.Config{ThreadID: "thread", CheckpointID: "cp"}
	codec := checkpointJSONCodec[nodeState]{}
	state, _ := codec.Encode(nodeState{Value: 1})
	input, _ := codec.Encode(nodeState{Value: 9})
	saver := &writeSaver{tuple: checkpoint.Tuple{Config: config, Checkpoint: checkpoint.Checkpoint{
		Version: checkpoint.CurrentVersion, ID: "cp", Timestamp: time.Now(),
		Values: map[string]checkpoint.EncodedValue{checkpoint.StateChannel: state},
		Next:   []checkpoint.Task{{ID: "send-task", Name: "worker", Input: &input}},
	}}}
	queue, _ := distributed.NewCodecMemoryQueue(distributed.QueueOptions{Clock: time.Now, IDGenerator: func() string { return "lease" }}, jsonCodec[distributed.GraphTask[nodeState]]{})
	scheduler, _ := distributed.NewScheduler(saver, codec, queue)
	_, _ = scheduler.Schedule(context.Background(), config)
	claimed, _ := queue.Claim(context.Background(), "worker", 1, time.Minute)
	if len(claimed) != 1 || claimed[0].Payload.State.Value != 9 {
		data, _ := json.Marshal(claimed)
		t.Fatalf("claimed=%s", data)
	}
}
