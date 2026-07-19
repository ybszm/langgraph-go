package distributed_test

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/wahanbo/langgraph-go/backend/distributed"
	"github.com/wahanbo/langgraph-go/checkpoint"
	"github.com/wahanbo/langgraph-go/graph"
)

type barrierState struct{ Total int }
type barrierDelta struct {
	Name string
	Add  int
}

func TestBarrierReducesOutOfOrderWritesInCheckpointTaskOrder(t *testing.T) {
	config := checkpoint.Config{ThreadID: "thread", CheckpointID: "cp"}
	stateCodec := checkpointJSONCodec[barrierState]{}
	commandCodec := checkpointJSONCodec[graph.Command[barrierDelta]]{}
	state, _ := stateCodec.Encode(barrierState{})
	commandA, _ := commandCodec.Encode(graph.Update(barrierDelta{Name: "a", Add: 1}))
	commandB, _ := commandCodec.Encode(graph.Update(barrierDelta{Name: "b", Add: 2}))
	saver := &writeSaver{tuple: checkpoint.Tuple{Config: config, Checkpoint: checkpoint.Checkpoint{
		Version: checkpoint.CurrentVersion, ID: "cp", Timestamp: time.Now(), Step: 3,
		Values: map[string]checkpoint.EncodedValue{checkpoint.StateChannel: state},
		Next:   []checkpoint.Task{{ID: "task-a", Name: "a"}, {ID: "task-b", Name: "b"}},
	}, PendingWrites: []checkpoint.PendingWrite{
		{TaskID: "task-b", Index: 0, Channel: checkpoint.TaskResultChannel, Value: commandB},
		{TaskID: "task-a", Index: 0, Channel: checkpoint.TaskResultChannel, Value: commandA},
	}}}
	var order []string
	barrier, err := distributed.NewBarrier(saver, stateCodec, commandCodec, func(_ context.Context, current barrierState, deltas []barrierDelta) (barrierState, error) {
		for _, delta := range deltas {
			order = append(order, delta.Name)
			current.Total += delta.Add
		}
		return current, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	result, err := barrier.Collect(context.Background(), config)
	if err != nil || !result.Ready || result.State.Total != 3 || !reflect.DeepEqual(order, []string{"a", "b"}) ||
		len(result.Commands) != 2 || result.Commands[0].Update.Name != "a" {
		t.Fatalf("result=%+v order=%v err=%v", result, order, err)
	}
}

func TestBarrierIncompleteReportsMissingWithoutReduce(t *testing.T) {
	config := checkpoint.Config{ThreadID: "thread", CheckpointID: "cp"}
	stateCodec := checkpointJSONCodec[barrierState]{}
	commandCodec := checkpointJSONCodec[graph.Command[barrierDelta]]{}
	state, _ := stateCodec.Encode(barrierState{})
	command, _ := commandCodec.Encode(graph.Update(barrierDelta{Name: "a"}))
	saver := &writeSaver{tuple: checkpoint.Tuple{Config: config, Checkpoint: checkpoint.Checkpoint{
		Version: checkpoint.CurrentVersion, ID: "cp", Timestamp: time.Now(),
		Values: map[string]checkpoint.EncodedValue{checkpoint.StateChannel: state},
		Next:   []checkpoint.Task{{ID: "task-a", Name: "a"}, {ID: "task-b", Name: "b"}},
	}, PendingWrites: []checkpoint.PendingWrite{{TaskID: "task-a", Index: 0, Channel: checkpoint.TaskResultChannel, Value: command}}}}
	reduced := false
	barrier, _ := distributed.NewBarrier(saver, stateCodec, commandCodec, func(context.Context, barrierState, []barrierDelta) (barrierState, error) {
		reduced = true
		return barrierState{}, nil
	})
	result, err := barrier.Collect(context.Background(), config)
	if err != nil || result.Ready || reduced || !reflect.DeepEqual(result.MissingTaskIDs, []string{"task-b"}) {
		t.Fatalf("result=%+v reduced=%v err=%v", result, reduced, err)
	}
}
