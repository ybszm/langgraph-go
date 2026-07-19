package distributed_test

import (
	"context"
	"testing"
	"time"

	"github.com/ybszm/langgraph-go/backend/distributed"
	"github.com/ybszm/langgraph-go/checkpoint"
	"github.com/ybszm/langgraph-go/graph"
)

type commitSaver struct {
	writeSaver
	parent   checkpoint.Config
	value    checkpoint.Checkpoint
	metadata checkpoint.Metadata
	versions map[string]string
}

func (s *commitSaver) Put(_ context.Context, parent checkpoint.Config, value checkpoint.Checkpoint, metadata checkpoint.Metadata, versions map[string]string) (checkpoint.Config, error) {
	s.parent, s.value, s.metadata, s.versions = parent, value, metadata, versions
	return checkpoint.Config{ThreadID: parent.ThreadID, Namespace: parent.Namespace, CheckpointID: value.ID}, nil
}

func TestStepCoordinatorRoutesAndPersistsReducedBarrier(t *testing.T) {
	parent := checkpoint.Config{ThreadID: "thread", Namespace: "ns", CheckpointID: "cp"}
	saver := &commitSaver{writeSaver: writeSaver{tuple: checkpoint.Tuple{Config: parent, Checkpoint: checkpoint.Checkpoint{
		Version: checkpoint.CurrentVersion, ID: "cp", Timestamp: time.Now(), Step: 3,
		ChannelVersions: map[string]string{checkpoint.StateChannel: "old"},
		VersionsSeen:    map[string]map[string]string{"node": {checkpoint.StateChannel: "old"}},
	}}}}
	clock := time.Date(2026, 7, 19, 7, 0, 0, 0, time.UTC)
	resolverCalled := false
	coordinator, err := distributed.NewStepCoordinator(saver, checkpointJSONCodec[barrierState]{}, func(_ context.Context, input distributed.StepInput[barrierState, barrierDelta]) (distributed.StepPlan, error) {
		resolverCalled = true
		if input.State.Total != 3 || input.Step != 4 || len(input.Commands) != 2 || input.TaskIDs[0] != "task-a" {
			t.Fatalf("input=%+v", input)
		}
		return distributed.StepPlan{Next: []checkpoint.Task{{ID: "next-task", Name: "next"}}, Metadata: checkpoint.Metadata{"route": "next"}}, nil
	}, distributed.StepCoordinatorOptions{
		Clock:        func() time.Time { return clock },
		CheckpointID: func(checkpoint.Config, int) string { return "cp-next" },
	})
	if err != nil {
		t.Fatal(err)
	}
	config, err := coordinator.Commit(context.Background(), parent, distributed.BarrierResult[barrierState, barrierDelta]{
		Ready: true, State: barrierState{Total: 3}, Step: 4, TaskIDs: []string{"task-a", "task-b"},
		Commands: []graph.Command[barrierDelta]{{}, {}},
	})
	if err != nil || !resolverCalled || config.CheckpointID != "cp-next" {
		t.Fatalf("config=%+v called=%v err=%v", config, resolverCalled, err)
	}
	if saver.parent != parent || saver.value.ID != "cp-next" || saver.value.Step != 4 || !saver.value.Timestamp.Equal(clock) ||
		len(saver.value.Next) != 1 || saver.value.Next[0].ID != "next-task" || saver.metadata["route"] != "next" ||
		saver.versions[checkpoint.StateChannel] == "old" {
		t.Fatalf("checkpoint=%+v metadata=%v versions=%v", saver.value, saver.metadata, saver.versions)
	}
}

func TestStepCoordinatorRejectsIncompleteBarrier(t *testing.T) {
	parent := checkpoint.Config{ThreadID: "thread", CheckpointID: "cp"}
	saver := &commitSaver{}
	coordinator, _ := distributed.NewStepCoordinator(saver, checkpointJSONCodec[barrierState]{}, func(context.Context, distributed.StepInput[barrierState, barrierDelta]) (distributed.StepPlan, error) {
		t.Fatal("resolver should not run")
		return distributed.StepPlan{}, nil
	}, distributed.StepCoordinatorOptions{Clock: time.Now, CheckpointID: func(checkpoint.Config, int) string { return "next" }})
	_, err := coordinator.Commit(context.Background(), parent, distributed.BarrierResult[barrierState, barrierDelta]{Ready: false})
	if err == nil {
		t.Fatal("expected incomplete barrier error")
	}
}
