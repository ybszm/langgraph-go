package distributed_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/wahanbo/langgraph-go/backend/distributed"
	"github.com/wahanbo/langgraph-go/checkpoint"
	"github.com/wahanbo/langgraph-go/graph"
)

type nodeState struct{ Value int }
type nodeDelta struct{ Add int }

func TestGraphNodeHandlerMapsTaskRuntimeAndCheckpointResult(t *testing.T) {
	handler, err := distributed.NewGraphNodeHandler(func(_ context.Context, state nodeState, runtime graph.Runtime) (graph.Command[nodeDelta], error) {
		if runtime.Node != "node" || runtime.TaskID != "task" || runtime.Step != 4 || runtime.Attempt != 2 ||
			runtime.ThreadID != "thread" || runtime.CheckpointNamespace != "ns" || runtime.CheckpointID != "cp" || runtime.Context != "dependency" {
			t.Fatalf("runtime=%+v", runtime)
		}
		return graph.Update(nodeDelta{Add: state.Value}), nil
	}, distributed.GraphNodeOptions{RuntimeContext: "dependency"})
	if err != nil {
		t.Fatal(err)
	}
	firstAttempt := time.Date(2026, 7, 19, 6, 0, 0, 0, time.UTC)
	result, err := handler(context.Background(), distributed.Task[distributed.GraphTask[nodeState]]{
		ID: "task", Attempt: 2,
		Payload: distributed.GraphTask[nodeState]{
			State: nodeState{Value: 5}, Node: "node", Step: 4, FirstAttemptTime: firstAttempt,
			Checkpoint: checkpoint.Config{ThreadID: "thread", Namespace: "ns", CheckpointID: "cp"},
			TaskPath:   "pull/node", ResultIndex: 0,
		},
	})
	if err != nil || !result.Value.HasUpdate || result.Value.Update.Add != 5 ||
		result.Config.CheckpointID != "cp" || result.Channel != checkpoint.TaskResultChannel || result.TaskPath != "pull/node" {
		t.Fatalf("result=%+v err=%v", result, err)
	}
}

func TestGraphNodeHandlerUsesExternalResumeProvider(t *testing.T) {
	handler, _ := distributed.NewGraphNodeHandler(func(_ context.Context, _ nodeState, runtime graph.Runtime) (graph.Command[nodeDelta], error) {
		value, err := graph.AwaitResume[int](runtime, "number")
		if err != nil {
			return graph.Command[nodeDelta]{}, err
		}
		return graph.Update(nodeDelta{Add: value}), nil
	}, distributed.GraphNodeOptions{Resume: func(context.Context, graph.InterruptRequest) (json.RawMessage, error) {
		return json.RawMessage(`6`), nil
	}})
	result, err := handler(context.Background(), distributed.Task[distributed.GraphTask[nodeState]]{
		ID: "task", Attempt: 1, Payload: distributed.GraphTask[nodeState]{
			Node: "node", Step: 1, Checkpoint: checkpoint.Config{ThreadID: "thread", CheckpointID: "cp"},
		},
	})
	if err != nil || result.Value.Update.Add != 6 {
		t.Fatalf("result=%+v err=%v", result, err)
	}
}

func TestGraphNodeHandlerRejectsNonExactCheckpoint(t *testing.T) {
	handler, _ := distributed.NewGraphNodeHandler(func(context.Context, nodeState, graph.Runtime) (graph.Command[nodeDelta], error) {
		return graph.Command[nodeDelta]{}, nil
	}, distributed.GraphNodeOptions{})
	_, err := handler(context.Background(), distributed.Task[distributed.GraphTask[nodeState]]{
		ID: "task", Attempt: 1, Payload: distributed.GraphTask[nodeState]{Node: "node", Checkpoint: checkpoint.Config{ThreadID: "thread"}},
	})
	if err == nil {
		t.Fatal("expected exact checkpoint validation error")
	}
}
