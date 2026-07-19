package temporal_test

import (
	"context"
	"errors"
	"testing"

	"github.com/wahanbo/langgraph-go/backend/temporal"
	"github.com/wahanbo/langgraph-go/graph"
)

type workflowInput struct{ Value int }
type workflowOutput struct{ Value int }

type fakeHandle struct {
	workflowID string
	runID      string
	get        func(context.Context) (workflowOutput, error)
}

func (h fakeHandle) WorkflowID() string                              { return h.workflowID }
func (h fakeHandle) RunID() string                                   { return h.runID }
func (h fakeHandle) Get(ctx context.Context) (workflowOutput, error) { return h.get(ctx) }

type fakeWorkflowClient struct {
	request temporal.StartRequest[workflowInput]
	handle  temporal.Handle[workflowOutput]
}

func (c *fakeWorkflowClient) StartWorkflow(_ context.Context, request temporal.StartRequest[workflowInput]) (temporal.Handle[workflowOutput], error) {
	c.request = request
	return c.handle, nil
}

func TestEngineMapsGraphRunToWorkflowAndWaits(t *testing.T) {
	client := &fakeWorkflowClient{handle: fakeHandle{workflowID: "wf", runID: "temporal-run", get: func(context.Context) (workflowOutput, error) {
		return workflowOutput{Value: 8}, nil
	}}}
	engine, err := temporal.NewEngine[workflowInput, workflowOutput](client, temporal.EngineOptions{
		WorkflowIDGenerator: func() string { return "generated" },
	})
	if err != nil {
		t.Fatal(err)
	}
	output, err := engine.Invoke(context.Background(), workflowInput{Value: 4}, graph.RunConfig{ThreadID: "thread", RunID: "run", Metadata: map[string]any{"k": "v"}})
	if err != nil || output.Value != 8 {
		t.Fatalf("output=%+v err=%v", output, err)
	}
	if client.request.WorkflowID != "langgraph/thread/run" || client.request.Input.Value != 4 || client.request.Config.Metadata["k"] != "v" {
		t.Fatalf("request=%+v", client.request)
	}
}

func TestEngineWaitHonorsCancellation(t *testing.T) {
	client := &fakeWorkflowClient{handle: fakeHandle{get: func(ctx context.Context) (workflowOutput, error) {
		<-ctx.Done()
		return workflowOutput{}, ctx.Err()
	}}}
	engine, _ := temporal.NewEngine[workflowInput, workflowOutput](client, temporal.EngineOptions{})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := engine.Invoke(ctx, workflowInput{}, graph.RunConfig{})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err=%v", err)
	}
}

type activityState struct{ Value int }
type activityDelta struct{ Add int }

func TestActivityMapsSerializableRuntimeAndCommand(t *testing.T) {
	activity, err := temporal.NewActivity[activityState, activityDelta](func(_ context.Context, state activityState, runtime graph.Runtime) (graph.Command[activityDelta], error) {
		if runtime.Step != 3 || runtime.Node != "work" || runtime.TaskID != "task" || runtime.ThreadID != "thread" || runtime.Context != "dependency" {
			t.Fatalf("runtime=%+v", runtime)
		}
		return graph.Update(activityDelta{Add: state.Value}), nil
	})
	if err != nil {
		t.Fatal(err)
	}
	result, err := activity.Execute(context.Background(), temporal.ActivityRequest[activityState]{
		State:   activityState{Value: 5},
		Runtime: temporal.ActivityRuntime{Step: 3, Node: "work", TaskID: "task", ThreadID: "thread", Context: "dependency"},
	})
	if err != nil || !result.HasUpdate || result.Update.Add != 5 {
		t.Fatalf("result=%+v err=%v", result, err)
	}
}
