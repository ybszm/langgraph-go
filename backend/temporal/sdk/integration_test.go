package sdk

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	temporaladapter "github.com/ybszm/langgraph-go/backend/temporal"
	"github.com/ybszm/langgraph-go/graph"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
)

func integrationWorkflow(_ workflow.Context, request WorkflowRequest[int]) (int, error) {
	return request.Input * 2, nil
}

func integrationControlWorkflow(ctx workflow.Context, request WorkflowRequest[int]) (int, error) {
	state := request.Input
	if err := workflow.SetQueryHandler(ctx, temporaladapter.StateQuery, func(temporaladapter.QueryRequest[string]) (int, error) {
		return state, nil
	}); err != nil {
		return 0, err
	}
	if err := workflow.SetUpdateHandler(ctx, temporaladapter.ResumeUpdate, func(_ workflow.Context, update temporaladapter.UpdateRequest[int]) (int, error) {
		state += update.Payload
		return state, nil
	}); err != nil {
		return 0, err
	}
	var signal temporaladapter.SignalRequest[int]
	workflow.GetSignalChannel(ctx, temporaladapter.ResumeSignal).Receive(ctx, &signal)
	return state + signal.Payload, nil
}

// TestTemporalServerIntegration is enabled by LANGGRAPH_TEMPORAL_ADDRESS. It
// exercises the official network client, worker, payload conversion, workflow
// start, and typed result path against a real Temporal server.
func TestTemporalServerIntegration(t *testing.T) {
	address := os.Getenv("LANGGRAPH_TEMPORAL_ADDRESS")
	if address == "" {
		t.Skip("set LANGGRAPH_TEMPORAL_ADDRESS to run the real Temporal integration test")
	}
	sdkClient, err := client.Dial(client.Options{HostPort: address})
	if err != nil {
		t.Fatal(err)
	}
	defer sdkClient.Close()
	suffix := fmt.Sprintf("%d", time.Now().UnixNano())
	queue := "langgraph-go-sdk-" + suffix
	workflowName := "langgraph.go.integration." + suffix
	controlWorkflowName := workflowName + ".control"
	w := worker.New(sdkClient, queue, worker.Options{DisableRegistrationAliasing: true})
	w.RegisterWorkflowWithOptions(integrationWorkflow, workflow.RegisterOptions{Name: workflowName})
	w.RegisterWorkflowWithOptions(integrationControlWorkflow, workflow.RegisterOptions{Name: controlWorkflowName})
	if err := w.Start(); err != nil {
		t.Fatal(err)
	}
	defer w.Stop()
	binding, err := NewClient[int, int](sdkClient, queue, workflowName)
	if err != nil {
		t.Fatal(err)
	}
	engine, err := temporaladapter.NewEngine[int, int](binding, temporaladapter.EngineOptions{})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	result, err := engine.Invoke(ctx, 21, graph.RunConfig{ThreadID: "integration", RunID: suffix})
	if err != nil {
		t.Fatal(err)
	}
	if result != 42 {
		t.Fatalf("result=%d, want 42", result)
	}
	controlBinding, err := NewClient[int, int](sdkClient, queue, controlWorkflowName)
	if err != nil {
		t.Fatal(err)
	}
	controlHandle, err := controlBinding.StartWorkflow(ctx, temporaladapter.StartRequest[int]{WorkflowID: "langgraph-go-control-" + suffix, Input: 21})
	if err != nil {
		t.Fatal(err)
	}
	controls, err := NewControlClient[int, int, int, string, int](sdkClient)
	if err != nil {
		t.Fatal(err)
	}
	ref := temporaladapter.WorkflowRef{WorkflowID: controlHandle.WorkflowID(), RunID: controlHandle.RunID()}
	updated, err := controls.UpdateWorkflow(ctx, temporaladapter.UpdateRequest[int]{Ref: ref, Name: temporaladapter.ResumeUpdate, RequestID: "update-1", Payload: 5})
	if err != nil || updated != 26 {
		t.Fatalf("updated=%d err=%v", updated, err)
	}
	queried, err := controls.QueryWorkflow(ctx, temporaladapter.QueryRequest[string]{Ref: ref, Name: temporaladapter.StateQuery, Args: "current"})
	if err != nil || queried != 26 {
		t.Fatalf("queried=%d err=%v", queried, err)
	}
	if err := controls.SignalWorkflow(ctx, temporaladapter.SignalRequest[int]{Ref: ref, Name: temporaladapter.ResumeSignal, RequestID: "signal-1", Payload: 2}); err != nil {
		t.Fatal(err)
	}
	controlResult, err := controlHandle.Get(ctx)
	if err != nil || controlResult != 28 {
		t.Fatalf("control result=%d err=%v", controlResult, err)
	}
}
