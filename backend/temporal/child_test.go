package temporal_test

import (
	"context"
	"testing"

	"github.com/ybszm/langgraph-go/backend/temporal"
	"github.com/ybszm/langgraph-go/graph"
)

type fakeChildClient struct {
	request temporal.ChildRequest[workflowInput]
	handle  temporal.Handle[workflowOutput]
}

func (c *fakeChildClient) StartChildWorkflow(_ context.Context, request temporal.ChildRequest[workflowInput]) (temporal.Handle[workflowOutput], error) {
	c.request = request
	return c.handle, nil
}

func TestChildAdapterMapsStatefulSubgraphIdentityAndNamespace(t *testing.T) {
	driver := &fakeChildClient{handle: fakeHandle{workflowID: "child", runID: "run", get: func(context.Context) (workflowOutput, error) {
		return workflowOutput{Value: 12}, nil
	}}}
	adapter, err := temporal.NewChildAdapter[workflowInput, workflowOutput](driver)
	if err != nil {
		t.Fatal(err)
	}
	output, err := adapter.Execute(context.Background(), temporal.ChildRequest[workflowInput]{
		Parent: temporal.WorkflowRef{WorkflowID: "parent", RunID: "parent-run"},
		Node:   "subgraph", TaskID: "task-2", Namespace: "subgraph:stable",
		Input:             workflowInput{Value: 6},
		Config:            graph.RunConfig{ThreadID: "thread"},
		ParentClosePolicy: temporal.ParentRequestCancel,
	})
	if err != nil || output.Value != 12 {
		t.Fatalf("output=%+v err=%v", output, err)
	}
	if driver.request.ChildWorkflowID != "parent/child/subgraph:stable/task-2" ||
		driver.request.Config.CheckpointNamespace != "subgraph:stable" ||
		driver.request.ParentClosePolicy != temporal.ParentRequestCancel {
		t.Fatalf("request=%+v", driver.request)
	}
}

func TestChildAdapterRequiresStableTaskIdentity(t *testing.T) {
	adapter, _ := temporal.NewChildAdapter[workflowInput, workflowOutput](&fakeChildClient{})
	_, err := adapter.Execute(context.Background(), temporal.ChildRequest[workflowInput]{
		Parent: temporal.WorkflowRef{WorkflowID: "parent"}, Node: "subgraph",
	})
	if err == nil {
		t.Fatal("expected missing task identity error")
	}
}
