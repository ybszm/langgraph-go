package temporal_test

import (
	"context"
	"errors"
	"testing"

	"github.com/wahanbo/langgraph-go/backend/temporal"
)

type resumePayload struct{ Value string }
type updatePayload struct{ Value int }
type updateResult struct{ Accepted bool }
type queryArgs struct{ CheckpointID string }
type queryState struct{ Count int }

type fakeControlClient struct {
	signal temporal.SignalRequest[resumePayload]
	update temporal.UpdateRequest[updatePayload]
	query  temporal.QueryRequest[queryArgs]
}

func (c *fakeControlClient) SignalWorkflow(_ context.Context, request temporal.SignalRequest[resumePayload]) error {
	c.signal = request
	return nil
}

func (c *fakeControlClient) UpdateWorkflow(_ context.Context, request temporal.UpdateRequest[updatePayload]) (updateResult, error) {
	c.update = request
	return updateResult{Accepted: true}, nil
}

func (c *fakeControlClient) QueryWorkflow(_ context.Context, request temporal.QueryRequest[queryArgs]) (queryState, error) {
	c.query = request
	return queryState{Count: 4}, nil
}

func TestControllerMapsResumeUpdateAndStateQuery(t *testing.T) {
	driver := &fakeControlClient{}
	controller, err := temporal.NewController[resumePayload, updatePayload, updateResult, queryArgs, queryState](driver)
	if err != nil {
		t.Fatal(err)
	}
	ref := temporal.WorkflowRef{WorkflowID: "workflow", RunID: "run"}
	if err := controller.SignalResume(context.Background(), ref, "interrupt-1", resumePayload{Value: "yes"}); err != nil {
		t.Fatal(err)
	}
	if driver.signal.Name != temporal.ResumeSignal || driver.signal.RequestID != "interrupt-1" || driver.signal.Ref != ref {
		t.Fatalf("signal=%+v", driver.signal)
	}
	result, err := controller.UpdateResume(context.Background(), ref, "update-1", updatePayload{Value: 3})
	if err != nil || !result.Accepted || driver.update.Name != temporal.ResumeUpdate || driver.update.RequestID != "update-1" {
		t.Fatalf("result=%+v update=%+v err=%v", result, driver.update, err)
	}
	state, err := controller.QueryState(context.Background(), ref, queryArgs{CheckpointID: "cp"})
	if err != nil || state.Count != 4 || driver.query.Name != temporal.StateQuery || driver.query.Args.CheckpointID != "cp" {
		t.Fatalf("state=%+v query=%+v err=%v", state, driver.query, err)
	}
}

func TestControllerValidatesIdentityAndCancellation(t *testing.T) {
	controller, _ := temporal.NewController[resumePayload, updatePayload, updateResult, queryArgs, queryState](&fakeControlClient{})
	err := controller.SignalResume(context.Background(), temporal.WorkflowRef{}, "request", resumePayload{})
	if !errors.Is(err, temporal.ErrInvalidConfiguration) {
		t.Fatalf("err=%v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = controller.QueryState(ctx, temporal.WorkflowRef{WorkflowID: "workflow"}, queryArgs{})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err=%v", err)
	}
}
