package sdk

import (
	"context"
	"errors"
	"testing"
	"time"

	temporaladapter "github.com/ybszm/langgraph-go/backend/temporal"
	"github.com/ybszm/langgraph-go/checkpoint"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"
)

func verifyCheckpoint(_ context.Context, config checkpoint.Config) (checkpoint.Config, error) {
	return config, nil
}
func continuationWorkflow(ctx workflow.Context, request temporaladapter.ContinueRequest[string]) error {
	return ContinueAsNewAfterCheckpoint(ctx, continuationWorkflow, verifyCheckpoint, request, workflow.ActivityOptions{StartToCloseTimeout: time.Second})
}
func childWorkflow(_ workflow.Context, request temporaladapter.ChildRequest[int]) (int, error) {
	return request.Input * 2, nil
}
func parentWorkflow(ctx workflow.Context) (int, error) {
	return ExecuteChild[int, int](ctx, childWorkflow, temporaladapter.ChildRequest[int]{ChildWorkflowID: "stable-child", Input: 4, ParentClosePolicy: temporaladapter.ParentAbandon}, "")
}

func TestContinueAsNewAfterVerifiedCheckpoint(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.RegisterActivity(verifyCheckpoint)
	request := temporaladapter.ContinueRequest[string]{Input: "input", Checkpoint: checkpoint.Config{ThreadID: "thread", Namespace: "ns", CheckpointID: "cp"}, Generation: 2}
	env.ExecuteWorkflow(continuationWorkflow, request)
	err := env.GetWorkflowError()
	if err == nil || !workflow.IsContinueAsNewError(errors.Unwrap(err)) && !workflow.IsContinueAsNewError(err) {
		t.Fatalf("expected continue-as-new error, got %v", err)
	}
}

func TestExecuteChildUsesOfficialSDK(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.RegisterWorkflow(childWorkflow)
	env.ExecuteWorkflow(parentWorkflow)
	if err := env.GetWorkflowError(); err != nil {
		t.Fatal(err)
	}
	var result int
	if err := env.GetWorkflowResult(&result); err != nil || result != 8 {
		t.Fatalf("result=%d err=%v", result, err)
	}
}
