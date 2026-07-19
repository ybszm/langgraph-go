package sdk

import (
	"context"
	"fmt"

	temporaladapter "github.com/wahanbo/langgraph-go/backend/temporal"
	"github.com/wahanbo/langgraph-go/checkpoint"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/workflow"
)

// ExecuteChild maps a stable ChildRequest to the official child-workflow API.
func ExecuteChild[I, O any](ctx workflow.Context, workflowType any, request temporaladapter.ChildRequest[I], taskQueue string) (O, error) {
	var output O
	if workflowType == nil || request.ChildWorkflowID == "" {
		return output, fmt.Errorf("%w: child workflow and stable ID are required", temporaladapter.ErrInvalidConfiguration)
	}
	policy := enumspb.PARENT_CLOSE_POLICY_TERMINATE
	switch request.ParentClosePolicy {
	case temporaladapter.ParentRequestCancel:
		policy = enumspb.PARENT_CLOSE_POLICY_REQUEST_CANCEL
	case temporaladapter.ParentAbandon:
		policy = enumspb.PARENT_CLOSE_POLICY_ABANDON
	}
	options := workflow.ChildWorkflowOptions{WorkflowID: request.ChildWorkflowID, TaskQueue: taskQueue, ParentClosePolicy: policy}
	ctx = workflow.WithChildOptions(ctx, options)
	err := workflow.ExecuteChildWorkflow(ctx, workflowType, request).Get(ctx, &output)
	return output, err
}

// CheckpointVerifier is registered as an Activity so workflow code never reads
// an external checkpoint database directly.
type CheckpointVerifier struct{ Saver checkpoint.Saver }

func (v CheckpointVerifier) Verify(ctx context.Context, config checkpoint.Config) (checkpoint.Config, error) {
	if v.Saver == nil {
		return checkpoint.Config{}, fmt.Errorf("%w: checkpoint saver is required", temporaladapter.ErrInvalidConfiguration)
	}
	tuple, found, err := v.Saver.GetTuple(ctx, config)
	if err != nil {
		return checkpoint.Config{}, err
	}
	if !found || tuple.Config.CheckpointID != config.CheckpointID {
		return checkpoint.Config{}, temporaladapter.ErrCheckpointNotCommitted
	}
	return tuple.Config, nil
}

// ContinueAsNewAfterCheckpoint runs a verifier Activity, checks the exact
// coordinate, then returns the SDK ContinueAsNew error to the workflow runner.
func ContinueAsNewAfterCheckpoint[I any](ctx workflow.Context, workflowType, verifyActivity any, request temporaladapter.ContinueRequest[I], activityOptions workflow.ActivityOptions) error {
	if workflowType == nil || verifyActivity == nil || request.Checkpoint.CheckpointID == "" {
		return fmt.Errorf("%w: workflow, verifier, and exact checkpoint are required", temporaladapter.ErrInvalidConfiguration)
	}
	ctx = workflow.WithActivityOptions(ctx, activityOptions)
	var verified checkpoint.Config
	if err := workflow.ExecuteActivity(ctx, verifyActivity, request.Checkpoint).Get(ctx, &verified); err != nil {
		return err
	}
	if verified != request.Checkpoint {
		return temporaladapter.ErrCheckpointNotCommitted
	}
	return workflow.NewContinueAsNewError(ctx, workflowType, request)
}
