package temporal

import (
	"context"
	"errors"
	"fmt"

	"github.com/wahanbo/langgraph-go/checkpoint"
)

// ErrCheckpointNotCommitted prevents continuation from outrunning the
// independent LangGraph checkpoint source of truth.
var ErrCheckpointNotCommitted = errors.New("temporal continuation checkpoint is not committed")

// HistoryStats is the deterministic workflow-history usage observed by a binding.
type HistoryStats struct {
	Events int
	Bytes  int64
	Steps  int
}

// ContinuePolicy requests continuation when any configured threshold is met.
type ContinuePolicy struct {
	MaxHistoryEvents int
	MaxHistoryBytes  int64
	MaxSteps         int
}

func (p ContinuePolicy) validate() error {
	if p.MaxHistoryEvents < 0 || p.MaxHistoryBytes < 0 || p.MaxSteps < 0 {
		return fmt.Errorf("%w: continuation thresholds cannot be negative", ErrInvalidConfiguration)
	}
	if p.MaxHistoryEvents == 0 && p.MaxHistoryBytes == 0 && p.MaxSteps == 0 {
		return fmt.Errorf("%w: at least one continuation threshold is required", ErrInvalidConfiguration)
	}
	return nil
}

func (p ContinuePolicy) reached(stats HistoryStats) bool {
	return p.MaxHistoryEvents > 0 && stats.Events >= p.MaxHistoryEvents ||
		p.MaxHistoryBytes > 0 && stats.Bytes >= p.MaxHistoryBytes ||
		p.MaxSteps > 0 && stats.Steps >= p.MaxSteps
}

// ContinueRequest hands a new workflow execution an exact checkpoint reference.
// Input may carry immutable run parameters, but graph state must be reloaded
// from Checkpoint instead of being copied from workflow history.
type ContinueRequest[I any] struct {
	Input      I
	Checkpoint checkpoint.Config
	Stats      HistoryStats
	Generation int
}

// ContinuationClient is implemented by an optional Temporal SDK binding.
type ContinuationClient[I any] interface {
	ContinueAsNew(context.Context, ContinueRequest[I]) error
}

// ContinuationCoordinator verifies checkpoint durability before continuation.
type ContinuationCoordinator[I any] struct {
	client ContinuationClient[I]
	saver  checkpoint.Saver
	policy ContinuePolicy
}

// NewContinuationCoordinator validates the driver, independent Saver, and policy.
func NewContinuationCoordinator[I any](client ContinuationClient[I], saver checkpoint.Saver, policy ContinuePolicy) (*ContinuationCoordinator[I], error) {
	if isNil(client) {
		return nil, fmt.Errorf("%w: continuation client is nil", ErrInvalidConfiguration)
	}
	if isNil(saver) {
		return nil, fmt.Errorf("%w: checkpoint saver is nil", ErrInvalidConfiguration)
	}
	if err := policy.validate(); err != nil {
		return nil, err
	}
	return &ContinuationCoordinator[I]{client: client, saver: saver, policy: policy}, nil
}

// MaybeContinue returns false without storage I/O below policy thresholds. At
// a threshold it requires an exact, readable checkpoint before calling the driver.
func (c *ContinuationCoordinator[I]) MaybeContinue(ctx context.Context, request ContinueRequest[I]) (bool, error) {
	if ctx == nil {
		return false, &Error{Operation: "continue", Err: fmt.Errorf("%w: context is nil", ErrInvalidConfiguration)}
	}
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if !c.policy.reached(request.Stats) {
		return false, nil
	}
	if request.Generation < 0 || request.Checkpoint.CheckpointID == "" {
		return false, &Error{Operation: "continue", WorkflowID: request.Checkpoint.ThreadID, Err: fmt.Errorf("%w: exact checkpoint and non-negative generation are required", ErrInvalidConfiguration)}
	}
	if err := request.Checkpoint.Validate(); err != nil {
		return false, &Error{Operation: "continue", WorkflowID: request.Checkpoint.ThreadID, Err: fmt.Errorf("%w: %v", ErrInvalidConfiguration, err)}
	}
	tuple, found, err := c.saver.GetTuple(ctx, request.Checkpoint)
	if err != nil {
		return false, &Error{Operation: "verify-checkpoint", WorkflowID: request.Checkpoint.ThreadID, RunID: request.Checkpoint.CheckpointID, Err: err}
	}
	if !found || tuple.Config.CheckpointID != request.Checkpoint.CheckpointID {
		return false, &Error{Operation: "verify-checkpoint", WorkflowID: request.Checkpoint.ThreadID, RunID: request.Checkpoint.CheckpointID, Err: ErrCheckpointNotCommitted}
	}
	if err := c.client.ContinueAsNew(ctx, request); err != nil {
		return false, &Error{Operation: "continue-as-new", WorkflowID: request.Checkpoint.ThreadID, RunID: request.Checkpoint.CheckpointID, Err: err}
	}
	return true, nil
}
