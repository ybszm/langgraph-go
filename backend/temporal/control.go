package temporal

import (
	"context"
	"fmt"
)

const (
	// ResumeSignal is the stable signal name for asynchronous interrupt resume.
	ResumeSignal = "langgraph.resume"
	// ResumeUpdate is the stable update name for acknowledged interrupt resume.
	ResumeUpdate = "langgraph.resume.update"
	// StateQuery is the stable query name for checkpoint-backed graph state.
	StateQuery = "langgraph.state"
)

// WorkflowRef identifies an existing workflow execution. RunID may be empty
// to address the current execution of WorkflowID.
type WorkflowRef struct {
	WorkflowID string
	RunID      string
}

// SignalRequest carries an asynchronous, typed workflow signal.
type SignalRequest[P any] struct {
	Ref       WorkflowRef
	Name      string
	RequestID string
	Payload   P
}

// UpdateRequest carries a typed, acknowledged workflow update.
type UpdateRequest[P any] struct {
	Ref       WorkflowRef
	Name      string
	RequestID string
	Payload   P
}

// QueryRequest carries typed query arguments.
type QueryRequest[A any] struct {
	Ref  WorkflowRef
	Name string
	Args A
}

// ControlClient is implemented by an optional Temporal SDK control binding.
type ControlClient[SP, UP, UR, QA, QR any] interface {
	SignalWorkflow(context.Context, SignalRequest[SP]) error
	UpdateWorkflow(context.Context, UpdateRequest[UP]) (UR, error)
	QueryWorkflow(context.Context, QueryRequest[QA]) (QR, error)
}

// Controller maps LangGraph resume and state operations to workflow controls.
type Controller[SP, UP, UR, QA, QR any] struct {
	client ControlClient[SP, UP, UR, QA, QR]
}

// NewController validates and constructs a typed workflow control adapter.
func NewController[SP, UP, UR, QA, QR any](client ControlClient[SP, UP, UR, QA, QR]) (*Controller[SP, UP, UR, QA, QR], error) {
	if isNil(client) {
		return nil, fmt.Errorf("%w: control client is nil", ErrInvalidConfiguration)
	}
	return &Controller[SP, UP, UR, QA, QR]{client: client}, nil
}

// SignalResume maps fire-and-forget interrupt resume to a workflow signal.
func (c *Controller[SP, UP, UR, QA, QR]) SignalResume(ctx context.Context, ref WorkflowRef, requestID string, payload SP) error {
	if err := validateControl(ctx, ref, requestID); err != nil {
		return err
	}
	err := c.client.SignalWorkflow(ctx, SignalRequest[SP]{Ref: ref, Name: ResumeSignal, RequestID: requestID, Payload: payload})
	if err != nil {
		return &Error{Operation: "signal-resume", WorkflowID: ref.WorkflowID, RunID: ref.RunID, Err: err}
	}
	return nil
}

// UpdateResume maps acknowledged interrupt resume to a workflow update.
func (c *Controller[SP, UP, UR, QA, QR]) UpdateResume(ctx context.Context, ref WorkflowRef, requestID string, payload UP) (UR, error) {
	var zero UR
	if err := validateControl(ctx, ref, requestID); err != nil {
		return zero, err
	}
	result, err := c.client.UpdateWorkflow(ctx, UpdateRequest[UP]{Ref: ref, Name: ResumeUpdate, RequestID: requestID, Payload: payload})
	if err != nil {
		return zero, &Error{Operation: "update-resume", WorkflowID: ref.WorkflowID, RunID: ref.RunID, Err: err}
	}
	return result, nil
}

// QueryState maps graph state inspection to a typed workflow query. The SDK
// binding or workflow must read LangGraph checkpoints; Temporal history is not
// treated as the state source of truth.
func (c *Controller[SP, UP, UR, QA, QR]) QueryState(ctx context.Context, ref WorkflowRef, args QA) (QR, error) {
	var zero QR
	if err := validateControl(ctx, ref, "query"); err != nil {
		return zero, err
	}
	result, err := c.client.QueryWorkflow(ctx, QueryRequest[QA]{Ref: ref, Name: StateQuery, Args: args})
	if err != nil {
		return zero, &Error{Operation: "query-state", WorkflowID: ref.WorkflowID, RunID: ref.RunID, Err: err}
	}
	return result, nil
}

func validateControl(ctx context.Context, ref WorkflowRef, requestID string) error {
	if ctx == nil {
		return &Error{Operation: "control", WorkflowID: ref.WorkflowID, RunID: ref.RunID, Err: fmt.Errorf("%w: context is nil", ErrInvalidConfiguration)}
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if ref.WorkflowID == "" {
		return &Error{Operation: "control", Err: fmt.Errorf("%w: workflow ID is empty", ErrInvalidConfiguration)}
	}
	if requestID == "" {
		return &Error{Operation: "control", WorkflowID: ref.WorkflowID, RunID: ref.RunID, Err: fmt.Errorf("%w: request ID is empty", ErrInvalidConfiguration)}
	}
	return nil
}
