package temporal

import (
	"context"
	"fmt"
	"net/url"

	"github.com/ybszm/langgraph-go/graph"
)

// ParentClosePolicy controls what a binding does to a child when its parent closes.
type ParentClosePolicy uint8

const (
	// ParentTerminate terminates the child with its parent.
	ParentTerminate ParentClosePolicy = iota
	// ParentRequestCancel requests graceful child cancellation.
	ParentRequestCancel
	// ParentAbandon leaves the child running independently.
	ParentAbandon
)

// ChildRequest maps one stateful subgraph task to a child workflow execution.
type ChildRequest[I any] struct {
	Parent            WorkflowRef
	ChildWorkflowID   string
	Node              graph.NodeID
	TaskID            string
	Namespace         string
	Input             I
	Config            graph.RunConfig
	ParentClosePolicy ParentClosePolicy
}

// ChildClient is implemented by an optional Temporal SDK child-workflow binding.
type ChildClient[I, O any] interface {
	StartChildWorkflow(context.Context, ChildRequest[I]) (Handle[O], error)
}

// ChildAdapter maps stateful subgraph tasks to stable child workflow identities.
type ChildAdapter[I, O any] struct{ client ChildClient[I, O] }

// NewChildAdapter validates and constructs a child workflow adapter.
func NewChildAdapter[I, O any](client ChildClient[I, O]) (*ChildAdapter[I, O], error) {
	if isNil(client) {
		return nil, fmt.Errorf("%w: child workflow client is nil", ErrInvalidConfiguration)
	}
	return &ChildAdapter[I, O]{client: client}, nil
}

// Execute starts a child workflow and waits for its typed result. The stable
// task ID prevents retries from creating a different child identity.
func (a *ChildAdapter[I, O]) Execute(ctx context.Context, request ChildRequest[I]) (O, error) {
	var zero O
	if ctx == nil {
		return zero, &Error{Operation: "child-start", Err: fmt.Errorf("%w: context is nil", ErrInvalidConfiguration)}
	}
	if err := ctx.Err(); err != nil {
		return zero, err
	}
	if request.Parent.WorkflowID == "" || request.Node == "" || request.TaskID == "" {
		return zero, &Error{Operation: "child-start", WorkflowID: request.Parent.WorkflowID, RunID: request.Parent.RunID, Err: fmt.Errorf("%w: parent workflow, node, and task ID are required", ErrInvalidConfiguration)}
	}
	if request.ParentClosePolicy > ParentAbandon {
		return zero, &Error{Operation: "child-start", WorkflowID: request.Parent.WorkflowID, RunID: request.Parent.RunID, Err: fmt.Errorf("%w: unknown parent close policy", ErrInvalidConfiguration)}
	}
	namespace := request.Namespace
	if namespace == "" {
		namespace = string(request.Node)
	}
	request.ChildWorkflowID = request.Parent.WorkflowID + "/child/" + url.PathEscape(namespace) + "/" + url.PathEscape(request.TaskID)
	request.Config = cloneRunConfig(request.Config)
	request.Config.CheckpointNamespace = namespace
	handle, err := a.client.StartChildWorkflow(ctx, request)
	if err != nil {
		return zero, &Error{Operation: "child-start", WorkflowID: request.ChildWorkflowID, Err: err}
	}
	if isNil(handle) {
		return zero, &Error{Operation: "child-start", WorkflowID: request.ChildWorkflowID, Err: fmt.Errorf("%w: child workflow handle is nil", ErrInvalidConfiguration)}
	}
	output, err := handle.Get(ctx)
	if err != nil {
		return zero, &Error{Operation: "child-get", WorkflowID: handle.WorkflowID(), RunID: handle.RunID(), Err: err}
	}
	return output, nil
}
