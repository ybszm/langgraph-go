package distributed

import (
	"context"
	"fmt"
	"time"

	"github.com/ybszm/langgraph-go/checkpoint"
	"github.com/ybszm/langgraph-go/graph"
	lgstore "github.com/ybszm/langgraph-go/store"
)

// GraphTask is the serializable input needed to execute one graph node remotely.
type GraphTask[S any] struct {
	State            S
	Node             graph.NodeID
	Step             int
	FirstAttemptTime time.Time
	Checkpoint       checkpoint.Config
	TaskPath         string
	ResultIndex      int
}

// GraphNodeOptions supplies worker-local runtime dependencies not persisted in tasks.
type GraphNodeOptions struct {
	RuntimeContext any
	Store          lgstore.Store
	ResultChannel  string
	Resume         graph.ResumeProvider
	WriteCustom    func(any) error
	WriteMessage   func(any, map[string]any) error
}

// NewGraphNodeHandler maps a typed graph node to a distributed result handler
// whose output is ready for CheckpointOutbox.
func NewGraphNodeHandler[S, D any](node graph.Node[S, D], options GraphNodeOptions) (ResultHandler[GraphTask[S], CheckpointResult[graph.Command[D]]], error) {
	if node == nil {
		return nil, fmt.Errorf("%w: graph node is nil", ErrInvalidRequest)
	}
	if options.ResultChannel == "" {
		options.ResultChannel = checkpoint.TaskResultChannel
	}
	if !validID(options.ResultChannel) {
		return nil, fmt.Errorf("%w: graph result channel is invalid", ErrInvalidRequest)
	}
	return func(ctx context.Context, task Task[GraphTask[S]]) (CheckpointResult[graph.Command[D]], error) {
		var zero CheckpointResult[graph.Command[D]]
		if err := validContext(ctx); err != nil {
			return zero, err
		}
		payload := task.Payload
		if !validID(task.ID) || task.Attempt < 1 || payload.Node == "" || payload.Step < 0 ||
			payload.ResultIndex < 0 || payload.Checkpoint.CheckpointID == "" {
			return zero, &WorkerError{Operation: "validate-graph-task", TaskID: task.ID, Err: ErrInvalidRequest}
		}
		if err := payload.Checkpoint.Validate(); err != nil {
			return zero, &WorkerError{Operation: "validate-graph-task", TaskID: task.ID, Err: err}
		}
		runtime := graph.Runtime{
			Step: payload.Step, Node: payload.Node, TaskID: task.ID, Attempt: task.Attempt,
			FirstAttemptTime: payload.FirstAttemptTime, ThreadID: payload.Checkpoint.ThreadID,
			CheckpointNamespace: payload.Checkpoint.Namespace, CheckpointID: payload.Checkpoint.CheckpointID,
			Store: options.Store, Context: options.RuntimeContext,
		}
		runtime, err := graph.AttachRuntimeServices(ctx, runtime, graph.RuntimeServices{
			Resume: options.Resume, WriteCustom: options.WriteCustom, WriteMessage: options.WriteMessage,
		})
		if err != nil {
			return zero, &WorkerError{Operation: "attach-runtime-services", TaskID: task.ID, Err: err}
		}
		command, err := node(ctx, payload.State, runtime)
		if err != nil {
			return zero, &WorkerError{Operation: "execute-graph-node", TaskID: task.ID, Err: err}
		}
		return CheckpointResult[graph.Command[D]]{
			Config: payload.Checkpoint, TaskPath: payload.TaskPath, Index: payload.ResultIndex,
			Channel: options.ResultChannel, Value: command,
		}, nil
	}, nil
}
