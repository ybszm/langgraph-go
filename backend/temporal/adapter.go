// Package temporal defines a provider-neutral adapter boundary for mapping
// LangGraph runs and nodes to Temporal-style workflows and activities. It does
// not import a Temporal SDK; SDK bindings implement the small Client contract.
package temporal

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"net/url"
	"reflect"
	"strings"
	"time"

	"github.com/ybszm/langgraph-go/graph"
)

// ErrInvalidConfiguration identifies an invalid adapter dependency or request.
var ErrInvalidConfiguration = errors.New("invalid temporal adapter configuration")

// Error adds workflow identity and operation context while preserving causes.
type Error struct {
	Operation  string
	WorkflowID string
	RunID      string
	Err        error
}

func (e *Error) Error() string {
	identity := e.WorkflowID
	if e.RunID != "" {
		identity += "/" + e.RunID
	}
	if identity == "" {
		return fmt.Sprintf("temporal %s: %v", e.Operation, e.Err)
	}
	return fmt.Sprintf("temporal %s %s: %v", e.Operation, identity, e.Err)
}

// Unwrap returns the underlying driver or context error.
func (e *Error) Unwrap() error { return e.Err }

// StartRequest is the serializable workflow start boundary.
type StartRequest[I any] struct {
	WorkflowID string
	Input      I
	Config     graph.RunConfig
}

// Handle represents one started workflow execution.
type Handle[O any] interface {
	WorkflowID() string
	RunID() string
	Get(context.Context) (O, error)
}

// Client is implemented by an optional Temporal SDK binding.
type Client[I, O any] interface {
	StartWorkflow(context.Context, StartRequest[I]) (Handle[O], error)
}

// EngineOptions controls deterministic workflow identity.
type EngineOptions struct {
	// WorkflowIDGenerator is used when RunConfig.RunID is empty.
	WorkflowIDGenerator func() string
}

// Engine maps the graph Invoker contract to workflow start plus result wait.
type Engine[I, O any] struct {
	client Client[I, O]
	ids    func() string
}

// NewEngine validates and constructs a provider-neutral workflow engine.
func NewEngine[I, O any](client Client[I, O], options EngineOptions) (*Engine[I, O], error) {
	if isNil(client) {
		return nil, fmt.Errorf("%w: workflow client is nil", ErrInvalidConfiguration)
	}
	if options.WorkflowIDGenerator == nil {
		options.WorkflowIDGenerator = randomID
	}
	return &Engine[I, O]{client: client, ids: options.WorkflowIDGenerator}, nil
}

// Invoke starts a workflow and waits for its typed result. Canceling ctx ends
// the wait; workflow cancellation remains an explicit driver operation.
func (e *Engine[I, O]) Invoke(ctx context.Context, input I, config graph.RunConfig) (O, error) {
	var zero O
	if ctx == nil {
		return zero, &Error{Operation: "start", Err: fmt.Errorf("%w: context is nil", ErrInvalidConfiguration)}
	}
	if err := ctx.Err(); err != nil {
		return zero, err
	}
	generated := ""
	if config.RunID == "" {
		generated = e.ids()
	}
	workflowID := workflowID(config.ThreadID, config.RunID, generated)
	request := StartRequest[I]{WorkflowID: workflowID, Input: input, Config: cloneRunConfig(config)}
	handle, err := e.client.StartWorkflow(ctx, request)
	if err != nil {
		return zero, &Error{Operation: "start", WorkflowID: workflowID, Err: err}
	}
	if isNil(handle) {
		return zero, &Error{Operation: "start", WorkflowID: workflowID, Err: fmt.Errorf("%w: workflow handle is nil", ErrInvalidConfiguration)}
	}
	output, err := handle.Get(ctx)
	if err != nil {
		return zero, &Error{Operation: "get", WorkflowID: handle.WorkflowID(), RunID: handle.RunID(), Err: err}
	}
	return output, nil
}

func workflowID(threadID, runID, generated string) string {
	segments := []string{"langgraph"}
	if threadID != "" {
		segments = append(segments, url.PathEscape(threadID))
	}
	if runID != "" {
		segments = append(segments, url.PathEscape(runID))
	} else {
		segments = append(segments, url.PathEscape(generated))
	}
	return strings.Join(segments, "/")
}

// ActivityRuntime is the serializable subset used to reconstruct graph.Runtime.
type ActivityRuntime struct {
	Step                int
	Node                graph.NodeID
	TaskID              string
	Attempt             int
	FirstAttemptTime    time.Time
	ThreadID            string
	CheckpointNamespace string
	CheckpointID        string
	Context             any
}

// ActivityRequest carries isolated state and deterministic task metadata.
type ActivityRequest[S any] struct {
	State   S
	Runtime ActivityRuntime
}

// Activity maps one graph node to a provider SDK activity registration target.
type Activity[S, D any] struct{ node graph.Node[S, D] }

// NewActivity validates and wraps one graph node.
func NewActivity[S, D any](node graph.Node[S, D]) (*Activity[S, D], error) {
	if node == nil {
		return nil, fmt.Errorf("%w: activity node is nil", ErrInvalidConfiguration)
	}
	return &Activity[S, D]{node: node}, nil
}

// Execute reconstructs graph.Runtime and returns the node's serializable command.
func (a *Activity[S, D]) Execute(ctx context.Context, request ActivityRequest[S]) (graph.Command[D], error) {
	var zero graph.Command[D]
	if ctx == nil {
		return zero, &Error{Operation: "activity", Err: fmt.Errorf("%w: context is nil", ErrInvalidConfiguration)}
	}
	if err := ctx.Err(); err != nil {
		return zero, err
	}
	runtime := graph.Runtime{
		Step: request.Runtime.Step, Node: request.Runtime.Node, TaskID: request.Runtime.TaskID,
		Attempt: request.Runtime.Attempt, FirstAttemptTime: request.Runtime.FirstAttemptTime,
		ThreadID: request.Runtime.ThreadID, CheckpointNamespace: request.Runtime.CheckpointNamespace,
		CheckpointID: request.Runtime.CheckpointID, Context: request.Runtime.Context,
	}
	command, err := a.node(ctx, request.State, runtime)
	if err != nil {
		return zero, &Error{Operation: "activity", WorkflowID: request.Runtime.ThreadID, RunID: request.Runtime.TaskID, Err: err}
	}
	return command, nil
}

func cloneRunConfig(config graph.RunConfig) graph.RunConfig {
	if config.Metadata != nil {
		source := config.Metadata
		config.Metadata = make(map[string]any, len(source))
		for key, value := range source {
			config.Metadata[key] = value
		}
	}
	return config
}

func randomID() string {
	value := make([]byte, 16)
	if _, err := rand.Read(value); err != nil {
		panic(fmt.Sprintf("temporal workflow ID generation failed: %v", err))
	}
	return hex.EncodeToString(value)
}

func isNil(value any) bool {
	if value == nil {
		return true
	}
	reflected := reflect.ValueOf(value)
	switch reflected.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return reflected.IsNil()
	default:
		return false
	}
}
