package graph

import (
	"errors"
	"fmt"
)

var (
	// ErrInvalidGraph is the common category for graph definition failures.
	ErrInvalidGraph = errors.New("invalid graph")
	// ErrDuplicateNode indicates that a node ID was registered more than once.
	ErrDuplicateNode = errors.New("duplicate node")
	// ErrDuplicateEdge indicates that the same static edge was added twice.
	ErrDuplicateEdge = errors.New("duplicate edge")
	// ErrUnknownNode indicates a reference to a node absent from the graph.
	ErrUnknownNode = errors.New("unknown node")
	// ErrInvalidRunConfig indicates an invalid execution configuration.
	ErrInvalidRunConfig = errors.New("invalid run config")
	// ErrUnsupportedDurability is retained for optional backends that cannot
	// implement a recognized durability mode.
	ErrUnsupportedDurability = errors.New("unsupported durability mode")
	// ErrRecursionLimit indicates that execution exceeded its super-step limit.
	ErrRecursionLimit = errors.New("graph recursion limit reached")
	// ErrCheckpointerRequired indicates that an API needs persistence but the
	// graph was compiled without a checkpoint saver.
	ErrCheckpointerRequired = errors.New("graph checkpointer required")
	// ErrInvalidStateUpdate indicates an invalid manual state update.
	ErrInvalidStateUpdate = errors.New("invalid state update")
	// ErrGraphInterrupt indicates that execution paused for external input.
	ErrGraphInterrupt = errors.New("graph interrupted")
	// ErrInvalidResume indicates that resume values do not match pending
	// interrupts.
	ErrInvalidResume = errors.New("invalid graph resume")
	// ErrRuntimeContextType indicates a typed RuntimeContext mismatch.
	ErrRuntimeContextType = errors.New("runtime context type mismatch")
	// ErrManagedValue indicates that node-visible managed state could not be
	// derived from canonical graph state.
	ErrManagedValue = errors.New("managed value projection failed")
	// ErrSchemaAdapter indicates a failure mapping a public input or internal
	// state across a typed schema boundary.
	ErrSchemaAdapter = errors.New("graph schema adapter failed")
	// ErrCheckpointChannel indicates an invalid projected checkpoint channel.
	ErrCheckpointChannel = errors.New("invalid projected checkpoint channel")
	// ErrSubgraphValidation indicates an invalid recursive subgraph compile
	// contract, such as missing inherited persistence or codecs.
	ErrSubgraphValidation = errors.New("invalid subgraph configuration")
	// ErrNodeInputSchema indicates that a node-specific input projection failed.
	ErrNodeInputSchema = errors.New("node input schema projection failed")
	// ErrNodeOutputSchema indicates that a node-specific output projection failed.
	ErrNodeOutputSchema = errors.New("node output schema projection failed")
	// ErrInvalidContentBlockEvent classifies malformed v2 stream lifecycle events.
	ErrInvalidContentBlockEvent = errors.New("invalid content-block stream event")
	// ErrUnsupportedStreamEventsVersion classifies unsupported run-event protocols.
	ErrUnsupportedStreamEventsVersion = errors.New("unsupported stream-events version")
	// ErrParentCommand indicates that a command targeted the closest parent but
	// execution was not embedded through a compatible SubgraphAdapter.
	ErrParentCommand = errors.New("graph parent command")
)

// SubgraphValidationError identifies the parent node whose embedded graph
// contract failed compile-time validation.
type SubgraphValidationError struct {
	Node NodeID
	Err  error
}

func (e *SubgraphValidationError) Error() string {
	return fmt.Sprintf("subgraph %q: %v", e.Node, e.Err)
}

func (e *SubgraphValidationError) Unwrap() []error {
	return []error{ErrSubgraphValidation, e.Err}
}

// ParentCommandError bubbles a parent-targeted command through the current
// graph without treating it as a retryable node failure.
type ParentCommandError struct{ Command any }

func (e *ParentCommandError) Error() string { return ErrParentCommand.Error() }
func (e *ParentCommandError) Unwrap() error { return ErrParentCommand }

// AsParentCommand extracts a typed parent command from an error chain.
func AsParentCommand[D any](err error) (Command[D], bool) {
	var parent *ParentCommandError
	if !errors.As(err, &parent) {
		return Command[D]{}, false
	}
	command, ok := parent.Command.(Command[D])
	return command, ok
}

// GraphInterruptError reports every interrupt reached in task order during a
// super-step. It is a control-flow result, not a node failure.
type GraphInterruptError struct {
	Interrupts []Interrupt
}

func (e *GraphInterruptError) Error() string {
	return fmt.Sprintf("%v: %d pending interrupt(s)", ErrGraphInterrupt, len(e.Interrupts))
}

// Unwrap supports errors.Is(err, ErrGraphInterrupt).
func (e *GraphInterruptError) Unwrap() error {
	return ErrGraphInterrupt
}

// RecursionError reports the configured limit reached by a looping graph.
type RecursionError struct {
	Limit int
	Step  int
}

func (e *RecursionError) Error() string {
	return fmt.Sprintf(
		"Recursion limit of %d reached without hitting a stop condition. You can increase the limit by setting the `recursion_limit` config key.\nFor troubleshooting, visit: https://docs.langchain.com/oss/python/langgraph/errors/GRAPH_RECURSION_LIMIT",
		e.Limit,
	)
}

// Code returns the stable upstream troubleshooting code.
func (e *RecursionError) Code() string { return "GRAPH_RECURSION_LIMIT" }

// Unwrap supports errors.Is(err, ErrRecursionLimit).
func (e *RecursionError) Unwrap() error {
	return ErrRecursionLimit
}

// NodeExecutionError adds graph task context to a node or state-cloning error.
type NodeExecutionError struct {
	Step   int
	Node   NodeID
	TaskID string
	Err    error
}

func (e *NodeExecutionError) Error() string {
	return fmt.Sprintf(
		"node %q failed at step %d (task %q): %v",
		e.Node,
		e.Step,
		e.TaskID,
		e.Err,
	)
}

// Unwrap exposes the underlying node error.
func (e *NodeExecutionError) Unwrap() error {
	return e.Err
}

// NodePanicError is returned when a node panics. Stack contains the recovered
// goroutine stack for diagnostics.
type NodePanicError struct {
	Value any
	Stack []byte
}

// BatchPanicError reports a panic escaping one batch invocation boundary,
// including callback panics that occur outside node panic recovery.
type BatchPanicError struct {
	Index int
	Value any
	Stack []byte
}

func (e *BatchPanicError) Error() string {
	return fmt.Sprintf("batch item %d panic: %v", e.Index, e.Value)
}

func (e *NodePanicError) Error() string {
	return fmt.Sprintf("node panic: %v", e.Value)
}

// RouterError adds source-node context to a conditional routing failure.
type RouterError struct {
	Step   int
	Source NodeID
	Branch string
	Err    error
}

func (e *RouterError) Error() string {
	if e.Branch != "" {
		return fmt.Sprintf("router branch %q for node %q failed after step %d: %v", e.Branch, e.Source, e.Step, e.Err)
	}
	return fmt.Sprintf("router for node %q failed after step %d: %v", e.Source, e.Step, e.Err)
}

// Unwrap exposes the underlying router error.
func (e *RouterError) Unwrap() error {
	return e.Err
}

// PersistenceError adds checkpoint operation and thread context.
type PersistenceError struct {
	Operation    string
	ThreadID     string
	Namespace    string
	CheckpointID string
	Err          error
}

// StreamModeError reports a requested mode not implemented by this runtime.
type StreamModeError struct{ Mode StreamMode }

func (e *StreamModeError) Error() string {
	return fmt.Sprintf("unsupported stream mode %q", e.Mode)
}

func (e *PersistenceError) Error() string {
	return fmt.Sprintf(
		"checkpoint %s failed for thread %q namespace %q checkpoint %q: %v",
		e.Operation,
		e.ThreadID,
		e.Namespace,
		e.CheckpointID,
		e.Err,
	)
}

// Unwrap exposes the saver or codec error.
func (e *PersistenceError) Unwrap() error {
	return e.Err
}
