// Package graph provides the typed graph definition and execution primitives.
package graph

import (
	"context"
	"fmt"
	"time"

	"github.com/ybszm/langgraph-go/checkpoint"
	lgstore "github.com/ybszm/langgraph-go/store"
)

// NodeID uniquely identifies a node in a graph.
type NodeID string

const (
	// START is the virtual node from which graph execution begins.
	START NodeID = "__start__"
	// END is the virtual node that terminates one execution branch.
	END NodeID = "__end__"
)

// Reducer applies the ordered deltas produced by one super-step to the current
// state. The runtime guarantees that updates are passed in deterministic task
// order rather than goroutine completion order.
type Reducer[S, D any] func(ctx context.Context, current S, updates []D) (S, error)

// StateCloner creates an isolated state value for one node invocation. A
// cloner is recommended when S contains maps, slices, pointers, or other
// reference-like values that a node could accidentally mutate.
type StateCloner[S any] func(state S) (S, error)

// InputMerger combines a completed thread's durable state with the input of
// an explicitly requested new invocation.
type InputMerger[S any] func(ctx context.Context, previous, input S) (S, error)

// DeltaNormalizer canonicalizes one node update before it is streamed,
// cached, persisted as a pending write, or passed to the Reducer.
type DeltaNormalizer[D any] func(ctx context.Context, delta D) (D, error)

// MessageEmission is one message plus integration-specific stream metadata.
// Message is intentionally type-erased so OpenAI, Anthropic, LangChain-Go,
// and application-native message types can share the runtime contract.
type MessageEmission struct {
	Message  any
	Metadata map[string]any
}

// MessageExtractor discovers messages carried by a node delta. Integrations
// that stream tokens directly can instead call Runtime.WriteMessage.
type MessageExtractor[D any] func(context.Context, D) ([]MessageEmission, error)

// InputMessageExtractor discovers messages already present in one node input.
// Their non-empty IDs seed messages-stream deduplication but are not emitted.
type InputMessageExtractor[S any] func(context.Context, S) ([]any, error)

// CheckpointChannelProjector derives additional versioned checkpoint channels
// from canonical state. Projected channels are observational persistence
// fields; canonical state restoration continues to use checkpoint.StateChannel.
type CheckpointChannelProjector[S any] func(context.Context, S) (map[string]checkpoint.EncodedValue, error)

// Node is a unit of graph computation. Nodes should treat state as immutable
// and return serializable changes in Command.Update.
type Node[S, D any] func(
	ctx context.Context,
	state S,
	runtime Runtime,
) (Command[D], error)

// Router chooses one or more destinations after a node's updates have been
// reduced into the graph state.
type Router[S any] func(ctx context.Context, state S) ([]NodeID, error)

// Send schedules one node with task-local input. Multiple Sends to the same
// node remain distinct tasks and are reduced in router order.
type Send[S any] struct {
	Node  NodeID
	State S
}

// TaskSend is the type-erased envelope used inside Command. Construct it with
// SendTo so the concrete state remains available for runtime type checking.
type TaskSend struct {
	Node  NodeID
	State any
}

// CommandTarget selects the graph that consumes a Command.
type CommandTarget uint8

const (
	// CommandCurrent targets the graph currently executing the node.
	CommandCurrent CommandTarget = iota
	// CommandParent targets the closest parent graph. It is valid only while
	// executing as a registered subgraph (or is surfaced as ParentCommandError).
	CommandParent
)

func SendTo[S any](node NodeID, state S) TaskSend {
	return TaskSend{Node: node, State: state}
}

// SendRouter creates dynamic fan-out tasks after a source super-step.
type SendRouter[S any] func(ctx context.Context, state S) ([]Send[S], error)

// Runtime describes the current node task.
type Runtime struct {
	Step                int
	Node                NodeID
	TaskID              string
	Attempt             int
	FirstAttemptTime    time.Time
	ThreadID            string
	CheckpointNamespace string
	CheckpointID        string
	// Store is shared across graph threads and inherited by embedded subgraphs.
	// It is nil when no Store was supplied at compile time.
	Store lgstore.Store
	// Checkpointer is the saver owning the current checkpoint coordinate. It is
	// exposed for nested durable runtimes such as Functional tasks; callers
	// must write only task-scoped pending values to the current coordinate.
	// It is nil for non-persistent graphs.
	Checkpointer checkpoint.Saver
	// Context is immutable run-scoped dependency data. It is inherited by
	// subgraphs and is never persisted as graph state.
	Context              any
	interrupt            interruptHandler
	writeCustom          func(any) error
	writeMessage         func(any, map[string]any) error
	writeContentBlock    func(ContentBlockStreamEvent, map[string]any) error
	heartbeat            func()
	replayUpperBound     string
	subgraphNamespace    string
	subgraphCheckpointID string
}

// Heartbeat reports node progress and refreshes an idle timeout. It is a
// no-op when this node attempt has no idle timeout.
func (runtime Runtime) Heartbeat() {
	if runtime.heartbeat != nil {
		runtime.heartbeat()
	}
}

// RuntimeContext returns the run-scoped context with a checked Go type.
func RuntimeContext[T any](runtime Runtime) (T, error) {
	value, ok := runtime.Context.(T)
	if !ok {
		var zero T
		return zero, fmt.Errorf("%w: expected %T, got %T", ErrRuntimeContextType, zero, runtime.Context)
	}
	return value, nil
}

// WriteCustom emits one payload in custom stream mode. It is a no-op during
// Invoke or when no stream consumer is attached.
func (runtime Runtime) WriteCustom(value any) error {
	runtime.Heartbeat()
	if runtime.writeCustom == nil {
		return nil
	}
	return runtime.writeCustom(value)
}

// WriteMessage emits one message chunk in messages stream mode. Optional
// metadata is merged with scheduler metadata; framework keys take precedence.
// It is a no-op during Invoke or when no stream consumer is attached.
func (runtime Runtime) WriteMessage(message any, metadata ...map[string]any) error {
	if len(metadata) > 1 {
		return fmt.Errorf("message stream accepts at most one metadata map")
	}
	runtime.Heartbeat()
	if runtime.writeMessage == nil {
		return nil
	}
	var supplied map[string]any
	if len(metadata) == 1 {
		supplied = metadata[0]
	}
	return runtime.writeMessage(message, supplied)
}

// WriteContentBlock emits one provider-neutral v2 message lifecycle event.
func (runtime Runtime) WriteContentBlock(event ContentBlockStreamEvent, metadata ...map[string]any) error {
	if len(metadata) > 1 {
		return fmt.Errorf("content-block stream accepts at most one metadata map")
	}
	if err := validateContentBlockStreamEvent(event); err != nil {
		return err
	}
	runtime.Heartbeat()
	if runtime.writeContentBlock == nil {
		return nil
	}
	var supplied map[string]any
	if len(metadata) == 1 {
		supplied = metadata[0]
	}
	return runtime.writeContentBlock(event, supplied)
}

// Command combines an optional state update with optional explicit routing.
// A nil Goto uses the graph's static and conditional edges. A non-nil empty
// Goto terminates this branch.
type Command[D any] struct {
	Update    D
	HasUpdate bool
	Goto      []NodeID
	Sends     []TaskSend
	Target    CommandTarget
	// Resume is an invocation-only durable resume payload. Construct commands
	// carrying it with ResumeAsCommand and pass them to InvokeCommand.
	Resume *ResumeCommand
}

// ToParent retargets a command to the closest parent graph. A heterogeneous
// subgraph can map its child delta/state through SubgraphAdapter.Parent.
func ToParent[D any](command Command[D]) Command[D] {
	command.Target = CommandParent
	return command
}

// ParentGoto routes in the closest parent graph without an update.
func ParentGoto[D any](destinations ...NodeID) Command[D] {
	return ToParent(Goto[D](destinations...))
}

// ParentUpdate applies a delta to the closest parent graph.
func ParentUpdate[D any](delta D) Command[D] {
	return ToParent(Update(delta))
}

// ParentUpdateAndGoto applies a delta and routes in the closest parent graph.
func ParentUpdateAndGoto[D any](delta D, destinations ...NodeID) Command[D] {
	return ToParent(UpdateAndGoto(delta, destinations...))
}

// ParentDispatch sends task-local inputs to nodes in the closest parent graph.
func ParentDispatch[D any](sends ...TaskSend) Command[D] {
	return ToParent(Dispatch[D](sends...))
}

// Dispatch returns a command containing task-local Send operations.
func Dispatch[D any](sends ...TaskSend) Command[D] {
	return Command[D]{Goto: []NodeID{}, Sends: cloneTaskSends(sends)}
}

// UpdateAndDispatch combines one state update with task-local Sends.
func UpdateAndDispatch[D any](delta D, sends ...TaskSend) Command[D] {
	return Command[D]{Update: delta, HasUpdate: true, Goto: []NodeID{}, Sends: cloneTaskSends(sends)}
}

// NoCommand returns a command without an update or explicit routing.
func NoCommand[D any]() Command[D] {
	return Command[D]{}
}

// Update returns a command containing a state delta.
func Update[D any](delta D) Command[D] {
	return Command[D]{Update: delta, HasUpdate: true}
}

// Goto returns a command that overrides the node's declared outgoing edges.
func Goto[D any](destinations ...NodeID) Command[D] {
	return Command[D]{Goto: cloneNodeIDs(destinations)}
}

// UpdateAndGoto returns a command with both a delta and explicit routing.
func UpdateAndGoto[D any](delta D, destinations ...NodeID) Command[D] {
	return Command[D]{
		Update:    delta,
		HasUpdate: true,
		Goto:      cloneNodeIDs(destinations),
	}
}

// RunConfig controls a single graph execution.
type RunConfig struct {
	// Context contains immutable dependencies scoped to this invocation. It is
	// available through Runtime.Context and is not checkpointed.
	Context any
	// NewRun starts a new invocation when the selected checkpoint is already
	// complete. Input replaces prior state unless an InputMerger is configured.
	NewRun                     bool
	invocationParentCheckpoint string
	replayUpperBound           string
	resuming                   bool
	forceResumeFork            bool
	subgraphNamespace          string
	subgraphCheckpointID       string
	// RecursionLimit is the maximum number of super-steps. Zero uses the
	// default. A negative value is invalid.
	RecursionLimit int
	// MaxConcurrency limits simultaneously executing nodes. Zero means the
	// number of tasks in the current super-step. A negative value is invalid.
	MaxConcurrency int
	// ThreadID selects the durable execution thread when the graph was
	// compiled with a checkpointer.
	ThreadID string
	// CheckpointNamespace isolates parent and subgraph checkpoint histories.
	CheckpointNamespace string
	// CheckpointID selects an exact historical checkpoint. An empty value
	// loads the latest checkpoint in the thread and namespace.
	CheckpointID string
	// RunID is recorded in checkpoint metadata when non-empty.
	RunID string
	// ParentRunID links this graph invocation into an external callback tree.
	ParentRunID string
	// RunName is the callback/display name for this graph scope.
	RunName string
	// Tags are detached observability labels inherited by nodes and subgraphs.
	Tags []string
	// Metadata is run-scoped observability data merged into messages events. It
	// is inherited by subgraphs and is not checkpointed as graph state.
	Metadata map[string]any
	// Callbacks synchronously observe graph interrupt/resume lifecycle events.
	// Handlers may also implement GraphRunCallback and NodeRunCallback. The
	// slice is copied at invocation start and inherited by subgraphs; handlers
	// shared by concurrent runs or nodes must be concurrency-safe.
	Callbacks   []GraphCallback
	streamModes map[StreamMode]struct{}
	// StreamSubgraphs forwards child graph events with their checkpoint
	// namespace in StreamEvent.Namespace and an untyped child payload.
	StreamSubgraphs bool
}

// DefaultRecursionLimit matches LangGraph's conventional default.
const DefaultRecursionLimit = 25

func resolvedRecursionLimit(config RunConfig) int {
	if config.RecursionLimit == 0 {
		return DefaultRecursionLimit
	}
	return config.RecursionLimit
}

// NodeUpdate identifies one node delta in an updates stream event.
type NodeUpdate[D any] struct {
	Node   NodeID
	TaskID string
	Delta  D
	Cached bool
}

// StreamMode identifies the payload carried by a StreamEvent.
type StreamMode string

const (
	// StreamValues carries a complete state snapshot.
	StreamValues StreamMode = "values"
	// StreamUpdates carries node deltas from one super-step.
	StreamUpdates StreamMode = "updates"
	// StreamDone marks successful graph completion.
	StreamDone StreamMode = "done"
	// StreamError reports terminal execution failure.
	StreamError StreamMode = "error"
	// StreamInterrupt reports durable human-in-the-loop requests.
	StreamInterrupt StreamMode = "interrupt"
	// StreamCustom carries a payload emitted directly by a node Runtime.
	StreamCustom StreamMode = "custom"
	// StreamDebug carries task lifecycle and checkpoint observability events.
	StreamDebug StreamMode = "debug"
	// StreamMessages carries message chunks plus scheduler/integration metadata.
	StreamMessages StreamMode = "messages"
)

// MessageStreamEvent is the payload for StreamMessages.
type MessageStreamEvent struct {
	Message      any
	ContentBlock *ContentBlockStreamEvent
	Metadata     map[string]any
	dedupe       bool
	remember     bool
}

// ContentBlockEventKind is the provider-neutral v2 message lifecycle protocol.
type ContentBlockEventKind string

const (
	ContentMessageStart  ContentBlockEventKind = "message-start"
	ContentBlockStart    ContentBlockEventKind = "content-block-start"
	ContentBlockDelta    ContentBlockEventKind = "content-block-delta"
	ContentBlockFinish   ContentBlockEventKind = "content-block-finish"
	ContentMessageFinish ContentBlockEventKind = "message-finish"
)

// ContentBlockStreamEvent carries one v2 message/content-block lifecycle event.
// ContentBlock remains an open map so provider extension fields survive.
type ContentBlockStreamEvent struct {
	Event        ContentBlockEventKind
	Role         string
	MessageID    string
	Index        int
	ContentBlock map[string]any
	Reason       string
}

func validateContentBlockStreamEvent(event ContentBlockStreamEvent) error {
	switch event.Event {
	case ContentMessageStart:
		if event.Role == "" || event.MessageID == "" {
			return fmt.Errorf("%w: message-start requires role and message ID", ErrInvalidContentBlockEvent)
		}
	case ContentBlockStart, ContentBlockDelta, ContentBlockFinish:
		if event.Index < 0 || event.ContentBlock == nil {
			return fmt.Errorf("%w: %s requires non-negative index and content block", ErrInvalidContentBlockEvent, event.Event)
		}
		blockType, ok := event.ContentBlock["type"].(string)
		if !ok || blockType == "" {
			return fmt.Errorf("%w: %s content block requires type", ErrInvalidContentBlockEvent, event.Event)
		}
	case ContentMessageFinish:
	default:
		return fmt.Errorf("%w: unknown event %q", ErrInvalidContentBlockEvent, event.Event)
	}
	return nil
}

// ValidateContentBlockStreamEvent validates one provider-neutral v2 message
// lifecycle event without emitting it. Adapter packages can use this at their
// boundary before forwarding provider chunks into a graph or Functional run.
func ValidateContentBlockStreamEvent(event ContentBlockStreamEvent) error {
	return validateContentBlockStreamEvent(event)
}

// MessageIdentifier is the minimal provider-neutral identity used to suppress
// a finalized node-output message already observed through streaming chunks.
type MessageIdentifier interface {
	MessageID() string
}

// Message is a copyable provider-neutral message whose identity can be
// assigned before it is exposed through the messages stream. Provider SDK
// adapters may implement this interface for their own concrete message types.
type Message interface {
	MessageIdentifier
	WithMessageID(string) Message
	CloneMessage() Message
}

// MessageIDGenerator creates one non-empty identity for a streamed Message.
type MessageIDGenerator func() (string, error)

// StreamEvent is the typed Go streaming envelope. Mode selects the populated
// payload; nested graphs use Namespace plus Subgraph for heterogeneous types.
type StreamEvent[S, D any] struct {
	Mode       StreamMode
	Step       int
	State      S
	Updates    []NodeUpdate[D]
	Interrupts []Interrupt
	Err        error
	// Namespace is empty for root events. Child events contain one component
	// per nested graph level.
	Namespace []string
	// Subgraph carries a type-erased child event when Namespace is non-empty.
	Subgraph *SubgraphStreamEvent
	Custom   any
	Debug    *DebugEvent
	Message  *MessageStreamEvent
}

type DebugKind string

const (
	DebugTask       DebugKind = "task"
	DebugTaskResult DebugKind = "task_result"
	DebugCheckpoint DebugKind = "checkpoint"
)

// DebugEvent describes one scheduler or durability lifecycle boundary.
type DebugEvent struct {
	Kind     DebugKind
	Step     int
	Node     NodeID
	TaskID   string
	Triggers []NodeID
	// Input and Metadata are populated for DebugTask events.
	Input    any
	Metadata map[string]any
	// Result is populated for successful DebugTaskResult events whose Command
	// contains an update.
	Result           any
	Cached           bool
	Recovered        bool
	Err              error
	Interrupts       []Interrupt
	Checkpoint       checkpoint.Config
	ParentCheckpoint checkpoint.Config
	Values           any
	Next             []NodeID
	Tasks            []DebugTaskSnapshot
}

// DebugTaskSnapshot identifies one task pending at a checkpoint boundary.
// Result, Err, and Interrupts are reserved for pending-write projections.
type DebugTaskSnapshot struct {
	TaskID     string
	Node       NodeID
	Result     any
	Err        error
	Interrupts []Interrupt
}

// SubgraphStreamEvent preserves child state/update payloads even when its
// state and delta types differ from the parent graph's generic parameters.
type SubgraphStreamEvent struct {
	Mode       StreamMode
	Step       int
	State      any
	Updates    any
	Interrupts []Interrupt
	Err        error
	Custom     any
	Debug      *DebugEvent
	Message    *MessageStreamEvent
}

// StreamOptions selects event modes and nested graph visibility. Empty Modes
// preserves Stream's legacy behavior of emitting every implemented mode.
type StreamOptions struct {
	Modes     []StreamMode
	Subgraphs bool
	// Buffer controls the event channel capacity. Zero uses one; negative is
	// invalid and produces a terminal StreamError event.
	Buffer int
	// MessageIDGenerator assigns IDs to Message values that have no ID. Nil
	// uses a cryptographically random UUID-compatible generator.
	MessageIDGenerator MessageIDGenerator
}

// Edge describes one compiled static edge.
type Edge struct {
	From NodeID
	To   NodeID
}

func cloneNodeIDs(ids []NodeID) []NodeID {
	if ids == nil {
		return nil
	}
	return append([]NodeID(nil), ids...)
}

func cloneTaskSends(sends []TaskSend) []TaskSend {
	if sends == nil {
		return nil
	}
	return append([]TaskSend(nil), sends...)
}
