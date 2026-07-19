package graph

import (
	"context"
	"fmt"

	"github.com/ybszm/langgraph-go/checkpoint"
)

// GraphLifecycleStatus identifies the Pregel loop state captured by a graph
// lifecycle callback.
type GraphLifecycleStatus string

const (
	LifecycleInput           GraphLifecycleStatus = "input"
	LifecyclePending         GraphLifecycleStatus = "pending"
	LifecycleDone            GraphLifecycleStatus = "done"
	LifecycleInterruptBefore GraphLifecycleStatus = "interrupt_before"
	LifecycleInterruptAfter  GraphLifecycleStatus = "interrupt_after"
	LifecycleOutOfSteps      GraphLifecycleStatus = "out_of_steps"
)

// GraphInterruptEvent reports one graph scope pausing for static or dynamic
// interrupts. Dynamic interrupts are ordered by stable task order.
type GraphInterruptEvent struct {
	RunID               string
	Status              GraphLifecycleStatus
	CheckpointID        string
	CheckpointNamespace string
	Interrupts          []Interrupt
}

// GraphResumeEvent reports one graph scope resuming from a durable checkpoint.
type GraphResumeEvent struct {
	RunID               string
	Status              GraphLifecycleStatus
	CheckpointID        string
	CheckpointNamespace string
}

// GraphRunStartEvent reports the input boundary of one graph or subgraph run.
type GraphRunStartEvent struct {
	RunID               string
	ParentRunID         string
	Name                string
	Tags                []string
	ThreadID            string
	CheckpointNamespace string
	Input               any
	Metadata            map[string]any
}

// GraphRunEndEvent reports the successful output boundary of one graph or
// subgraph run. A durable interrupt is a successful pause: the graph-specific
// OnInterrupt callback is emitted before this generic run-end boundary.
type GraphRunEndEvent struct {
	RunID               string
	ParentRunID         string
	Name                string
	Tags                []string
	ThreadID            string
	CheckpointNamespace string
	Output              any
	Metadata            map[string]any
}

// GraphRunErrorEvent reports a terminal graph or subgraph failure.
type GraphRunErrorEvent struct {
	RunID               string
	ParentRunID         string
	Name                string
	Tags                []string
	ThreadID            string
	CheckpointNamespace string
	Err                 error
	Metadata            map[string]any
}

// NodeRunStartEvent reports the input boundary of one node attempt.
type NodeRunStartEvent struct {
	RunID               string
	ParentRunID         string
	Name                string
	Tags                []string
	ThreadID            string
	CheckpointID        string
	CheckpointNamespace string
	Step                int
	Node                NodeID
	TaskID              string
	Attempt             int
	Input               any
	Metadata            map[string]any
}

// NodeRunEndEvent reports the output boundary of one successful node attempt.
type NodeRunEndEvent struct {
	RunID               string
	ParentRunID         string
	Name                string
	Tags                []string
	ThreadID            string
	CheckpointID        string
	CheckpointNamespace string
	Step                int
	Node                NodeID
	TaskID              string
	Attempt             int
	Output              any
	Metadata            map[string]any
}

// NodeRunErrorEvent reports one failed node attempt. A later retry produces a
// new start event with an incremented Attempt.
type NodeRunErrorEvent struct {
	RunID               string
	ParentRunID         string
	Name                string
	Tags                []string
	ThreadID            string
	CheckpointID        string
	CheckpointNamespace string
	Step                int
	Node                NodeID
	TaskID              string
	Attempt             int
	Err                 error
	Metadata            map[string]any
}

// GraphCallback observes graph-specific interrupt and resume transitions.
// Implementations shared by concurrent invocations must be concurrency-safe.
type GraphCallback interface {
	OnInterrupt(context.Context, GraphInterruptEvent)
	OnResume(context.Context, GraphResumeEvent)
}

// GraphRunCallback optionally observes generic graph run boundaries. A
// GraphCallback may implement this interface without changing the lifecycle
// interrupt/resume contract.
type GraphRunCallback interface {
	OnGraphStart(context.Context, GraphRunStartEvent)
	OnGraphEnd(context.Context, GraphRunEndEvent)
	OnGraphError(context.Context, GraphRunErrorEvent)
}

// NodeRunCallback optionally observes individual node attempt boundaries.
type NodeRunCallback interface {
	OnNodeStart(context.Context, NodeRunStartEvent)
	OnNodeEnd(context.Context, NodeRunEndEvent)
	OnNodeError(context.Context, NodeRunErrorEvent)
}

// GraphCallbackFuncs adapts optional functions to GraphCallback.
type GraphCallbackFuncs struct {
	Interrupt  func(context.Context, GraphInterruptEvent)
	Resume     func(context.Context, GraphResumeEvent)
	GraphStart func(context.Context, GraphRunStartEvent)
	GraphEnd   func(context.Context, GraphRunEndEvent)
	GraphError func(context.Context, GraphRunErrorEvent)
	NodeStart  func(context.Context, NodeRunStartEvent)
	NodeEnd    func(context.Context, NodeRunEndEvent)
	NodeError  func(context.Context, NodeRunErrorEvent)
}

func (callback GraphCallbackFuncs) OnGraphStart(ctx context.Context, event GraphRunStartEvent) {
	if callback.GraphStart != nil {
		callback.GraphStart(ctx, event)
	}
}

func (callback GraphCallbackFuncs) OnGraphEnd(ctx context.Context, event GraphRunEndEvent) {
	if callback.GraphEnd != nil {
		callback.GraphEnd(ctx, event)
	}
}

func (callback GraphCallbackFuncs) OnGraphError(ctx context.Context, event GraphRunErrorEvent) {
	if callback.GraphError != nil {
		callback.GraphError(ctx, event)
	}
}

func (callback GraphCallbackFuncs) OnNodeStart(ctx context.Context, event NodeRunStartEvent) {
	if callback.NodeStart != nil {
		callback.NodeStart(ctx, event)
	}
}

func (callback GraphCallbackFuncs) OnNodeEnd(ctx context.Context, event NodeRunEndEvent) {
	if callback.NodeEnd != nil {
		callback.NodeEnd(ctx, event)
	}
}

func (callback GraphCallbackFuncs) OnNodeError(ctx context.Context, event NodeRunErrorEvent) {
	if callback.NodeError != nil {
		callback.NodeError(ctx, event)
	}
}

func (callback GraphCallbackFuncs) OnInterrupt(ctx context.Context, event GraphInterruptEvent) {
	if callback.Interrupt != nil {
		callback.Interrupt(ctx, event)
	}
}

func (callback GraphCallbackFuncs) OnResume(ctx context.Context, event GraphResumeEvent) {
	if callback.Resume != nil {
		callback.Resume(ctx, event)
	}
}

func notifyGraphInterrupt(
	ctx context.Context,
	config RunConfig,
	status GraphLifecycleStatus,
	coordinate checkpoint.Config,
	interrupts []Interrupt,
) {
	for _, callback := range config.Callbacks {
		if callback == nil {
			continue
		}
		callback.OnInterrupt(ctx, GraphInterruptEvent{
			RunID: config.RunID, Status: status,
			CheckpointID: coordinate.CheckpointID, CheckpointNamespace: coordinate.Namespace,
			Interrupts: cloneInterrupts(interrupts),
		})
	}
}

func notifyGraphResume(ctx context.Context, config RunConfig, coordinate checkpoint.Config) {
	for _, callback := range config.Callbacks {
		if callback == nil {
			continue
		}
		callback.OnResume(ctx, GraphResumeEvent{
			RunID: config.RunID, Status: LifecyclePending,
			CheckpointID: coordinate.CheckpointID, CheckpointNamespace: coordinate.Namespace,
		})
	}
}

func notifyGraphRunStart(ctx context.Context, config RunConfig, input any) {
	for _, callback := range config.Callbacks {
		observer, ok := callback.(GraphRunCallback)
		if callback == nil || !ok {
			continue
		}
		observer.OnGraphStart(ctx, GraphRunStartEvent{
			RunID: config.RunID, ParentRunID: config.ParentRunID, Name: graphRunName(config), Tags: cloneTags(config.Tags), ThreadID: config.ThreadID,
			CheckpointNamespace: config.CheckpointNamespace, Input: input,
			Metadata: cloneMessageMetadata(config.Metadata),
		})
	}
}

func notifyGraphRunEnd(ctx context.Context, config RunConfig, output any) {
	for _, callback := range config.Callbacks {
		observer, ok := callback.(GraphRunCallback)
		if callback == nil || !ok {
			continue
		}
		observer.OnGraphEnd(ctx, GraphRunEndEvent{
			RunID: config.RunID, ParentRunID: config.ParentRunID, Name: graphRunName(config), Tags: cloneTags(config.Tags), ThreadID: config.ThreadID,
			CheckpointNamespace: config.CheckpointNamespace, Output: output,
			Metadata: cloneMessageMetadata(config.Metadata),
		})
	}
}

func notifyGraphRunError(ctx context.Context, config RunConfig, err error) {
	for _, callback := range config.Callbacks {
		observer, ok := callback.(GraphRunCallback)
		if callback == nil || !ok {
			continue
		}
		observer.OnGraphError(ctx, GraphRunErrorEvent{
			RunID: config.RunID, ParentRunID: config.ParentRunID, Name: graphRunName(config), Tags: cloneTags(config.Tags), ThreadID: config.ThreadID,
			CheckpointNamespace: config.CheckpointNamespace, Err: err,
			Metadata: cloneMessageMetadata(config.Metadata),
		})
	}
}

func nodeRunStartEvent[S any](config RunConfig, coordinate checkpoint.Config, step int, node NodeID, taskID string, attempt int, input S) NodeRunStartEvent {
	return NodeRunStartEvent{
		RunID: nodeCallbackRunID(config.RunID, taskID, attempt), ParentRunID: config.RunID,
		Name: string(node), Tags: cloneTags(config.Tags), ThreadID: config.ThreadID,
		CheckpointID: coordinate.CheckpointID, CheckpointNamespace: coordinate.Namespace,
		Step: step, Node: node, TaskID: taskID, Attempt: attempt, Input: input,
		Metadata: cloneMessageMetadata(config.Metadata),
	}
}

func notifyNodeRunStart(ctx context.Context, config RunConfig, event NodeRunStartEvent) {
	for _, callback := range config.Callbacks {
		if observer, ok := callback.(NodeRunCallback); callback != nil && ok {
			observer.OnNodeStart(ctx, event)
		}
	}
}

func notifyNodeRunEnd(ctx context.Context, config RunConfig, start NodeRunStartEvent, output any) {
	for _, callback := range config.Callbacks {
		if observer, ok := callback.(NodeRunCallback); callback != nil && ok {
			observer.OnNodeEnd(ctx, NodeRunEndEvent{
				RunID: start.RunID, ParentRunID: start.ParentRunID, Name: start.Name, Tags: cloneTags(start.Tags), ThreadID: start.ThreadID,
				CheckpointID: start.CheckpointID, CheckpointNamespace: start.CheckpointNamespace,
				Step: start.Step, Node: start.Node, TaskID: start.TaskID, Attempt: start.Attempt,
				Output: output, Metadata: cloneMessageMetadata(start.Metadata),
			})
		}
	}
}

func notifyNodeRunError(ctx context.Context, config RunConfig, start NodeRunStartEvent, err error) {
	for _, callback := range config.Callbacks {
		if observer, ok := callback.(NodeRunCallback); callback != nil && ok {
			observer.OnNodeError(ctx, NodeRunErrorEvent{
				RunID: start.RunID, ParentRunID: start.ParentRunID, Name: start.Name, Tags: cloneTags(start.Tags), ThreadID: start.ThreadID,
				CheckpointID: start.CheckpointID, CheckpointNamespace: start.CheckpointNamespace,
				Step: start.Step, Node: start.Node, TaskID: start.TaskID, Attempt: start.Attempt,
				Err: err, Metadata: cloneMessageMetadata(start.Metadata),
			})
		}
	}
}

func graphRunName(config RunConfig) string {
	if config.RunName != "" {
		return config.RunName
	}
	return "LangGraph"
}

func nodeCallbackRunID(parent, taskID string, attempt int) string {
	return fmt.Sprintf("%s/node/%s/attempt/%d", parent, taskID, attempt)
}

func cloneTags(tags []string) []string { return append([]string(nil), tags...) }
