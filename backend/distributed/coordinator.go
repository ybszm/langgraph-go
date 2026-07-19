package distributed

import (
	"context"
	"fmt"
	"time"

	"github.com/ybszm/langgraph-go/checkpoint"
	"github.com/ybszm/langgraph-go/graph"
)

// StepInput is the complete ordered barrier payload supplied to route resolution.
type StepInput[S, D any] struct {
	State    S
	Commands []graph.Command[D]
	TaskIDs  []string
	Nodes    []graph.NodeID
	Waiting  map[string][]string
	Step     int
}

// StepPlan is the public route-resolution output persisted in the next checkpoint.
type StepPlan struct {
	Next     []checkpoint.Task
	Waiting  map[string][]string
	Metadata checkpoint.Metadata
}

// RouteResolver maps reduced state and ordered commands to the next durable tasks.
type RouteResolver[S, D any] func(context.Context, StepInput[S, D]) (StepPlan, error)

// StepCoordinatorOptions supplies deterministic checkpoint time and identity.
type StepCoordinatorOptions struct {
	Clock        func() time.Time
	CheckpointID func(checkpoint.Config, int) string
}

// StepCoordinator persists a ready barrier after explicit route resolution.
type StepCoordinator[S, D any] struct {
	saver    checkpoint.Saver
	codec    checkpoint.Codec[S]
	resolver RouteResolver[S, D]
	options  StepCoordinatorOptions
}

// NewStepCoordinator validates and constructs a barrier commit coordinator.
func NewStepCoordinator[S, D any](
	saver checkpoint.Saver,
	codec checkpoint.Codec[S],
	resolver RouteResolver[S, D],
	options StepCoordinatorOptions,
) (*StepCoordinator[S, D], error) {
	if nilLike(saver) || nilLike(codec) || resolver == nil || options.Clock == nil || options.CheckpointID == nil {
		return nil, fmt.Errorf("%w: coordinator saver, codec, resolver, clock, and checkpoint ID are required", ErrInvalidRequest)
	}
	return &StepCoordinator[S, D]{saver: saver, codec: codec, resolver: resolver, options: options}, nil
}

// Commit resolves routing and writes one immutable child checkpoint. Calling it
// again with the same parent/step is safe when CheckpointID is deterministic.
func (c *StepCoordinator[S, D]) Commit(
	ctx context.Context,
	parent checkpoint.Config,
	barrier BarrierResult[S, D],
) (checkpoint.Config, error) {
	if err := validContext(ctx); err != nil {
		return checkpoint.Config{}, err
	}
	if !barrier.Ready {
		return checkpoint.Config{}, &SchedulerError{Operation: "commit-barrier", Config: parent, Err: fmt.Errorf("%w: barrier is not ready", ErrInvalidRequest)}
	}
	if parent.CheckpointID == "" || barrier.Step < 0 {
		return checkpoint.Config{}, &SchedulerError{Operation: "commit-validate", Config: parent, Err: ErrInvalidRequest}
	}
	parentTuple, found, err := c.saver.GetTuple(ctx, parent)
	if err != nil {
		return checkpoint.Config{}, &SchedulerError{Operation: "commit-get-parent", Config: parent, Err: err}
	}
	if !found || parentTuple.Config.CheckpointID != parent.CheckpointID {
		return checkpoint.Config{}, &SchedulerError{Operation: "commit-get-parent", Config: parent, Err: checkpoint.ErrNotFound}
	}
	plan, err := c.resolver(ctx, StepInput[S, D]{
		State: barrier.State, Commands: append([]graph.Command[D](nil), barrier.Commands...),
		TaskIDs: append([]string(nil), barrier.TaskIDs...), Nodes: append([]graph.NodeID(nil), barrier.Nodes...),
		Waiting: cloneWaiting(barrier.Waiting), Step: barrier.Step,
	})
	if err != nil {
		return checkpoint.Config{}, &SchedulerError{Operation: "commit-route", Config: parent, Err: err}
	}
	if err := validateStepPlan(plan); err != nil {
		return checkpoint.Config{}, &SchedulerError{Operation: "commit-route", Config: parent, Err: err}
	}
	encodedState, err := c.codec.Encode(barrier.State)
	if err != nil {
		return checkpoint.Config{}, &SchedulerError{Operation: "commit-encode-state", Config: parent, Err: err}
	}
	if _, err := c.codec.Decode(encodedState); err != nil {
		return checkpoint.Config{}, &SchedulerError{Operation: "commit-decode-state", Config: parent, Err: err}
	}
	id := c.options.CheckpointID(parent, barrier.Step)
	if !validID(id) || id == parent.CheckpointID {
		return checkpoint.Config{}, &SchedulerError{Operation: "commit-id", Config: parent, Err: fmt.Errorf("%w: new checkpoint ID is invalid", ErrInvalidRequest)}
	}
	timestamp := c.options.Clock()
	if timestamp.IsZero() {
		return checkpoint.Config{}, &SchedulerError{Operation: "commit-clock", Config: parent, Err: fmt.Errorf("%w: checkpoint timestamp is zero", ErrInvalidRequest)}
	}
	value := checkpoint.CloneCheckpoint(parentTuple.Checkpoint)
	value.Version = checkpoint.CurrentVersion
	value.ID = id
	value.Timestamp = timestamp
	value.Step = barrier.Step
	value.Values = map[string]checkpoint.EncodedValue{checkpoint.StateChannel: checkpoint.CloneEncodedValue(encodedState)}
	value.Next = cloneTasks(plan.Next)
	value.Waiting = cloneWaiting(plan.Waiting)
	value.UpdatedChannels = []string{checkpoint.StateChannel}
	if value.ChannelVersions == nil {
		value.ChannelVersions = make(map[string]string)
	}
	stateVersion := id + ":" + checkpoint.StateChannel
	value.ChannelVersions[checkpoint.StateChannel] = stateVersion
	metadata := checkpoint.Metadata{"source": string(checkpoint.SourceLoop), "step": barrier.Step}
	for key, item := range plan.Metadata {
		metadata[key] = item
	}
	config, err := c.saver.Put(ctx, parent, value, metadata, map[string]string{checkpoint.StateChannel: stateVersion})
	if err != nil {
		return checkpoint.Config{}, &SchedulerError{Operation: "commit-put", Config: parent, Err: err}
	}
	if config.CheckpointID != id {
		return checkpoint.Config{}, &SchedulerError{Operation: "commit-verify", Config: parent, Err: fmt.Errorf("saver returned checkpoint %q, want %q", config.CheckpointID, id)}
	}
	return config, nil
}

func validateStepPlan(plan StepPlan) error {
	seen := make(map[string]struct{}, len(plan.Next))
	for _, task := range plan.Next {
		if !validID(task.ID) || !validID(task.Name) {
			return fmt.Errorf("%w: next task has invalid ID or name", ErrInvalidRequest)
		}
		if _, duplicate := seen[task.ID]; duplicate {
			return fmt.Errorf("%w: duplicate next task ID %q", ErrInvalidRequest, task.ID)
		}
		seen[task.ID] = struct{}{}
	}
	return nil
}

func cloneTasks(source []checkpoint.Task) []checkpoint.Task {
	result := make([]checkpoint.Task, len(source))
	for index, task := range source {
		result[index] = task
		if task.Input != nil {
			copy := checkpoint.CloneEncodedValue(*task.Input)
			result[index].Input = &copy
		}
	}
	return result
}

func cloneWaiting(source map[string][]string) map[string][]string {
	if source == nil {
		return nil
	}
	result := make(map[string][]string, len(source))
	for key, values := range source {
		result[key] = append([]string(nil), values...)
	}
	return result
}
