package distributed

import (
	"bytes"
	"context"
	"fmt"

	"github.com/wahanbo/langgraph-go/checkpoint"
	"github.com/wahanbo/langgraph-go/graph"
)

// BarrierResult contains one complete, deterministically reduced super-step.
type BarrierResult[S, D any] struct {
	Ready          bool
	State          S
	Commands       []graph.Command[D]
	TaskIDs        []string
	Nodes          []graph.NodeID
	Waiting        map[string][]string
	MissingTaskIDs []string
	Step           int
}

// Barrier collects distributed pending writes for one checkpoint super-step.
type Barrier[S, D any] struct {
	saver        checkpoint.Saver
	stateCodec   checkpoint.Codec[S]
	commandCodec checkpoint.Codec[graph.Command[D]]
	reducer      graph.Reducer[S, D]
}

// NewBarrier validates and constructs a deterministic barrier collector.
func NewBarrier[S, D any](
	saver checkpoint.Saver,
	stateCodec checkpoint.Codec[S],
	commandCodec checkpoint.Codec[graph.Command[D]],
	reducer graph.Reducer[S, D],
) (*Barrier[S, D], error) {
	if nilLike(saver) || nilLike(stateCodec) || nilLike(commandCodec) || reducer == nil {
		return nil, fmt.Errorf("%w: barrier saver, codecs, and reducer are required", ErrInvalidRequest)
	}
	return &Barrier[S, D]{saver: saver, stateCodec: stateCodec, commandCodec: commandCodec, reducer: reducer}, nil
}

// Collect returns Ready=false without decoding or reducing state until every
// checkpoint Next task has a task-result write at index zero.
func (b *Barrier[S, D]) Collect(ctx context.Context, config checkpoint.Config) (BarrierResult[S, D], error) {
	var zero BarrierResult[S, D]
	if err := validContext(ctx); err != nil {
		return zero, err
	}
	if config.CheckpointID == "" {
		return zero, &SchedulerError{Operation: "barrier-validate", Config: config, Err: fmt.Errorf("%w: exact checkpoint is required", ErrInvalidRequest)}
	}
	tuple, found, err := b.saver.GetTuple(ctx, config)
	if err != nil {
		return zero, &SchedulerError{Operation: "barrier-get", Config: config, Err: err}
	}
	if !found || tuple.Config.CheckpointID != config.CheckpointID {
		return zero, &SchedulerError{Operation: "barrier-get", Config: config, Err: checkpoint.ErrNotFound}
	}
	if err := tuple.Checkpoint.Validate(); err != nil {
		return zero, &SchedulerError{Operation: "barrier-validate", Config: config, Err: err}
	}
	writes := make(map[string]checkpoint.PendingWrite)
	for _, write := range tuple.PendingWrites {
		if write.Channel != checkpoint.TaskResultChannel || write.Index != 0 {
			continue
		}
		if existing, duplicate := writes[write.TaskID]; duplicate {
			if existing.Value.Type != write.Value.Type || existing.Value.Version != write.Value.Version || !bytes.Equal(existing.Value.Data, write.Value.Data) {
				return zero, &SchedulerError{Operation: "barrier-duplicate", Config: config, TaskID: write.TaskID, Err: ErrCompletionConflict}
			}
			continue
		}
		writes[write.TaskID] = write
	}
	missing := make([]string, 0)
	for _, task := range tuple.Checkpoint.Next {
		if _, completed := writes[task.ID]; !completed {
			missing = append(missing, task.ID)
		}
	}
	if len(missing) > 0 {
		return BarrierResult[S, D]{Ready: false, MissingTaskIDs: missing, Step: tuple.Checkpoint.Step + 1}, nil
	}
	encodedState, exists := tuple.Checkpoint.Values[checkpoint.StateChannel]
	if !exists {
		return zero, &SchedulerError{Operation: "barrier-decode-state", Config: config, Err: fmt.Errorf("checkpoint state channel is missing")}
	}
	state, err := b.stateCodec.Decode(encodedState)
	if err != nil {
		return zero, &SchedulerError{Operation: "barrier-decode-state", Config: config, Err: err}
	}
	commands := make([]graph.Command[D], 0, len(tuple.Checkpoint.Next))
	taskIDs := make([]string, 0, len(tuple.Checkpoint.Next))
	nodes := make([]graph.NodeID, 0, len(tuple.Checkpoint.Next))
	deltas := make([]D, 0, len(tuple.Checkpoint.Next))
	for _, task := range tuple.Checkpoint.Next {
		command, err := b.commandCodec.Decode(writes[task.ID].Value)
		if err != nil {
			return zero, &SchedulerError{Operation: "barrier-decode-command", Config: config, TaskID: task.ID, Err: err}
		}
		commands = append(commands, command)
		taskIDs = append(taskIDs, task.ID)
		nodes = append(nodes, graph.NodeID(task.Name))
		if command.HasUpdate {
			deltas = append(deltas, command.Update)
		}
	}
	if len(deltas) > 0 {
		state, err = b.reducer(ctx, state, deltas)
		if err != nil {
			return zero, &SchedulerError{Operation: "barrier-reduce", Config: config, Err: err}
		}
	}
	return BarrierResult[S, D]{
		Ready: true, State: state, Commands: commands, TaskIDs: taskIDs, Nodes: nodes,
		Waiting: cloneWaiting(tuple.Checkpoint.Waiting), Step: tuple.Checkpoint.Step + 1,
	}, nil
}
