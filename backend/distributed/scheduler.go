package distributed

import (
	"context"
	"fmt"

	"github.com/wahanbo/langgraph-go/checkpoint"
	"github.com/wahanbo/langgraph-go/graph"
)

// SchedulerError adds checkpoint and graph task context while preserving causes.
type SchedulerError struct {
	Operation string
	Config    checkpoint.Config
	TaskID    string
	Err       error
}

func (e *SchedulerError) Error() string {
	return fmt.Sprintf("distributed scheduler %s thread=%s checkpoint=%s task=%s: %v",
		e.Operation, e.Config.ThreadID, e.Config.CheckpointID, e.TaskID, e.Err)
}

// Unwrap returns the saver, codec, or queue cause.
func (e *SchedulerError) Unwrap() error { return e.Err }

// Scheduler maps exact checkpoint Next tasks to idempotent GraphTask queue items.
type Scheduler[S any] struct {
	saver checkpoint.Saver
	codec checkpoint.Codec[S]
	queue Queue[GraphTask[S]]
}

// NewScheduler validates and constructs a checkpoint task scheduler.
func NewScheduler[S any](saver checkpoint.Saver, codec checkpoint.Codec[S], queue Queue[GraphTask[S]]) (*Scheduler[S], error) {
	if nilLike(saver) || nilLike(codec) || nilLike(queue) {
		return nil, fmt.Errorf("%w: scheduler saver, state codec, and queue are required", ErrInvalidRequest)
	}
	return &Scheduler[S]{saver: saver, codec: codec, queue: queue}, nil
}

// Schedule enqueues incomplete Next tasks in checkpoint order. Partial failure
// is safe to retry because each enqueue is bound to checkpoint plus graph task ID.
func (s *Scheduler[S]) Schedule(ctx context.Context, config checkpoint.Config) ([]string, error) {
	if err := validContext(ctx); err != nil {
		return nil, err
	}
	if config.CheckpointID == "" {
		return nil, &SchedulerError{Operation: "validate", Config: config, Err: fmt.Errorf("%w: exact checkpoint is required", ErrInvalidRequest)}
	}
	if err := config.Validate(); err != nil {
		return nil, &SchedulerError{Operation: "validate", Config: config, Err: err}
	}
	tuple, found, err := s.saver.GetTuple(ctx, config)
	if err != nil {
		return nil, &SchedulerError{Operation: "get-checkpoint", Config: config, Err: err}
	}
	if !found || tuple.Config.CheckpointID != config.CheckpointID {
		return nil, &SchedulerError{Operation: "get-checkpoint", Config: config, Err: checkpoint.ErrNotFound}
	}
	if err := tuple.Checkpoint.Validate(); err != nil {
		return nil, &SchedulerError{Operation: "validate-checkpoint", Config: config, Err: err}
	}
	completed := make(map[string]struct{})
	for _, write := range tuple.PendingWrites {
		if write.Channel == checkpoint.TaskResultChannel && write.Index == 0 {
			completed[write.TaskID] = struct{}{}
		}
	}
	stateValue, hasState := tuple.Checkpoint.Values[checkpoint.StateChannel]
	ids := make([]string, 0, len(tuple.Checkpoint.Next))
	for index, pending := range tuple.Checkpoint.Next {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if _, done := completed[pending.ID]; done {
			continue
		}
		encoded := pending.Input
		if encoded == nil {
			if !hasState {
				return nil, &SchedulerError{Operation: "decode-state", Config: config, TaskID: pending.ID, Err: fmt.Errorf("checkpoint state channel is missing")}
			}
			copy := stateValue
			encoded = &copy
		}
		state, err := s.codec.Decode(*encoded)
		if err != nil {
			return nil, &SchedulerError{Operation: "decode-state", Config: config, TaskID: pending.ID, Err: err}
		}
		payload := GraphTask[S]{
			State: state, Node: graph.NodeID(pending.Name), Step: tuple.Checkpoint.Step + 1,
			FirstAttemptTime: tuple.Checkpoint.Timestamp, Checkpoint: config,
			TaskPath: fmt.Sprintf("pull/%d/%s", index, pending.Name), ResultIndex: 0,
		}
		id, err := s.queue.Enqueue(ctx, EnqueueRequest[GraphTask[S]]{
			TaskID: pending.ID, Payload: payload,
			IdempotencyKey: "checkpoint:" + config.ThreadID + ":" + config.Namespace + ":" + config.CheckpointID + ":" + pending.ID,
		})
		if err != nil {
			return nil, &SchedulerError{Operation: "enqueue", Config: config, TaskID: pending.ID, Err: err}
		}
		ids = append(ids, id)
	}
	return ids, nil
}
