package distributed

import (
	"bytes"
	"context"
	"fmt"

	"github.com/wahanbo/langgraph-go/checkpoint"
)

// CheckpointResult describes one typed pending write at an exact checkpoint.
type CheckpointResult[R any] struct {
	Config   checkpoint.Config
	TaskPath string
	Index    int
	Channel  string
	Value    R
}

// CheckpointOutbox commits worker results as checkpoint pending writes.
type CheckpointOutbox[R any] struct {
	saver checkpoint.Saver
	codec checkpoint.Codec[R]
}

// NewCheckpointOutbox validates and constructs a checkpoint completion adapter.
func NewCheckpointOutbox[R any](saver checkpoint.Saver, codec checkpoint.Codec[R]) (*CheckpointOutbox[R], error) {
	if nilLike(saver) || nilLike(codec) {
		return nil, fmt.Errorf("%w: checkpoint saver and result codec are required", ErrInvalidRequest)
	}
	return &CheckpointOutbox[R]{saver: saver, codec: codec}, nil
}

// Commit first-writes a pending result, then reads the exact checkpoint back
// and verifies the stored bytes before allowing the worker to Ack.
func (o *CheckpointOutbox[R]) Commit(ctx context.Context, completion Completion[CheckpointResult[R]]) error {
	if err := validContext(ctx); err != nil {
		return err
	}
	result := completion.Value
	if !validID(completion.TaskID) || !validID(completion.LeaseToken) || completion.Attempt < 1 ||
		result.Config.CheckpointID == "" || result.Index < 0 || !validID(result.Channel) {
		return fmt.Errorf("%w: exact checkpoint, task, lease, channel, and non-negative index are required", ErrInvalidRequest)
	}
	if err := result.Config.Validate(); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidRequest, err)
	}
	encoded, err := o.codec.Encode(result.Value)
	if err != nil {
		return &WorkerError{Operation: "encode-checkpoint-result", TaskID: completion.TaskID, Err: err}
	}
	if _, err := o.codec.Decode(encoded); err != nil {
		return &WorkerError{Operation: "decode-checkpoint-result", TaskID: completion.TaskID, Err: err}
	}
	tuple, found, err := o.saver.GetTuple(ctx, result.Config)
	if err != nil {
		return &WorkerError{Operation: "get-checkpoint", TaskID: completion.TaskID, Err: err}
	}
	if !found || tuple.Config.CheckpointID != result.Config.CheckpointID {
		return &WorkerError{Operation: "get-checkpoint", TaskID: completion.TaskID, Err: checkpoint.ErrNotFound}
	}
	if existing, exists := pendingWrite(tuple.PendingWrites, completion.TaskID, result.Index); exists {
		return comparePending(existing, result.Channel, encoded)
	}
	write := checkpoint.PendingWrite{
		TaskID: completion.TaskID, TaskPath: result.TaskPath, Index: result.Index,
		Channel: result.Channel, Value: checkpoint.CloneEncodedValue(encoded),
	}
	if err := o.saver.PutWrites(ctx, result.Config, []checkpoint.PendingWrite{write}); err != nil {
		return &WorkerError{Operation: "put-checkpoint-result", TaskID: completion.TaskID, Err: err}
	}
	verified, found, err := o.saver.GetTuple(ctx, result.Config)
	if err != nil {
		return &WorkerError{Operation: "verify-checkpoint-result", TaskID: completion.TaskID, Err: err}
	}
	if !found {
		return &WorkerError{Operation: "verify-checkpoint-result", TaskID: completion.TaskID, Err: checkpoint.ErrNotFound}
	}
	stored, exists := pendingWrite(verified.PendingWrites, completion.TaskID, result.Index)
	if !exists {
		return &WorkerError{Operation: "verify-checkpoint-result", TaskID: completion.TaskID, Err: fmt.Errorf("pending write is missing after commit")}
	}
	return comparePending(stored, result.Channel, encoded)
}

func pendingWrite(writes []checkpoint.PendingWrite, taskID string, index int) (checkpoint.PendingWrite, bool) {
	for _, write := range writes {
		if write.TaskID == taskID && write.Index == index {
			return write, true
		}
	}
	return checkpoint.PendingWrite{}, false
}

func comparePending(existing checkpoint.PendingWrite, channel string, encoded checkpoint.EncodedValue) error {
	if existing.Channel != channel || existing.Value.Type != encoded.Type || existing.Value.Version != encoded.Version ||
		!bytes.Equal(existing.Value.Data, encoded.Data) {
		return ErrCompletionConflict
	}
	return nil
}
