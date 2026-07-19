package functional

import (
	"context"
	"fmt"

	"github.com/ybszm/langgraph-go/checkpoint"
)

// DurableState is one decoded Functional checkpoint snapshot.
type DurableState[S any] struct {
	Config        checkpoint.Config
	Previous      *S
	Status        string
	Step          int
	Metadata      checkpoint.Metadata
	ParentConfig  *checkpoint.Config
	PendingWrites []checkpoint.PendingWrite
}

// GetState returns the latest or exact durable checkpoint for a thread.
func (e *DurableEntrypoint[I, O, S]) GetState(
	ctx context.Context,
	runConfig DurableRunConfig,
) (DurableState[S], bool, error) {
	if e == nil || e.config.Saver == nil {
		return DurableState[S]{}, false, fmt.Errorf("%w: nil durable entrypoint", ErrInvalidDefinition)
	}
	config := checkpoint.Config{
		ThreadID: runConfig.ThreadID, Namespace: e.config.Namespace, CheckpointID: runConfig.CheckpointID,
	}
	if err := config.Validate(); err != nil {
		return DurableState[S]{}, false, err
	}
	tuple, found, err := e.config.Saver.GetTuple(ctx, config)
	if err != nil || !found {
		return DurableState[S]{}, found, err
	}
	state, err := e.decodeDurableState(ctx, tuple)
	return state, err == nil, err
}

// GetStateHistory returns newest-first decoded checkpoints for one thread.
func (e *DurableEntrypoint[I, O, S]) GetStateHistory(
	ctx context.Context,
	threadID string,
	limit int,
) ([]DurableState[S], error) {
	if e == nil || e.config.Saver == nil {
		return nil, fmt.Errorf("%w: nil durable entrypoint", ErrInvalidDefinition)
	}
	config := checkpoint.Config{ThreadID: threadID, Namespace: e.config.Namespace}
	if err := config.Validate(); err != nil {
		return nil, err
	}
	tuples, err := e.config.Saver.List(ctx, checkpoint.ListOptions{Config: &config, Limit: limit})
	if err != nil {
		return nil, err
	}
	result := make([]DurableState[S], len(tuples))
	for index, tuple := range tuples {
		result[index], err = e.decodeDurableState(ctx, tuple)
		if err != nil {
			return nil, err
		}
	}
	return result, nil
}

func (e *DurableEntrypoint[I, O, S]) decodeDurableState(ctx context.Context, tuple checkpoint.Tuple) (DurableState[S], error) {
	state := DurableState[S]{
		Config: tuple.Config, Step: tuple.Checkpoint.Step,
		PendingWrites: clonePendingWrites(tuple.PendingWrites),
	}
	metadata, err := checkpoint.CloneMetadata(tuple.Metadata)
	if err != nil {
		return state, err
	}
	state.Metadata = metadata
	state.Status, _ = metadata["status"].(string)
	if tuple.ParentConfig != nil {
		parent := *tuple.ParentConfig
		state.ParentConfig = &parent
	}
	if encoded, exists := tuple.Checkpoint.Values[functionalPreviousChannel]; exists {
		value, _, err := e.decodePrevious(ctx, tuple, encoded, false)
		if err != nil {
			return state, fmt.Errorf("decode functional previous state: %w", err)
		}
		state.Previous = &value
	}
	return state, nil
}

func clonePendingWrites(source []checkpoint.PendingWrite) []checkpoint.PendingWrite {
	if source == nil {
		return nil
	}
	result := make([]checkpoint.PendingWrite, len(source))
	for index, write := range source {
		result[index] = write
		result[index].Value = checkpoint.CloneEncodedValue(write.Value)
	}
	return result
}
