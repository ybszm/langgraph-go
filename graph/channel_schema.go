package graph

import (
	"context"
	"fmt"
	"sort"

	"github.com/ybszm/langgraph-go/checkpoint"
)

// CheckpointChannelBinding is one fixed, typed state-to-channel projection.
// Use BindCheckpointChannel to construct it.
type CheckpointChannelBinding[S any] struct {
	name    string
	project func(context.Context, S) (checkpoint.EncodedValue, error)
}

// BindCheckpointChannel binds a typed state selector and codec to one stable
// checkpoint channel name.
func BindCheckpointChannel[S, V any](
	name string,
	codec checkpoint.Codec[V],
	selectValue func(S) V,
) (CheckpointChannelBinding[S], error) {
	if isReservedCheckpointChannel(name) {
		return CheckpointChannelBinding[S]{}, fmt.Errorf("%w: reserved channel %q", ErrCheckpointChannel, name)
	}
	if codec == nil || selectValue == nil {
		return CheckpointChannelBinding[S]{}, fmt.Errorf("%w: channel %q requires codec and selector", ErrCheckpointChannel, name)
	}
	return CheckpointChannelBinding[S]{
		name: name,
		project: func(_ context.Context, state S) (checkpoint.EncodedValue, error) {
			return codec.Encode(selectValue(state))
		},
	}, nil
}

// SetCheckpointChannelSchema registers fixed channel names used by checkpoint
// projection and node read-set validation.
func (g *StateGraph[S, D]) SetCheckpointChannelSchema(bindings ...CheckpointChannelBinding[S]) error {
	if len(bindings) == 0 {
		return fmt.Errorf("%w: checkpoint channel schema is empty", ErrInvalidGraph)
	}
	for _, binding := range bindings {
		if isReservedCheckpointChannel(binding.name) || binding.project == nil {
			return fmt.Errorf("%w: %w: invalid channel binding %q", ErrInvalidGraph, ErrCheckpointChannel, binding.name)
		}
		if _, exists := g.checkpointChannelSchema[binding.name]; exists {
			return fmt.Errorf("%w: %w: duplicate channel %q", ErrInvalidGraph, ErrCheckpointChannel, binding.name)
		}
		g.checkpointChannelSchema[binding.name] = binding
	}
	return nil
}

func cloneCheckpointChannelSchema[S any](source map[string]CheckpointChannelBinding[S]) map[string]CheckpointChannelBinding[S] {
	result := make(map[string]CheckpointChannelBinding[S], len(source))
	for name, binding := range source {
		result[name] = binding
	}
	return result
}

func channelReads(stateChannel bool, extras []string) []string {
	result := make([]string, 0, len(extras)+1)
	if stateChannel {
		result = append(result, checkpoint.StateChannel)
	}
	result = append(result, extras...)
	sort.Strings(result)
	return result
}
