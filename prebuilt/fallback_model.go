package prebuilt

import (
	"context"
	"errors"
	"fmt"

	"github.com/ybszm/langgraph-go/graph"
)

// FallbackChatModel tries provider-neutral models in order. ShouldFallback can
// stop fallback for permanent errors; nil falls back on every model error.
type FallbackChatModel[S any] struct {
	Models         []ChatModel[S]
	ShouldFallback func(error) bool
}

func (model FallbackChatModel[S]) Invoke(ctx context.Context, state S, runtime graph.Runtime) (AssistantMessage, error) {
	if len(model.Models) == 0 {
		return AssistantMessage{}, errors.New("fallback chat model has no models")
	}
	errs := make([]error, 0, len(model.Models))
	for index, candidate := range model.Models {
		if isNilChatModel(candidate) {
			return AssistantMessage{}, fmt.Errorf("fallback chat model %d is nil", index)
		}
		message, err := candidate.Invoke(ctx, state, runtime)
		if err == nil {
			return message, nil
		}
		errs = append(errs, fmt.Errorf("model %d: %w", index, err))
		if model.ShouldFallback != nil && !model.ShouldFallback(err) {
			break
		}
		if ctx.Err() != nil {
			break
		}
	}
	return AssistantMessage{}, errors.Join(errs...)
}

func (model FallbackChatModel[S]) BindTools(definitions []ToolDefinition) (ChatModel[S], error) {
	bound := FallbackChatModel[S]{Models: make([]ChatModel[S], len(model.Models)), ShouldFallback: model.ShouldFallback}
	for index, candidate := range model.Models {
		if isNilChatModel(candidate) {
			return nil, fmt.Errorf("fallback chat model %d is nil", index)
		}
		if binder, ok := candidate.(ToolBindingChatModel[S]); ok {
			var err error
			bound.Models[index], err = binder.BindTools(definitions)
			if err != nil {
				return nil, fmt.Errorf("bind fallback model %d: %w", index, err)
			}
		} else {
			bound.Models[index] = candidate
		}
	}
	return bound, nil
}
