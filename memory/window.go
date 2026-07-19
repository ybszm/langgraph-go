package memory

import (
	"context"
	"errors"

	"github.com/ybszm/langgraph-go/graph"
	"github.com/ybszm/langgraph-go/prebuilt"
)

// Window projects a bounded suffix of conversation history for a model call.
// Leading system messages are always preserved and do not count toward MaxMessages.
type Window[S any] struct {
	Adapter     MessageAdapter[S]
	MaxMessages int
}

// NewWindow validates and constructs sliding-window model middleware.
func NewWindow[S any](adapter MessageAdapter[S], maxMessages int) (*Window[S], error) {
	if err := adapter.validate(); err != nil {
		return nil, err
	}
	if maxMessages < 1 {
		return nil, errors.New("memory window MaxMessages must be positive")
	}
	return &Window[S]{Adapter: adapter, MaxMessages: maxMessages}, nil
}

// InvokeModel implements prebuilt.ModelMiddleware.
func (middleware *Window[S]) InvokeModel(ctx context.Context, state S, runtime graph.Runtime, next prebuilt.ModelHandler[S]) (prebuilt.AssistantMessage, error) {
	messages, err := middleware.Adapter.Messages(ctx, state)
	if err != nil {
		return prebuilt.AssistantMessage{}, err
	}
	projected, err := middleware.Adapter.WithMessages(state, WindowMessages(messages, middleware.MaxMessages))
	if err != nil {
		return prebuilt.AssistantMessage{}, err
	}
	return next(ctx, projected, runtime)
}

// WindowMessages retains leading system messages plus a bounded conversation
// suffix. An assistant tool call and its immediately following tool results are
// indivisible, so the returned suffix can slightly exceed maxMessages.
func WindowMessages(messages []prebuilt.Message, maxMessages int) []prebuilt.Message {
	if maxMessages < 1 || len(messages) == 0 {
		return nil
	}
	systemEnd := leadingSystemEnd(messages)
	units := conversationUnits(messages[systemEnd:])
	selected := len(units)
	count := 0
	for selected > 0 {
		unitSize := len(units[selected-1])
		if count > 0 && count+unitSize > maxMessages {
			break
		}
		count += unitSize
		selected--
		if count >= maxMessages {
			break
		}
	}
	result := cloneMessages(messages[:systemEnd])
	for _, unit := range units[selected:] {
		result = append(result, cloneMessages(unit)...)
	}
	return result
}

func leadingSystemEnd(messages []prebuilt.Message) int {
	index := 0
	for index < len(messages) {
		if _, ok := messages[index].(prebuilt.SystemMessage); !ok {
			break
		}
		index++
	}
	return index
}

func conversationUnits(messages []prebuilt.Message) [][]prebuilt.Message {
	units := make([][]prebuilt.Message, 0, len(messages))
	for index := 0; index < len(messages); {
		end := index + 1
		if assistant, ok := messages[index].(prebuilt.AssistantMessage); ok && len(assistant.ToolCalls) > 0 {
			for end < len(messages) {
				if _, ok := messages[end].(prebuilt.ToolMessage); !ok {
					break
				}
				end++
			}
		}
		units = append(units, messages[index:end])
		index = end
	}
	return units
}
