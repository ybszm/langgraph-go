package memory

import (
	"context"
	"errors"
	"strings"

	"github.com/wahanbo/langgraph-go/graph"
	"github.com/wahanbo/langgraph-go/prebuilt"
)

// Summarizer condenses evicted conversation messages into plain text.
type Summarizer interface {
	Summarize(context.Context, []prebuilt.Message) (string, error)
}

// SummarizerFunc adapts a function to Summarizer.
type SummarizerFunc func(context.Context, []prebuilt.Message) (string, error)

func (summarizer SummarizerFunc) Summarize(ctx context.Context, messages []prebuilt.Message) (string, error) {
	return summarizer(ctx, messages)
}

// Summary projects old history into one system summary while retaining a
// recent tool-call-safe suffix.
type Summary[S any] struct {
	Adapter         MessageAdapter[S]
	Summarizer      Summarizer
	TriggerMessages int
	KeepMessages    int
	Prefix          string
}

// NewSummary validates and constructs summarizing model middleware.
func NewSummary[S any](adapter MessageAdapter[S], summarizer Summarizer, triggerMessages, keepMessages int) (*Summary[S], error) {
	if err := adapter.validate(); err != nil {
		return nil, err
	}
	if summarizer == nil {
		return nil, errors.New("memory summary requires a summarizer")
	}
	if triggerMessages < 1 || keepMessages < 1 || keepMessages >= triggerMessages {
		return nil, errors.New("memory summary requires 0 < KeepMessages < TriggerMessages")
	}
	return &Summary[S]{Adapter: adapter, Summarizer: summarizer, TriggerMessages: triggerMessages, KeepMessages: keepMessages, Prefix: "Conversation summary:\n"}, nil
}

// InvokeModel implements prebuilt.ModelMiddleware.
func (middleware *Summary[S]) InvokeModel(ctx context.Context, state S, runtime graph.Runtime, next prebuilt.ModelHandler[S]) (prebuilt.AssistantMessage, error) {
	messages, err := middleware.Adapter.Messages(ctx, state)
	if err != nil {
		return prebuilt.AssistantMessage{}, err
	}
	if len(messages) <= middleware.TriggerMessages {
		return next(ctx, state, runtime)
	}
	systemEnd := leadingSystemEnd(messages)
	recent := WindowMessages(messages[systemEnd:], middleware.KeepMessages)
	evictedCount := len(messages) - systemEnd - len(recent)
	if evictedCount <= 0 {
		return next(ctx, state, runtime)
	}
	summary, err := middleware.Summarizer.Summarize(ctx, cloneMessages(messages[systemEnd:systemEnd+evictedCount]))
	if err != nil {
		return prebuilt.AssistantMessage{}, err
	}
	summary = strings.TrimSpace(summary)
	if summary == "" {
		return next(ctx, state, runtime)
	}
	projectedMessages := cloneMessages(messages[:systemEnd])
	projectedMessages = append(projectedMessages, prebuilt.SystemMessage{Content: middleware.Prefix + summary})
	projectedMessages = append(projectedMessages, recent...)
	projected, err := middleware.Adapter.WithMessages(state, projectedMessages)
	if err != nil {
		return prebuilt.AssistantMessage{}, err
	}
	return next(ctx, projected, runtime)
}
