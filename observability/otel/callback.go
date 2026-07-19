// Package otel exports LangGraph Go run and node callbacks as OpenTelemetry
// spans without adding OpenTelemetry dependencies to the core graph module.
package otel

import (
	"context"
	"sync"

	"github.com/wahanbo/langgraph-go/graph"
	globalotel "go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

const instrumentationName = "github.com/wahanbo/langgraph-go"

// Callback translates graph lifecycle callbacks into OpenTelemetry spans.
// A Callback can be reused safely by concurrent graph invocations.
type Callback struct {
	tracer trace.Tracer
	mu     sync.Mutex
	spans  map[string]trace.Span
}

// New creates a callback using tracerProvider. A nil provider uses the global
// OpenTelemetry tracer provider.
func New(tracerProvider trace.TracerProvider) *Callback {
	if tracerProvider == nil {
		tracerProvider = globalotel.GetTracerProvider()
	}
	return &Callback{tracer: tracerProvider.Tracer(instrumentationName), spans: make(map[string]trace.Span)}
}

func (c *Callback) OnGraphStart(ctx context.Context, event graph.GraphRunStartEvent) {
	ctx = c.parentContext(ctx, event.ParentRunID)
	_, span := c.tracer.Start(ctx, event.Name,
		trace.WithSpanKind(trace.SpanKindInternal),
		trace.WithAttributes(commonAttributes(event.RunID, event.ParentRunID, event.ThreadID, event.CheckpointNamespace, event.Tags)...),
	)
	c.put(event.RunID, span)
}

func (c *Callback) OnGraphEnd(_ context.Context, event graph.GraphRunEndEvent) {
	if span := c.take(event.RunID); span != nil {
		span.SetStatus(codes.Ok, "")
		span.End()
	}
}

func (c *Callback) OnGraphError(_ context.Context, event graph.GraphRunErrorEvent) {
	if span := c.take(event.RunID); span != nil {
		recordError(span, event.Err)
		span.End()
	}
}

func (c *Callback) OnNodeStart(ctx context.Context, event graph.NodeRunStartEvent) {
	ctx = c.parentContext(ctx, event.ParentRunID)
	attributes := commonAttributes(event.RunID, event.ParentRunID, event.ThreadID, event.CheckpointNamespace, event.Tags)
	attributes = append(attributes,
		attribute.String("langgraph.node", string(event.Node)),
		attribute.String("langgraph.task.id", event.TaskID),
		attribute.Int("langgraph.step", event.Step),
		attribute.Int("langgraph.attempt", event.Attempt),
	)
	_, span := c.tracer.Start(ctx, event.Name, trace.WithSpanKind(trace.SpanKindInternal), trace.WithAttributes(attributes...))
	c.put(event.RunID, span)
}

func (c *Callback) OnNodeEnd(_ context.Context, event graph.NodeRunEndEvent) {
	if span := c.take(event.RunID); span != nil {
		span.SetStatus(codes.Ok, "")
		span.End()
	}
}

func (c *Callback) OnNodeError(_ context.Context, event graph.NodeRunErrorEvent) {
	if span := c.take(event.RunID); span != nil {
		recordError(span, event.Err)
		span.End()
	}
}

func (c *Callback) OnInterrupt(_ context.Context, event graph.GraphInterruptEvent) {
	if span := c.get(event.RunID); span != nil {
		span.AddEvent("langgraph.interrupt", trace.WithAttributes(
			attribute.String("langgraph.lifecycle.status", string(event.Status)),
			attribute.Int("langgraph.interrupt.count", len(event.Interrupts)),
		))
	}
}

func (c *Callback) OnResume(_ context.Context, event graph.GraphResumeEvent) {
	if span := c.get(event.RunID); span != nil {
		span.AddEvent("langgraph.resume", trace.WithAttributes(attribute.String("langgraph.lifecycle.status", string(event.Status))))
	}
}

func commonAttributes(runID, parentRunID, threadID, namespace string, tags []string) []attribute.KeyValue {
	return []attribute.KeyValue{
		attribute.String("langgraph.run.id", runID),
		attribute.String("langgraph.parent_run.id", parentRunID),
		attribute.String("langgraph.thread.id", threadID),
		attribute.String("langgraph.checkpoint.namespace", namespace),
		attribute.StringSlice("langgraph.tags", tags),
	}
}

func recordError(span trace.Span, err error) {
	if err == nil {
		span.SetStatus(codes.Error, "unknown error")
		return
	}
	span.RecordError(err)
	span.SetStatus(codes.Error, err.Error())
}

func (c *Callback) parentContext(ctx context.Context, parentID string) context.Context {
	if parent := c.get(parentID); parent != nil {
		return trace.ContextWithSpan(ctx, parent)
	}
	return ctx
}

func (c *Callback) put(id string, span trace.Span) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if previous := c.spans[id]; previous != nil {
		previous.SetStatus(codes.Error, "duplicate run id")
		previous.End()
	}
	c.spans[id] = span
}

func (c *Callback) get(id string) trace.Span {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.spans[id]
}

func (c *Callback) take(id string) trace.Span {
	c.mu.Lock()
	defer c.mu.Unlock()
	span := c.spans[id]
	delete(c.spans, id)
	return span
}

var _ graph.GraphCallback = (*Callback)(nil)
var _ graph.GraphRunCallback = (*Callback)(nil)
var _ graph.NodeRunCallback = (*Callback)(nil)
