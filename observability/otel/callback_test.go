package otel_test

import (
	"context"
	"errors"
	"testing"

	"github.com/ybszm/langgraph-go/graph"
	langotel "github.com/ybszm/langgraph-go/observability/otel"
	"go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

func TestCallbackExportsParentedGraphAndNodeSpans(t *testing.T) {
	recorder := tracetest.NewSpanRecorder()
	provider := trace.NewTracerProvider(trace.WithSpanProcessor(recorder))
	callback := langotel.New(provider)

	callback.OnGraphStart(context.Background(), graph.GraphRunStartEvent{RunID: "graph", Name: "agent", ThreadID: "thread"})
	callback.OnNodeStart(context.Background(), graph.NodeRunStartEvent{RunID: "node", ParentRunID: "graph", Name: "model", Node: "model", Step: 1, Attempt: 1})
	callback.OnNodeError(context.Background(), graph.NodeRunErrorEvent{RunID: "node", Err: errors.New("boom")})
	callback.OnGraphEnd(context.Background(), graph.GraphRunEndEvent{RunID: "graph"})

	spans := recorder.Ended()
	if len(spans) != 2 {
		t.Fatalf("ended spans = %d, want 2", len(spans))
	}
	if spans[0].Parent().SpanID() != spans[1].SpanContext().SpanID() {
		t.Fatal("node span is not parented by graph span")
	}
	if spans[0].Status().Code.String() != "Error" {
		t.Fatalf("node status = %s, want Error", spans[0].Status().Code)
	}
}
