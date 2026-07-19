package remote

import (
	"bufio"
	"context"
	"errors"
	"net"
	"net/http"
	"strings"
	"time"
	"unicode"

	"github.com/wahanbo/langgraph-go/graph"
)

type traceContextKey struct{}

// HTTPTrace is one completed remote transport span.
type HTTPTrace struct {
	TraceID    string
	Method     string
	Path       string
	Status     int
	StartedAt  time.Time
	FinishedAt time.Time
}

// TraceSink persists or exports completed remote HTTP spans.
type TraceSink interface {
	Record(context.Context, HTTPTrace) error
}

// TraceSinkFunc adapts a function to TraceSink.
type TraceSinkFunc func(context.Context, HTTPTrace) error

func (sink TraceSinkFunc) Record(ctx context.Context, trace HTTPTrace) error {
	return sink(ctx, trace)
}

type traceResponseWriter struct {
	http.ResponseWriter
	status int
}

func (writer *traceResponseWriter) Unwrap() http.ResponseWriter { return writer.ResponseWriter }

func (writer *traceResponseWriter) WriteHeader(status int) {
	if writer.status != 0 {
		return
	}
	writer.status = status
	writer.ResponseWriter.WriteHeader(status)
}

func (writer *traceResponseWriter) Write(value []byte) (int, error) {
	if writer.status == 0 {
		writer.WriteHeader(http.StatusOK)
	}
	return writer.ResponseWriter.Write(value)
}

func (writer *traceResponseWriter) Flush() {
	if writer.status == 0 {
		writer.WriteHeader(http.StatusOK)
	}
	if flusher, ok := writer.ResponseWriter.(http.Flusher); ok {
		flusher.Flush()
	}
}

func (writer *traceResponseWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	hijacker, ok := writer.ResponseWriter.(http.Hijacker)
	if !ok {
		return nil, nil, errors.New("response writer does not support hijacking")
	}
	return hijacker.Hijack()
}

// TraceIDFromContext returns the remote request correlation ID, if present.
func TraceIDFromContext(ctx context.Context) string {
	if ctx == nil {
		return ""
	}
	value, _ := ctx.Value(traceContextKey{}).(string)
	return value
}

func validTraceID(id string) bool {
	if id == "" || len(id) > 256 || strings.TrimSpace(id) != id {
		return false
	}
	for _, character := range id {
		if unicode.IsControl(character) {
			return false
		}
	}
	return true
}

func tracedGraphConfig(ctx context.Context, config graph.RunConfig) graph.RunConfig {
	traceID := TraceIDFromContext(ctx)
	if traceID == "" {
		return config
	}
	config.Metadata = cloneMetadata(config.Metadata)
	if config.Metadata == nil {
		config.Metadata = make(map[string]any)
	}
	config.Metadata[TraceMetadataKey] = traceID
	return config
}

func withTraceID(ctx context.Context, traceID string) context.Context {
	if traceID == "" {
		return ctx
	}
	return context.WithValue(ctx, traceContextKey{}, traceID)
}
