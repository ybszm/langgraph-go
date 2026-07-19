// Package remote provides provider-neutral LangGraph HTTP server and SDK contracts.
package remote

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/ybszm/langgraph-go/backend/distributed"
	"github.com/ybszm/langgraph-go/graph"
)

const (
	// ProtocolHeader carries the wire protocol version.
	ProtocolHeader = "LangGraph-Protocol-Version"
	// ProtocolVersion is the current Go remote protocol version.
	ProtocolVersion = "1"
	// IdempotencyHeader deduplicates asynchronous run creation requests.
	IdempotencyHeader = "Idempotency-Key"
	// TraceHeader carries the correlation ID for one HTTP request.
	TraceHeader = "LangGraph-Trace-ID"
	// TraceMetadataKey is the RunConfig metadata key populated from TraceHeader.
	TraceMetadataKey = "langgraph_trace_id"
	// InvokePath is the synchronous typed invocation endpoint.
	InvokePath = "/v1/runs/invoke"
	// CommandPath is the synchronous typed Command/resume endpoint.
	CommandPath = "/v1/runs/command"
	// BatchPath is the synchronous typed, input-order batch endpoint.
	BatchPath = "/v1/runs/batch"
	// StreamPath is the server-sent events streaming endpoint.
	StreamPath = "/v1/runs/stream"
)

// ErrorCode is a stable remote error category.
type ErrorCode string

const (
	// CodeInvalidRequest indicates malformed HTTP or JSON input.
	CodeInvalidRequest ErrorCode = "invalid_request"
	// CodeExecution indicates the graph returned an execution error.
	CodeExecution ErrorCode = "execution_error"
	// CodeProtocol indicates an incompatible or malformed server response.
	CodeProtocol ErrorCode = "protocol_error"
	// CodeNotFound indicates a missing thread or run resource.
	CodeNotFound ErrorCode = "not_found"
	// CodeConflict indicates an existing resource or invalid lifecycle transition.
	CodeConflict ErrorCode = "conflict"
	// CodeUnauthorized indicates authorization was rejected before dispatch.
	CodeUnauthorized ErrorCode = "unauthorized"
)

// Error is the JSON-compatible remote error envelope.
type Error struct {
	Code    ErrorCode `json:"code"`
	Message string    `json:"message"`
	Status  int       `json:"-"`
	cause   error
}

func (e *Error) Error() string {
	return fmt.Sprintf("remote %s: %s", e.Code, e.Message)
}

// Unwrap exposes a local transport or decoding cause when one is available.
func (e *Error) Unwrap() error { return e.cause }

// RunConfig is the transport-safe subset of graph.RunConfig.
type RunConfig struct {
	NewRun              bool           `json:"new_run,omitempty"`
	RecursionLimit      int            `json:"recursion_limit,omitempty"`
	MaxConcurrency      int            `json:"max_concurrency,omitempty"`
	ThreadID            string         `json:"thread_id,omitempty"`
	CheckpointNamespace string         `json:"checkpoint_namespace,omitempty"`
	CheckpointID        string         `json:"checkpoint_id,omitempty"`
	RunID               string         `json:"run_id,omitempty"`
	Metadata            map[string]any `json:"metadata,omitempty"`
}

func (c RunConfig) graphConfig() graph.RunConfig {
	return graph.RunConfig{
		NewRun: c.NewRun, RecursionLimit: c.RecursionLimit, MaxConcurrency: c.MaxConcurrency,
		ThreadID: c.ThreadID, CheckpointNamespace: c.CheckpointNamespace,
		CheckpointID: c.CheckpointID, RunID: c.RunID, Metadata: cloneMetadata(c.Metadata),
	}
}

// Invoker is implemented by a compiled graph or compatible abstraction.
type Invoker[I, O any] interface {
	Invoke(context.Context, I, graph.RunConfig) (O, error)
}

// ServerOptions controls HTTP request limits.
type ServerOptions struct {
	// MaxBodyBytes defaults to 1 MiB.
	MaxBodyBytes int64
	// Clock controls resource timestamps and defaults to time.Now.
	Clock func() time.Time
	// IDGenerator creates thread and run IDs and defaults to a random generator.
	IDGenerator func() string
	// Context owns asynchronous runs and defaults to context.Background().
	Context context.Context
	// Authorize runs before request decoding and graph dispatch.
	Authorize func(context.Context, *http.Request) error
	// TraceIDGenerator creates correlation IDs when TraceHeader is absent.
	TraceIDGenerator func() string
	// TraceSink receives completed HTTP request spans. Sink errors never change
	// an already-produced protocol response.
	TraceSink TraceSink
	// TraceErrorHandler observes TraceSink failures.
	TraceErrorHandler func(error)
	// InterruptStore optionally exposes distributed interrupt list/resume endpoints.
	InterruptStore distributed.InterruptStore
	// EventLog optionally exposes durable thread/run SSE endpoints.
	EventLog distributed.EventLog
	// ControlStore shares thread, run, and idempotency state across server
	// instances. SQLiteControlStore also survives process restarts.
	ControlStore ControlStore
	// ControlPollInterval controls cross-instance cancel/join observation and
	// defaults to 50 milliseconds.
	ControlPollInterval time.Duration
	// RunRetention automatically prunes terminal runs older than this duration.
	// Zero disables automatic pruning.
	RunRetention time.Duration
	// LangGraphProtocol exposes the pinned Python SDK-compatible /threads
	// facade in addition to the Go-native /v1 API.
	LangGraphProtocol bool
	// AssistantID identifies this single-graph server to the compatibility
	// facade and defaults to "graph".
	AssistantID string
}

// Server exposes one typed Invoker over HTTP.
type Server[I, O any] struct {
	invoker Invoker[I, O]
	options ServerOptions
	control *controlState[O]
	state   stateController
	command commandController
}

// NewServer validates and constructs a remote HTTP handler.
func NewServer[I, O any](invoker Invoker[I, O], options ServerOptions) (*Server[I, O], error) {
	if isNil(invoker) {
		return nil, fmt.Errorf("remote server requires invoker")
	}
	if options.MaxBodyBytes < 0 {
		return nil, fmt.Errorf("remote server max body bytes cannot be negative")
	}
	if options.MaxBodyBytes == 0 {
		options.MaxBodyBytes = 1 << 20
	}
	if options.Clock == nil {
		options.Clock = time.Now
	}
	if options.IDGenerator == nil {
		options.IDGenerator = randomID
	}
	if options.Context == nil {
		options.Context = context.Background()
	}
	if options.ControlPollInterval < 0 || options.RunRetention < 0 {
		return nil, fmt.Errorf("remote control intervals cannot be negative")
	}
	if options.ControlPollInterval == 0 {
		options.ControlPollInterval = 50 * time.Millisecond
	}
	if options.AssistantID == "" {
		options.AssistantID = "graph"
	}
	if options.TraceIDGenerator == nil {
		options.TraceIDGenerator = randomID
	}
	controlContext, cancel := context.WithCancel(options.Context)
	return &Server[I, O]{invoker: invoker, options: options, control: newControlState[O](controlContext, cancel)}, nil
}

type invokeRequest[I any] struct {
	Input  I         `json:"input"`
	Config RunConfig `json:"config,omitempty"`
}

type invokeResponse[O any] struct {
	Output *O     `json:"output,omitempty"`
	Error  *Error `json:"error,omitempty"`
}

// ServeHTTP implements http.Handler.
func (s *Server[I, O]) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	writer.Header().Set(ProtocolHeader, ProtocolVersion)
	if version := request.Header.Get(ProtocolHeader); version != "" && version != ProtocolVersion {
		writer.Header().Set("Content-Type", "application/json")
		s.writeError(writer, http.StatusUpgradeRequired, CodeProtocol, fmt.Sprintf("unsupported protocol version %q", version))
		return
	}
	traceID := request.Header.Get(TraceHeader)
	if traceID == "" {
		traceID = s.options.TraceIDGenerator()
	}
	if !validTraceID(traceID) {
		writer.Header().Set("Content-Type", "application/json")
		s.writeError(writer, http.StatusBadRequest, CodeInvalidRequest, "trace ID is invalid")
		return
	}
	writer.Header().Set(TraceHeader, traceID)
	request = request.WithContext(context.WithValue(request.Context(), traceContextKey{}, traceID))
	if s.options.TraceSink != nil {
		tracked := &traceResponseWriter{ResponseWriter: writer}
		writer = tracked
		started := s.options.Clock()
		defer func() {
			status := tracked.status
			if status == 0 {
				status = http.StatusOK
			}
			err := s.options.TraceSink.Record(context.WithoutCancel(request.Context()), HTTPTrace{
				TraceID: traceID, Method: request.Method, Path: request.URL.Path,
				Status: status, StartedAt: started, FinishedAt: s.options.Clock(),
			})
			if err != nil && s.options.TraceErrorHandler != nil {
				s.options.TraceErrorHandler(err)
			}
		}()
	}
	if s.options.Authorize != nil {
		if err := s.options.Authorize(request.Context(), request); err != nil {
			writer.Header().Set("Content-Type", "application/json")
			s.writeError(writer, http.StatusUnauthorized, CodeUnauthorized, err.Error())
			return
		}
	}
	if request.URL.Path != InvokePath && request.URL.Path != StreamPath && request.URL.Path != CommandPath && request.URL.Path != BatchPath {
		if s.options.LangGraphProtocol && (request.URL.Path == "/threads" || strings.HasPrefix(request.URL.Path, "/threads/")) {
			s.serveLangGraphProtocol(writer, request)
			return
		}
		if request.URL.Path == "/v1/threads" || strings.HasPrefix(request.URL.Path, "/v1/threads/") {
			s.serveControl(writer, request)
			return
		}
		writer.Header().Set("Content-Type", "application/json")
		s.writeError(writer, http.StatusNotFound, CodeInvalidRequest, "endpoint not found")
		return
	}
	if request.Method != http.MethodPost {
		writer.Header().Set("Content-Type", "application/json")
		writer.Header().Set("Allow", http.MethodPost)
		s.writeError(writer, http.StatusMethodNotAllowed, CodeInvalidRequest, "method must be POST")
		return
	}
	if request.URL.Path == StreamPath {
		s.serveStream(writer, request)
		return
	}
	writer.Header().Set("Content-Type", "application/json")
	if request.URL.Path == CommandPath {
		s.serveCommand(writer, request)
		return
	}
	if request.URL.Path == BatchPath {
		s.serveBatch(writer, request)
		return
	}
	s.serveInvoke(writer, request)
}

type batchItemRequest[I any] struct {
	Input  I         `json:"input"`
	Config RunConfig `json:"config,omitempty"`
}

type batchRequest[I any] struct {
	Items          []batchItemRequest[I] `json:"items"`
	MaxConcurrency int                   `json:"max_concurrency,omitempty"`
}

type batchItemResponse[O any] struct {
	Output *O     `json:"output,omitempty"`
	Error  *Error `json:"error,omitempty"`
}

type batchResponse[O any] struct {
	Results []batchItemResponse[O] `json:"results"`
	Error   *Error                 `json:"error,omitempty"`
}

func (s *Server[I, O]) serveBatch(writer http.ResponseWriter, request *http.Request) {
	request.Body = http.MaxBytesReader(writer, request.Body, s.options.MaxBodyBytes)
	decoder := json.NewDecoder(request.Body)
	decoder.DisallowUnknownFields()
	var payload batchRequest[I]
	if err := decoder.Decode(&payload); err != nil {
		s.writeError(writer, http.StatusBadRequest, CodeInvalidRequest, err.Error())
		return
	}
	if err := ensureJSONEOF(decoder); err != nil {
		s.writeError(writer, http.StatusBadRequest, CodeInvalidRequest, err.Error())
		return
	}
	if payload.MaxConcurrency < 0 {
		s.writeError(writer, http.StatusBadRequest, CodeInvalidRequest, "batch max_concurrency cannot be negative")
		return
	}
	results := make([]batchItemResponse[O], len(payload.Items))
	limit := payload.MaxConcurrency
	if limit == 0 || limit > len(payload.Items) {
		limit = len(payload.Items)
	}
	if limit > 0 {
		semaphore := make(chan struct{}, limit)
		var workers sync.WaitGroup
		for index, item := range payload.Items {
			index, item := index, item
			workers.Add(1)
			go func() {
				defer workers.Done()
				select {
				case semaphore <- struct{}{}:
					defer func() { <-semaphore }()
				case <-request.Context().Done():
					results[index].Error = &Error{Code: CodeExecution, Message: request.Context().Err().Error()}
					return
				}
				defer func() {
					if recovered := recover(); recovered != nil {
						results[index].Error = &Error{Code: CodeExecution, Message: fmt.Sprintf("batch item %d panic: %v", index, recovered)}
					}
				}()
				output, err := s.invoker.Invoke(
					request.Context(), item.Input,
					tracedGraphConfig(request.Context(), item.Config.graphConfig()),
				)
				if err != nil {
					results[index].Error = &Error{Code: CodeExecution, Message: err.Error()}
					return
				}
				results[index].Output = &output
			}()
		}
		workers.Wait()
	}
	writer.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(writer).Encode(batchResponse[O]{Results: results})
}

func (s *Server[I, O]) serveInvoke(writer http.ResponseWriter, request *http.Request) {
	request.Body = http.MaxBytesReader(writer, request.Body, s.options.MaxBodyBytes)
	decoder := json.NewDecoder(request.Body)
	decoder.DisallowUnknownFields()
	var payload invokeRequest[I]
	if err := decoder.Decode(&payload); err != nil {
		s.writeError(writer, http.StatusBadRequest, CodeInvalidRequest, err.Error())
		return
	}
	if err := ensureJSONEOF(decoder); err != nil {
		s.writeError(writer, http.StatusBadRequest, CodeInvalidRequest, err.Error())
		return
	}
	output, err := s.invoker.Invoke(request.Context(), payload.Input, tracedGraphConfig(request.Context(), payload.Config.graphConfig()))
	if err != nil {
		s.writeError(writer, http.StatusInternalServerError, CodeExecution, err.Error())
		return
	}
	writer.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(writer).Encode(invokeResponse[O]{Output: &output})
}

func (s *Server[I, O]) writeError(writer http.ResponseWriter, status int, code ErrorCode, message string) {
	writer.WriteHeader(status)
	_ = json.NewEncoder(writer).Encode(invokeResponse[O]{Error: &Error{Code: code, Message: message}})
}

// Client is a typed Go SDK for a remote Invoker.
type Client[I, O any] struct {
	baseURL string
	http    *http.Client
	headers http.Header
}

// ClientOptions controls headers shared by all SDK requests.
type ClientOptions struct {
	// Headers commonly carries authorization and caller-selected trace headers.
	Headers http.Header
}

// NewClient validates and constructs a typed remote client.
func NewClient[I, O any](baseURL string, client *http.Client) (*Client[I, O], error) {
	return NewClientWithOptions[I, O](baseURL, client, ClientOptions{})
}

// NewClientWithOptions validates and constructs a client with shared headers.
func NewClientWithOptions[I, O any](baseURL string, client *http.Client, options ClientOptions) (*Client[I, O], error) {
	parsed, err := url.Parse(baseURL)
	if err != nil || parsed.Host == "" || parsed.Scheme != "http" && parsed.Scheme != "https" {
		return nil, fmt.Errorf("invalid remote base URL %q", baseURL)
	}
	if client == nil {
		client = http.DefaultClient
	}
	return &Client[I, O]{baseURL: strings.TrimRight(baseURL, "/"), http: client, headers: options.Headers.Clone()}, nil
}

// Invoke calls the synchronous remote graph endpoint.
func (c *Client[I, O]) Invoke(ctx context.Context, input I, config RunConfig) (O, error) {
	var zero O
	payload, err := json.Marshal(invokeRequest[I]{Input: input, Config: config})
	if err != nil {
		return zero, fmt.Errorf("encode remote invoke request: %w", err)
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+InvokePath, bytes.NewReader(payload))
	if err != nil {
		return zero, err
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set(ProtocolHeader, ProtocolVersion)
	c.applyHeaders(request)
	response, err := c.http.Do(request)
	if err != nil {
		if ctx.Err() != nil {
			return zero, ctx.Err()
		}
		return zero, fmt.Errorf("remote invoke: %w", err)
	}
	defer response.Body.Close()
	if version := response.Header.Get(ProtocolHeader); version != ProtocolVersion {
		return zero, &Error{Code: CodeProtocol, Message: fmt.Sprintf("protocol version %q", version), Status: response.StatusCode}
	}
	decoder := json.NewDecoder(io.LimitReader(response.Body, 2<<20))
	var result invokeResponse[O]
	if err := decoder.Decode(&result); err != nil {
		return zero, &Error{Code: CodeProtocol, Message: err.Error(), Status: response.StatusCode}
	}
	if result.Error != nil {
		result.Error.Status = response.StatusCode
		return zero, result.Error
	}
	if response.StatusCode != http.StatusOK || result.Output == nil {
		return zero, &Error{Code: CodeProtocol, Message: fmt.Sprintf("unexpected HTTP status %d", response.StatusCode), Status: response.StatusCode}
	}
	return *result.Output, nil
}

// Batch invokes independent remote inputs and returns per-item results in
// input order. Execution errors remain attached to their item.
func (c *Client[I, O]) Batch(
	ctx context.Context,
	items []graph.BatchItem[I],
	options graph.BatchOptions,
) ([]graph.BatchResult[O], error) {
	if options.MaxConcurrency < 0 {
		return nil, fmt.Errorf("remote batch max concurrency cannot be negative")
	}
	wireItems := make([]batchItemRequest[I], len(items))
	for index, item := range items {
		wireItems[index] = batchItemRequest[I]{Input: item.Input, Config: runConfigFromGraph(item.Config)}
	}
	payload, err := json.Marshal(batchRequest[I]{Items: wireItems, MaxConcurrency: options.MaxConcurrency})
	if err != nil {
		return nil, fmt.Errorf("encode remote batch request: %w", err)
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+BatchPath, bytes.NewReader(payload))
	if err != nil {
		return nil, err
	}
	request.Header.Set("Content-Type", "application/json")
	c.applyHeaders(request)
	response, err := c.http.Do(request)
	if err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, fmt.Errorf("remote batch: %w", err)
	}
	defer response.Body.Close()
	if version := response.Header.Get(ProtocolHeader); version != ProtocolVersion {
		return nil, &Error{Code: CodeProtocol, Message: fmt.Sprintf("protocol version %q", version), Status: response.StatusCode}
	}
	decoder := json.NewDecoder(io.LimitReader(response.Body, 2<<20))
	var wire batchResponse[O]
	if err := decoder.Decode(&wire); err != nil {
		return nil, &Error{Code: CodeProtocol, Message: err.Error(), Status: response.StatusCode}
	}
	if response.StatusCode != http.StatusOK {
		if wire.Error != nil {
			wire.Error.Status = response.StatusCode
			return nil, wire.Error
		}
		return nil, &Error{Code: CodeProtocol, Message: fmt.Sprintf("unexpected HTTP status %d", response.StatusCode), Status: response.StatusCode}
	}
	if len(wire.Results) != len(items) {
		return nil, &Error{Code: CodeProtocol, Message: fmt.Sprintf("batch result count %d, want %d", len(wire.Results), len(items)), Status: response.StatusCode}
	}
	results := make([]graph.BatchResult[O], len(wire.Results))
	for index, item := range wire.Results {
		if (item.Output == nil) == (item.Error == nil) {
			return nil, &Error{Code: CodeProtocol, Message: fmt.Sprintf("batch result %d must contain exactly one of output/error", index), Status: response.StatusCode}
		}
		if item.Error != nil {
			item.Error.Status = response.StatusCode
			results[index].Err = item.Error
		} else {
			results[index].Output = *item.Output
		}
	}
	return results, nil
}

func runConfigFromGraph(config graph.RunConfig) RunConfig {
	return RunConfig{
		NewRun: config.NewRun, RecursionLimit: config.RecursionLimit, MaxConcurrency: config.MaxConcurrency,
		ThreadID: config.ThreadID, CheckpointNamespace: config.CheckpointNamespace,
		CheckpointID: config.CheckpointID, RunID: config.RunID, Metadata: cloneMetadata(config.Metadata),
	}
}

func (c *Client[I, O]) applyHeaders(request *http.Request) {
	for key, values := range c.headers {
		request.Header[key] = append([]string(nil), values...)
	}
	request.Header.Set(ProtocolHeader, ProtocolVersion)
}

func ensureJSONEOF(decoder *json.Decoder) error {
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		if err == nil {
			return fmt.Errorf("request contains multiple JSON values")
		}
		return err
	}
	return nil
}

func cloneMetadata(source map[string]any) map[string]any {
	if source == nil {
		return nil
	}
	result := make(map[string]any, len(source))
	for key, value := range source {
		result[key] = value
	}
	return result
}

func isNil(value any) bool {
	if value == nil {
		return true
	}
	reflected := reflect.ValueOf(value)
	switch reflected.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return reflected.IsNil()
	default:
		return false
	}
}
