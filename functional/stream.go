package functional

import (
	"context"
	"errors"
	"fmt"

	"github.com/wahanbo/langgraph-go/graph"
)

// StreamMode identifies a Functional stream event payload.
type StreamMode string

const (
	// StreamCustom carries a value emitted through Write.
	StreamCustom StreamMode = "custom"
	// StreamOutput carries the successful entrypoint result.
	StreamOutput StreamMode = "output"
	// StreamError carries a terminal workflow error.
	StreamError StreamMode = "error"
	// StreamDebug carries entrypoint/task lifecycle events.
	StreamDebug StreamMode = "debug"
	// StreamMessages carries explicitly emitted provider-neutral message chunks.
	StreamMessages StreamMode = "messages"
)

type DebugKind string

const (
	DebugEntrypointStart  DebugKind = "entrypoint_start"
	DebugEntrypointResult DebugKind = "entrypoint_result"
	DebugTaskStart        DebugKind = "task_start"
	DebugTaskResult       DebugKind = "task_result"
)

// DebugEvent identifies one Functional scheduler lifecycle boundary.
type DebugEvent struct {
	Kind       DebugKind
	Entrypoint string
	TaskName   string
	TaskID     string
	Err        error
}

// MessageEvent carries an application message/chunk plus trusted scheduler
// metadata added by the Functional runtime.
type MessageEvent struct {
	Message      any
	ContentBlock *graph.ContentBlockStreamEvent
	Metadata     map[string]any
}

// ProviderChunk is the detached result of adapting one provider SDK chunk.
// An adapter may emit a legacy message chunk, one or more content-block v2
// lifecycle events, or both. Provider extension fields remain open maps.
type ProviderChunk struct {
	Message       any
	ContentBlocks []graph.ContentBlockStreamEvent
	Metadata      map[string]any
}

// ProviderChunkAdapter converts a concrete provider SDK chunk without adding
// an SDK dependency to the Functional runtime.
type ProviderChunkAdapter[C any] interface {
	AdaptChunk(context.Context, C) (ProviderChunk, error)
}

// ProviderChunkAdapterFunc adapts an ordinary function to ProviderChunkAdapter.
type ProviderChunkAdapterFunc[C any] func(context.Context, C) (ProviderChunk, error)

func (adapter ProviderChunkAdapterFunc[C]) AdaptChunk(ctx context.Context, chunk C) (ProviderChunk, error) {
	return adapter(ctx, chunk)
}

type contentBlockEmission struct {
	event graph.ContentBlockStreamEvent
}

// StreamEvent is one typed Functional stream envelope.
type StreamEvent[O any] struct {
	Mode       StreamMode
	Custom     any
	Output     O
	Err        error
	Interrupts []Interrupt
	Debug      *DebugEvent
	Message    *MessageEvent
}

// StreamOptions controls Functional stream buffering.
type StreamOptions struct {
	// Buffer is the channel capacity. Zero is unbuffered; negative is invalid.
	Buffer int
	// Modes selects payloads. Empty preserves legacy custom/output/error only.
	Modes []StreamMode
}

// Write emits one custom value when called inside Stream. It is a no-op when
// the same workflow is executed through Invoke.
func Write(ctx context.Context, value any) error {
	Heartbeat(ctx)
	manager, ok := ctx.Value(runtimeContextKey{}).(*taskManager)
	if !ok || manager == nil || manager.emit == nil {
		return nil
	}
	return manager.emit(value)
}

type taskStreamMetadataKey struct{}

type taskStreamMetadata struct {
	name string
	id   string
}

// WriteMessage emits one explicit message/chunk. Framework task metadata
// overrides same-named caller keys. It is a no-op outside message streaming.
func WriteMessage(ctx context.Context, message any, metadata ...map[string]any) error {
	Heartbeat(ctx)
	if len(metadata) > 1 {
		return fmt.Errorf("functional message stream accepts at most one metadata map")
	}
	manager, ok := ctx.Value(runtimeContextKey{}).(*taskManager)
	if !ok || manager == nil || manager.emitMessage == nil {
		return nil
	}
	merged := make(map[string]any)
	if len(metadata) == 1 {
		for key, value := range metadata[0] {
			merged[key] = value
		}
	}
	if task, ok := ctx.Value(taskStreamMetadataKey{}).(taskStreamMetadata); ok {
		merged["task_name"] = task.name
		merged["task_id"] = task.id
	} else if manager.entrypoint != "" {
		merged["entrypoint"] = manager.entrypoint
	}
	return manager.emitMessage(message, merged)
}

// WriteContentBlock emits one validated provider-neutral v2 lifecycle event.
// Trusted task/entrypoint metadata follows the same precedence as WriteMessage.
func WriteContentBlock(ctx context.Context, event graph.ContentBlockStreamEvent, metadata ...map[string]any) error {
	if err := graph.ValidateContentBlockStreamEvent(event); err != nil {
		return err
	}
	return WriteMessage(ctx, contentBlockEmission{event: cloneFunctionalContentBlock(event)}, metadata...)
}

// WriteProviderChunk adapts and emits one concrete provider SDK chunk. Adapter
// metadata is merged first, optional call-site metadata overrides it, and
// scheduler-owned task/entrypoint keys retain final precedence.
func WriteProviderChunk[C any](
	ctx context.Context,
	chunk C,
	adapter ProviderChunkAdapter[C],
	metadata ...map[string]any,
) error {
	if adapter == nil {
		return fmt.Errorf("functional provider chunk adapter is nil")
	}
	if len(metadata) > 1 {
		return fmt.Errorf("functional provider chunk accepts at most one metadata map")
	}
	adapted, err := adapter.AdaptChunk(ctx, chunk)
	if err != nil {
		return fmt.Errorf("adapt functional provider chunk: %w", err)
	}
	merged := cloneFunctionalMetadata(adapted.Metadata)
	if merged == nil {
		merged = make(map[string]any)
	}
	if len(metadata) == 1 {
		for key, value := range metadata[0] {
			merged[key] = cloneFunctionalMetadataValue(value)
		}
	}
	if adapted.Message != nil {
		if err := WriteMessage(ctx, adapted.Message, merged); err != nil {
			return err
		}
	}
	for _, event := range adapted.ContentBlocks {
		if err := WriteContentBlock(ctx, event, merged); err != nil {
			return err
		}
	}
	return nil
}

// Stream runs the entrypoint and emits custom values followed by one output
// or error event. Cancel ctx when the consumer stops reading.
func (e *Entrypoint[I, O]) Stream(
	ctx context.Context,
	input I,
	options StreamOptions,
) <-chan StreamEvent[O] {
	return streamRun(ctx, options, func(emit func(any) error, emitDebug func(DebugEvent) error, emitMessage func(any, map[string]any) error) (O, error) {
		return e.invoke(ctx, input, emit, emitDebug, emitMessage)
	})
}

// Stream runs one durable invocation with the same checkpoint semantics as
// Invoke while exposing custom/output/error events.
func (e *DurableEntrypoint[I, O, S]) Stream(
	ctx context.Context,
	input I,
	runConfig DurableRunConfig,
	options StreamOptions,
) <-chan StreamEvent[O] {
	return streamRun(ctx, options, func(emit func(any) error, emitDebug func(DebugEvent) error, emitMessage func(any, map[string]any) error) (O, error) {
		return e.invoke(ctx, input, runConfig, emit, emitDebug, emitMessage)
	})
}

func streamRun[O any](
	ctx context.Context,
	options StreamOptions,
	run func(func(any) error, func(DebugEvent) error, func(any, map[string]any) error) (O, error),
) <-chan StreamEvent[O] {
	capacity := options.Buffer
	if capacity < 0 {
		events := make(chan StreamEvent[O], 1)
		events <- StreamEvent[O]{Mode: StreamError, Err: fmt.Errorf("functional stream buffer cannot be negative")}
		close(events)
		return events
	}
	events := make(chan StreamEvent[O], capacity)
	go func() {
		defer close(events)
		selected := make(map[StreamMode]struct{})
		if len(options.Modes) == 0 {
			selected[StreamCustom], selected[StreamOutput], selected[StreamError] = struct{}{}, struct{}{}, struct{}{}
		} else {
			for _, mode := range options.Modes {
				selected[mode] = struct{}{}
			}
			// Never hide terminal failures.
			selected[StreamError] = struct{}{}
		}
		send := func(event StreamEvent[O]) error {
			if _, ok := selected[event.Mode]; !ok {
				return nil
			}
			select {
			case events <- event:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		emit := func(value any) error {
			return send(StreamEvent[O]{Mode: StreamCustom, Custom: value})
		}
		emitDebug := func(value DebugEvent) error {
			copy := value
			return send(StreamEvent[O]{Mode: StreamDebug, Debug: &copy})
		}
		emitMessage := func(message any, metadata map[string]any) error {
			copied := cloneFunctionalMetadata(metadata)
			event := &MessageEvent{Metadata: copied}
			if block, ok := message.(contentBlockEmission); ok {
				cloned := cloneFunctionalContentBlock(block.event)
				event.ContentBlock = &cloned
			} else {
				event.Message = message
			}
			return send(StreamEvent[O]{Mode: StreamMessages, Message: event})
		}
		output, err := run(emit, emitDebug, emitMessage)
		if err != nil {
			event := StreamEvent[O]{Mode: StreamError, Err: err}
			var interruptErr *InterruptError
			if errors.As(err, &interruptErr) {
				event.Interrupts = append([]Interrupt(nil), interruptErr.Interrupts...)
			}
			_ = send(event)
			return
		}
		_ = send(StreamEvent[O]{Mode: StreamOutput, Output: output})
	}()
	return events
}

func cloneFunctionalContentBlock(source graph.ContentBlockStreamEvent) graph.ContentBlockStreamEvent {
	result := source
	result.ContentBlock = cloneFunctionalMetadata(source.ContentBlock)
	return result
}

func cloneFunctionalMetadata(source map[string]any) map[string]any {
	if source == nil {
		return nil
	}
	result := make(map[string]any, len(source))
	for key, value := range source {
		result[key] = cloneFunctionalMetadataValue(value)
	}
	return result
}

func cloneFunctionalMetadataValue(value any) any {
	switch typed := value.(type) {
	case map[string]any:
		return cloneFunctionalMetadata(typed)
	case []any:
		result := make([]any, len(typed))
		for index, item := range typed {
			result[index] = cloneFunctionalMetadataValue(item)
		}
		return result
	case []string:
		return append([]string(nil), typed...)
	case []byte:
		return append([]byte(nil), typed...)
	default:
		return value
	}
}
