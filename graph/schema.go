package graph

import (
	"context"
	"errors"
	"fmt"
)

// SchemaAdapter maps a public input schema into internal graph state and maps
// final internal state into a public output schema. It is the explicit Go
// equivalent of StateGraph's separate input/state/output schemas.
type SchemaAdapter[I, S, O any] struct {
	Input  func(context.Context, I) (S, error)
	Output func(context.Context, S) (O, error)
}

// CompiledSchemaGraph is an immutable typed input/output facade over one
// CompiledGraph. Internal state remains available through Graph for durable
// inspection and administrative APIs.
type CompiledSchemaGraph[I, S, D, O any] struct {
	graph   *CompiledGraph[S, D]
	adapter SchemaAdapter[I, S, O]
}

// CompileSchemaGraph validates schema adapters and compiles the internal
// StateGraph using options.
func CompileSchemaGraph[I, S, D, O any](
	builder *StateGraph[S, D],
	adapter SchemaAdapter[I, S, O],
	options ...CompileOption[S, D],
) (*CompiledSchemaGraph[I, S, D, O], error) {
	if builder == nil {
		return nil, fmt.Errorf("%w: schema graph builder is nil", ErrInvalidGraph)
	}
	if adapter.Input == nil || adapter.Output == nil {
		return nil, fmt.Errorf("%w: schema graph requires input and output adapters", ErrInvalidGraph)
	}
	compiled, err := builder.Compile(options...)
	if err != nil {
		return nil, err
	}
	return &CompiledSchemaGraph[I, S, D, O]{graph: compiled, adapter: adapter}, nil
}

// Graph returns the compiled internal-state graph for persistence, state
// history, inspection, and advanced execution APIs.
func (g *CompiledSchemaGraph[I, S, D, O]) Graph() *CompiledGraph[S, D] {
	if g == nil {
		return nil
	}
	return g.graph
}

// Invoke maps public input, runs the internal graph, and maps successful or
// interrupted partial state to the public output schema.
func (g *CompiledSchemaGraph[I, S, D, O]) Invoke(
	ctx context.Context,
	input I,
	config RunConfig,
) (O, error) {
	var zero O
	if ctx == nil {
		return zero, fmt.Errorf("%w: context is nil", ErrInvalidRunConfig)
	}
	state, err := g.adapter.Input(ctx, input)
	if err != nil {
		return zero, schemaAdapterError("input", err)
	}
	internal, runErr := g.graph.Invoke(ctx, state, config)
	if runErr != nil && !errors.Is(runErr, ErrGraphInterrupt) {
		return zero, runErr
	}
	output, err := g.adapter.Output(ctx, internal)
	if err != nil {
		return zero, schemaAdapterError("output", err)
	}
	return output, runErr
}

// Resume continues a durable internal graph and maps its resulting state to
// the public output schema.
func (g *CompiledSchemaGraph[I, S, D, O]) Resume(
	ctx context.Context,
	config RunConfig,
	command ResumeCommand,
) (O, error) {
	var zero O
	if ctx == nil {
		return zero, fmt.Errorf("%w: context is nil", ErrInvalidRunConfig)
	}
	internal, runErr := g.graph.Resume(ctx, config, command)
	if runErr != nil && !errors.Is(runErr, ErrGraphInterrupt) {
		return zero, runErr
	}
	output, err := g.adapter.Output(ctx, internal)
	if err != nil {
		return zero, schemaAdapterError("output", err)
	}
	return output, runErr
}

// InvokeCommand is the schema-aware unified Command resume entrypoint.
func (g *CompiledSchemaGraph[I, S, D, O]) InvokeCommand(
	ctx context.Context,
	command Command[D],
	config RunConfig,
) (O, error) {
	var zero O
	internal, runErr := g.graph.InvokeCommand(ctx, command, config)
	if runErr != nil && !errors.Is(runErr, ErrGraphInterrupt) {
		return zero, runErr
	}
	output, err := g.adapter.Output(ctx, internal)
	if err != nil {
		return zero, schemaAdapterError("output", err)
	}
	return output, runErr
}

// Stream maps public input and projects state-bearing stream events to the
// public output schema.
func (g *CompiledSchemaGraph[I, S, D, O]) Stream(
	ctx context.Context,
	input I,
	config RunConfig,
) <-chan StreamEvent[O, D] {
	return g.StreamWithOptions(ctx, input, config, StreamOptions{})
}

// StreamWithOptions is the schema-aware form of CompiledGraph.StreamWithOptions.
func (g *CompiledSchemaGraph[I, S, D, O]) StreamWithOptions(
	ctx context.Context,
	input I,
	config RunConfig,
	options StreamOptions,
) <-chan StreamEvent[O, D] {
	if ctx == nil {
		return schemaTerminalEvent[O, D](fmt.Errorf("%w: context is nil", ErrInvalidRunConfig))
	}
	state, err := g.adapter.Input(ctx, input)
	if err != nil {
		return schemaTerminalEvent[O, D](schemaAdapterError("input", err))
	}
	streamCtx, cancel := context.WithCancel(ctx)
	return g.projectStream(streamCtx, cancel, g.graph.StreamWithOptions(streamCtx, state, config, options))
}

// ResumeStream maps state-bearing continuation events to the public output schema.
func (g *CompiledSchemaGraph[I, S, D, O]) ResumeStream(
	ctx context.Context,
	config RunConfig,
	command ResumeCommand,
) <-chan StreamEvent[O, D] {
	return g.ResumeStreamWithOptions(ctx, config, command, StreamOptions{})
}

// ResumeStreamWithOptions is the schema-aware continuation stream.
func (g *CompiledSchemaGraph[I, S, D, O]) ResumeStreamWithOptions(
	ctx context.Context,
	config RunConfig,
	command ResumeCommand,
	options StreamOptions,
) <-chan StreamEvent[O, D] {
	if ctx == nil {
		return schemaTerminalEvent[O, D](fmt.Errorf("%w: context is nil", ErrInvalidRunConfig))
	}
	streamCtx, cancel := context.WithCancel(ctx)
	return g.projectStream(streamCtx, cancel, g.graph.ResumeStreamWithOptions(streamCtx, config, command, options))
}

// InvokeCommandStream is the schema-aware mixed Command stream entrypoint.
func (g *CompiledSchemaGraph[I, S, D, O]) InvokeCommandStream(
	ctx context.Context,
	command Command[D],
	config RunConfig,
) <-chan StreamEvent[O, D] {
	return g.InvokeCommandStreamWithOptions(ctx, command, config, StreamOptions{})
}

// InvokeCommandStreamWithOptions projects state-bearing mixed Command events
// through the public output schema.
func (g *CompiledSchemaGraph[I, S, D, O]) InvokeCommandStreamWithOptions(
	ctx context.Context,
	command Command[D],
	config RunConfig,
	options StreamOptions,
) <-chan StreamEvent[O, D] {
	if ctx == nil {
		return schemaTerminalEvent[O, D](fmt.Errorf("%w: context is nil", ErrInvalidRunConfig))
	}
	streamCtx, cancel := context.WithCancel(ctx)
	return g.projectStream(streamCtx, cancel, g.graph.InvokeCommandStreamWithOptions(streamCtx, command, config, options))
}

func (g *CompiledSchemaGraph[I, S, D, O]) projectStream(
	ctx context.Context,
	cancel context.CancelFunc,
	source <-chan StreamEvent[S, D],
) <-chan StreamEvent[O, D] {
	events := make(chan StreamEvent[O, D], 1)
	go func() {
		defer cancel()
		defer close(events)
		for event := range source {
			projected := StreamEvent[O, D]{
				Mode: event.Mode, Step: event.Step, Updates: event.Updates,
				Interrupts: cloneInterrupts(event.Interrupts), Err: event.Err,
				Namespace: append([]string(nil), event.Namespace...), Subgraph: event.Subgraph,
				Custom: event.Custom, Debug: event.Debug, Message: cloneMessageStreamEvent(event.Message),
			}
			switch event.Mode {
			case StreamValues, StreamUpdates, StreamDone, StreamInterrupt:
				output, err := g.adapter.Output(ctx, event.State)
				if err != nil {
					projected = StreamEvent[O, D]{Mode: StreamError, Step: event.Step, Err: schemaAdapterError("output", err)}
				}
				projected.State = output
			}
			select {
			case events <- projected:
			case <-ctx.Done():
				return
			}
			if projected.Mode == StreamError && errors.Is(projected.Err, ErrSchemaAdapter) {
				return
			}
		}
	}()
	return events
}

func schemaTerminalEvent[S, D any](err error) <-chan StreamEvent[S, D] {
	events := make(chan StreamEvent[S, D], 1)
	events <- StreamEvent[S, D]{Mode: StreamError, Step: -1, Err: err}
	close(events)
	return events
}

func schemaAdapterError(direction string, err error) error {
	return fmt.Errorf("%w: %s: %w", ErrSchemaAdapter, direction, err)
}
