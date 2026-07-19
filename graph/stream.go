package graph

import (
	"context"
	"errors"
	"sync"
)

// Stream executes the graph asynchronously and emits state/update events.
// Consumers should cancel ctx if they stop reading before the channel closes.
func (g *CompiledGraph[S, D]) Stream(
	ctx context.Context,
	input S,
	config RunConfig,
) <-chan StreamEvent[S, D] {
	return g.StreamWithOptions(ctx, input, config, StreamOptions{})
}

// StreamWithOptions executes the graph while filtering non-terminal events.
// Error and interrupt events are always delivered because suppressing them
// would make an incomplete run indistinguishable from successful completion.
func (g *CompiledGraph[S, D]) StreamWithOptions(
	ctx context.Context,
	input S,
	config RunConfig,
	options StreamOptions,
) <-chan StreamEvent[S, D] {
	return g.streamWithOptions(ctx, options, func(modes map[StreamMode]struct{}, emit eventEmitter[S, D]) (S, error) {
		config.StreamSubgraphs = config.StreamSubgraphs || options.Subgraphs
		config.streamModes = modes
		return g.run(ctx, input, config, emit)
	})
}

// ResumeStream stores resume values and streams continuation events using the
// default mode set. Consumers should cancel ctx if they stop reading early.
func (g *CompiledGraph[S, D]) ResumeStream(
	ctx context.Context,
	config RunConfig,
	command ResumeCommand,
) <-chan StreamEvent[S, D] {
	return g.ResumeStreamWithOptions(ctx, config, command, StreamOptions{})
}

// ResumeStreamWithOptions stores resume values and streams the replay with the
// same filtering, backpressure, cancellation, and terminal-event semantics as
// StreamWithOptions.
func (g *CompiledGraph[S, D]) ResumeStreamWithOptions(
	ctx context.Context,
	config RunConfig,
	command ResumeCommand,
	options StreamOptions,
) <-chan StreamEvent[S, D] {
	return g.streamWithOptions(ctx, options, func(modes map[StreamMode]struct{}, emit eventEmitter[S, D]) (S, error) {
		config.StreamSubgraphs = config.StreamSubgraphs || options.Subgraphs
		config.streamModes = modes
		return g.resumeInternal(ctx, config, command, emit, nil)
	})
}

// InvokeCommandStream streams a unified resume/update/routing invocation with
// the default mode set.
func (g *CompiledGraph[S, D]) InvokeCommandStream(
	ctx context.Context,
	command Command[D],
	config RunConfig,
) <-chan StreamEvent[S, D] {
	return g.InvokeCommandStreamWithOptions(ctx, command, config, StreamOptions{})
}

// InvokeCommandStreamWithOptions is the streaming counterpart of
// InvokeCommand and preserves the same validation-before-fork guarantee.
func (g *CompiledGraph[S, D]) InvokeCommandStreamWithOptions(
	ctx context.Context,
	command Command[D],
	config RunConfig,
	options StreamOptions,
) <-chan StreamEvent[S, D] {
	return g.streamWithOptions(ctx, options, func(modes map[StreamMode]struct{}, emit eventEmitter[S, D]) (S, error) {
		var zero S
		if command.Resume == nil {
			return zero, ErrInvalidResume
		}
		if command.Target != CommandCurrent {
			return zero, ErrInvalidResume
		}
		config.StreamSubgraphs = config.StreamSubgraphs || options.Subgraphs
		config.streamModes = modes
		resume := cloneResumeCommand(*command.Resume)
		return g.resumeInternal(ctx, config, resume, emit, &command)
	})
}

func (g *CompiledGraph[S, D]) streamWithOptions(
	ctx context.Context,
	options StreamOptions,
	execute func(map[StreamMode]struct{}, eventEmitter[S, D]) (S, error),
) <-chan StreamEvent[S, D] {
	buffer := options.Buffer
	if buffer == 0 {
		buffer = 1
	}
	if buffer < 0 {
		buffer = 1
	}
	events := make(chan StreamEvent[S, D], buffer)

	go func() {
		defer close(events)
		modes, modeErr := resolveStreamModes(options.Modes)
		if options.Buffer < 0 || modeErr != nil {
			if modeErr == nil {
				modeErr = ErrInvalidRunConfig
			}
			events <- StreamEvent[S, D]{Mode: StreamError, Step: -1, Err: modeErr}
			return
		}
		if ctx == nil {
			events <- StreamEvent[S, D]{
				Mode: StreamError,
				Step: -1,
				Err:  ErrInvalidRunConfig,
			}
			return
		}
		messageIDGenerator := options.MessageIDGenerator
		if messageIDGenerator == nil {
			messageIDGenerator = randomMessageID
		}
		seenMessages := make(map[string]struct{})
		var messagesMu sync.Mutex

		emit := func(event StreamEvent[S, D]) error {
			if _, enabled := modes[event.Mode]; !enabled {
				return nil
			}
			if event.Mode == StreamMessages && event.Message != nil {
				if event.Message.ContentBlock != nil {
					event.Message = cloneMessageStreamEvent(event.Message)
					select {
					case events <- event:
						return nil
					case <-ctx.Done():
						return ctx.Err()
					}
				}
				messagesMu.Lock()
				if event.Message.remember {
					if id := streamMessageID(event.Message.Message); id != "" {
						seenMessages[id] = struct{}{}
					}
					messagesMu.Unlock()
					return nil
				}
				normalized, err := normalizeStreamMessage(event.Message.Message, messageIDGenerator)
				if err != nil {
					messagesMu.Unlock()
					return err
				}
				event.Message = cloneMessageStreamEvent(event.Message)
				event.Message.Message = normalized
				if id := streamMessageID(event.Message.Message); id != "" {
					_, duplicate := seenMessages[id]
					if !event.Message.dedupe || !duplicate {
						seenMessages[id] = struct{}{}
					}
					if event.Message.dedupe && duplicate {
						messagesMu.Unlock()
						return nil
					}
				}
				messagesMu.Unlock()
			}
			select {
			case events <- event:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		}

		state, err := execute(modes, emit)
		if err == nil {
			return
		}
		var interruptErr *GraphInterruptError
		if errors.As(err, &interruptErr) {
			event := StreamEvent[S, D]{
				Mode:       StreamInterrupt,
				Step:       -1,
				State:      state,
				Interrupts: append([]Interrupt(nil), interruptErr.Interrupts...),
			}
			select {
			case events <- event:
			case <-ctx.Done():
			}
			return
		}

		event := StreamEvent[S, D]{
			Mode:  StreamError,
			Step:  -1,
			State: state,
			Err:   err,
		}
		select {
		case events <- event:
		case <-ctx.Done():
		}
	}()

	return events
}

func resolveStreamModes(requested []StreamMode) (map[StreamMode]struct{}, error) {
	implemented := []StreamMode{
		StreamValues, StreamUpdates, StreamCustom, StreamDone,
		StreamError, StreamInterrupt, StreamDebug, StreamMessages,
	}
	allowed := make(map[StreamMode]struct{}, len(implemented))
	for _, mode := range implemented {
		allowed[mode] = struct{}{}
	}
	if len(requested) == 0 {
		delete(allowed, StreamDebug)
		return allowed, nil
	}
	result := make(map[StreamMode]struct{}, len(requested))
	for _, mode := range requested {
		if _, exists := allowed[mode]; !exists {
			return nil, &StreamModeError{Mode: mode}
		}
		result[mode] = struct{}{}
	}
	return result, nil
}
