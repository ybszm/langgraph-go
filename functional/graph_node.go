package functional

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"runtime/debug"
	"sort"

	"github.com/wahanbo/langgraph-go/checkpoint"
	"github.com/wahanbo/langgraph-go/graph"
)

// NewNode wraps a graph node with a Functional task scope.
func NewNode[S, D any](
	name string,
	run graph.Node[S, D],
	options EntrypointOptions,
) (graph.Node[S, D], error) {
	if name == "" || run == nil {
		return nil, fmt.Errorf("%w: functional graph node requires name and function", ErrInvalidDefinition)
	}
	if options.MaxConcurrency < 0 {
		return nil, fmt.Errorf("%w: functional graph node max concurrency cannot be negative", ErrInvalidDefinition)
	}
	if err := validateTimeoutPolicy(options.Timeout); err != nil {
		return nil, fmt.Errorf("%w: functional graph node timeout: %v", ErrInvalidDefinition, err)
	}
	return func(ctx context.Context, state S, runtime graph.Runtime) (graph.Command[D], error) {
		executionCtx, finishTimeout := withTimeoutPolicy(ctx, options.Timeout)
		defer finishTimeout()
		for {
			manager, err := graphNodeTaskManager(executionCtx, runtime, options.MaxConcurrency)
			if err != nil {
				return graph.Command[D]{}, err
			}
			pending, err := pendingGraphNodeInterrupts(manager.durable)
			if err != nil {
				manager.cancel()
				return graph.Command[D]{}, err
			}
			if len(pending) > 0 {
				resumes, resumeErr := graph.AwaitResume[map[string]json.RawMessage](runtime, pending)
				if resumeErr != nil {
					manager.cancel()
					return graph.Command[D]{}, resumeErr
				}
				if err := persistGraphNodeResumes(ctx, manager.durable, resumes); err != nil {
					manager.cancel()
					return graph.Command[D]{}, err
				}
			}
			manager.emit = runtime.WriteCustom
			nodeCtx := context.WithValue(manager.ctx, runtimeContextKey{}, manager)
			nodeCtx = context.WithValue(nodeCtx, interruptScopeKey{}, &interruptScope{id: runtime.TaskID})
			command, nodeErr := invokeGraphNodeSafely(nodeCtx, state, runtime, run)
			var nodeInterrupted *InterruptError
			if nodeErr != nil && !errors.As(nodeErr, &nodeInterrupted) {
				manager.cancel()
			}
			manager.wait()
			manager.cancel()
			if timeout := timeoutCause(executionCtx); timeout != nil {
				nodeErr = timeout
			}
			if nodeErr != nil && nodeInterrupted == nil {
				return graph.Command[D]{}, nodeErr
			}
			taskErr := manager.err()
			var interrupted *InterruptError
			if !errors.As(taskErr, &interrupted) {
				if taskErr != nil {
					return graph.Command[D]{}, taskErr
				}
				return command, nil
			}
			resumes, err := graph.AwaitResume[map[string]json.RawMessage](runtime, interrupted.Interrupts)
			if err != nil {
				return graph.Command[D]{}, err
			}
			if err := persistGraphNodeResumes(ctx, manager.durable, resumes); err != nil {
				return graph.Command[D]{}, err
			}
		}
	}, nil
}

func pendingGraphNodeInterrupts(durable *durableTaskRuntime) ([]Interrupt, error) {
	if durable == nil {
		return nil, nil
	}
	pending := make([]Interrupt, 0)
	for _, write := range durable.recovered {
		if write.Channel != checkpoint.InterruptChannel || write.Value.Type != "functional_interrupt" {
			continue
		}
		stored, err := decodePersistedInterrupt(write)
		if err != nil {
			return nil, err
		}
		if len(stored.Resume) == 0 {
			pending = append(pending, Interrupt{ID: stored.ID, Value: append(json.RawMessage(nil), stored.Value...)})
		}
	}
	sort.Slice(pending, func(i, j int) bool { return pending[i].ID < pending[j].ID })
	return pending, nil
}

func graphNodeTaskManager(
	ctx context.Context,
	runtime graph.Runtime,
	maxConcurrency int,
) (*taskManager, error) {
	manager := newTaskManager(ctx, maxConcurrency)
	if runtime.Checkpointer == nil {
		return manager, nil
	}
	config := checkpoint.Config{
		ThreadID: runtime.ThreadID, Namespace: runtime.CheckpointNamespace, CheckpointID: runtime.CheckpointID,
	}
	tuple, found, err := runtime.Checkpointer.GetTuple(ctx, config)
	if err != nil {
		manager.cancel()
		return nil, fmt.Errorf("functional graph-node checkpoint get: %w", err)
	}
	if !found {
		manager.cancel()
		return nil, fmt.Errorf("%w: functional graph-node checkpoint %q", checkpoint.ErrNotFound, runtime.CheckpointID)
	}
	recovered := make(map[string]checkpoint.PendingWrite)
	for _, write := range tuple.PendingWrites {
		recovered[write.TaskID] = write
	}
	manager.durable = &durableTaskRuntime{
		saver: runtime.Checkpointer, config: tuple.Config, recovered: recovered,
	}
	return manager, nil
}

func persistGraphNodeResumes(
	ctx context.Context,
	durable *durableTaskRuntime,
	resumes map[string]json.RawMessage,
) error {
	if durable == nil {
		return ErrInterruptRequiresDurability
	}
	if len(resumes) == 0 {
		return fmt.Errorf("%w: graph-node resume map is empty", ErrInvalidResume)
	}
	writes := make([]checkpoint.PendingWrite, 0, len(resumes))
	for id, raw := range resumes {
		write, exists := durable.recovered[id]
		if !exists || write.Channel != checkpoint.InterruptChannel {
			return fmt.Errorf("%w: unknown graph-node functional interrupt ID %q", ErrInvalidResume, id)
		}
		stored, err := decodePersistedInterrupt(write)
		if err != nil {
			return err
		}
		stored.Resume = append(json.RawMessage(nil), raw...)
		write.Value, err = encodePersistedInterrupt(stored)
		if err != nil {
			return err
		}
		writes = append(writes, write)
		durable.recovered[id] = write
	}
	if err := durable.saver.PutWrites(ctx, durable.config, writes); err != nil {
		return fmt.Errorf("persist functional graph-node resume: %w", err)
	}
	return nil
}

func invokeGraphNodeSafely[S, D any](
	ctx context.Context,
	state S,
	runtime graph.Runtime,
	run graph.Node[S, D],
) (command graph.Command[D], err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = &EntrypointPanicError{Value: recovered, Stack: debug.Stack()}
		}
	}()
	return run(ctx, state, runtime)
}
