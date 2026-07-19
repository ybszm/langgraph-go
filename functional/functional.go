// Package functional provides typed task and entrypoint workflows.
package functional

import (
	"context"
	"errors"
	"fmt"
	"runtime/debug"
	"sort"
	"sync"
	"sync/atomic"
)

var (
	// ErrTaskOutsideEntrypoint indicates Task.Call was used without an active workflow.
	ErrTaskOutsideEntrypoint = errors.New("functional task called outside entrypoint")
	// ErrInvalidDefinition classifies invalid task or entrypoint construction.
	ErrInvalidDefinition = errors.New("invalid functional definition")
	// ErrIncompatibleRevision indicates persisted Functional state cannot be
	// safely executed by the currently configured workflow revision.
	ErrIncompatibleRevision = errors.New("incompatible functional revision")
)

// TaskPanicError converts a task panic into an ordinary error with a stack.
type TaskPanicError struct {
	Value any
	Stack []byte
}

func (e *TaskPanicError) Error() string { return fmt.Sprintf("functional task panic: %v", e.Value) }

// EntrypointPanicError converts an entrypoint panic into an ordinary error.
type EntrypointPanicError struct {
	Value any
	Stack []byte
}

func (e *EntrypointPanicError) Error() string {
	return fmt.Sprintf("functional entrypoint panic: %v", e.Value)
}

// TaskError adds stable task name and invocation ID context.
type TaskError struct {
	Name string
	ID   string
	Err  error
}

func (e *TaskError) Error() string {
	return fmt.Sprintf("functional task %q (%s) failed: %v", e.Name, e.ID, e.Err)
}

func (e *TaskError) Unwrap() error { return e.Err }

// EntrypointError adds workflow name context.
type EntrypointError struct {
	Name string
	Err  error
}

func (e *EntrypointError) Error() string {
	return fmt.Sprintf("functional entrypoint %q failed: %v", e.Name, e.Err)
}

func (e *EntrypointError) Unwrap() error { return e.Err }

type futureResult[T any] struct {
	value T
	err   error
}

// Future is the eventual result of one Task call.
type Future[T any] struct {
	result <-chan futureResult[T]
}

// Await waits for the task result or caller cancellation.
func (f *Future[T]) Await(ctx context.Context) (T, error) {
	if f == nil || f.result == nil {
		var zero T
		return zero, fmt.Errorf("nil functional future")
	}
	select {
	case result := <-f.result:
		return result.value, result.err
	default:
	}
	select {
	case result := <-f.result:
		return result.value, result.err
	case <-ctx.Done():
		select {
		case result := <-f.result:
			return result.value, result.err
		default:
			var zero T
			return zero, ctx.Err()
		}
	}
}

// Task is a typed unit of work callable only inside an Entrypoint.
type Task[I, O any] struct {
	name     string
	run      func(context.Context, I) (O, error)
	options  TaskOptions[I, O]
	cacheMu  sync.Mutex
	inflight map[string]*cacheFlight[O]
}

// NewTask validates and constructs a typed functional task.
func NewTask[I, O any](
	name string,
	run func(context.Context, I) (O, error),
	options ...TaskOptions[I, O],
) (*Task[I, O], error) {
	if name == "" {
		return nil, fmt.Errorf("%w: task name is empty", ErrInvalidDefinition)
	}
	if run == nil {
		return nil, fmt.Errorf("%w: task %q function is nil", ErrInvalidDefinition, name)
	}
	if len(options) > 1 {
		return nil, fmt.Errorf("%w: task %q accepts at most one options value", ErrInvalidDefinition, name)
	}
	configured := TaskOptions[I, O]{}
	if len(options) == 1 {
		configured = options[0]
	}
	configured, err := normalizeTaskOptions(configured)
	if err != nil {
		return nil, fmt.Errorf("%w: task %q options: %w", ErrInvalidDefinition, name, err)
	}
	return &Task[I, O]{name: name, run: run, options: configured, inflight: make(map[string]*cacheFlight[O])}, nil
}

// Name returns the task's stable logical name.
func (t *Task[I, O]) Name() string {
	if t == nil {
		return ""
	}
	return t.name
}

// Call schedules the task and immediately returns a Future.
func (t *Task[I, O]) Call(ctx context.Context, input I) *Future[O] {
	manager, ok := ctx.Value(runtimeContextKey{}).(*taskManager)
	if !ok || manager == nil {
		return completedFuture[O](zero[O](), ErrTaskOutsideEntrypoint)
	}
	if t == nil || t.run == nil {
		return completedFuture[O](zero[O](), fmt.Errorf("%w: nil task", ErrInvalidDefinition))
	}
	return submit(manager, ctx, t, input)
}

// EntrypointOptions controls task scheduling for one workflow definition.
type EntrypointOptions struct {
	// MaxConcurrency limits active tasks. Zero is unlimited.
	MaxConcurrency int
	// Timeout applies to the complete entrypoint execution.
	Timeout TimeoutPolicy
}

// Entrypoint is a typed invokable functional workflow.
type Entrypoint[I, O any] struct {
	name    string
	run     func(context.Context, I) (O, error)
	options EntrypointOptions
}

// NewEntrypoint validates and constructs a functional workflow.
func NewEntrypoint[I, O any](
	name string,
	run func(context.Context, I) (O, error),
	options EntrypointOptions,
) (*Entrypoint[I, O], error) {
	if name == "" {
		return nil, fmt.Errorf("%w: entrypoint name is empty", ErrInvalidDefinition)
	}
	if run == nil {
		return nil, fmt.Errorf("%w: entrypoint %q function is nil", ErrInvalidDefinition, name)
	}
	if options.MaxConcurrency < 0 {
		return nil, fmt.Errorf("%w: entrypoint max concurrency cannot be negative", ErrInvalidDefinition)
	}
	if err := validateTimeoutPolicy(options.Timeout); err != nil {
		return nil, fmt.Errorf("%w: entrypoint timeout: %v", ErrInvalidDefinition, err)
	}
	return &Entrypoint[I, O]{name: name, run: run, options: options}, nil
}

// Name returns the entrypoint's stable logical name.
func (e *Entrypoint[I, O]) Name() string {
	if e == nil {
		return ""
	}
	return e.name
}

// Invoke runs the entrypoint and waits for every scheduled task, including
// tasks whose futures the entrypoint did not explicitly await.
func (e *Entrypoint[I, O]) Invoke(ctx context.Context, input I) (output O, err error) {
	return e.invoke(ctx, input, nil, nil, nil)
}

func (e *Entrypoint[I, O]) invoke(
	ctx context.Context,
	input I,
	emit func(any) error,
	emitDebug func(DebugEvent) error,
	emitMessage func(any, map[string]any) error,
) (output O, err error) {
	if e == nil || e.run == nil {
		return output, fmt.Errorf("%w: nil entrypoint", ErrInvalidDefinition)
	}
	if err := ctx.Err(); err != nil {
		return output, err
	}
	if emitDebug != nil {
		if err := emitDebug(DebugEvent{Kind: DebugEntrypointStart, Entrypoint: e.name}); err != nil {
			return output, err
		}
		defer func() {
			if emitErr := emitDebug(DebugEvent{Kind: DebugEntrypointResult, Entrypoint: e.name, Err: err}); err == nil && emitErr != nil {
				err = emitErr
			}
		}()
	}
	executionCtx, finishTimeout := withTimeoutPolicy(ctx, e.options.Timeout)
	defer finishTimeout()
	manager := newTaskManager(executionCtx, e.options.MaxConcurrency)
	manager.emit = emit
	manager.emitDebug = emitDebug
	manager.emitMessage = emitMessage
	manager.entrypoint = e.name
	entryCtx := context.WithValue(manager.ctx, runtimeContextKey{}, manager)
	output, entryErr := invokeEntrypointSafely(entryCtx, input, e.run)
	if entryErr != nil {
		manager.cancel()
	}
	manager.wait()
	manager.cancel()
	if timeout := timeoutCause(executionCtx); timeout != nil {
		entryErr = timeout
	}
	if entryErr != nil {
		return output, &EntrypointError{Name: e.name, Err: entryErr}
	}
	if taskErr := manager.err(); taskErr != nil {
		return output, &EntrypointError{Name: e.name, Err: taskErr}
	}
	return output, nil
}

type runtimeContextKey struct{}

type taskManager struct {
	ctx         context.Context
	cancel      context.CancelFunc
	semaphore   chan struct{}
	wg          sync.WaitGroup
	nextID      atomic.Uint64
	errMu       sync.Mutex
	firstErr    error
	interrupts  map[string]Interrupt
	durable     *durableTaskRuntime
	emit        func(any) error
	emitDebug   func(DebugEvent) error
	emitMessage func(any, map[string]any) error
	entrypoint  string
}

func newTaskManager(ctx context.Context, maxConcurrency int) *taskManager {
	managerCtx, cancel := context.WithCancel(ctx)
	manager := &taskManager{ctx: managerCtx, cancel: cancel, interrupts: make(map[string]Interrupt)}
	if maxConcurrency > 0 {
		manager.semaphore = make(chan struct{}, maxConcurrency)
	}
	return manager
}

func (m *taskManager) fail(err error) {
	if err == nil {
		return
	}
	var interrupted *InterruptError
	if errors.As(err, &interrupted) {
		m.errMu.Lock()
		for _, item := range interrupted.Interrupts {
			m.interrupts[item.ID] = Interrupt{ID: item.ID, Value: append([]byte(nil), item.Value...)}
		}
		m.errMu.Unlock()
		// An interrupt is task control flow. Let sibling tasks reach their own
		// durable boundaries so the caller receives one complete resume set.
		return
	}
	m.errMu.Lock()
	if m.firstErr == nil {
		m.firstErr = err
	}
	m.errMu.Unlock()
	m.cancel()
}

func (m *taskManager) err() error {
	m.errMu.Lock()
	defer m.errMu.Unlock()
	if m.firstErr != nil {
		return m.firstErr
	}
	if len(m.interrupts) == 0 {
		return nil
	}
	ids := make([]string, 0, len(m.interrupts))
	for id := range m.interrupts {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	interrupts := make([]Interrupt, 0, len(ids))
	for _, id := range ids {
		item := m.interrupts[id]
		item.Value = append([]byte(nil), item.Value...)
		interrupts = append(interrupts, item)
	}
	return &InterruptError{Interrupts: interrupts}
}

func (m *taskManager) wait() { m.wg.Wait() }

func submit[I, O any](manager *taskManager, ctx context.Context, task *Task[I, O], input I) *Future[O] {
	result := make(chan futureResult[O], 1)
	id := fmt.Sprintf("task:%s:%d", task.name, manager.nextID.Add(1)-1)
	manager.wg.Add(1)
	go func() {
		defer manager.wg.Done()
		if manager.semaphore != nil {
			select {
			case manager.semaphore <- struct{}{}:
				defer func() { <-manager.semaphore }()
			case <-ctx.Done():
				err := &TaskError{Name: task.name, ID: id, Err: ctx.Err()}
				result <- futureResult[O]{err: err}
				manager.fail(err)
				return
			}
		}
		taskCtx := context.WithValue(ctx, interruptScopeKey{}, &interruptScope{id: id})
		taskCtx = context.WithValue(taskCtx, taskStreamMetadataKey{}, taskStreamMetadata{name: task.name, id: id})
		if manager.emitDebug != nil {
			if emitErr := manager.emitDebug(DebugEvent{Kind: DebugTaskStart, TaskName: task.name, TaskID: id}); emitErr != nil {
				err := &TaskError{Name: task.name, ID: id, Err: emitErr}
				result <- futureResult[O]{err: err}
				manager.fail(err)
				return
			}
		}
		value, recovered, err := recoverTaskResult(manager, task, id, input)
		if err == nil && !recovered {
			value, err = task.execute(taskCtx, input)
			if err == nil {
				err = persistTaskResult(manager, task, id, input, value)
			}
		}
		if err != nil {
			err = &TaskError{Name: task.name, ID: id, Err: err}
		}
		if manager.emitDebug != nil {
			if emitErr := manager.emitDebug(DebugEvent{Kind: DebugTaskResult, TaskName: task.name, TaskID: id, Err: err}); err == nil && emitErr != nil {
				err = &TaskError{Name: task.name, ID: id, Err: emitErr}
			}
		}
		result <- futureResult[O]{value: value, err: err}
		manager.fail(err)
	}()
	return &Future[O]{result: result}
}

func completedFuture[T any](value T, err error) *Future[T] {
	result := make(chan futureResult[T], 1)
	result <- futureResult[T]{value: value, err: err}
	return &Future[T]{result: result}
}

func invokeTaskSafely[I, O any](
	ctx context.Context,
	input I,
	run func(context.Context, I) (O, error),
) (output O, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = &TaskPanicError{Value: recovered, Stack: debug.Stack()}
		}
	}()
	return run(ctx, input)
}

func invokeEntrypointSafely[I, O any](
	ctx context.Context,
	input I,
	run func(context.Context, I) (O, error),
) (output O, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = &EntrypointPanicError{Value: recovered, Stack: debug.Stack()}
		}
	}()
	return run(ctx, input)
}

func zero[T any]() (value T) { return value }
