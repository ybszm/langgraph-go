package graph_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	cachememory "github.com/wahanbo/langgraph-go/cache/memory"
	"github.com/wahanbo/langgraph-go/checkpoint"
	checkpointmemory "github.com/wahanbo/langgraph-go/checkpoint/memory"
	"github.com/wahanbo/langgraph-go/graph"
)

func TestNodeErrorHandlerRunsAfterRetryAndStopsFailedBranch(t *testing.T) {
	want := errors.New("node failed")
	var attempts atomic.Int32
	var successorCalls atomic.Int32
	var got graph.NodeError
	var handlerRuntime graph.Runtime

	builder := graph.NewStateGraph(testReducer)
	err := builder.AddNode("failing", func(context.Context, testState, graph.Runtime) (graph.Command[testDelta], error) {
		attempts.Add(1)
		return graph.NoCommand[testDelta](), want
	}, graph.WithRetryPolicies(graph.RetryPolicy{
		InitialInterval: time.Nanosecond,
		MaxAttempts:     2,
		RetryOn:         func(err error) bool { return errors.Is(err, want) },
	}), graph.WithErrorHandler[testState, testDelta](func(
		_ context.Context,
		state testState,
		failure graph.NodeError,
		runtime graph.Runtime,
	) (graph.Command[testDelta], error) {
		if state.Total != 3 {
			t.Fatalf("handler state=%+v", state)
		}
		got = failure
		handlerRuntime = runtime
		return graph.Update(testDelta{Add: 7, Label: "handled"}), nil
	}))
	if err != nil {
		t.Fatal(err)
	}
	addNode(t, builder, "successor", func(context.Context, testState, graph.Runtime) (graph.Command[testDelta], error) {
		successorCalls.Add(1)
		return graph.Update(testDelta{Add: 100}), nil
	})
	addEdge(t, builder, graph.START, "failing")
	addEdge(t, builder, "failing", "successor")
	addEdge(t, builder, "successor", graph.END)

	result, err := compileGraph(t, builder).Invoke(context.Background(), testState{Total: 3}, graph.RunConfig{})
	if err != nil {
		t.Fatal(err)
	}
	if attempts.Load() != 2 || successorCalls.Load() != 0 {
		t.Fatalf("attempts=%d successor calls=%d", attempts.Load(), successorCalls.Load())
	}
	if result.Total != 10 || len(result.Path) != 1 || result.Path[0] != "handled" {
		t.Fatalf("result=%+v", result)
	}
	if got.Node != "failing" || !errors.Is(got.Err, want) {
		t.Fatalf("NodeError=%+v", got)
	}
	if handlerRuntime.Node != "__error_handler__failing" || handlerRuntime.Attempt != 1 {
		t.Fatalf("handler runtime=%+v", handlerRuntime)
	}
}

type failNthPutSaver struct {
	checkpoint.Saver
	mu      sync.Mutex
	putCall int
	failAt  int
	err     error
}

type failAfterNodeErrorWriteSaver struct {
	checkpoint.Saver
	failed atomic.Bool
	err    error
}

type identityErrorCodec struct{ err error }

func (c identityErrorCodec) Encode(err error) (checkpoint.EncodedValue, error) {
	if !errors.Is(err, c.err) {
		return checkpoint.EncodedValue{}, errors.New("unexpected error identity")
	}
	return checkpoint.EncodedValue{Type: "tests/identity-error", Version: 1, Data: []byte(err.Error())}, nil
}

func (c identityErrorCodec) Decode(value checkpoint.EncodedValue) (error, error) {
	if value.Type != "tests/identity-error" || value.Version != 1 || len(value.Data) == 0 {
		return nil, checkpoint.ErrCodecMismatch
	}
	return c.err, nil
}

func (s *failAfterNodeErrorWriteSaver) PutWrites(
	ctx context.Context,
	config checkpoint.Config,
	writes []checkpoint.PendingWrite,
) error {
	if err := s.Saver.PutWrites(ctx, config, writes); err != nil {
		return err
	}
	for _, write := range writes {
		if write.Channel == checkpoint.NodeErrorChannel && s.failed.CompareAndSwap(false, true) {
			return s.err
		}
	}
	return nil
}

func (s *failNthPutSaver) Put(
	ctx context.Context,
	parent checkpoint.Config,
	value checkpoint.Checkpoint,
	metadata checkpoint.Metadata,
	versions map[string]string,
) (checkpoint.Config, error) {
	s.mu.Lock()
	s.putCall++
	fail := s.putCall == s.failAt
	s.mu.Unlock()
	if fail {
		return checkpoint.Config{}, s.err
	}
	return s.Saver.Put(ctx, parent, value, metadata, versions)
}

func TestDefaultNodeErrorHandlerIsLateBoundAndExplicitWins(t *testing.T) {
	failed := errors.New("failed")
	var defaultCalls atomic.Int32
	var explicitCalls atomic.Int32
	builder := graph.NewStateGraph(testReducer)
	if err := builder.AddNode("explicit", func(context.Context, testState, graph.Runtime) (graph.Command[testDelta], error) {
		return graph.NoCommand[testDelta](), failed
	}, graph.WithErrorHandler[testState, testDelta](func(context.Context, testState, graph.NodeError, graph.Runtime) (graph.Command[testDelta], error) {
		explicitCalls.Add(1)
		return graph.Update(testDelta{Add: 2}), nil
	})); err != nil {
		t.Fatal(err)
	}
	addEdge(t, builder, graph.START, "explicit")
	addEdge(t, builder, "explicit", graph.END)
	if err := builder.SetDefaultErrorHandler(func(context.Context, testState, graph.NodeError, graph.Runtime) (graph.Command[testDelta], error) {
		defaultCalls.Add(1)
		return graph.Update(testDelta{Add: 100}), nil
	}); err != nil {
		t.Fatal(err)
	}
	result, err := compileGraph(t, builder).Invoke(context.Background(), testState{}, graph.RunConfig{})
	if err != nil {
		t.Fatal(err)
	}
	if result.Total != 2 || explicitCalls.Load() != 1 || defaultCalls.Load() != 0 {
		t.Fatalf("result=%+v explicit=%d default=%d", result, explicitCalls.Load(), defaultCalls.Load())
	}
}

func TestDefaultNodeErrorHandlerDoesNotHandleItself(t *testing.T) {
	sourceErr := errors.New("source")
	handlerErr := errors.New("handler")
	var calls atomic.Int32
	builder := graph.NewStateGraph(testReducer)
	addNode(t, builder, "failing", func(context.Context, testState, graph.Runtime) (graph.Command[testDelta], error) {
		return graph.NoCommand[testDelta](), sourceErr
	})
	addEdge(t, builder, graph.START, "failing")
	addEdge(t, builder, "failing", graph.END)
	if err := builder.SetDefaultErrorHandler(func(context.Context, testState, graph.NodeError, graph.Runtime) (graph.Command[testDelta], error) {
		calls.Add(1)
		return graph.NoCommand[testDelta](), handlerErr
	}); err != nil {
		t.Fatal(err)
	}
	_, err := compileGraph(t, builder).Invoke(context.Background(), testState{}, graph.RunConfig{})
	if !errors.Is(err, handlerErr) || calls.Load() != 1 {
		t.Fatalf("error=%v handler calls=%d", err, calls.Load())
	}
	var execution *graph.NodeExecutionError
	if !errors.As(err, &execution) || execution.Node != "__default_error_handler__" {
		t.Fatalf("execution error=%+v", execution)
	}
}

func TestErrorHandlerInheritsDefaultRetryAndBypassesCache(t *testing.T) {
	sourceErr := errors.New("source")
	transient := errors.New("handler transient")
	var sourceCalls atomic.Int32
	var handlerCalls atomic.Int32
	builder := graph.NewStateGraph(testReducer)
	if err := builder.AddNode("failing", func(context.Context, testState, graph.Runtime) (graph.Command[testDelta], error) {
		sourceCalls.Add(1)
		return graph.NoCommand[testDelta](), sourceErr
	}, graph.WithRetryPolicies(graph.RetryPolicy{
		MaxAttempts: 1,
		RetryOn:     func(error) bool { return false },
	})); err != nil {
		t.Fatal(err)
	}
	addEdge(t, builder, graph.START, "failing")
	addEdge(t, builder, "failing", graph.END)
	if err := builder.SetDefaultErrorHandler(func(context.Context, testState, graph.NodeError, graph.Runtime) (graph.Command[testDelta], error) {
		if handlerCalls.Add(1)%2 == 1 {
			return graph.NoCommand[testDelta](), transient
		}
		return graph.Update(testDelta{Add: 1}), nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := builder.SetDefaultRetryPolicies(graph.RetryPolicy{
		InitialInterval: time.Nanosecond,
		MaxAttempts:     2,
		RetryOn:         func(err error) bool { return errors.Is(err, transient) },
	}); err != nil {
		t.Fatal(err)
	}
	if err := builder.SetDefaultCachePolicy(graph.CachePolicy[testState]{}); err != nil {
		t.Fatal(err)
	}
	compiled, err := builder.Compile(graph.WithTaskCache[testState, testDelta](graph.TaskCacheConfig[testDelta]{
		Store: cachememory.New(), Namespace: "error-handler", DeltaCodec: checkpoint.MustJSONCodec[testDelta]("tests.error-handler", 1),
	}))
	if err != nil {
		t.Fatal(err)
	}
	for range 2 {
		result, err := compiled.Invoke(context.Background(), testState{}, graph.RunConfig{})
		if err != nil || result.Total != 1 {
			t.Fatalf("result=%+v error=%v", result, err)
		}
	}
	if sourceCalls.Load() != 2 || handlerCalls.Load() != 4 {
		t.Fatalf("source calls=%d handler calls=%d", sourceCalls.Load(), handlerCalls.Load())
	}
}

func TestErrorHandlerPendingResultRecoversWithoutRerunningEitherNode(t *testing.T) {
	sourceErr := errors.New("source")
	commitErr := errors.New("commit failed")
	baseSaver := checkpointmemory.NewSaver()
	saver := &failNthPutSaver{Saver: baseSaver, failAt: 2, err: commitErr}
	var sourceCalls atomic.Int32
	var handlerCalls atomic.Int32
	builder := graph.NewStateGraph(testReducer)
	if err := builder.AddNode("failing", func(context.Context, testState, graph.Runtime) (graph.Command[testDelta], error) {
		sourceCalls.Add(1)
		return graph.NoCommand[testDelta](), sourceErr
	}, graph.WithErrorHandler[testState, testDelta](func(context.Context, testState, graph.NodeError, graph.Runtime) (graph.Command[testDelta], error) {
		handlerCalls.Add(1)
		return graph.Update(testDelta{Add: 5, Label: "recovered"}), nil
	})); err != nil {
		t.Fatal(err)
	}
	addEdge(t, builder, graph.START, "failing")
	addEdge(t, builder, "failing", graph.END)
	compiled := compilePersistentGraph(t, builder, saver)
	config := graph.RunConfig{ThreadID: "error-handler-recovery"}
	if _, err := compiled.Invoke(context.Background(), testState{}, config); !errors.Is(err, commitErr) {
		t.Fatalf("first error=%v", err)
	}
	result, err := compiled.Invoke(context.Background(), testState{Total: 100}, config)
	if err != nil {
		t.Fatal(err)
	}
	if result.Total != 5 || len(result.Path) != 1 || result.Path[0] != "recovered" {
		t.Fatalf("result=%+v", result)
	}
	if sourceCalls.Load() != 1 || handlerCalls.Load() != 1 {
		t.Fatalf("source calls=%d handler calls=%d", sourceCalls.Load(), handlerCalls.Load())
	}
}

func TestErrorHandlerUsesDistinctDebugAndUpdateIdentity(t *testing.T) {
	builder := graph.NewStateGraph(testReducer)
	if err := builder.AddNode("failing", func(context.Context, testState, graph.Runtime) (graph.Command[testDelta], error) {
		return graph.NoCommand[testDelta](), errors.New("boom")
	}, graph.WithErrorHandler[testState, testDelta](func(context.Context, testState, graph.NodeError, graph.Runtime) (graph.Command[testDelta], error) {
		return graph.Update(testDelta{Add: 1}), nil
	})); err != nil {
		t.Fatal(err)
	}
	addEdge(t, builder, graph.START, "failing")
	addEdge(t, builder, "failing", graph.END)
	compiled := compileGraph(t, builder)
	var debugKinds []string
	var updateNode graph.NodeID
	for event := range compiled.StreamWithOptions(context.Background(), testState{}, graph.RunConfig{}, graph.StreamOptions{
		Modes: []graph.StreamMode{graph.StreamDebug, graph.StreamUpdates}, Buffer: 16,
	}) {
		if event.Err != nil {
			t.Fatal(event.Err)
		}
		if event.Debug != nil && (event.Debug.Kind == graph.DebugTask || event.Debug.Kind == graph.DebugTaskResult) {
			kind := string(event.Debug.Kind) + ":" + string(event.Debug.Node)
			if event.Debug.Err != nil {
				kind += ":error"
			}
			debugKinds = append(debugKinds, kind)
		}
		for _, update := range event.Updates {
			updateNode = update.Node
		}
	}
	want := []string{
		"task:failing",
		"task_result:failing:error",
		"task:__error_handler__failing",
		"task_result:__error_handler__failing",
	}
	if len(debugKinds) != len(want) {
		t.Fatalf("debug lifecycle=%v", debugKinds)
	}
	for index := range want {
		if debugKinds[index] != want[index] {
			t.Fatalf("debug lifecycle=%v", debugKinds)
		}
	}
	if updateNode != "__error_handler__failing" {
		t.Fatalf("update node=%q", updateNode)
	}
}

func TestErrorHandlerInheritsDefaultTimeout(t *testing.T) {
	builder := graph.NewStateGraph(testReducer)
	addNode(t, builder, "failing", func(context.Context, testState, graph.Runtime) (graph.Command[testDelta], error) {
		return graph.NoCommand[testDelta](), errors.New("source")
	})
	addEdge(t, builder, graph.START, "failing")
	addEdge(t, builder, "failing", graph.END)
	if err := builder.SetDefaultErrorHandler(func(ctx context.Context, _ testState, _ graph.NodeError, _ graph.Runtime) (graph.Command[testDelta], error) {
		<-ctx.Done()
		return graph.NoCommand[testDelta](), ctx.Err()
	}); err != nil {
		t.Fatal(err)
	}
	if err := builder.SetDefaultNodeTimeout(graph.NodeTimeoutPolicy{RunTimeout: 5 * time.Millisecond}); err != nil {
		t.Fatal(err)
	}
	_, err := compileGraph(t, builder).Invoke(context.Background(), testState{}, graph.RunConfig{})
	if !errors.Is(err, graph.ErrNodeTimeout) {
		t.Fatalf("error=%v", err)
	}
	var execution *graph.NodeExecutionError
	if !errors.As(err, &execution) || execution.Node != "__default_error_handler__" {
		t.Fatalf("execution error=%+v", execution)
	}
}

func TestErrorHandlerDefinitionValidation(t *testing.T) {
	t.Run("nil", func(t *testing.T) {
		builder := graph.NewStateGraph(testReducer)
		err := builder.AddNode("node", func(context.Context, testState, graph.Runtime) (graph.Command[testDelta], error) {
			return graph.NoCommand[testDelta](), nil
		}, graph.WithErrorHandler[testState, testDelta](nil))
		if !errors.Is(err, graph.ErrInvalidGraph) {
			t.Fatalf("error=%v", err)
		}
		if err := builder.SetDefaultErrorHandler(nil); !errors.Is(err, graph.ErrInvalidGraph) {
			t.Fatalf("default error=%v", err)
		}
	})

	t.Run("generated node collision", func(t *testing.T) {
		builder := graph.NewStateGraph(testReducer)
		if err := builder.AddNode("failing", func(context.Context, testState, graph.Runtime) (graph.Command[testDelta], error) {
			return graph.NoCommand[testDelta](), errors.New("source")
		}, graph.WithErrorHandler[testState, testDelta](func(context.Context, testState, graph.NodeError, graph.Runtime) (graph.Command[testDelta], error) {
			return graph.NoCommand[testDelta](), nil
		})); err != nil {
			t.Fatal(err)
		}
		addNode(t, builder, "__error_handler__failing", func(context.Context, testState, graph.Runtime) (graph.Command[testDelta], error) {
			return graph.NoCommand[testDelta](), nil
		})
		addEdge(t, builder, graph.START, "failing")
		addEdge(t, builder, graph.START, "__error_handler__failing")
		addEdge(t, builder, "failing", graph.END)
		addEdge(t, builder, "__error_handler__failing", graph.END)
		if _, err := builder.Compile(); !errors.Is(err, graph.ErrInvalidGraph) {
			t.Fatalf("Compile error=%v", err)
		}
	})
}

func TestDurableNodeErrorMarkerResumesHandlerWithoutRerunningSource(t *testing.T) {
	sourceErr := errors.New("source exploded")
	crashErr := errors.New("simulated crash after node-error write")
	saver := &failAfterNodeErrorWriteSaver{Saver: checkpointmemory.NewSaver(), err: crashErr}
	var sourceCalls atomic.Int32
	var handlerCalls atomic.Int32
	var recoveredFailure graph.NodeError
	builder := graph.NewStateGraph(testReducer)
	if err := builder.AddNode("failing", func(context.Context, testState, graph.Runtime) (graph.Command[testDelta], error) {
		sourceCalls.Add(1)
		return graph.NoCommand[testDelta](), sourceErr
	}, graph.WithErrorHandler[testState, testDelta](func(_ context.Context, _ testState, failure graph.NodeError, _ graph.Runtime) (graph.Command[testDelta], error) {
		handlerCalls.Add(1)
		recoveredFailure = failure
		return graph.Update(testDelta{Add: 9, Label: "durable-handler"}), nil
	})); err != nil {
		t.Fatal(err)
	}
	addEdge(t, builder, graph.START, "failing")
	addEdge(t, builder, "failing", graph.END)
	compiled := compilePersistentGraph(t, builder, saver)
	config := graph.RunConfig{ThreadID: "durable-error-handler"}
	if _, err := compiled.Invoke(context.Background(), testState{}, config); !errors.Is(err, crashErr) {
		t.Fatalf("first error=%v", err)
	}
	if sourceCalls.Load() != 1 || handlerCalls.Load() != 0 {
		t.Fatalf("after crash source=%d handler=%d", sourceCalls.Load(), handlerCalls.Load())
	}
	result, err := compiled.Invoke(context.Background(), testState{Total: 100}, config)
	if err != nil {
		t.Fatal(err)
	}
	if result.Total != 9 || len(result.Path) != 1 || result.Path[0] != "durable-handler" {
		t.Fatalf("result=%+v", result)
	}
	if sourceCalls.Load() != 1 || handlerCalls.Load() != 1 {
		t.Fatalf("source=%d handler=%d", sourceCalls.Load(), handlerCalls.Load())
	}
	if recoveredFailure.Node != "failing" || recoveredFailure.Err == nil || recoveredFailure.Err.Error() == "" {
		t.Fatalf("recovered failure=%+v", recoveredFailure)
	}
	var recovered *graph.RecoveredNodeError
	if !errors.As(recoveredFailure.Err, &recovered) || recovered.Message == "" {
		t.Fatalf("recovered error=%T %v", recoveredFailure.Err, recoveredFailure.Err)
	}
}

func TestDurableNodeErrorUsesConfiguredErrorCodec(t *testing.T) {
	sourceErr := errors.New("typed source failure")
	crashErr := errors.New("crash")
	saver := &failAfterNodeErrorWriteSaver{Saver: checkpointmemory.NewSaver(), err: crashErr}
	var captured error
	builder := graph.NewStateGraph(testReducer)
	if err := builder.AddNode("failing", func(context.Context, testState, graph.Runtime) (graph.Command[testDelta], error) {
		return graph.NoCommand[testDelta](), sourceErr
	}, graph.WithErrorHandler[testState, testDelta](func(_ context.Context, _ testState, failure graph.NodeError, _ graph.Runtime) (graph.Command[testDelta], error) {
		captured = failure.Err
		return graph.Update(testDelta{Add: 1}), nil
	})); err != nil {
		t.Fatal(err)
	}
	addEdge(t, builder, graph.START, "failing")
	addEdge(t, builder, "failing", graph.END)
	compiled, err := builder.Compile(graph.WithPersistence(graph.PersistenceConfig[testState, testDelta]{
		Saver:       saver,
		StateCodec:  checkpoint.MustJSONCodec[testState]("tests/error-codec-state", 1),
		DeltaCodec:  checkpoint.MustJSONCodec[testDelta]("tests/error-codec-delta", 1),
		ErrorCodec:  identityErrorCodec{err: sourceErr},
		Clock:       checkpoint.ClockFunc(func() time.Time { return time.Unix(1_700_000_000, 0) }),
		IDGenerator: &sequenceIDGenerator{},
	}))
	if err != nil {
		t.Fatal(err)
	}
	config := graph.RunConfig{ThreadID: "custom-error-codec"}
	if _, err := compiled.Invoke(context.Background(), testState{}, config); !errors.Is(err, crashErr) {
		t.Fatalf("first error=%v", err)
	}
	if _, err := compiled.Invoke(context.Background(), testState{}, config); err != nil {
		t.Fatal(err)
	}
	if !errors.Is(captured, sourceErr) {
		t.Fatalf("captured error=%T %v", captured, captured)
	}
}

func TestDurableErrorHandlerInterruptResumesWithoutSourceRerun(t *testing.T) {
	var sourceCalls atomic.Int32
	var handlerCalls atomic.Int32
	builder := graph.NewStateGraph(testReducer)
	if err := builder.AddNode("failing", func(context.Context, testState, graph.Runtime) (graph.Command[testDelta], error) {
		sourceCalls.Add(1)
		return graph.NoCommand[testDelta](), errors.New("source")
	}, graph.WithErrorHandler[testState, testDelta](func(_ context.Context, _ testState, _ graph.NodeError, runtime graph.Runtime) (graph.Command[testDelta], error) {
		handlerCalls.Add(1)
		answer, err := graph.AwaitResume[string](runtime, "recover?")
		if err != nil {
			return graph.NoCommand[testDelta](), err
		}
		return graph.Update(testDelta{Add: 1, Label: answer}), nil
	})); err != nil {
		t.Fatal(err)
	}
	addEdge(t, builder, graph.START, "failing")
	addEdge(t, builder, "failing", graph.END)
	compiled := compilePersistentGraph(t, builder, checkpointmemory.NewSaver())
	config := graph.RunConfig{ThreadID: "durable-handler-interrupt"}
	if _, err := compiled.Invoke(context.Background(), testState{}, config); !errors.Is(err, graph.ErrGraphInterrupt) {
		t.Fatalf("Invoke error=%v", err)
	}
	command, err := graph.Resume("approved")
	if err != nil {
		t.Fatal(err)
	}
	result, err := compiled.Resume(context.Background(), config, command)
	if err != nil {
		t.Fatal(err)
	}
	if result.Total != 1 || len(result.Path) != 1 || result.Path[0] != "approved" {
		t.Fatalf("result=%+v", result)
	}
	if sourceCalls.Load() != 1 || handlerCalls.Load() != 2 {
		t.Fatalf("source=%d handler=%d", sourceCalls.Load(), handlerCalls.Load())
	}
}
