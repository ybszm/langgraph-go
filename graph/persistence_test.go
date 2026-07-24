package graph_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ybszm/langgraph-go/checkpoint"
	"github.com/ybszm/langgraph-go/checkpoint/memory"
	checkpointsqlite "github.com/ybszm/langgraph-go/checkpoint/sqlite"
	"github.com/ybszm/langgraph-go/graph"
)

type sequenceIDGenerator struct {
	mu   sync.Mutex
	next int
}

type failingStateCodec struct{ err error }

func (c failingStateCodec) Encode(testState) (checkpoint.EncodedValue, error) {
	return checkpoint.EncodedValue{}, c.err
}

func (c failingStateCodec) Decode(checkpoint.EncodedValue) (testState, error) {
	return testState{}, c.err
}

type recordingPutSaver struct {
	checkpoint.Saver
	mu      sync.Mutex
	parents []checkpoint.Config
	failAt  int
	err     error
}

type blockingFirstPutSaver struct {
	checkpoint.Saver
	once    sync.Once
	started chan struct{}
	release chan struct{}
}

func (s *blockingFirstPutSaver) Put(
	ctx context.Context,
	parent checkpoint.Config,
	value checkpoint.Checkpoint,
	metadata checkpoint.Metadata,
	versions map[string]string,
) (checkpoint.Config, error) {
	blocked := false
	s.once.Do(func() {
		blocked = true
		close(s.started)
	})
	if blocked {
		select {
		case <-s.release:
		case <-ctx.Done():
			return checkpoint.Config{}, ctx.Err()
		}
	}
	return s.Saver.Put(ctx, parent, value, metadata, versions)
}

func (s *recordingPutSaver) Put(
	ctx context.Context,
	parent checkpoint.Config,
	value checkpoint.Checkpoint,
	metadata checkpoint.Metadata,
	versions map[string]string,
) (checkpoint.Config, error) {
	s.mu.Lock()
	s.parents = append(s.parents, parent)
	call := len(s.parents)
	s.mu.Unlock()
	if call == s.failAt {
		return checkpoint.Config{}, s.err
	}
	return s.Saver.Put(ctx, parent, value, metadata, versions)
}

func (g *sequenceIDGenerator) NewID(time.Time) (string, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.next++
	return fmt.Sprintf("checkpoint-%04d", g.next), nil
}

func compilePersistentGraph(
	t *testing.T,
	builder *graph.StateGraph[testState, testDelta],
	saver checkpoint.Saver,
) *graph.CompiledGraph[testState, testDelta] {
	t.Helper()
	compiled, err := builder.Compile(graph.WithPersistence(
		graph.PersistenceConfig[testState, testDelta]{
			Saver:       saver,
			StateCodec:  checkpoint.MustJSONCodec[testState]("tests/state", 1),
			DeltaCodec:  checkpoint.MustJSONCodec[testDelta]("tests/delta", 1),
			Clock:       checkpoint.ClockFunc(func() time.Time { return time.Unix(1_700_000_000, 0) }),
			IDGenerator: &sequenceIDGenerator{},
		},
	))
	if err != nil {
		t.Fatalf("Compile(WithPersistence): %v", err)
	}
	return compiled
}

func TestCheckpointPrepareFailureDoesNotWriteDraft(t *testing.T) {
	want := errors.New("encode state")
	saver := &recordingPutSaver{Saver: memory.NewSaver()}
	builder := graph.NewStateGraph(testReducer)
	addNode(t, builder, "node", func(context.Context, testState, graph.Runtime) (graph.Command[testDelta], error) {
		return graph.NoCommand[testDelta](), nil
	})
	addEdge(t, builder, graph.START, "node")
	addEdge(t, builder, "node", graph.END)
	compiled, err := builder.Compile(graph.WithPersistence(
		graph.PersistenceConfig[testState, testDelta]{
			Saver: saver, StateCodec: failingStateCodec{err: want},
			DeltaCodec: checkpoint.MustJSONCodec[testDelta]("tests.prepare-delta", 1),
		},
	))
	if err != nil {
		t.Fatal(err)
	}
	_, err = compiled.Invoke(context.Background(), testState{}, graph.RunConfig{ThreadID: "prepare-failure"})
	if !errors.Is(err, want) {
		t.Fatalf("err=%v", err)
	}
	if len(saver.parents) != 0 {
		t.Fatalf("prepare failure persisted %d draft(s)", len(saver.parents))
	}
}

func TestCheckpointPersistFailureKeepsParentCoordinate(t *testing.T) {
	want := errors.New("persist checkpoint")
	saver := &recordingPutSaver{Saver: memory.NewSaver(), failAt: 2, err: want}
	builder := graph.NewStateGraph(testReducer)
	addNode(t, builder, "node", func(context.Context, testState, graph.Runtime) (graph.Command[testDelta], error) {
		return graph.Update(testDelta{Add: 1}), nil
	})
	addEdge(t, builder, graph.START, "node")
	addEdge(t, builder, "node", graph.END)
	compiled := compilePersistentGraph(t, builder, saver)
	_, err := compiled.Invoke(context.Background(), testState{}, graph.RunConfig{ThreadID: "persist-failure"})
	if !errors.Is(err, want) {
		t.Fatalf("err=%v", err)
	}
	if len(saver.parents) != 2 || saver.parents[1].CheckpointID == "" ||
		saver.parents[1].ThreadID != "persist-failure" {
		t.Fatalf("parents=%+v", saver.parents)
	}
}

func TestExitDurabilityPersistsOnlyFinalCheckpoint(t *testing.T) {
	saver := memory.NewSaver()
	builder := graph.NewStateGraph(testReducer)
	for _, item := range []struct {
		node graph.NodeID
		add  int
	}{
		{node: "first", add: 1},
		{node: "second", add: 2},
	} {
		item := item
		addNode(t, builder, item.node, func(context.Context, testState, graph.Runtime) (graph.Command[testDelta], error) {
			return graph.Update(testDelta{Add: item.add, Label: string(item.node)}), nil
		})
	}
	addEdge(t, builder, graph.START, "first")
	addEdge(t, builder, "first", "second")
	addEdge(t, builder, "second", graph.END)
	compiled := compilePersistentGraph(t, builder, saver)
	config := graph.RunConfig{ThreadID: "exit-final-only", Durability: graph.DurabilityExit}
	result, err := compiled.Invoke(context.Background(), testState{}, config)
	if err != nil {
		t.Fatal(err)
	}
	if result.Total != 3 {
		t.Fatalf("result=%+v", result)
	}
	snapshot, err := compiled.GetState(context.Background(), config)
	if err != nil || snapshot.Values.Total != 3 {
		t.Fatalf("snapshot=%+v err=%v", snapshot, err)
	}
	history, err := saver.List(context.Background(), checkpoint.ListOptions{
		Config: &checkpoint.Config{ThreadID: config.ThreadID},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(history) != 1 || history[0].Checkpoint.Step != 1 ||
		history[0].ParentConfig != nil || len(history[0].Checkpoint.Next) != 0 {
		t.Fatalf("history=%+v", history)
	}
}

func TestExitDurabilityFlushesAndResumesInterruptBoundary(t *testing.T) {
	saver := memory.NewSaver()
	builder := graph.NewStateGraph(testReducer)
	addNode(t, builder, "node", func(context.Context, testState, graph.Runtime) (graph.Command[testDelta], error) {
		return graph.Update(testDelta{Add: 1, Label: "node"}), nil
	})
	addEdge(t, builder, graph.START, "node")
	addEdge(t, builder, "node", graph.END)
	compiled, err := builder.Compile(
		graph.WithPersistence(graph.PersistenceConfig[testState, testDelta]{
			Saver: saver, StateCodec: checkpoint.MustJSONCodec[testState]("tests.exit-interrupt-state", 1),
			DeltaCodec: checkpoint.MustJSONCodec[testDelta]("tests.exit-interrupt-delta", 1),
		}),
		graph.WithInterruptBefore[testState, testDelta]("node"),
	)
	if err != nil {
		t.Fatal(err)
	}
	config := graph.RunConfig{ThreadID: "exit-interrupt", Durability: graph.DurabilityExit}
	if _, err := compiled.Invoke(context.Background(), testState{}, config); !errors.Is(err, graph.ErrGraphInterrupt) {
		t.Fatalf("Invoke() err=%v", err)
	}
	paused, err := compiled.GetState(context.Background(), config)
	if err != nil {
		t.Fatal(err)
	}
	if paused.Values.Total != 0 || len(paused.Next) != 1 || paused.Next[0] != "node" {
		t.Fatalf("paused=%+v", paused)
	}
	result, err := compiled.Resume(context.Background(), config, graph.Continue())
	if err != nil {
		t.Fatal(err)
	}
	if result.Total != 1 {
		t.Fatalf("result=%+v", result)
	}
	history, err := saver.List(context.Background(), checkpoint.ListOptions{
		Config: &checkpoint.Config{ThreadID: config.ThreadID},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(history) != 2 || history[0].Checkpoint.Step != 0 ||
		history[0].ParentConfig == nil ||
		history[0].ParentConfig.CheckpointID != history[1].Config.CheckpointID {
		t.Fatalf("history=%+v", history)
	}
}

func TestBufferedDurabilityFlushesDynamicInterrupt(t *testing.T) {
	for _, durability := range []graph.Durability{graph.DurabilityAsync, graph.DurabilityExit} {
		t.Run(string(durability), func(t *testing.T) {
			saver := memory.NewSaver()
			builder := graph.NewStateGraph(testReducer)
			if err := builder.AddNode("human", func(
				_ context.Context,
				_ testState,
				runtime graph.Runtime,
			) (graph.Command[testDelta], error) {
				answer, err := graph.AwaitResume[string](runtime, "approve?")
				if err != nil {
					return graph.NoCommand[testDelta](), err
				}
				return graph.Update(testDelta{Add: 1, Label: answer}), nil
			}, graph.WithDynamicInterrupts()); err != nil {
				t.Fatal(err)
			}
			addEdge(t, builder, graph.START, "human")
			addEdge(t, builder, "human", graph.END)
			compiled := compilePersistentGraph(t, builder, saver)
			config := graph.RunConfig{
				ThreadID: "dynamic-interrupt-" + string(durability), Durability: durability,
			}
			if _, err := compiled.Invoke(context.Background(), testState{}, config); !errors.Is(err, graph.ErrGraphInterrupt) {
				t.Fatalf("Invoke() err=%v", err)
			}
			paused, err := compiled.GetState(context.Background(), config)
			if err != nil || len(paused.Interrupts) != 1 {
				t.Fatalf("paused=%+v err=%v", paused, err)
			}
			command, err := graph.Resume("approved")
			if err != nil {
				t.Fatal(err)
			}
			result, err := compiled.Resume(context.Background(), config, command)
			if err != nil {
				t.Fatal(err)
			}
			if result.Total != 1 || !reflect.DeepEqual(result.Path, []string{"approved"}) {
				t.Fatalf("result=%+v", result)
			}
		})
	}
}

func TestExitDurabilityRecoversPendingWritesAfterFailure(t *testing.T) {
	saver := memory.NewSaver()
	builder := graph.NewStateGraph(testReducer)
	var successCalls, failureCalls atomic.Int32
	addNode(t, builder, "success", func(context.Context, testState, graph.Runtime) (graph.Command[testDelta], error) {
		successCalls.Add(1)
		return graph.Update(testDelta{Add: 1, Label: "success"}), nil
	})
	addNode(t, builder, "failure", func(context.Context, testState, graph.Runtime) (graph.Command[testDelta], error) {
		if failureCalls.Add(1) == 1 {
			return graph.NoCommand[testDelta](), errors.New("transient failure")
		}
		return graph.Update(testDelta{Add: 2, Label: "failure"}), nil
	})
	addEdge(t, builder, graph.START, "success")
	addEdge(t, builder, graph.START, "failure")
	addEdge(t, builder, "success", graph.END)
	addEdge(t, builder, "failure", graph.END)
	compiled := compilePersistentGraph(t, builder, saver)
	config := graph.RunConfig{ThreadID: "exit-failure", Durability: graph.DurabilityExit}
	if _, err := compiled.Invoke(context.Background(), testState{}, config); err == nil {
		t.Fatal("first Invoke() succeeded")
	}
	result, err := compiled.Invoke(context.Background(), testState{}, config)
	if err != nil {
		t.Fatal(err)
	}
	if result.Total != 3 || successCalls.Load() != 1 || failureCalls.Load() != 2 {
		t.Fatalf("result=%+v calls=%d/%d", result, successCalls.Load(), failureCalls.Load())
	}
	history, err := saver.List(context.Background(), checkpoint.ListOptions{
		Config: &checkpoint.Config{ThreadID: config.ThreadID},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(history) != 2 || history[0].Checkpoint.Step != 0 {
		t.Fatalf("history=%+v", history)
	}
}

func TestExitDurabilityReturnsFlushFailure(t *testing.T) {
	want := errors.New("exit flush failed")
	saver := &recordingPutSaver{Saver: memory.NewSaver(), failAt: 1, err: want}
	builder := graph.NewStateGraph(testReducer)
	addNode(t, builder, "node", func(context.Context, testState, graph.Runtime) (graph.Command[testDelta], error) {
		return graph.Update(testDelta{Add: 1}), nil
	})
	addEdge(t, builder, graph.START, "node")
	addEdge(t, builder, "node", graph.END)
	compiled := compilePersistentGraph(t, builder, saver)
	result, err := compiled.Invoke(context.Background(), testState{}, graph.RunConfig{
		ThreadID: "exit-flush-failure", Durability: graph.DurabilityExit,
	})
	if !errors.Is(err, want) || result.Total != 1 {
		t.Fatalf("result=%+v err=%v", result, err)
	}
}

func TestAsyncDurabilityPersistsFullHistory(t *testing.T) {
	saver := memory.NewSaver()
	builder := graph.NewStateGraph(testReducer)
	for _, item := range []struct {
		node graph.NodeID
		add  int
	}{
		{node: "first", add: 1},
		{node: "second", add: 2},
	} {
		item := item
		addNode(t, builder, item.node, func(context.Context, testState, graph.Runtime) (graph.Command[testDelta], error) {
			return graph.Update(testDelta{Add: item.add, Label: string(item.node)}), nil
		})
	}
	addEdge(t, builder, graph.START, "first")
	addEdge(t, builder, "first", "second")
	addEdge(t, builder, "second", graph.END)
	compiled := compilePersistentGraph(t, builder, saver)
	config := graph.RunConfig{ThreadID: "async-history", Durability: graph.DurabilityAsync}
	result, err := compiled.Invoke(context.Background(), testState{}, config)
	if err != nil {
		t.Fatal(err)
	}
	if result.Total != 3 {
		t.Fatalf("result=%+v", result)
	}
	history, err := saver.List(context.Background(), checkpoint.ListOptions{
		Config: &checkpoint.Config{ThreadID: config.ThreadID},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(history) != 3 ||
		history[0].Checkpoint.Step != 1 ||
		history[1].Checkpoint.Step != 0 ||
		history[2].Checkpoint.Step != -1 {
		t.Fatalf("history=%+v", history)
	}
}

func TestAsyncDurabilityOverlapsCheckpointIOWithNodeExecution(t *testing.T) {
	saver := &blockingFirstPutSaver{
		Saver: memory.NewSaver(), started: make(chan struct{}), release: make(chan struct{}),
	}
	builder := graph.NewStateGraph(testReducer)
	nodeStarted := make(chan struct{})
	addNode(t, builder, "node", func(context.Context, testState, graph.Runtime) (graph.Command[testDelta], error) {
		close(nodeStarted)
		return graph.Update(testDelta{Add: 1}), nil
	})
	addEdge(t, builder, graph.START, "node")
	addEdge(t, builder, "node", graph.END)
	compiled := compilePersistentGraph(t, builder, saver)
	type invocation struct {
		state testState
		err   error
	}
	finished := make(chan invocation, 1)
	go func() {
		state, err := compiled.Invoke(context.Background(), testState{}, graph.RunConfig{
			ThreadID: "async-overlap", Durability: graph.DurabilityAsync,
		})
		finished <- invocation{state: state, err: err}
	}()
	select {
	case <-saver.started:
	case <-time.After(time.Second):
		t.Fatal("checkpoint write did not start")
	}
	select {
	case <-nodeStarted:
	case <-time.After(time.Second):
		t.Fatal("node did not overlap blocked checkpoint IO")
	}
	close(saver.release)
	result := <-finished
	if result.err != nil || result.state.Total != 1 {
		t.Fatalf("result=%+v err=%v", result.state, result.err)
	}
}

func TestAsyncDurabilityReturnsBackgroundFailure(t *testing.T) {
	want := errors.New("async persistence failed")
	saver := &recordingPutSaver{Saver: memory.NewSaver(), failAt: 1, err: want}
	builder := graph.NewStateGraph(testReducer)
	addNode(t, builder, "node", func(context.Context, testState, graph.Runtime) (graph.Command[testDelta], error) {
		return graph.Update(testDelta{Add: 1}), nil
	})
	addEdge(t, builder, graph.START, "node")
	addEdge(t, builder, "node", graph.END)
	compiled := compilePersistentGraph(t, builder, saver)
	_, err := compiled.Invoke(context.Background(), testState{}, graph.RunConfig{
		ThreadID: "async-failure", Durability: graph.DurabilityAsync,
	})
	if !errors.Is(err, want) {
		t.Fatalf("err=%v", err)
	}
}

func TestAsyncDurabilityCancellationUnblocksCheckpointWriter(t *testing.T) {
	saver := &blockingFirstPutSaver{
		Saver: memory.NewSaver(), started: make(chan struct{}), release: make(chan struct{}),
	}
	builder := graph.NewStateGraph(testReducer)
	addNode(t, builder, "node", func(context.Context, testState, graph.Runtime) (graph.Command[testDelta], error) {
		return graph.Update(testDelta{Add: 1}), nil
	})
	addEdge(t, builder, graph.START, "node")
	addEdge(t, builder, "node", graph.END)
	compiled := compilePersistentGraph(t, builder, saver)

	ctx, cancel := context.WithCancel(context.Background())
	finished := make(chan error, 1)
	go func() {
		_, err := compiled.Invoke(ctx, testState{}, graph.RunConfig{
			ThreadID: "async-cancel", Durability: graph.DurabilityAsync,
		})
		finished <- err
	}()
	select {
	case <-saver.started:
	case <-time.After(time.Second):
		t.Fatal("checkpoint write did not start")
	}
	cancel()
	select {
	case err := <-finished:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("err=%v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("canceled invocation did not unblock checkpoint writer")
	}
}

func TestAsyncDurabilityCancellationUnblocksSaturatedQueue(t *testing.T) {
	saver := &blockingFirstPutSaver{
		Saver: memory.NewSaver(), started: make(chan struct{}), release: make(chan struct{}),
	}
	builder := graph.NewStateGraph(testReducer)
	var nodeCalls atomic.Int32
	queueSaturated := make(chan struct{})
	for index := 0; index < 80; index++ {
		node := graph.NodeID(fmt.Sprintf("node-%02d", index))
		addNode(t, builder, node, func(context.Context, testState, graph.Runtime) (graph.Command[testDelta], error) {
			if nodeCalls.Add(1) == 33 {
				close(queueSaturated)
			}
			return graph.Update(testDelta{Add: 1}), nil
		})
		if index == 0 {
			addEdge(t, builder, graph.START, node)
		} else {
			addEdge(t, builder, graph.NodeID(fmt.Sprintf("node-%02d", index-1)), node)
		}
		if index == 79 {
			addEdge(t, builder, node, graph.END)
		}
	}
	compiled := compilePersistentGraph(t, builder, saver)

	ctx, cancel := context.WithCancel(context.Background())
	finished := make(chan error, 1)
	go func() {
		_, err := compiled.Invoke(ctx, testState{}, graph.RunConfig{
			ThreadID: "async-saturated-cancel", Durability: graph.DurabilityAsync,
			RecursionLimit: 100,
		})
		finished <- err
	}()
	select {
	case <-saver.started:
	case <-time.After(time.Second):
		t.Fatal("checkpoint write did not start")
	}
	select {
	case <-queueSaturated:
	case <-time.After(time.Second):
		t.Fatalf("queue did not saturate; node calls=%d", nodeCalls.Load())
	}
	time.Sleep(20 * time.Millisecond)
	if calls := nodeCalls.Load(); calls != 33 {
		t.Fatalf("execution did not stop at the saturated queue boundary; node calls=%d", calls)
	}
	cancel()
	select {
	case err := <-finished:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("err=%v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("canceled invocation deadlocked with a saturated persistence queue")
	}
}

func TestAsyncDurabilityFlushesBeforeSameThreadLockRelease(t *testing.T) {
	saver := &blockingFirstPutSaver{
		Saver: memory.NewSaver(), started: make(chan struct{}), release: make(chan struct{}),
	}
	builder := graph.NewStateGraph(testReducer)
	var nodeCalls atomic.Int32
	firstNodeFinished := make(chan struct{})
	addNode(t, builder, "node", func(context.Context, testState, graph.Runtime) (graph.Command[testDelta], error) {
		if nodeCalls.Add(1) == 1 {
			close(firstNodeFinished)
		}
		return graph.Update(testDelta{Add: 1}), nil
	})
	addEdge(t, builder, graph.START, "node")
	addEdge(t, builder, "node", graph.END)
	compiled := compilePersistentGraph(t, builder, saver)
	config := graph.RunConfig{ThreadID: "async-serialized", Durability: graph.DurabilityAsync}

	firstFinished := make(chan error, 1)
	go func() {
		_, err := compiled.Invoke(context.Background(), testState{}, config)
		firstFinished <- err
	}()
	select {
	case <-saver.started:
	case <-time.After(time.Second):
		t.Fatal("checkpoint write did not start")
	}
	select {
	case <-firstNodeFinished:
	case <-time.After(time.Second):
		t.Fatal("first node did not execute while checkpoint IO was blocked")
	}

	type invocation struct {
		state testState
		err   error
	}
	secondFinished := make(chan invocation, 1)
	go func() {
		state, err := compiled.Invoke(context.Background(), testState{}, config)
		secondFinished <- invocation{state: state, err: err}
	}()
	select {
	case result := <-secondFinished:
		t.Fatalf("second same-thread run returned before the first run flushed: %+v", result)
	case <-time.After(50 * time.Millisecond):
	}
	close(saver.release)
	if err := <-firstFinished; err != nil {
		t.Fatalf("first invocation: %v", err)
	}
	select {
	case result := <-secondFinished:
		if result.err != nil || result.state.Total != 1 {
			t.Fatalf("second invocation result=%+v err=%v", result.state, result.err)
		}
	case <-time.After(time.Second):
		t.Fatal("second same-thread run did not return after flush")
	}
	if calls := nodeCalls.Load(); calls != 1 {
		t.Fatalf("completed node reran %d times", calls)
	}
}

func TestAsyncDurabilityIsolatesConcurrentThreads(t *testing.T) {
	saver := memory.NewSaver()
	builder := graph.NewStateGraph(testReducer)
	addNode(t, builder, "node", func(context.Context, testState, graph.Runtime) (graph.Command[testDelta], error) {
		return graph.Update(testDelta{Add: 1}), nil
	})
	addEdge(t, builder, graph.START, "node")
	addEdge(t, builder, "node", graph.END)
	compiled := compilePersistentGraph(t, builder, saver)

	const runs = 24
	var wait sync.WaitGroup
	errs := make(chan error, runs)
	for index := range runs {
		wait.Add(1)
		go func() {
			defer wait.Done()
			threadID := fmt.Sprintf("async-concurrent-%02d", index)
			result, err := compiled.Invoke(context.Background(), testState{}, graph.RunConfig{
				ThreadID: threadID, Durability: graph.DurabilityAsync,
			})
			if err != nil {
				errs <- fmt.Errorf("%s invoke: %w", threadID, err)
				return
			}
			if result.Total != 1 {
				errs <- fmt.Errorf("%s result=%+v", threadID, result)
				return
			}
			snapshot, err := compiled.GetState(context.Background(), graph.RunConfig{ThreadID: threadID})
			if err != nil {
				errs <- fmt.Errorf("%s get state: %w", threadID, err)
				return
			}
			if snapshot.Values.Total != 1 || len(snapshot.Next) != 0 {
				errs <- fmt.Errorf("%s snapshot=%+v", threadID, snapshot)
			}
		}()
	}
	wait.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
}

func TestDurabilityModesRoundTripSQLite(t *testing.T) {
	for _, test := range []struct {
		mode        graph.Durability
		checkpoints int
	}{
		{mode: graph.DurabilitySync, checkpoints: 2},
		{mode: graph.DurabilityAsync, checkpoints: 2},
		{mode: graph.DurabilityExit, checkpoints: 1},
	} {
		t.Run(string(test.mode), func(t *testing.T) {
			saver, err := checkpointsqlite.Open(
				context.Background(), filepath.Join(t.TempDir(), "checkpoints.db"),
			)
			if err != nil {
				t.Fatal(err)
			}
			defer saver.Close()
			builder := graph.NewStateGraph(testReducer)
			addNode(t, builder, "node", func(context.Context, testState, graph.Runtime) (graph.Command[testDelta], error) {
				return graph.Update(testDelta{Add: 7, Label: "node"}), nil
			})
			addEdge(t, builder, graph.START, "node")
			addEdge(t, builder, "node", graph.END)
			compiled := compilePersistentGraph(t, builder, saver)
			config := graph.RunConfig{
				ThreadID: "sqlite-" + string(test.mode), Durability: test.mode,
			}
			if _, err := compiled.Invoke(context.Background(), testState{}, config); err != nil {
				t.Fatal(err)
			}
			snapshot, err := compiled.GetState(context.Background(), config)
			if err != nil || snapshot.Values.Total != 7 || len(snapshot.Next) != 0 {
				t.Fatalf("snapshot=%+v err=%v", snapshot, err)
			}
			history, err := compiled.GetStateHistory(
				context.Background(), config, graph.StateHistoryOptions{},
			)
			if err != nil {
				t.Fatal(err)
			}
			if len(history) != test.checkpoints {
				t.Fatalf("history count=%d want=%d", len(history), test.checkpoints)
			}
		})
	}
}

func TestPersistentRunCreatesSuperStepCheckpoints(t *testing.T) {
	saver := memory.NewSaver()
	builder := graph.NewStateGraph(testReducer)
	addNode(t, builder, "first", func(
		context.Context,
		testState,
		graph.Runtime,
	) (graph.Command[testDelta], error) {
		return graph.Update(testDelta{Add: 1, Label: "first"}), nil
	})
	addNode(t, builder, "second", func(
		context.Context,
		testState,
		graph.Runtime,
	) (graph.Command[testDelta], error) {
		return graph.Update(testDelta{Add: 2, Label: "second"}), nil
	})
	addEdge(t, builder, graph.START, "first")
	addEdge(t, builder, "first", "second")
	addEdge(t, builder, "second", graph.END)

	compiled := compilePersistentGraph(t, builder, saver)
	result, err := compiled.Invoke(context.Background(), testState{}, graph.RunConfig{
		ThreadID: "thread-checkpoints",
		RunID:    "run-1",
	})
	if err != nil {
		t.Fatalf("Invoke(): %v", err)
	}
	if result.Total != 3 || !reflect.DeepEqual(result.Path, []string{"first", "second"}) {
		t.Fatalf("result = %#v", result)
	}

	history, err := saver.List(context.Background(), checkpoint.ListOptions{
		Config: &checkpoint.Config{ThreadID: "thread-checkpoints"},
	})
	if err != nil {
		t.Fatalf("List(): %v", err)
	}
	if len(history) != 3 {
		t.Fatalf("checkpoint count = %d, want 3", len(history))
	}
	if got := []int{
		history[0].Checkpoint.Step,
		history[1].Checkpoint.Step,
		history[2].Checkpoint.Step,
	}; !reflect.DeepEqual(got, []int{1, 0, -1}) {
		t.Fatalf("steps = %#v, want [1 0 -1]", got)
	}
	if len(history[0].Checkpoint.Next) != 0 {
		t.Fatalf("final checkpoint still has tasks: %#v", history[0].Checkpoint.Next)
	}
	if history[0].ParentConfig == nil ||
		history[1].ParentConfig == nil ||
		history[2].ParentConfig != nil {
		t.Fatalf("unexpected parent chain: %#v", history)
	}
	if history[0].ParentConfig.CheckpointID != history[1].Config.CheckpointID ||
		history[1].ParentConfig.CheckpointID != history[2].Config.CheckpointID {
		t.Fatalf("checkpoint parent chain is not contiguous")
	}
	if len(history[2].PendingWrites) != 1 || len(history[1].PendingWrites) != 1 {
		t.Fatalf("task results were not attached to their parent checkpoints")
	}
	if history[0].Metadata["source"] != string(checkpoint.SourceLoop) ||
		history[2].Metadata["source"] != string(checkpoint.SourceInput) ||
		history[0].Metadata["run_id"] != "run-1" {
		t.Fatalf("unexpected metadata: %#v / %#v", history[0].Metadata, history[2].Metadata)
	}

	stateCodec := checkpoint.MustJSONCodec[testState]("tests/state", 1)
	storedState, err := stateCodec.Decode(history[0].Checkpoint.Values[checkpoint.StateChannel])
	if err != nil {
		t.Fatalf("Decode(latest state): %v", err)
	}
	if !reflect.DeepEqual(storedState, result) {
		t.Fatalf("stored state = %#v, want %#v", storedState, result)
	}
}

func TestPersistentRunRecoversSuccessfulPendingWrites(t *testing.T) {
	saver := memory.NewSaver()
	builder := graph.NewStateGraph(testReducer)
	successFinished := make(chan struct{})
	var successOnce sync.Once
	var successCalls atomic.Int32
	var flakyCalls atomic.Int32

	addNode(t, builder, "success", func(
		context.Context,
		testState,
		graph.Runtime,
	) (graph.Command[testDelta], error) {
		successCalls.Add(1)
		successOnce.Do(func() { close(successFinished) })
		return graph.Update(testDelta{Add: 1, Label: "success"}), nil
	})
	addNode(t, builder, "flaky", func(
		ctx context.Context,
		_ testState,
		_ graph.Runtime,
	) (graph.Command[testDelta], error) {
		select {
		case <-successFinished:
		case <-ctx.Done():
			return graph.NoCommand[testDelta](), ctx.Err()
		}
		if flakyCalls.Add(1) == 1 {
			return graph.NoCommand[testDelta](), errors.New("transient failure")
		}
		return graph.Update(testDelta{Add: 2, Label: "flaky"}), nil
	})
	addEdge(t, builder, graph.START, "success")
	addEdge(t, builder, graph.START, "flaky")
	addEdge(t, builder, "success", graph.END)
	addEdge(t, builder, "flaky", graph.END)
	compiled := compilePersistentGraph(t, builder, saver)
	config := graph.RunConfig{ThreadID: "thread-recovery"}

	firstState, err := compiled.Invoke(context.Background(), testState{}, config)
	if err == nil {
		t.Fatal("first Invoke() succeeded, want transient failure")
	}
	if firstState.Total != 0 {
		t.Fatalf("failed super-step committed state %#v", firstState)
	}
	tuple, found, getErr := saver.GetTuple(context.Background(), checkpoint.Config{
		ThreadID: "thread-recovery",
	})
	if getErr != nil || !found {
		t.Fatalf("GetTuple() = found %v, err %v", found, getErr)
	}
	if tuple.Checkpoint.Step != -1 || len(tuple.PendingWrites) != 1 {
		t.Fatalf("failed step checkpoint = %#v, writes = %#v", tuple.Checkpoint, tuple.PendingWrites)
	}

	result, err := compiled.Invoke(context.Background(), testState{Total: 100}, config)
	if err != nil {
		t.Fatalf("retry Invoke(): %v", err)
	}
	if result.Total != 3 || !reflect.DeepEqual(result.Path, []string{"success", "flaky"}) {
		t.Fatalf("retry result = %#v", result)
	}
	if successCalls.Load() != 1 {
		t.Fatalf("successful node calls = %d, want 1", successCalls.Load())
	}
	if flakyCalls.Load() != 2 {
		t.Fatalf("flaky node calls = %d, want 2", flakyCalls.Load())
	}
}

func TestCompletedPersistentThreadReturnsStoredStateWithoutRerun(t *testing.T) {
	saver := memory.NewSaver()
	builder := graph.NewStateGraph(testReducer)
	var calls atomic.Int32
	addNode(t, builder, "once", func(
		context.Context,
		testState,
		graph.Runtime,
	) (graph.Command[testDelta], error) {
		calls.Add(1)
		return graph.Update(testDelta{Add: 7, Label: "once"}), nil
	})
	addEdge(t, builder, graph.START, "once")
	addEdge(t, builder, "once", graph.END)
	compiled := compilePersistentGraph(t, builder, saver)
	config := graph.RunConfig{ThreadID: "thread-complete"}

	first, err := compiled.Invoke(context.Background(), testState{}, config)
	if err != nil {
		t.Fatalf("first Invoke(): %v", err)
	}
	second, err := compiled.Invoke(context.Background(), testState{Total: 100}, config)
	if err != nil {
		t.Fatalf("second Invoke(): %v", err)
	}
	if !reflect.DeepEqual(first, second) || calls.Load() != 1 {
		t.Fatalf("first=%#v second=%#v calls=%d", first, second, calls.Load())
	}
}

func TestPersistentGraphUsesEncryptedStateAndDeltaCodecs(t *testing.T) {
	saver := memory.NewSaver()
	builder := graph.NewStateGraph(testReducer)
	addNode(t, builder, "secret", func(context.Context, testState, graph.Runtime) (graph.Command[testDelta], error) {
		return graph.Update(testDelta{Add: 1, Label: "classified"}), nil
	})
	addEdge(t, builder, graph.START, "secret")
	addEdge(t, builder, "secret", graph.END)
	key := bytes.Repeat([]byte{0x33}, 32)
	stateCodec, err := checkpoint.NewEncryptedCodec(
		checkpoint.MustJSONCodec[testState]("tests.encrypted-state", 1), key,
	)
	if err != nil {
		t.Fatal(err)
	}
	deltaCodec, err := checkpoint.NewEncryptedCodec(
		checkpoint.MustJSONCodec[testDelta]("tests.encrypted-delta", 1), key,
	)
	if err != nil {
		t.Fatal(err)
	}
	compiled, err := builder.Compile(graph.WithPersistence(graph.PersistenceConfig[testState, testDelta]{
		Saver: saver, StateCodec: stateCodec, DeltaCodec: deltaCodec,
	}))
	if err != nil {
		t.Fatal(err)
	}
	config := graph.RunConfig{ThreadID: "encrypted-thread"}
	result, err := compiled.Invoke(context.Background(), testState{}, config)
	if err != nil || result.Total != 1 || !reflect.DeepEqual(result.Path, []string{"classified"}) {
		t.Fatalf("result=%#v error=%v", result, err)
	}
	tuple, found, err := saver.GetTuple(context.Background(), checkpoint.Config{ThreadID: config.ThreadID})
	if err != nil || !found {
		t.Fatalf("tuple found=%v error=%v", found, err)
	}
	stored := tuple.Checkpoint.Values[checkpoint.StateChannel]
	if stored.Type != checkpoint.EncryptedTypePrefix+"tests.encrypted-state" || bytes.Contains(stored.Data, []byte("classified")) {
		t.Fatalf("stored=%+v", stored)
	}
	wrongState, _ := checkpoint.NewEncryptedCodec(checkpoint.MustJSONCodec[testState]("tests.encrypted-state", 1), bytes.Repeat([]byte{0x44}, 32))
	wrongDelta, _ := checkpoint.NewEncryptedCodec(checkpoint.MustJSONCodec[testDelta]("tests.encrypted-delta", 1), bytes.Repeat([]byte{0x44}, 32))
	wrong, err := builder.Compile(graph.WithPersistence(graph.PersistenceConfig[testState, testDelta]{
		Saver: saver, StateCodec: wrongState, DeltaCodec: wrongDelta,
	}))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := wrong.GetState(context.Background(), config); !errors.Is(err, checkpoint.ErrEncryption) {
		t.Fatalf("wrong-key state error=%v", err)
	}
}

func TestPersistentRunRequiresThreadAndReportsMissingCheckpoint(t *testing.T) {
	saver := memory.NewSaver()
	builder := graph.NewStateGraph(testReducer)
	addNode(t, builder, "only", func(
		context.Context,
		testState,
		graph.Runtime,
	) (graph.Command[testDelta], error) {
		return graph.NoCommand[testDelta](), nil
	})
	addEdge(t, builder, graph.START, "only")
	addEdge(t, builder, "only", graph.END)
	compiled := compilePersistentGraph(t, builder, saver)

	_, err := compiled.Invoke(context.Background(), testState{}, graph.RunConfig{})
	if !errors.Is(err, graph.ErrInvalidRunConfig) {
		t.Fatalf("missing-thread error = %v", err)
	}
	_, err = compiled.Invoke(context.Background(), testState{}, graph.RunConfig{
		ThreadID:     "thread-missing",
		CheckpointID: "does-not-exist",
	})
	if !errors.Is(err, checkpoint.ErrNotFound) {
		t.Fatalf("missing-checkpoint error = %v", err)
	}
	var persistenceErr *graph.PersistenceError
	if !errors.As(err, &persistenceErr) || persistenceErr.Operation != "get" {
		t.Fatalf("error = %T %#v, want get PersistenceError", err, err)
	}
}

func TestPersistentNamespacesAreIndependent(t *testing.T) {
	saver := memory.NewSaver()
	builder := graph.NewStateGraph(testReducer)
	var calls atomic.Int32
	addNode(t, builder, "only", func(
		context.Context,
		testState,
		graph.Runtime,
	) (graph.Command[testDelta], error) {
		calls.Add(1)
		return graph.Update(testDelta{Add: 1}), nil
	})
	addEdge(t, builder, graph.START, "only")
	addEdge(t, builder, "only", graph.END)
	compiled := compilePersistentGraph(t, builder, saver)

	for _, namespace := range []string{"one", "two"} {
		result, err := compiled.Invoke(context.Background(), testState{}, graph.RunConfig{
			ThreadID:            "shared-thread",
			CheckpointNamespace: namespace,
		})
		if err != nil || result.Total != 1 {
			t.Fatalf("namespace %q result=%#v err=%v", namespace, result, err)
		}
	}
	if calls.Load() != 2 {
		t.Fatalf("calls = %d, want 2 independent executions", calls.Load())
	}
	history, err := saver.List(context.Background(), checkpoint.ListOptions{
		Config:        &checkpoint.Config{ThreadID: "shared-thread"},
		AllNamespaces: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(history) != 4 {
		t.Fatalf("all-namespace checkpoint count = %d, want 4", len(history))
	}
}

func TestGetStateProjectsPendingWritesOnlyForLatest(t *testing.T) {
	saver := memory.NewSaver()
	builder := graph.NewStateGraph(testReducer)
	successFinished := make(chan struct{})
	addNode(t, builder, "success", func(
		context.Context,
		testState,
		graph.Runtime,
	) (graph.Command[testDelta], error) {
		close(successFinished)
		return graph.Update(testDelta{Add: 1, Label: "success"}), nil
	})
	addNode(t, builder, "failure", func(
		ctx context.Context,
		_ testState,
		_ graph.Runtime,
	) (graph.Command[testDelta], error) {
		select {
		case <-successFinished:
			return graph.NoCommand[testDelta](), errors.New("failure")
		case <-ctx.Done():
			return graph.NoCommand[testDelta](), ctx.Err()
		}
	})
	addEdge(t, builder, graph.START, "success")
	addEdge(t, builder, graph.START, "failure")
	addEdge(t, builder, "success", graph.END)
	addEdge(t, builder, "failure", graph.END)
	compiled := compilePersistentGraph(t, builder, saver)
	config := graph.RunConfig{ThreadID: "thread-state-view"}
	if _, err := compiled.Invoke(context.Background(), testState{}, config); err == nil {
		t.Fatal("Invoke() succeeded, want node failure")
	}

	latest, err := compiled.GetState(context.Background(), config)
	if err != nil {
		t.Fatalf("GetState(latest): %v", err)
	}
	if latest.Values.Total != 1 || !reflect.DeepEqual(latest.Next, []graph.NodeID{"failure"}) {
		t.Fatalf("latest snapshot = %#v", latest)
	}
	if len(latest.Tasks) != 2 || !latest.Tasks[0].Completed || latest.Tasks[1].Completed {
		t.Fatalf("latest tasks = %#v", latest.Tasks)
	}

	exact, err := compiled.GetState(context.Background(), graph.RunConfig{
		ThreadID:     config.ThreadID,
		CheckpointID: latest.Config.CheckpointID,
	})
	if err != nil {
		t.Fatalf("GetState(exact): %v", err)
	}
	if exact.Values.Total != 0 || !reflect.DeepEqual(
		exact.Next,
		[]graph.NodeID{"success", "failure"},
	) {
		t.Fatalf("exact snapshot = %#v", exact)
	}
}

func TestGetStateHistoryFiltersAndPaginates(t *testing.T) {
	saver := memory.NewSaver()
	builder := graph.NewStateGraph(testReducer)
	for _, node := range []graph.NodeID{"first", "second"} {
		node := node
		addNode(t, builder, node, func(
			context.Context,
			testState,
			graph.Runtime,
		) (graph.Command[testDelta], error) {
			return graph.Update(testDelta{Add: 1, Label: string(node)}), nil
		})
	}
	addEdge(t, builder, graph.START, "first")
	addEdge(t, builder, "first", "second")
	addEdge(t, builder, "second", graph.END)
	compiled := compilePersistentGraph(t, builder, saver)
	config := graph.RunConfig{ThreadID: "thread-history"}
	if _, err := compiled.Invoke(context.Background(), testState{}, config); err != nil {
		t.Fatal(err)
	}

	history, err := compiled.GetStateHistory(
		context.Background(),
		config,
		graph.StateHistoryOptions{},
	)
	if err != nil || len(history) != 3 {
		t.Fatalf("GetStateHistory() len=%d err=%v", len(history), err)
	}
	if got := []int{
		history[0].Metadata["step"].(int),
		history[1].Metadata["step"].(int),
		history[2].Metadata["step"].(int),
	}; !reflect.DeepEqual(got, []int{1, 0, -1}) {
		t.Fatalf("history steps = %#v", got)
	}
	page, err := compiled.GetStateHistory(
		context.Background(),
		config,
		graph.StateHistoryOptions{
			Filter:             checkpoint.Metadata{"source": string(checkpoint.SourceLoop)},
			BeforeCheckpointID: history[0].Config.CheckpointID,
			Limit:              1,
		},
	)
	if err != nil || len(page) != 1 || page[0].Config != history[1].Config {
		t.Fatalf("history page = %#v err=%v", page, err)
	}
}

func TestUpdateStateCreatesBranchAndSchedulesAsNode(t *testing.T) {
	saver := memory.NewSaver()
	builder := graph.NewStateGraph(testReducer)
	addNode(t, builder, "first", func(
		context.Context,
		testState,
		graph.Runtime,
	) (graph.Command[testDelta], error) {
		return graph.Update(testDelta{Add: 1, Label: "first"}), nil
	})
	addNode(t, builder, "second", func(
		context.Context,
		testState,
		graph.Runtime,
	) (graph.Command[testDelta], error) {
		return graph.Update(testDelta{Add: 2, Label: "second"}), nil
	})
	addEdge(t, builder, graph.START, "first")
	addEdge(t, builder, "first", "second")
	addEdge(t, builder, "second", graph.END)
	compiled := compilePersistentGraph(t, builder, saver)
	config := graph.RunConfig{ThreadID: "thread-update"}
	if _, err := compiled.Invoke(context.Background(), testState{}, config); err != nil {
		t.Fatal(err)
	}
	history, err := compiled.GetStateHistory(
		context.Background(),
		config,
		graph.StateHistoryOptions{},
	)
	if err != nil {
		t.Fatal(err)
	}
	inputCheckpoint := history[len(history)-1].Config

	updatedConfig, err := compiled.UpdateState(
		context.Background(),
		graph.RunConfig{
			ThreadID:     config.ThreadID,
			CheckpointID: inputCheckpoint.CheckpointID,
		},
		graph.StateUpdate[testDelta]{
			Delta:  testDelta{Add: 10, Label: "manual"},
			AsNode: "first",
		},
	)
	if err != nil {
		t.Fatalf("UpdateState(): %v", err)
	}
	updated, err := compiled.GetState(context.Background(), graph.RunConfig{
		ThreadID:     config.ThreadID,
		CheckpointID: updatedConfig.CheckpointID,
	})
	if err != nil {
		t.Fatal(err)
	}
	if updated.Values.Total != 10 || !reflect.DeepEqual(updated.Next, []graph.NodeID{"second"}) {
		t.Fatalf("updated snapshot = %#v", updated)
	}
	if updated.ParentConfig == nil || *updated.ParentConfig != inputCheckpoint {
		t.Fatalf("updated parent = %#v, want %#v", updated.ParentConfig, inputCheckpoint)
	}
	if updated.Metadata["source"] != string(checkpoint.SourceUpdate) ||
		updated.Metadata["as_node"] != "first" {
		t.Fatalf("updated metadata = %#v", updated.Metadata)
	}

	result, err := compiled.Invoke(context.Background(), testState{}, config)
	if err != nil {
		t.Fatalf("Invoke(updated branch): %v", err)
	}
	if result.Total != 12 || !reflect.DeepEqual(result.Path, []string{"manual", "second"}) {
		t.Fatalf("updated branch result = %#v", result)
	}
}

func TestStateAPIsRequirePersistence(t *testing.T) {
	builder := graph.NewStateGraph(testReducer)
	addNode(t, builder, "only", func(
		context.Context,
		testState,
		graph.Runtime,
	) (graph.Command[testDelta], error) {
		return graph.NoCommand[testDelta](), nil
	})
	addEdge(t, builder, graph.START, "only")
	addEdge(t, builder, "only", graph.END)
	compiled := compileGraph(t, builder)
	_, err := compiled.GetState(context.Background(), graph.RunConfig{ThreadID: "thread"})
	if !errors.Is(err, graph.ErrCheckpointerRequired) {
		t.Fatalf("GetState error = %v", err)
	}
}

func TestInvokeHistoricalCheckpointCreatesForkAndRerunsFollowingNodes(t *testing.T) {
	saver := memory.NewSaver()
	builder := graph.NewStateGraph(testReducer)
	var firstCalls atomic.Int32
	var secondCalls atomic.Int32
	addNode(t, builder, "first", func(
		context.Context,
		testState,
		graph.Runtime,
	) (graph.Command[testDelta], error) {
		firstCalls.Add(1)
		return graph.Update(testDelta{Add: 1, Label: "first"}), nil
	})
	addNode(t, builder, "second", func(
		context.Context,
		testState,
		graph.Runtime,
	) (graph.Command[testDelta], error) {
		secondCalls.Add(1)
		return graph.Update(testDelta{Add: 2, Label: "second"}), nil
	})
	addEdge(t, builder, graph.START, "first")
	addEdge(t, builder, "first", "second")
	addEdge(t, builder, "second", graph.END)
	compiled := compilePersistentGraph(t, builder, saver)
	config := graph.RunConfig{ThreadID: "thread-replay"}
	if _, err := compiled.Invoke(context.Background(), testState{}, config); err != nil {
		t.Fatal(err)
	}
	original, err := compiled.GetStateHistory(
		context.Background(),
		config,
		graph.StateHistoryOptions{},
	)
	if err != nil {
		t.Fatal(err)
	}
	beforeSecond := original[1]

	replayed, err := compiled.Invoke(context.Background(), testState{Total: 100}, graph.RunConfig{
		ThreadID:     config.ThreadID,
		CheckpointID: beforeSecond.Config.CheckpointID,
	})
	if err != nil {
		t.Fatalf("Invoke(historical): %v", err)
	}
	if replayed.Total != 3 || firstCalls.Load() != 1 || secondCalls.Load() != 2 {
		t.Fatalf(
			"replayed=%#v firstCalls=%d secondCalls=%d",
			replayed,
			firstCalls.Load(),
			secondCalls.Load(),
		)
	}

	history, err := compiled.GetStateHistory(
		context.Background(),
		config,
		graph.StateHistoryOptions{},
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(history) != len(original)+2 {
		t.Fatalf("history len = %d, want %d", len(history), len(original)+2)
	}
	fork := history[1]
	if fork.Metadata["source"] != string(checkpoint.SourceFork) ||
		fork.ParentConfig == nil ||
		*fork.ParentConfig != beforeSecond.Config ||
		!reflect.DeepEqual(fork.Next, []graph.NodeID{"second"}) {
		t.Fatalf("fork snapshot = %#v", fork)
	}
}
