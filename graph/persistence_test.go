package graph_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ybszm/langgraph-go/checkpoint"
	"github.com/ybszm/langgraph-go/checkpoint/memory"
	"github.com/ybszm/langgraph-go/graph"
)

type sequenceIDGenerator struct {
	mu   sync.Mutex
	next int
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
