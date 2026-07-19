package graph_test

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ybszm/langgraph-go/graph"
)

func TestBatchPreservesInputOrderAndLimitsConcurrency(t *testing.T) {
	builder := graph.NewStateGraph(testReducer)
	var active atomic.Int32
	var peak atomic.Int32
	if err := builder.AddNode("node", func(ctx context.Context, state testState, _ graph.Runtime) (graph.Command[testDelta], error) {
		current := active.Add(1)
		defer active.Add(-1)
		for {
			old := peak.Load()
			if current <= old || peak.CompareAndSwap(old, current) {
				break
			}
		}
		timer := time.NewTimer(time.Duration(5-state.Total) * time.Millisecond)
		defer timer.Stop()
		select {
		case <-timer.C:
		case <-ctx.Done():
			return graph.NoCommand[testDelta](), ctx.Err()
		}
		return graph.Update(testDelta{Add: 10}), nil
	}); err != nil {
		t.Fatal(err)
	}
	addEdge(t, builder, graph.START, "node")
	addEdge(t, builder, "node", graph.END)
	compiled := compileGraph(t, builder)
	items := make([]graph.BatchItem[testState], 5)
	for index := range items {
		items[index].Input.Total = index
	}
	results, err := compiled.Batch(context.Background(), items, graph.BatchOptions{MaxConcurrency: 2})
	if err != nil {
		t.Fatal(err)
	}
	if peak.Load() != 2 {
		t.Fatalf("peak concurrency=%d", peak.Load())
	}
	got := make([]int, len(results))
	for index, result := range results {
		if result.Err != nil {
			t.Fatalf("result %d err=%v", index, result.Err)
		}
		got[index] = result.Output.Total
	}
	if !reflect.DeepEqual(got, []int{10, 11, 12, 13, 14}) {
		t.Fatalf("ordered outputs=%v", got)
	}
}

func TestBatchKeepsPerItemErrorsWithoutCancelingPeers(t *testing.T) {
	itemErr := errors.New("batch item failed")
	builder := graph.NewStateGraph(testReducer)
	if err := builder.AddNode("node", func(_ context.Context, state testState, _ graph.Runtime) (graph.Command[testDelta], error) {
		if state.Total == 2 {
			return graph.NoCommand[testDelta](), itemErr
		}
		return graph.Update(testDelta{Add: 1}), nil
	}); err != nil {
		t.Fatal(err)
	}
	addEdge(t, builder, graph.START, "node")
	addEdge(t, builder, "node", graph.END)
	compiled := compileGraph(t, builder)
	items := []graph.BatchItem[testState]{
		{Input: testState{Total: 1}}, {Input: testState{Total: 2}}, {Input: testState{Total: 3}},
	}
	results, err := compiled.Batch(context.Background(), items, graph.BatchOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if results[0].Output.Total != 2 || results[0].Err != nil ||
		!errors.Is(results[1].Err, itemErr) || results[2].Output.Total != 4 || results[2].Err != nil {
		t.Fatalf("results=%+v", results)
	}
	completed, err := compiled.BatchAsCompleted(context.Background(), items, graph.BatchOptions{})
	if err != nil {
		t.Fatal(err)
	}
	seen := make(map[int]graph.BatchCompletion[testState])
	for result := range completed {
		seen[result.Index] = result
	}
	if len(seen) != 3 || seen[0].Output.Total != 2 || !errors.Is(seen[1].Err, itemErr) || seen[2].Output.Total != 4 {
		t.Fatalf("completion results=%+v", seen)
	}
}

func TestBatchValidatesOptionsAndPropagatesCanceledContext(t *testing.T) {
	builder := graph.NewStateGraph(testReducer)
	addNode(t, builder, "node", func(context.Context, testState, graph.Runtime) (graph.Command[testDelta], error) {
		return graph.NoCommand[testDelta](), nil
	})
	addEdge(t, builder, graph.START, "node")
	addEdge(t, builder, "node", graph.END)
	compiled := compileGraph(t, builder)
	if _, err := compiled.Batch(context.Background(), nil, graph.BatchOptions{MaxConcurrency: -1}); !errors.Is(err, graph.ErrInvalidRunConfig) {
		t.Fatalf("negative concurrency err=%v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	results, err := compiled.Batch(ctx, []graph.BatchItem[testState]{{}, {}}, graph.BatchOptions{})
	if err != nil || len(results) != 2 || !errors.Is(results[0].Err, context.Canceled) || !errors.Is(results[1].Err, context.Canceled) {
		t.Fatalf("canceled results=%+v err=%v", results, err)
	}
}

func TestSchemaGraphBatchMapsEachItem(t *testing.T) {
	builder := graph.NewStateGraph(schemaReducer)
	if err := builder.AddNode("node", func(context.Context, schemaState, graph.Runtime) (graph.Command[schemaDelta], error) {
		return graph.Update(schemaDelta{Add: 2}), nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := builder.AddEdge(graph.START, "node"); err != nil {
		t.Fatal(err)
	}
	if err := builder.AddEdge("node", graph.END); err != nil {
		t.Fatal(err)
	}
	compiled, err := graph.CompileSchemaGraph(builder, graph.SchemaAdapter[schemaInput, schemaState, schemaOutput]{
		Input: func(_ context.Context, input schemaInput) (schemaState, error) {
			return schemaState{Total: input.Seed}, nil
		},
		Output: func(_ context.Context, state schemaState) (schemaOutput, error) {
			return schemaOutput{Total: state.Total}, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	results, err := compiled.Batch(context.Background(), []graph.BatchItem[schemaInput]{
		{Input: schemaInput{Seed: 1}}, {Input: schemaInput{Seed: 5}},
	}, graph.BatchOptions{MaxConcurrency: 2})
	if err != nil || results[0].Output.Total != 3 || results[1].Output.Total != 7 {
		t.Fatalf("results=%+v err=%v", results, err)
	}
}

func TestBatchAsCompletedYieldsCompletionOrderAndInputIndex(t *testing.T) {
	builder := graph.NewStateGraph(testReducer)
	addNode(t, builder, "work", func(ctx context.Context, state testState, _ graph.Runtime) (graph.Command[testDelta], error) {
		select {
		case <-time.After(time.Duration(state.Total) * time.Millisecond):
			return graph.Update(testDelta{Label: fmt.Sprintf("done-%d", state.Total)}), nil
		case <-ctx.Done():
			return graph.NoCommand[testDelta](), ctx.Err()
		}
	})
	addEdge(t, builder, graph.START, "work")
	addEdge(t, builder, "work", graph.END)
	completed, err := compileGraph(t, builder).BatchAsCompleted(context.Background(), []graph.BatchItem[testState]{
		{Input: testState{Total: 60}},
		{Input: testState{Total: 10}},
		{Input: testState{Total: 20}},
	}, graph.BatchOptions{MaxConcurrency: 2})
	if err != nil {
		t.Fatal(err)
	}
	var indexes []int
	for result := range completed {
		if result.Err != nil {
			t.Fatal(result.Err)
		}
		indexes = append(indexes, result.Index)
		if len(result.Output.Path) != 1 || result.Output.Path[0] != fmt.Sprintf("done-%d", result.Output.Total) {
			t.Fatalf("completion = %#v", result)
		}
	}
	if !reflect.DeepEqual(indexes, []int{1, 2, 0}) {
		t.Fatalf("completion indexes = %v", indexes)
	}
	if _, err := compileGraph(t, builder).BatchAsCompleted(context.Background(), nil, graph.BatchOptions{MaxConcurrency: -1}); !errors.Is(err, graph.ErrInvalidRunConfig) {
		t.Fatalf("negative concurrency err=%v", err)
	}
	cancelCtx, cancel := context.WithCancel(context.Background())
	cancelCompleted, err := compileGraph(t, builder).BatchAsCompleted(cancelCtx, []graph.BatchItem[testState]{
		{Input: testState{Total: 5}}, {Input: testState{Total: 500}}, {Input: testState{Total: 500}},
	}, graph.BatchOptions{MaxConcurrency: 2})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := <-cancelCompleted; !ok {
		t.Fatal("completion channel closed before first result")
	}
	cancel()
	closed := make(chan struct{})
	go func() {
		for range cancelCompleted {
		}
		close(closed)
	}()
	select {
	case <-closed:
	case <-time.After(time.Second):
		t.Fatal("completion iterator did not close after cancellation")
	}
}
