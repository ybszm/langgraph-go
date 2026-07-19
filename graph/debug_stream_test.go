package graph_test

import (
	"context"
	"errors"
	"reflect"
	"sort"
	"sync/atomic"
	"testing"

	cachememory "github.com/wahanbo/langgraph-go/cache/memory"
	"github.com/wahanbo/langgraph-go/checkpoint"
	checkpointmemory "github.com/wahanbo/langgraph-go/checkpoint/memory"
	"github.com/wahanbo/langgraph-go/graph"
)

func TestDebugStreamTaskResultAndCheckpointLifecycle(t *testing.T) {
	builder := graph.NewStateGraph(customReducer)
	for _, id := range []graph.NodeID{"a", "b"} {
		id := id
		if err := builder.AddNode(id, func(_ context.Context, _ customState, _ graph.Runtime) (graph.Command[customDelta], error) {
			return graph.Update(customDelta{Add: 1}), nil
		}); err != nil {
			t.Fatal(err)
		}
		_ = builder.AddEdge(graph.START, id)
		_ = builder.AddEdge(id, graph.END)
	}
	compiled, err := builder.Compile()
	if err != nil {
		t.Fatal(err)
	}
	events := compiled.StreamWithOptions(context.Background(), customState{}, graph.RunConfig{}, graph.StreamOptions{
		Modes: []graph.StreamMode{graph.StreamDebug}, Buffer: 8,
	})
	var starts, results []string
	var checkpoints []*graph.DebugEvent
	for event := range events {
		if event.Debug == nil || event.Mode != graph.StreamDebug {
			t.Fatalf("invalid debug envelope: %+v", event)
		}
		switch event.Debug.Kind {
		case graph.DebugTask:
			starts = append(starts, string(event.Debug.Node))
		case graph.DebugTaskResult:
			results = append(results, string(event.Debug.Node))
		case graph.DebugCheckpoint:
			checkpoints = append(checkpoints, event.Debug)
		}
	}
	if !reflect.DeepEqual(starts, []string{"a", "b"}) {
		t.Fatalf("task start order=%v", starts)
	}
	sort.Strings(results)
	if !reflect.DeepEqual(results, []string{"a", "b"}) {
		t.Fatalf("task results=%v", results)
	}
	if len(checkpoints) != 1 || checkpoints[0].Step != 0 || len(checkpoints[0].Next) != 0 {
		t.Fatalf("checkpoint events=%+v", checkpoints)
	}
}

func TestDebugTaskPayloadIncludesInputResultAndRunMetadata(t *testing.T) {
	builder := graph.NewStateGraph(customReducer)
	_ = builder.AddNode("node", func(_ context.Context, _ customState, _ graph.Runtime) (graph.Command[customDelta], error) {
		return graph.Update(customDelta{Add: 3}), nil
	})
	_ = builder.AddEdge(graph.START, "node")
	_ = builder.AddEdge("node", graph.END)
	compiled, err := builder.Compile()
	if err != nil {
		t.Fatal(err)
	}
	metadata := map[string]any{"request": "original", "nested": map[string]any{"value": 1}}
	var start, result *graph.DebugEvent
	for event := range compiled.StreamWithOptions(context.Background(), customState{Count: 4}, graph.RunConfig{Metadata: metadata}, graph.StreamOptions{
		Modes: []graph.StreamMode{graph.StreamDebug},
	}) {
		if event.Err != nil {
			t.Fatal(event.Err)
		}
		if event.Debug == nil {
			continue
		}
		switch event.Debug.Kind {
		case graph.DebugTask:
			start = event.Debug
		case graph.DebugTaskResult:
			result = event.Debug
		}
	}
	metadata["request"] = "mutated"
	metadata["nested"].(map[string]any)["value"] = 2
	if start == nil || !reflect.DeepEqual(start.Input, customState{Count: 4}) {
		t.Fatalf("task start=%+v", start)
	}
	if result == nil || !reflect.DeepEqual(result.Result, customDelta{Add: 3}) {
		t.Fatalf("task result=%+v", result)
	}
	if !reflect.DeepEqual(start.Metadata, map[string]any{"request": "original", "nested": map[string]any{"value": 1}}) {
		t.Fatalf("metadata=%v", start.Metadata)
	}
}

func TestDebugCheckpointPayloadIncludesValuesMetadataTasksAndParent(t *testing.T) {
	builder := graph.NewStateGraph(customReducer)
	for _, name := range []graph.NodeID{"first", "second"} {
		_ = builder.AddNode(name, func(_ context.Context, _ customState, _ graph.Runtime) (graph.Command[customDelta], error) {
			return graph.Update(customDelta{Add: 1}), nil
		})
	}
	_ = builder.AddEdge(graph.START, "first")
	_ = builder.AddEdge("first", "second")
	_ = builder.AddEdge("second", graph.END)
	compiled, err := builder.Compile()
	if err != nil {
		t.Fatal(err)
	}
	var first *graph.DebugEvent
	for event := range compiled.StreamWithOptions(context.Background(), customState{}, graph.RunConfig{RunID: "debug-run"}, graph.StreamOptions{
		Modes: []graph.StreamMode{graph.StreamDebug},
	}) {
		if event.Err != nil {
			t.Fatal(event.Err)
		}
		if event.Debug != nil && event.Debug.Kind == graph.DebugCheckpoint && event.Debug.Step == 0 {
			first = event.Debug
		}
	}
	if first == nil || !reflect.DeepEqual(first.Values, customState{Count: 1}) {
		t.Fatalf("checkpoint=%+v", first)
	}
	if first.Metadata["source"] != "loop" || first.Metadata["step"] != 0 || first.Metadata["run_id"] != "debug-run" {
		t.Fatalf("metadata=%v", first.Metadata)
	}
	if len(first.Tasks) != 1 || first.Tasks[0].Node != "second" || first.Tasks[0].TaskID != "step:1:task:0:node:second" {
		t.Fatalf("tasks=%+v", first.Tasks)
	}
	if first.ParentCheckpoint.CheckpointID != "" {
		t.Fatalf("parent=%+v", first.ParentCheckpoint)
	}
}

func TestPersistentDebugCheckpointCarriesParentConfig(t *testing.T) {
	builder := graph.NewStateGraph(customReducer)
	_ = builder.AddNode("node", func(context.Context, customState, graph.Runtime) (graph.Command[customDelta], error) {
		return graph.Update(customDelta{Add: 1}), nil
	})
	_ = builder.AddEdge(graph.START, "node")
	_ = builder.AddEdge("node", graph.END)
	compiled, err := builder.Compile(graph.WithPersistence(graph.PersistenceConfig[customState, customDelta]{
		Saver:       checkpointmemory.NewSaver(),
		StateCodec:  checkpoint.MustJSONCodec[customState]("tests.debug-state", 1),
		DeltaCodec:  checkpoint.MustJSONCodec[customDelta]("tests.debug-delta", 1),
		IDGenerator: &sequenceIDGenerator{},
	}))
	if err != nil {
		t.Fatal(err)
	}
	var boundary *graph.DebugEvent
	for event := range compiled.StreamWithOptions(context.Background(), customState{}, graph.RunConfig{ThreadID: "debug-parent"}, graph.StreamOptions{
		Modes: []graph.StreamMode{graph.StreamDebug},
	}) {
		if event.Err != nil {
			t.Fatal(event.Err)
		}
		if event.Debug != nil && event.Debug.Kind == graph.DebugCheckpoint {
			boundary = event.Debug
		}
	}
	if boundary == nil || boundary.Checkpoint.CheckpointID == "" || boundary.ParentCheckpoint.CheckpointID == "" {
		t.Fatalf("boundary=%+v", boundary)
	}
	if boundary.Checkpoint.CheckpointID == boundary.ParentCheckpoint.CheckpointID || boundary.ParentCheckpoint.ThreadID != "debug-parent" {
		t.Fatalf("checkpoint=%+v parent=%+v", boundary.Checkpoint, boundary.ParentCheckpoint)
	}
}

func TestDebugTaskTriggersPreserveFanInProvenance(t *testing.T) {
	builder := graph.NewStateGraph(customReducer)
	for _, name := range []graph.NodeID{"a", "b", "c"} {
		_ = builder.AddNode(name, func(context.Context, customState, graph.Runtime) (graph.Command[customDelta], error) {
			return graph.NoCommand[customDelta](), nil
		})
	}
	_ = builder.AddEdge(graph.START, "a")
	_ = builder.AddEdge(graph.START, "b")
	_ = builder.AddEdge("a", "c")
	_ = builder.AddEdge("b", "c")
	_ = builder.AddEdge("c", graph.END)
	compiled, err := builder.Compile()
	if err != nil {
		t.Fatal(err)
	}
	triggers := make(map[graph.NodeID][]graph.NodeID)
	for event := range compiled.StreamWithOptions(context.Background(), customState{}, graph.RunConfig{}, graph.StreamOptions{
		Modes: []graph.StreamMode{graph.StreamDebug},
	}) {
		if event.Err != nil {
			t.Fatal(event.Err)
		}
		if event.Debug != nil && event.Debug.Kind == graph.DebugTask {
			triggers[event.Debug.Node] = event.Debug.Triggers
		}
	}
	want := map[graph.NodeID][]graph.NodeID{
		"a": {"branch:to:a"},
		"b": {"branch:to:b"},
		"c": {"branch:to:c"},
	}
	if !reflect.DeepEqual(triggers, want) {
		t.Fatalf("triggers=%v", triggers)
	}
}

func TestDebugTaskTriggersSurviveCheckpointRecovery(t *testing.T) {
	builder := graph.NewStateGraph(customReducer)
	var firstCalls atomic.Int32
	var secondCalls atomic.Int32
	_ = builder.AddNode("first", func(context.Context, customState, graph.Runtime) (graph.Command[customDelta], error) {
		firstCalls.Add(1)
		return graph.NoCommand[customDelta](), nil
	})
	_ = builder.AddNode("second", func(context.Context, customState, graph.Runtime) (graph.Command[customDelta], error) {
		if secondCalls.Add(1) == 1 {
			return graph.NoCommand[customDelta](), errors.New("transient")
		}
		return graph.NoCommand[customDelta](), nil
	})
	_ = builder.AddEdge(graph.START, "first")
	_ = builder.AddEdge("first", "second")
	_ = builder.AddEdge("second", graph.END)
	compiled, err := builder.Compile(graph.WithPersistence(graph.PersistenceConfig[customState, customDelta]{
		Saver:       checkpointmemory.NewSaver(),
		StateCodec:  checkpoint.MustJSONCodec[customState]("tests.debug-trigger-state", 1),
		DeltaCodec:  checkpoint.MustJSONCodec[customDelta]("tests.debug-trigger-delta", 1),
		IDGenerator: &sequenceIDGenerator{},
	}))
	if err != nil {
		t.Fatal(err)
	}
	config := graph.RunConfig{ThreadID: "debug-trigger-recovery"}
	if _, err := compiled.Invoke(context.Background(), customState{}, config); err == nil {
		t.Fatal("first Invoke succeeded")
	}
	var triggers []graph.NodeID
	for event := range compiled.StreamWithOptions(context.Background(), customState{}, config, graph.StreamOptions{Modes: []graph.StreamMode{graph.StreamDebug}}) {
		if event.Err != nil {
			t.Fatal(event.Err)
		}
		if event.Debug != nil && event.Debug.Kind == graph.DebugTask && event.Debug.Node == "second" {
			triggers = event.Debug.Triggers
		}
	}
	if !reflect.DeepEqual(triggers, []graph.NodeID{"branch:to:second"}) || firstCalls.Load() != 1 || secondCalls.Load() != 2 {
		t.Fatalf("triggers=%v first=%d second=%d", triggers, firstCalls.Load(), secondCalls.Load())
	}
}

func TestDebugCheckpointProjectsPendingResultsAndInterrupts(t *testing.T) {
	builder := graph.NewStateGraph(customReducer)
	_ = builder.AddNode("completed", func(context.Context, customState, graph.Runtime) (graph.Command[customDelta], error) {
		return graph.Update(customDelta{Add: 1}), nil
	})
	if err := builder.AddNode("paused", func(_ context.Context, _ customState, runtime graph.Runtime) (graph.Command[customDelta], error) {
		_, err := graph.AwaitResume[string](runtime, "continue?")
		return graph.NoCommand[customDelta](), err
	}, graph.WithDynamicInterrupts()); err != nil {
		t.Fatal(err)
	}
	for _, node := range []graph.NodeID{"completed", "paused"} {
		_ = builder.AddEdge(graph.START, node)
		_ = builder.AddEdge(node, graph.END)
	}
	compiled, err := builder.Compile(graph.WithPersistence(graph.PersistenceConfig[customState, customDelta]{
		Saver:      checkpointmemory.NewSaver(),
		StateCodec: checkpoint.MustJSONCodec[customState]("tests.debug-pending-state", 1),
		DeltaCodec: checkpoint.MustJSONCodec[customDelta]("tests.debug-pending-delta", 1),
	}))
	if err != nil {
		t.Fatal(err)
	}
	var boundary *graph.DebugEvent
	var streamErr error
	for event := range compiled.StreamWithOptions(context.Background(), customState{}, graph.RunConfig{ThreadID: "debug-pending"}, graph.StreamOptions{Modes: []graph.StreamMode{graph.StreamDebug}}) {
		if event.Debug != nil && event.Debug.Kind == graph.DebugCheckpoint {
			boundary = event.Debug
		}
		if event.Err != nil {
			streamErr = event.Err
		}
	}
	if streamErr != nil || boundary == nil || len(boundary.Tasks) != 2 {
		t.Fatalf("boundary=%+v err=%v", boundary, streamErr)
	}
	projected := make(map[graph.NodeID]graph.DebugTaskSnapshot)
	for _, task := range boundary.Tasks {
		projected[task.Node] = task
	}
	command, ok := projected["completed"].Result.(graph.Command[customDelta])
	if !ok || !command.HasUpdate || command.Update.Add != 1 || len(projected["paused"].Interrupts) != 1 ||
		!reflect.DeepEqual(boundary.Next, []graph.NodeID{"paused"}) {
		t.Fatalf("boundary=%+v", boundary)
	}
}

func TestDebugStreamMarksCachedTaskResults(t *testing.T) {
	builder := graph.NewStateGraph(customReducer)
	if err := builder.AddNode("cached", func(_ context.Context, _ customState, _ graph.Runtime) (graph.Command[customDelta], error) {
		return graph.Update(customDelta{Add: 1}), nil
	}, graph.WithCachePolicy(graph.CachePolicy[customState]{})); err != nil {
		t.Fatal(err)
	}
	_ = builder.AddEdge(graph.START, "cached")
	_ = builder.AddEdge("cached", graph.END)
	compiled, err := builder.Compile(graph.WithTaskCache[customState, customDelta](graph.TaskCacheConfig[customDelta]{
		Store: cachememory.New(), Namespace: "debug", DeltaCodec: checkpoint.MustJSONCodec[customDelta]("tests.debug-delta", 1),
	}))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := compiled.Invoke(context.Background(), customState{}, graph.RunConfig{}); err != nil {
		t.Fatal(err)
	}
	events := compiled.StreamWithOptions(context.Background(), customState{}, graph.RunConfig{}, graph.StreamOptions{Modes: []graph.StreamMode{graph.StreamDebug}})
	found := false
	for event := range events {
		if event.Debug != nil && event.Debug.Kind == graph.DebugTaskResult {
			found = true
			if !event.Debug.Cached {
				t.Fatalf("cached result not marked: %+v", event.Debug)
			}
			if !reflect.DeepEqual(event.Debug.Result, customDelta{Add: 1}) {
				t.Fatalf("cached result payload=%#v", event.Debug.Result)
			}
		}
	}
	if !found {
		t.Fatal("no task_result debug event")
	}
}

func TestLegacyStreamDoesNotEnableDebugByDefault(t *testing.T) {
	builder := graph.NewStateGraph(customReducer)
	_ = builder.AddNode("node", func(_ context.Context, _ customState, _ graph.Runtime) (graph.Command[customDelta], error) {
		return graph.NoCommand[customDelta](), nil
	})
	_ = builder.AddEdge(graph.START, "node")
	_ = builder.AddEdge("node", graph.END)
	compiled, _ := builder.Compile()
	for event := range compiled.Stream(context.Background(), customState{}, graph.RunConfig{}) {
		if event.Mode == graph.StreamDebug || event.Debug != nil {
			t.Fatalf("legacy Stream unexpectedly emitted debug: %+v", event)
		}
	}
}
