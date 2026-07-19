package graph_test

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"testing"

	"github.com/wahanbo/langgraph-go/checkpoint"
	checkpointmemory "github.com/wahanbo/langgraph-go/checkpoint/memory"
	"github.com/wahanbo/langgraph-go/graph"
)

func TestChannelSchemaTracksTaskReadsAndVersionsSeen(t *testing.T) {
	builder := graph.NewStateGraph(projectedChannelReducer)
	totalBinding, err := graph.BindCheckpointChannel(
		"total",
		checkpoint.MustJSONCodec[int]("tests/runtime-channel-total", 1),
		func(state projectedChannelState) int { return state.Total },
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := builder.SetCheckpointChannelSchema(totalBinding); err != nil {
		t.Fatal(err)
	}
	if err := builder.AddNode("a", func(context.Context, projectedChannelState, graph.Runtime) (graph.Command[projectedChannelDelta], error) {
		return graph.Update(projectedChannelDelta{Add: 1}), nil
	}, graph.WithChannelReads("total")); err != nil {
		t.Fatal(err)
	}
	if err := builder.AddNode("b", func(context.Context, projectedChannelState, graph.Runtime) (graph.Command[projectedChannelDelta], error) {
		return graph.NoCommand[projectedChannelDelta](), nil
	}, graph.WithChannelReads("total")); err != nil {
		t.Fatal(err)
	}
	for _, edge := range [][2]graph.NodeID{{graph.START, "a"}, {"a", "b"}, {"b", graph.END}} {
		if err := builder.AddEdge(edge[0], edge[1]); err != nil {
			t.Fatal(err)
		}
	}
	saver := checkpointmemory.NewSaver()
	compiled, err := builder.Compile(graph.WithPersistence(graph.PersistenceConfig[projectedChannelState, projectedChannelDelta]{
		Saver:      saver,
		StateCodec: checkpoint.MustJSONCodec[projectedChannelState]("tests/runtime-channel-state", 1),
		DeltaCodec: checkpoint.MustJSONCodec[projectedChannelDelta]("tests/runtime-channel-delta", 1),
	}))
	if err != nil {
		t.Fatal(err)
	}
	config := graph.RunConfig{ThreadID: "runtime-channel-schema"}
	if _, err := compiled.Invoke(context.Background(), projectedChannelState{}, config); err != nil {
		t.Fatal(err)
	}
	history, err := saver.List(context.Background(), checkpoint.ListOptions{Config: &checkpoint.Config{ThreadID: config.ThreadID}})
	if err != nil || len(history) != 3 {
		t.Fatalf("history len=%d err=%v", len(history), err)
	}
	latest, afterA, input := history[0].Checkpoint, history[1].Checkpoint, history[2].Checkpoint
	wantReads := []string{checkpoint.StateChannel, "total"}
	if !reflect.DeepEqual(input.Next[0].ReadChannels, wantReads) || !reflect.DeepEqual(afterA.Next[0].ReadChannels, wantReads) {
		t.Fatalf("input reads=%v after-a reads=%v", input.Next[0].ReadChannels, afterA.Next[0].ReadChannels)
	}
	if afterA.VersionsSeen["a"]["total"] != input.ChannelVersions["total"] ||
		afterA.VersionsSeen["a"][checkpoint.StateChannel] != input.ChannelVersions[checkpoint.StateChannel] {
		t.Fatalf("a seen=%v input versions=%v", afterA.VersionsSeen["a"], input.ChannelVersions)
	}
	if latest.VersionsSeen["b"]["total"] != afterA.ChannelVersions["total"] ||
		latest.VersionsSeen["b"][checkpoint.StateChannel] != afterA.ChannelVersions[checkpoint.StateChannel] {
		t.Fatalf("b seen=%v after-a versions=%v", latest.VersionsSeen["b"], afterA.ChannelVersions)
	}
}

func TestChannelSchemaRejectsUnknownNodeReadAtCompile(t *testing.T) {
	builder := graph.NewStateGraph(projectedChannelReducer)
	if err := builder.AddNode("node", func(context.Context, projectedChannelState, graph.Runtime) (graph.Command[projectedChannelDelta], error) {
		return graph.NoCommand[projectedChannelDelta](), nil
	}, graph.WithChannelReads("missing")); err != nil {
		t.Fatal(err)
	}
	_ = builder.AddEdge(graph.START, "node")
	_ = builder.AddEdge("node", graph.END)
	_, err := builder.Compile()
	if !errors.Is(err, graph.ErrInvalidGraph) || !errors.Is(err, graph.ErrCheckpointChannel) {
		t.Fatalf("Compile() err=%v", err)
	}
}

func TestCheckpointCloneIsolatesTaskReadChannels(t *testing.T) {
	source := checkpoint.Checkpoint{Next: []checkpoint.Task{{ID: "task", Name: "node", ReadChannels: []string{"a"}}}}
	cloned := checkpoint.CloneCheckpoint(source)
	cloned.Next[0].ReadChannels[0] = "mutated"
	if source.Next[0].ReadChannels[0] != "a" {
		t.Fatalf("source reads=%v", source.Next[0].ReadChannels)
	}
}

func TestChannelTriggerSchedulesOnlyFreshVersions(t *testing.T) {
	builder := graph.NewStateGraph(projectedChannelReducer)
	total, err := graph.BindCheckpointChannel(
		"total",
		checkpoint.MustJSONCodec[int]("tests/runtime-channel-trigger-total", 1),
		func(state projectedChannelState) int { return state.Total },
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := builder.SetCheckpointChannelSchema(total); err != nil {
		t.Fatal(err)
	}
	calls := 0
	if err := builder.AddNode("loop", func(_ context.Context, state projectedChannelState, _ graph.Runtime) (graph.Command[projectedChannelDelta], error) {
		calls++
		if state.Total < 2 {
			return graph.Update(projectedChannelDelta{Add: 1}), nil
		}
		return graph.NoCommand[projectedChannelDelta](), nil
	}, graph.WithChannelTriggers("total")); err != nil {
		t.Fatal(err)
	}
	for _, edge := range [][2]graph.NodeID{{graph.START, "loop"}, {"loop", "loop"}, {"loop", graph.END}} {
		if err := builder.AddEdge(edge[0], edge[1]); err != nil {
			t.Fatal(err)
		}
	}
	compiled, err := builder.Compile(graph.WithPersistence(graph.PersistenceConfig[projectedChannelState, projectedChannelDelta]{
		Saver:      checkpointmemory.NewSaver(),
		StateCodec: checkpoint.MustJSONCodec[projectedChannelState]("tests/runtime-channel-trigger-state", 1),
		DeltaCodec: checkpoint.MustJSONCodec[projectedChannelDelta]("tests/runtime-channel-trigger-delta", 1),
	}))
	if err != nil {
		t.Fatal(err)
	}
	state, err := compiled.Invoke(context.Background(), projectedChannelState{}, graph.RunConfig{ThreadID: "runtime-channel-trigger", RecursionLimit: 10})
	if err != nil {
		t.Fatal(err)
	}
	if state.Total != 2 || calls != 3 {
		t.Fatalf("state=%+v calls=%d", state, calls)
	}
}

func TestChannelTriggerRejectsUnknownChannelAtCompile(t *testing.T) {
	builder := graph.NewStateGraph(projectedChannelReducer)
	if err := builder.AddNode("node", func(context.Context, projectedChannelState, graph.Runtime) (graph.Command[projectedChannelDelta], error) {
		return graph.NoCommand[projectedChannelDelta](), nil
	}, graph.WithChannelTriggers("missing")); err != nil {
		t.Fatal(err)
	}
	_ = builder.AddEdge(graph.START, "node")
	_ = builder.AddEdge("node", graph.END)
	_, err := builder.Compile()
	if !errors.Is(err, graph.ErrInvalidGraph) || !errors.Is(err, graph.ErrCheckpointChannel) {
		t.Fatalf("Compile() err=%v", err)
	}
}

func TestChannelFreshnessAcrossNestedWaitingBarriers(t *testing.T) {
	builder := graph.NewStateGraph(projectedChannelReducer)
	total, err := graph.BindCheckpointChannel(
		"total",
		checkpoint.MustJSONCodec[int]("tests/nested-barrier-total", 1),
		func(state projectedChannelState) int { return state.Total },
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := builder.SetCheckpointChannelSchema(total); err != nil {
		t.Fatal(err)
	}
	calls := map[graph.NodeID]int{}
	var callsMu sync.Mutex
	pass := func(id graph.NodeID) graph.Node[projectedChannelState, projectedChannelDelta] {
		return func(context.Context, projectedChannelState, graph.Runtime) (graph.Command[projectedChannelDelta], error) {
			callsMu.Lock()
			calls[id]++
			callsMu.Unlock()
			return graph.NoCommand[projectedChannelDelta](), nil
		}
	}
	for _, id := range []graph.NodeID{"left", "right", "inner-left", "inner-right", "gate"} {
		if err := builder.AddNode(id, pass(id)); err != nil {
			t.Fatal(err)
		}
	}
	if err := builder.AddNode("join", func(_ context.Context, state projectedChannelState, _ graph.Runtime) (graph.Command[projectedChannelDelta], error) {
		callsMu.Lock()
		calls["join"]++
		callsMu.Unlock()
		if state.Total < 2 {
			return graph.Update(projectedChannelDelta{Add: 1}), nil
		}
		return graph.NoCommand[projectedChannelDelta](), nil
	}, graph.WithChannelTriggers("total")); err != nil {
		t.Fatal(err)
	}
	for _, edge := range [][2]graph.NodeID{
		{graph.START, "left"}, {graph.START, "right"},
		{"join", "inner-left"}, {"join", "inner-right"},
		{"gate", "left"}, {"gate", "right"}, {"gate", graph.END},
	} {
		if err := builder.AddEdge(edge[0], edge[1]); err != nil {
			t.Fatal(err)
		}
	}
	if err := builder.AddWaitingEdge([]graph.NodeID{"left", "right"}, "join"); err != nil {
		t.Fatal(err)
	}
	if err := builder.AddWaitingEdge([]graph.NodeID{"inner-left", "inner-right"}, "gate"); err != nil {
		t.Fatal(err)
	}
	compiled, err := builder.Compile(graph.WithPersistence(graph.PersistenceConfig[projectedChannelState, projectedChannelDelta]{
		Saver:      checkpointmemory.NewSaver(),
		StateCodec: checkpoint.MustJSONCodec[projectedChannelState]("tests/nested-barrier-state", 1),
		DeltaCodec: checkpoint.MustJSONCodec[projectedChannelDelta]("tests/nested-barrier-delta", 1),
	}))
	if err != nil {
		t.Fatal(err)
	}
	state, err := compiled.Invoke(context.Background(), projectedChannelState{}, graph.RunConfig{ThreadID: "nested-barrier", RecursionLimit: 20})
	if err != nil {
		t.Fatal(err)
	}
	want := map[graph.NodeID]int{"left": 4, "right": 4, "join": 3, "inner-left": 3, "inner-right": 3, "gate": 3}
	if state.Total != 2 || !reflect.DeepEqual(calls, want) {
		t.Fatalf("state=%+v calls=%v want=%v", state, calls, want)
	}
}
