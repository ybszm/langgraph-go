package graph_test

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/ybszm/langgraph-go/checkpoint"
	checkpointmemory "github.com/ybszm/langgraph-go/checkpoint/memory"
	"github.com/ybszm/langgraph-go/graph"
)

type projectedChannelState struct {
	Total int
	Flag  bool
}

type projectedChannelDelta struct{ Add int }

func projectedChannelReducer(_ context.Context, state projectedChannelState, updates []projectedChannelDelta) (projectedChannelState, error) {
	for _, update := range updates {
		state.Total += update.Add
	}
	return state, nil
}

func TestCheckpointChannelsPreserveVersionsForUnchangedValues(t *testing.T) {
	builder := graph.NewStateGraph(projectedChannelReducer)
	intCodec := checkpoint.MustJSONCodec[int]("tests/projected-total", 1)
	boolCodec := checkpoint.MustJSONCodec[bool]("tests/projected-flag", 1)
	builder.SetCheckpointChannels(func(_ context.Context, state projectedChannelState) (map[string]checkpoint.EncodedValue, error) {
		total, err := intCodec.Encode(state.Total)
		if err != nil {
			return nil, err
		}
		values := map[string]checkpoint.EncodedValue{"total": total}
		if state.Flag {
			flag, err := boolCodec.Encode(true)
			if err != nil {
				return nil, err
			}
			values["flag"] = flag
		}
		return values, nil
	})
	if err := builder.AddNode("change", func(context.Context, projectedChannelState, graph.Runtime) (graph.Command[projectedChannelDelta], error) {
		return graph.Update(projectedChannelDelta{Add: 1}), nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := builder.AddNode("noop", func(context.Context, projectedChannelState, graph.Runtime) (graph.Command[projectedChannelDelta], error) {
		return graph.NoCommand[projectedChannelDelta](), nil
	}); err != nil {
		t.Fatal(err)
	}
	for _, edge := range [][2]graph.NodeID{{graph.START, "change"}, {"change", "noop"}, {"noop", graph.END}} {
		if err := builder.AddEdge(edge[0], edge[1]); err != nil {
			t.Fatal(err)
		}
	}
	saver := checkpointmemory.NewSaver()
	compiled, err := builder.Compile(graph.WithPersistence(graph.PersistenceConfig[projectedChannelState, projectedChannelDelta]{
		Saver:      saver,
		StateCodec: checkpoint.MustJSONCodec[projectedChannelState]("tests/projected-state", 1),
		DeltaCodec: checkpoint.MustJSONCodec[projectedChannelDelta]("tests/projected-delta", 1),
	}))
	if err != nil {
		t.Fatal(err)
	}
	config := graph.RunConfig{ThreadID: "projected-channels"}
	if _, err := compiled.Invoke(context.Background(), projectedChannelState{Flag: true}, config); err != nil {
		t.Fatal(err)
	}
	history, err := saver.List(context.Background(), checkpoint.ListOptions{Config: &checkpoint.Config{ThreadID: config.ThreadID}})
	if err != nil || len(history) != 3 {
		t.Fatalf("history len=%d err=%v", len(history), err)
	}
	latest, changed, input := history[0].Checkpoint, history[1].Checkpoint, history[2].Checkpoint
	if len(latest.UpdatedChannels) != 0 {
		t.Fatalf("noop updated channels=%v", latest.UpdatedChannels)
	}
	if !reflect.DeepEqual(latest.ChannelVersions, changed.ChannelVersions) {
		t.Fatalf("noop versions=%v previous=%v", latest.ChannelVersions, changed.ChannelVersions)
	}
	if !reflect.DeepEqual(changed.UpdatedChannels, []string{checkpoint.StateChannel, "total"}) {
		t.Fatalf("changed channels=%v", changed.UpdatedChannels)
	}
	if changed.ChannelVersions["flag"] != input.ChannelVersions["flag"] ||
		changed.ChannelVersions["total"] == input.ChannelVersions["total"] ||
		changed.ChannelVersions[checkpoint.StateChannel] == input.ChannelVersions[checkpoint.StateChannel] {
		t.Fatalf("input versions=%v changed=%v", input.ChannelVersions, changed.ChannelVersions)
	}
	decodedTotal, err := intCodec.Decode(latest.Values["total"])
	if err != nil || decodedTotal != 1 {
		t.Fatalf("projected total=%d err=%v values=%v", decodedTotal, err, latest.Values)
	}
}

func TestCheckpointChannelRemovalWritesTombstoneVersion(t *testing.T) {
	builder := graph.NewStateGraph(projectedChannelReducer)
	boolCodec := checkpoint.MustJSONCodec[bool]("tests/projected-remove", 1)
	builder.SetCheckpointChannels(func(_ context.Context, state projectedChannelState) (map[string]checkpoint.EncodedValue, error) {
		if !state.Flag {
			return nil, nil
		}
		value, err := boolCodec.Encode(true)
		return map[string]checkpoint.EncodedValue{"flag": value}, err
	})
	if err := builder.AddNode("remove", func(context.Context, projectedChannelState, graph.Runtime) (graph.Command[projectedChannelDelta], error) {
		return graph.NoCommand[projectedChannelDelta](), nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := builder.AddEdge(graph.START, "remove"); err != nil {
		t.Fatal(err)
	}
	if err := builder.AddEdge("remove", graph.END); err != nil {
		t.Fatal(err)
	}
	// Input merger clears Flag when the completed thread starts a new run,
	// causing the projected channel to disappear while canonical state changes.
	builder.SetInputMerger(func(_ context.Context, _ projectedChannelState, input projectedChannelState) (projectedChannelState, error) {
		return input, nil
	})
	saver := checkpointmemory.NewSaver()
	compiled, err := builder.Compile(graph.WithPersistence(graph.PersistenceConfig[projectedChannelState, projectedChannelDelta]{
		Saver:      saver,
		StateCodec: checkpoint.MustJSONCodec[projectedChannelState]("tests/projected-remove-state", 1),
		DeltaCodec: checkpoint.MustJSONCodec[projectedChannelDelta]("tests/projected-remove-delta", 1),
	}))
	if err != nil {
		t.Fatal(err)
	}
	config := graph.RunConfig{ThreadID: "projected-remove"}
	if _, err := compiled.Invoke(context.Background(), projectedChannelState{Flag: true}, config); err != nil {
		t.Fatal(err)
	}
	config.NewRun = true
	if _, err := compiled.Invoke(context.Background(), projectedChannelState{}, config); err != nil {
		t.Fatal(err)
	}
	history, err := saver.List(context.Background(), checkpoint.ListOptions{Config: &checkpoint.Config{ThreadID: config.ThreadID}})
	if err != nil || len(history) < 2 {
		t.Fatalf("history len=%d err=%v", len(history), err)
	}
	var removed checkpoint.Checkpoint
	for _, tuple := range history {
		if containsString(tuple.Checkpoint.UpdatedChannels, "flag") {
			if _, present := tuple.Checkpoint.Values["flag"]; !present {
				removed = tuple.Checkpoint
				break
			}
		}
	}
	if removed.ID == "" || removed.ChannelVersions["flag"] == "" {
		t.Fatalf("no flag tombstone checkpoint in history=%+v", history)
	}
}

func TestCheckpointChannelRejectsReservedNames(t *testing.T) {
	builder := graph.NewStateGraph(projectedChannelReducer)
	builder.SetCheckpointChannels(func(context.Context, projectedChannelState) (map[string]checkpoint.EncodedValue, error) {
		return map[string]checkpoint.EncodedValue{checkpoint.StateChannel: {Type: "bad", Version: 1}}, nil
	})
	if err := builder.AddNode("node", func(context.Context, projectedChannelState, graph.Runtime) (graph.Command[projectedChannelDelta], error) {
		return graph.NoCommand[projectedChannelDelta](), nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := builder.AddEdge(graph.START, "node"); err != nil {
		t.Fatal(err)
	}
	if err := builder.AddEdge("node", graph.END); err != nil {
		t.Fatal(err)
	}
	compiled, err := builder.Compile(graph.WithPersistence(graph.PersistenceConfig[projectedChannelState, projectedChannelDelta]{
		Saver:      checkpointmemory.NewSaver(),
		StateCodec: checkpoint.MustJSONCodec[projectedChannelState]("tests/projected-invalid-state", 1),
		DeltaCodec: checkpoint.MustJSONCodec[projectedChannelDelta]("tests/projected-invalid-delta", 1),
	}))
	if err != nil {
		t.Fatal(err)
	}
	_, err = compiled.Invoke(context.Background(), projectedChannelState{}, graph.RunConfig{ThreadID: "projected-invalid"})
	if !errors.Is(err, graph.ErrCheckpointChannel) {
		t.Fatalf("Invoke() err=%v", err)
	}
}

func containsString(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}
