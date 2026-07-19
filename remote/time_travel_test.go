package remote_test

import (
	"context"
	"net/http/httptest"
	"reflect"
	"testing"

	"github.com/wahanbo/langgraph-go/checkpoint"
	checkpointmemory "github.com/wahanbo/langgraph-go/checkpoint/memory"
	"github.com/wahanbo/langgraph-go/graph"
	"github.com/wahanbo/langgraph-go/remote"
)

type remoteTravelState struct {
	Total int      `json:"total"`
	Path  []string `json:"path"`
}

type remoteTravelDelta struct {
	Add   int    `json:"add"`
	Label string `json:"label"`
}

func remoteTravelReducer(_ context.Context, state remoteTravelState, deltas []remoteTravelDelta) (remoteTravelState, error) {
	result := remoteTravelState{Total: state.Total, Path: append([]string(nil), state.Path...)}
	for _, delta := range deltas {
		result.Total += delta.Add
		if delta.Label != "" {
			result.Path = append(result.Path, delta.Label)
		}
	}
	return result, nil
}

func TestRemoteTimeTravelExactUpdateForkAndReplay(t *testing.T) {
	builder := graph.NewStateGraph(remoteTravelReducer)
	if err := builder.AddNode("first", func(context.Context, remoteTravelState, graph.Runtime) (graph.Command[remoteTravelDelta], error) {
		return graph.Update(remoteTravelDelta{Add: 1, Label: "first"}), nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := builder.AddNode("second", func(context.Context, remoteTravelState, graph.Runtime) (graph.Command[remoteTravelDelta], error) {
		return graph.Update(remoteTravelDelta{Add: 2, Label: "second"}), nil
	}); err != nil {
		t.Fatal(err)
	}
	_ = builder.AddEdge(graph.START, "first")
	_ = builder.AddEdge("first", "second")
	_ = builder.AddEdge("second", graph.END)
	compiled, err := builder.Compile(graph.WithPersistence(graph.PersistenceConfig[remoteTravelState, remoteTravelDelta]{
		Saver:      checkpointmemory.NewSaver(),
		StateCodec: checkpoint.MustJSONCodec[remoteTravelState]("tests.remote-travel-state", 1),
		DeltaCodec: checkpoint.MustJSONCodec[remoteTravelDelta]("tests.remote-travel-delta", 1),
	}))
	if err != nil {
		t.Fatal(err)
	}
	handler, err := remote.NewStateServer[remoteTravelState, remoteTravelState, remoteTravelState, remoteTravelDelta](compiled, compiled, remote.ServerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer handler.Close()
	server := httptest.NewServer(handler)
	defer server.Close()
	client, _ := remote.NewStateClient[remoteTravelState, remoteTravelState, remoteTravelState, remoteTravelDelta](server.URL, server.Client())
	thread, err := client.CreateThread(context.Background(), "remote-time-travel")
	if err != nil {
		t.Fatal(err)
	}
	config := remote.RunConfig{ThreadID: thread.ID}
	initial, err := client.Invoke(context.Background(), remoteTravelState{}, config)
	if err != nil || initial.Total != 3 {
		t.Fatalf("initial=%+v err=%v", initial, err)
	}
	history, err := client.GetStateHistory(context.Background(), config, graph.StateHistoryOptions{})
	if err != nil || len(history) < 3 {
		t.Fatalf("history=%+v err=%v", history, err)
	}
	base := history[len(history)-1]
	baseBefore, err := client.GetState(context.Background(), remote.RunConfig{
		ThreadID: thread.ID, CheckpointID: base.Config.CheckpointID,
	}, false)
	if err != nil {
		t.Fatal(err)
	}
	branched, err := client.UpdateState(context.Background(), remote.RunConfig{
		ThreadID: thread.ID, CheckpointID: base.Config.CheckpointID,
	}, graph.StateUpdate[remoteTravelDelta]{Delta: remoteTravelDelta{Add: 10, Label: "manual"}, AsNode: "first"})
	if err != nil {
		t.Fatal(err)
	}
	branchSnapshot, err := client.GetState(context.Background(), remote.RunConfig{
		ThreadID: thread.ID, CheckpointID: branched.CheckpointID,
	}, false)
	if err != nil || branchSnapshot.Values.Total != 10 || !reflect.DeepEqual(branchSnapshot.Next, []graph.NodeID{"second"}) ||
		branchSnapshot.ParentConfig == nil || branchSnapshot.ParentConfig.CheckpointID != base.Config.CheckpointID {
		t.Fatalf("branch=%+v err=%v", branchSnapshot, err)
	}
	replayed, err := client.Invoke(context.Background(), remoteTravelState{Total: 999}, remote.RunConfig{
		ThreadID: thread.ID, CheckpointID: branched.CheckpointID,
	})
	if err != nil || replayed.Total != 12 || !reflect.DeepEqual(replayed.Path, []string{"manual", "second"}) {
		t.Fatalf("replayed=%+v err=%v", replayed, err)
	}
	baseAfter, err := client.GetState(context.Background(), remote.RunConfig{
		ThreadID: thread.ID, CheckpointID: base.Config.CheckpointID,
	}, false)
	if err != nil || !reflect.DeepEqual(baseAfter.Values, baseBefore.Values) || baseAfter.Config != baseBefore.Config {
		t.Fatalf("historical checkpoint mutated: before=%+v after=%+v err=%v", baseBefore, baseAfter, err)
	}
}
