package remote_test

import (
	"context"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/ybszm/langgraph-go/checkpoint"
	"github.com/ybszm/langgraph-go/graph"
	"github.com/ybszm/langgraph-go/remote"
)

type remoteState struct{ Count int }
type remoteDelta struct{ Add int }

type fakeStateGraph struct {
	config graph.RunConfig
	update graph.StateUpdate[remoteDelta]
}

func (f *fakeStateGraph) Invoke(context.Context, invokeInput, graph.RunConfig) (invokeOutput, error) {
	return invokeOutput{}, nil
}

func (f *fakeStateGraph) GetState(_ context.Context, config graph.RunConfig, _ ...graph.GetStateOption) (graph.StateSnapshot[remoteState, remoteDelta], error) {
	f.config = config
	return graph.StateSnapshot[remoteState, remoteDelta]{
		Values: remoteState{Count: 3}, Next: []graph.NodeID{"next"},
		Config:    checkpoint.Config{ThreadID: config.ThreadID, CheckpointID: "cp-2"},
		CreatedAt: time.Date(2026, 7, 19, 2, 0, 0, 0, time.UTC),
	}, nil
}

func (f *fakeStateGraph) GetStateHistory(_ context.Context, config graph.RunConfig, options graph.StateHistoryOptions) ([]graph.StateSnapshot[remoteState, remoteDelta], error) {
	f.config = config
	return []graph.StateSnapshot[remoteState, remoteDelta]{{Values: remoteState{Count: options.Limit}}}, nil
}

func (f *fakeStateGraph) UpdateState(_ context.Context, config graph.RunConfig, update graph.StateUpdate[remoteDelta]) (checkpoint.Config, error) {
	f.config, f.update = config, update
	return checkpoint.Config{ThreadID: config.ThreadID, CheckpointID: "cp-3"}, nil
}

func TestRemoteTypedStateHistoryAndUpdate(t *testing.T) {
	backend := &fakeStateGraph{}
	handler, err := remote.NewStateServer[invokeInput, invokeOutput, remoteState, remoteDelta](backend, backend, remote.ServerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(handler)
	defer server.Close()
	client, _ := remote.NewStateClient[invokeInput, invokeOutput, remoteState, remoteDelta](server.URL, server.Client())
	thread, _ := client.CreateThread(context.Background(), "state-thread")

	snapshot, err := client.GetState(context.Background(), remote.RunConfig{ThreadID: thread.ID, CheckpointID: "cp-2"}, false)
	if err != nil || snapshot.Values.Count != 3 || snapshot.Config.CheckpointID != "cp-2" || backend.config.ThreadID != thread.ID {
		t.Fatalf("snapshot=%+v config=%+v err=%v", snapshot, backend.config, err)
	}
	history, err := client.GetStateHistory(context.Background(), remote.RunConfig{ThreadID: thread.ID}, graph.StateHistoryOptions{Limit: 4})
	if err != nil || len(history) != 1 || history[0].Values.Count != 4 {
		t.Fatalf("history=%+v err=%v", history, err)
	}
	updated, err := client.UpdateState(context.Background(), remote.RunConfig{ThreadID: thread.ID}, graph.StateUpdate[remoteDelta]{Delta: remoteDelta{Add: 2}, AsNode: "next"})
	if err != nil || updated.CheckpointID != "cp-3" || backend.update.Delta.Add != 2 || backend.update.AsNode != "next" {
		t.Fatalf("updated=%+v backend=%+v err=%v", updated, backend.update, err)
	}
}

func TestRemoteStateEndpointRequiresCapabilityAndMatchingThread(t *testing.T) {
	handler, _ := remote.NewServer(fakeInvoker{}, remote.ServerOptions{})
	server := httptest.NewServer(handler)
	defer server.Close()
	client, _ := remote.NewStateClient[invokeInput, invokeOutput, remoteState, remoteDelta](server.URL, server.Client())
	_, err := client.GetState(context.Background(), remote.RunConfig{ThreadID: "missing"}, false)
	if err == nil {
		t.Fatal("expected state capability error")
	}
}
