package remote_test

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/wahanbo/langgraph-go/backend/distributed"
	"github.com/wahanbo/langgraph-go/graph"
	"github.com/wahanbo/langgraph-go/remote"
)

func TestRemoteListsAndResumesDistributedInterrupt(t *testing.T) {
	store, _ := distributed.NewMemoryInterruptStore(time.Now)
	handler, err := remote.NewServer[invokeInput, invokeOutput](fakeInvoker{}, remote.ServerOptions{InterruptStore: store})
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(handler)
	defer server.Close()
	client, _ := remote.NewClient[invokeInput, invokeOutput](server.URL, server.Client())
	thread, _ := client.CreateThread(context.Background(), "thread")
	request := graph.InterruptRequest{
		Interrupt: graph.Interrupt{ID: "interrupt", Value: json.RawMessage(`{"question":"continue?"}`)},
		ThreadID:  thread.ID, TaskID: "task", CheckpointID: "cp",
	}
	_, _ = store.Await(context.Background(), request)
	records, err := client.ListInterrupts(context.Background(), thread.ID)
	if err != nil || len(records) != 1 || records[0].Interrupt.ID != "interrupt" || records[0].Status != remote.InterruptPending {
		t.Fatalf("records=%+v err=%v", records, err)
	}
	if err := client.ResumeInterrupt(context.Background(), thread.ID, "interrupt", map[string]int{"value": 8}); err != nil {
		t.Fatal(err)
	}
	raw, err := store.Await(context.Background(), request)
	if err != nil || string(raw) != `{"value":8}` {
		t.Fatalf("raw=%s err=%v", raw, err)
	}
}

func TestRemoteInterruptEndpointRequiresCapability(t *testing.T) {
	handler, _ := remote.NewServer(fakeInvoker{}, remote.ServerOptions{})
	server := httptest.NewServer(handler)
	defer server.Close()
	client, _ := remote.NewClient[invokeInput, invokeOutput](server.URL, server.Client())
	thread, _ := client.CreateThread(context.Background(), "thread")
	if _, err := client.ListInterrupts(context.Background(), thread.ID); err == nil {
		t.Fatal("expected missing interrupt capability error")
	}
}
