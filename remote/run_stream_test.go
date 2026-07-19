package remote_test

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/wahanbo/langgraph-go/backend/distributed"
	"github.com/wahanbo/langgraph-go/graph"
	"github.com/wahanbo/langgraph-go/remote"
)

func TestRemoteRunStreamTailsDurableEventLogFromCursor(t *testing.T) {
	eventIDs := []string{"event-1", "event-2"}
	eventIndex := 0
	log, _ := distributed.NewMemoryEventLog(distributed.EventLogOptions{Clock: time.Now, IDGenerator: func() string {
		id := eventIDs[eventIndex]
		eventIndex++
		return id
	}})
	handler, _ := remote.NewServer[invokeInput, invokeOutput](fakeInvoker{run: func(context.Context, invokeInput, graph.RunConfig) (invokeOutput, error) { return invokeOutput{}, nil }}, remote.ServerOptions{
		IDGenerator: func() string { return "run-1" }, EventLog: log,
	})
	server := httptest.NewServer(handler)
	defer server.Close()
	client, _ := remote.NewClient[invokeInput, invokeOutput](server.URL, server.Client())
	thread, _ := client.CreateThread(context.Background(), "thread")
	run, _ := client.CreateRun(context.Background(), thread.ID, invokeInput{}, remote.RunConfig{})
	first, _ := log.Append(context.Background(), distributed.AppendEvent{ThreadID: thread.ID, RunID: run.ID, Mode: "updates", Data: json.RawMessage(`{"value":1}`)})
	_, _ = log.Append(context.Background(), distributed.AppendEvent{ThreadID: thread.ID, RunID: run.ID, Mode: "done", Data: json.RawMessage(`null`), Terminal: true})
	var all []remote.StreamEvent
	for event := range client.StreamRun(context.Background(), thread.ID, run.ID, "") {
		if event.Error != nil {
			t.Fatal(event.Error)
		}
		all = append(all, event)
	}
	if len(all) != 2 || all[0].ID != "event-1" || all[1].Mode != "done" {
		t.Fatalf("events=%+v", all)
	}
	var resumed []remote.StreamEvent
	for event := range client.StreamRun(context.Background(), thread.ID, run.ID, first.ID) {
		if event.Error != nil {
			t.Fatal(event.Error)
		}
		resumed = append(resumed, event)
	}
	if len(resumed) != 1 || resumed[0].ID != "event-2" {
		t.Fatalf("resumed=%+v", resumed)
	}
}

func TestRemoteRunStreamAutomaticallyReconnectsFromDeliveredCursor(t *testing.T) {
	requests := 0
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		requests++
		writer.Header().Set(remote.ProtocolHeader, remote.ProtocolVersion)
		writer.Header().Set("Content-Type", "text/event-stream")
		if requests == 1 {
			_, _ = io.WriteString(writer, "id: one\nevent: updates\ndata: {\"id\":\"one\",\"mode\":\"updates\",\"data\":1}\n\n")
			return
		}
		if got := request.Header.Get("Last-Event-ID"); got != "one" {
			t.Errorf("cursor=%q", got)
		}
		_, _ = io.WriteString(writer, "id: two\nevent: done\ndata: {\"id\":\"two\",\"mode\":\"done\",\"data\":null}\n\n: langgraph-stream-complete\n\n")
	}))
	defer server.Close()
	client, _ := remote.NewClient[invokeInput, invokeOutput](server.URL, server.Client())
	var ids []string
	for event := range client.StreamRunWithOptions(context.Background(), "thread", "run", "", remote.StreamOptions{
		MaxReconnectAttempts: 1, InitialReconnectDelay: time.Millisecond,
	}) {
		if event.Error != nil {
			t.Fatal(event.Error)
		}
		ids = append(ids, event.ID)
	}
	if len(ids) != 2 || ids[0] != "one" || ids[1] != "two" || requests != 2 {
		t.Fatalf("ids=%v requests=%d", ids, requests)
	}
}

func TestRemoteRunStreamRequiresEventLogCapability(t *testing.T) {
	handler, _ := remote.NewServer(fakeInvoker{run: func(context.Context, invokeInput, graph.RunConfig) (invokeOutput, error) { return invokeOutput{}, nil }}, remote.ServerOptions{IDGenerator: func() string { return "run" }})
	server := httptest.NewServer(handler)
	defer server.Close()
	client, _ := remote.NewClient[invokeInput, invokeOutput](server.URL, server.Client())
	thread, _ := client.CreateThread(context.Background(), "thread")
	run, _ := client.CreateRun(context.Background(), thread.ID, invokeInput{}, remote.RunConfig{})
	event := <-client.StreamRun(context.Background(), thread.ID, run.ID, "")
	if event.Error == nil || event.Error.Code != remote.CodeProtocol {
		t.Fatalf("event=%+v", event)
	}
}
