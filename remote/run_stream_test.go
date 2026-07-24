package remote_test

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/ybszm/langgraph-go/backend/distributed"
	"github.com/ybszm/langgraph-go/graph"
	"github.com/ybszm/langgraph-go/remote"
)

type unsafeEventLog struct {
	event distributed.Event
}

func (l unsafeEventLog) Append(context.Context, distributed.AppendEvent) (distributed.Event, error) {
	return distributed.Event{}, nil
}

func (l unsafeEventLog) List(context.Context, distributed.EventQuery) ([]distributed.Event, error) {
	return []distributed.Event{l.event}, nil
}

func (l unsafeEventLog) Tail(context.Context, distributed.EventQuery) <-chan distributed.EventResult {
	output := make(chan distributed.EventResult, 1)
	output <- distributed.EventResult{Event: l.event}
	close(output)
	return output
}

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

func TestRemoteRunStreamRejectsUnsafeEventLogFields(t *testing.T) {
	handler, err := remote.NewServer[invokeInput, invokeOutput](
		fakeInvoker{run: func(context.Context, invokeInput, graph.RunConfig) (invokeOutput, error) {
			return invokeOutput{}, nil
		}},
		remote.ServerOptions{
			IDGenerator: func() string { return "run-1" },
			EventLog: unsafeEventLog{event: distributed.Event{
				ID: "safe\nevent: forged", Mode: "updates", Data: json.RawMessage(`1`), Terminal: true,
			}},
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(handler)
	defer server.Close()
	client, err := remote.NewClient[invokeInput, invokeOutput](server.URL, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	thread, err := client.CreateThread(context.Background(), "thread")
	if err != nil {
		t.Fatal(err)
	}
	run, err := client.CreateRun(context.Background(), thread.ID, invokeInput{}, remote.RunConfig{})
	if err != nil {
		t.Fatal(err)
	}
	var events []remote.StreamEvent
	for event := range client.StreamRun(context.Background(), thread.ID, run.ID, "") {
		events = append(events, event)
	}
	if len(events) != 1 || events[0].Error == nil || events[0].Error.Code != remote.CodeProtocol {
		t.Fatalf("events=%+v, want one protocol error", events)
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
