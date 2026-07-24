package remote_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/ybszm/langgraph-go/graph"
	"github.com/ybszm/langgraph-go/remote"
)

type fakeRemoteGraph struct {
	stream func(context.Context, invokeInput, graph.RunConfig) <-chan remote.StreamEvent
}

func (f fakeRemoteGraph) Invoke(context.Context, invokeInput, graph.RunConfig) (invokeOutput, error) {
	return invokeOutput{}, nil
}

func (f fakeRemoteGraph) Stream(ctx context.Context, input invokeInput, config graph.RunConfig) <-chan remote.StreamEvent {
	return f.stream(ctx, input, config)
}

func TestSSEServerAndClientPreserveEventOrderAndIDs(t *testing.T) {
	backend := fakeRemoteGraph{stream: func(_ context.Context, input invokeInput, config graph.RunConfig) <-chan remote.StreamEvent {
		events := make(chan remote.StreamEvent, 2)
		events <- remote.StreamEvent{Mode: "updates", Data: json.RawMessage(`{"value":` + string(rune('0'+input.Value)) + `}`)}
		events <- remote.StreamEvent{ID: "final-id", Mode: "values", Data: json.RawMessage(`{"thread":"` + config.ThreadID + `"}`)}
		close(events)
		return events
	}}
	server, err := remote.NewServer[invokeInput, invokeOutput](backend, remote.ServerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	httpServer := httptest.NewServer(server)
	defer httpServer.Close()
	client, _ := remote.NewClient[invokeInput, invokeOutput](httpServer.URL, httpServer.Client())
	var events []remote.StreamEvent
	for event := range client.Stream(context.Background(), invokeInput{Value: 4}, remote.RunConfig{ThreadID: "thread"}) {
		if event.Error != nil {
			t.Fatal(event.Error)
		}
		events = append(events, event)
	}
	if len(events) != 2 || events[0].ID != "1" || events[1].ID != "final-id" ||
		!reflect.DeepEqual([]string{events[0].Mode, events[1].Mode}, []string{"updates", "values"}) {
		t.Fatalf("events=%+v", events)
	}
}

func TestSSEServerRejectsInjectedEventFields(t *testing.T) {
	for _, test := range []struct {
		name  string
		event remote.StreamEvent
	}{
		{name: "id", event: remote.StreamEvent{ID: "safe\nevent: forged", Mode: "updates", Data: json.RawMessage(`1`)}},
		{name: "mode", event: remote.StreamEvent{ID: "safe", Mode: "updates\n\nevent: forged", Data: json.RawMessage(`1`)}},
	} {
		test := test
		t.Run(test.name, func(t *testing.T) {
			backend := fakeRemoteGraph{stream: func(context.Context, invokeInput, graph.RunConfig) <-chan remote.StreamEvent {
				events := make(chan remote.StreamEvent, 1)
				events <- test.event
				close(events)
				return events
			}}
			handler, err := remote.NewServer[invokeInput, invokeOutput](backend, remote.ServerOptions{})
			if err != nil {
				t.Fatal(err)
			}
			server := httptest.NewServer(handler)
			defer server.Close()
			client, err := remote.NewClient[invokeInput, invokeOutput](server.URL, server.Client())
			if err != nil {
				t.Fatal(err)
			}
			var events []remote.StreamEvent
			for event := range client.Stream(context.Background(), invokeInput{}, remote.RunConfig{}) {
				events = append(events, event)
			}
			if len(events) != 1 || events[0].Error == nil ||
				events[0].Error.Code != remote.CodeProtocol ||
				!strings.Contains(events[0].Error.Message, "forbidden") {
				t.Fatalf("events=%+v, want one protocol error", events)
			}
		})
	}
}

func TestSSEClientCancellationReachesServer(t *testing.T) {
	started := make(chan struct{})
	canceled := make(chan struct{})
	backend := fakeRemoteGraph{stream: func(ctx context.Context, _ invokeInput, _ graph.RunConfig) <-chan remote.StreamEvent {
		events := make(chan remote.StreamEvent)
		go func() {
			defer close(events)
			close(started)
			<-ctx.Done()
			close(canceled)
		}()
		return events
	}}
	server, _ := remote.NewServer[invokeInput, invokeOutput](backend, remote.ServerOptions{})
	httpServer := httptest.NewServer(server)
	defer httpServer.Close()
	client, _ := remote.NewClient[invokeInput, invokeOutput](httpServer.URL, httpServer.Client())
	ctx, cancel := context.WithCancel(context.Background())
	events := client.Stream(ctx, invokeInput{}, remote.RunConfig{})
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("stream did not start")
	}
	cancel()
	for range events {
	}
	select {
	case <-canceled:
	case <-time.After(time.Second):
		t.Fatal("server stream context was not canceled")
	}
}

func TestSSEUnavailableAndProtocolErrors(t *testing.T) {
	server, _ := remote.NewServer(fakeInvoker{run: func(context.Context, invokeInput, graph.RunConfig) (invokeOutput, error) {
		return invokeOutput{}, nil
	}}, remote.ServerOptions{})
	httpServer := httptest.NewServer(server)
	defer httpServer.Close()
	client, _ := remote.NewClient[invokeInput, invokeOutput](httpServer.URL, httpServer.Client())
	event := <-client.Stream(context.Background(), invokeInput{}, remote.RunConfig{})
	if event.Error == nil || event.Error.Code != remote.CodeProtocol {
		t.Fatalf("event=%+v", event)
	}

	badServer := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		writer.Header().Set(remote.ProtocolHeader, remote.ProtocolVersion)
		writer.Header().Set("Content-Type", "text/event-stream")
		_, _ = writer.Write([]byte("data: not-json\n\n"))
	}))
	defer badServer.Close()
	badClient, _ := remote.NewClient[invokeInput, invokeOutput](badServer.URL, badServer.Client())
	event = <-badClient.Stream(context.Background(), invokeInput{}, remote.RunConfig{})
	if event.Error == nil || !errors.Is(event.Error, remote.ErrInvalidSSE) {
		t.Fatalf("event=%+v", event)
	}
}
