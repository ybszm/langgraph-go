package remote_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ybszm/langgraph-go/graph"
	"github.com/ybszm/langgraph-go/remote"
)

func TestSSEClientReconnectsFromLastDeliveredEvent(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		attempt := requests.Add(1)
		writer.Header().Set(remote.ProtocolHeader, remote.ProtocolVersion)
		writer.Header().Set("Content-Type", "text/event-stream")
		if attempt == 1 {
			_, _ = io.WriteString(writer, "id: 1\nevent: updates\ndata: {\"id\":\"1\",\"mode\":\"updates\",\"data\":{\"n\":1}}\n\n")
			return // no protocol completion marker: simulate a dropped connection
		}
		if got := request.Header.Get("Last-Event-ID"); got != "1" {
			t.Errorf("Last-Event-ID=%q", got)
		}
		_, _ = io.WriteString(writer, "id: 2\nevent: values\ndata: {\"id\":\"2\",\"mode\":\"values\",\"data\":{\"n\":2}}\n\n: langgraph-stream-complete\n\n")
	}))
	defer server.Close()
	client, _ := remote.NewClient[invokeInput, invokeOutput](server.URL, server.Client())
	var ids []string
	for event := range client.StreamWithOptions(context.Background(), invokeInput{}, remote.RunConfig{}, remote.StreamOptions{
		MaxReconnectAttempts:  1,
		InitialReconnectDelay: time.Millisecond,
	}) {
		if event.Error != nil {
			t.Fatal(event.Error)
		}
		ids = append(ids, event.ID)
	}
	if !reflect.DeepEqual(ids, []string{"1", "2"}) || requests.Load() != 2 {
		t.Fatalf("ids=%v requests=%d", ids, requests.Load())
	}
}

func TestSSEServerResumesAfterLastEventID(t *testing.T) {
	backend := fakeRemoteGraph{stream: func(context.Context, invokeInput, graph.RunConfig) <-chan remote.StreamEvent {
		events := make(chan remote.StreamEvent, 3)
		for _, id := range []string{"a", "b", "c"} {
			events <- remote.StreamEvent{ID: id, Mode: "updates", Data: json.RawMessage(`{"id":"` + id + `"}`)}
		}
		close(events)
		return events
	}}
	handler, _ := remote.NewServer[invokeInput, invokeOutput](backend, remote.ServerOptions{})
	server := httptest.NewServer(handler)
	defer server.Close()
	payload, _ := json.Marshal(map[string]any{"input": invokeInput{}})
	request, _ := http.NewRequest(http.MethodPost, server.URL+remote.StreamPath, bytes.NewReader(payload))
	request.Header.Set("Last-Event-ID", "b")
	response, err := server.Client().Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	body, _ := io.ReadAll(response.Body)
	if bytes.Contains(body, []byte(`"id":"a"`)) || bytes.Contains(body, []byte(`"id":"b"`)) || !bytes.Contains(body, []byte(`"id":"c"`)) {
		t.Fatalf("body=%s", body)
	}
}

func TestSSEReconnectBackoffHonorsCancellation(t *testing.T) {
	started := make(chan struct{}, 1)
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		writer.Header().Set(remote.ProtocolHeader, remote.ProtocolVersion)
		writer.Header().Set("Content-Type", "text/event-stream")
		started <- struct{}{}
	}))
	defer server.Close()
	client, _ := remote.NewClient[invokeInput, invokeOutput](server.URL, server.Client())
	ctx, cancel := context.WithCancel(context.Background())
	events := client.StreamWithOptions(ctx, invokeInput{}, remote.RunConfig{}, remote.StreamOptions{
		MaxReconnectAttempts:  3,
		InitialReconnectDelay: time.Second,
	})
	<-started
	cancel()
	select {
	case _, open := <-events:
		if open {
			t.Fatal("unexpected event after cancellation")
		}
	case <-time.After(200 * time.Millisecond):
		t.Fatal("cancellation did not interrupt reconnect backoff")
	}
}
