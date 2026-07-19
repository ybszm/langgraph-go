package remote_test

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ybszm/langgraph-go/graph"
	"github.com/ybszm/langgraph-go/remote"
)

func TestRemoteRejectsIncompatibleRequestProtocol(t *testing.T) {
	handler, _ := remote.NewServer(fakeInvoker{}, remote.ServerOptions{})
	server := httptest.NewServer(handler)
	defer server.Close()
	request, _ := http.NewRequest(http.MethodPost, server.URL+remote.InvokePath, bytes.NewBufferString(`{"input":{}}`))
	request.Header.Set(remote.ProtocolHeader, "999")
	response, err := server.Client().Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusUpgradeRequired || response.Header.Get(remote.ProtocolHeader) != remote.ProtocolVersion {
		t.Fatalf("status=%d version=%q", response.StatusCode, response.Header.Get(remote.ProtocolHeader))
	}
}

func TestCreateRunIdempotencyKeyDeduplicatesAndDetectsConflict(t *testing.T) {
	release := make(chan struct{})
	var calls atomic.Int32
	backend := fakeInvoker{run: func(ctx context.Context, input invokeInput, _ graph.RunConfig) (invokeOutput, error) {
		calls.Add(1)
		select {
		case <-release:
			return invokeOutput{Value: input.Value}, nil
		case <-ctx.Done():
			return invokeOutput{}, ctx.Err()
		}
	}}
	handler, _ := remote.NewServer[invokeInput, invokeOutput](backend, remote.ServerOptions{})
	defer handler.Close()
	server := httptest.NewServer(handler)
	defer server.Close()
	client, _ := remote.NewClient[invokeInput, invokeOutput](server.URL, server.Client())
	thread, _ := client.CreateThread(context.Background(), "idem-thread")
	options := remote.RunCreateOptions{IdempotencyKey: "request-1"}
	first, err := client.CreateRunWithOptions(context.Background(), thread.ID, invokeInput{Value: 5}, remote.RunConfig{}, options)
	if err != nil {
		t.Fatal(err)
	}
	second, err := client.CreateRunWithOptions(context.Background(), thread.ID, invokeInput{Value: 5}, remote.RunConfig{}, options)
	if err != nil || second.ID != first.ID {
		t.Fatalf("first=%+v second=%+v err=%v", first, second, err)
	}
	_, err = client.CreateRunWithOptions(context.Background(), thread.ID, invokeInput{Value: 6}, remote.RunConfig{}, options)
	var remoteErr *remote.Error
	if !errors.As(err, &remoteErr) || remoteErr.Code != remote.CodeConflict {
		t.Fatalf("err=%v", err)
	}
	time.Sleep(10 * time.Millisecond)
	if calls.Load() != 1 {
		t.Fatalf("calls=%d", calls.Load())
	}
	close(release)
}
