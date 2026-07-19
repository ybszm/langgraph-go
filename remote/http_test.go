package remote_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/wahanbo/langgraph-go/graph"
	"github.com/wahanbo/langgraph-go/remote"
)

type invokeInput struct {
	Value int `json:"value"`
}
type invokeOutput struct {
	Value int `json:"value"`
}

type fakeInvoker struct {
	run func(context.Context, invokeInput, graph.RunConfig) (invokeOutput, error)
}

func (f fakeInvoker) Invoke(ctx context.Context, input invokeInput, config graph.RunConfig) (invokeOutput, error) {
	return f.run(ctx, input, config)
}

func TestHTTPInvokeServerAndTypedClient(t *testing.T) {
	server, err := remote.NewServer(fakeInvoker{run: func(_ context.Context, input invokeInput, config graph.RunConfig) (invokeOutput, error) {
		if config.ThreadID != "thread" || config.RunID != "run" || config.RecursionLimit != 9 || config.Metadata["request"] != "one" {
			t.Fatalf("config=%+v", config)
		}
		return invokeOutput{Value: input.Value * 2}, nil
	}}, remote.ServerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	httpServer := httptest.NewServer(server)
	defer httpServer.Close()
	client, err := remote.NewClient[invokeInput, invokeOutput](httpServer.URL, httpServer.Client())
	if err != nil {
		t.Fatal(err)
	}
	output, err := client.Invoke(context.Background(), invokeInput{Value: 4}, remote.RunConfig{
		ThreadID: "thread", RunID: "run", RecursionLimit: 9, Metadata: map[string]any{"request": "one"},
	})
	if err != nil || output.Value != 8 {
		t.Fatalf("output=%+v error=%v", output, err)
	}
}

func TestHTTPInvokeErrorAndInvalidRequest(t *testing.T) {
	boom := errors.New("boom")
	server, _ := remote.NewServer(fakeInvoker{run: func(context.Context, invokeInput, graph.RunConfig) (invokeOutput, error) {
		return invokeOutput{}, boom
	}}, remote.ServerOptions{})
	httpServer := httptest.NewServer(server)
	defer httpServer.Close()
	client, _ := remote.NewClient[invokeInput, invokeOutput](httpServer.URL, httpServer.Client())
	_, err := client.Invoke(context.Background(), invokeInput{}, remote.RunConfig{})
	var remoteErr *remote.Error
	if !errors.As(err, &remoteErr) || remoteErr.Code != remote.CodeExecution {
		t.Fatalf("error=%v", err)
	}
	request, _ := http.NewRequest(http.MethodPost, httpServer.URL+remote.InvokePath, strings.NewReader("invalid JSON"))
	response, err := httpServer.Client().Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusBadRequest || response.Header.Get(remote.ProtocolHeader) != remote.ProtocolVersion {
		t.Fatalf("status=%d headers=%v", response.StatusCode, response.Header)
	}
}

func TestHTTPInvokePropagatesClientCancellation(t *testing.T) {
	started := make(chan struct{})
	observed := make(chan struct{})
	var startedOnce atomic.Bool
	var observedOnce atomic.Bool
	server, _ := remote.NewServer(fakeInvoker{run: func(ctx context.Context, _ invokeInput, _ graph.RunConfig) (invokeOutput, error) {
		if startedOnce.CompareAndSwap(false, true) {
			close(started)
		}
		<-ctx.Done()
		if observedOnce.CompareAndSwap(false, true) {
			close(observed)
		}
		return invokeOutput{}, ctx.Err()
	}}, remote.ServerOptions{})
	httpServer := httptest.NewServer(server)
	defer httpServer.Close()
	client, _ := remote.NewClient[invokeInput, invokeOutput](httpServer.URL, httpServer.Client())
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := client.Invoke(ctx, invokeInput{}, remote.RunConfig{})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error=%v", err)
	}
	ctx, cancel = context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := client.Invoke(ctx, invokeInput{}, remote.RunConfig{})
		done <- err
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("request was not dispatched")
	}
	cancel()
	select {
	case <-observed:
	case <-time.After(time.Second):
		t.Fatal("server did not observe cancellation")
	}
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("dispatched error=%v", err)
	}
}

func TestRemoteConstructorsValidateConfiguration(t *testing.T) {
	if _, err := remote.NewServer[invokeInput, invokeOutput](nil, remote.ServerOptions{}); err == nil {
		t.Fatal("expected nil invoker error")
	}
	if _, err := remote.NewClient[invokeInput, invokeOutput]("://bad", nil); err == nil {
		t.Fatal("expected invalid URL error")
	}
}
