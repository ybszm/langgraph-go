package remote_test

import (
	"context"
	"errors"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/wahanbo/langgraph-go/graph"
	"github.com/wahanbo/langgraph-go/remote"
)

func TestRemoteThreadAndRunLifecycle(t *testing.T) {
	entered := make(chan graph.RunConfig, 1)
	release := make(chan struct{})
	backend := fakeInvoker{run: func(ctx context.Context, input invokeInput, config graph.RunConfig) (invokeOutput, error) {
		entered <- config
		select {
		case <-release:
			return invokeOutput{Value: input.Value * 3}, nil
		case <-ctx.Done():
			return invokeOutput{}, ctx.Err()
		}
	}}
	ids := []string{"thread-1", "run-1"}
	var next atomic.Int32
	clock := time.Date(2026, 7, 19, 1, 2, 3, 0, time.UTC)
	handler, err := remote.NewServer[invokeInput, invokeOutput](backend, remote.ServerOptions{
		Clock:       func() time.Time { return clock },
		IDGenerator: func() string { return ids[int(next.Add(1))-1] },
	})
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(handler)
	defer server.Close()
	client, _ := remote.NewClient[invokeInput, invokeOutput](server.URL, server.Client())

	thread, err := client.CreateThread(context.Background(), "")
	if err != nil || thread.ID != "thread-1" || !thread.CreatedAt.Equal(clock) {
		t.Fatalf("thread=%+v err=%v", thread, err)
	}
	run, err := client.CreateRun(context.Background(), thread.ID, invokeInput{Value: 7}, remote.RunConfig{})
	if err != nil || run.ID != "run-1" || run.Status != remote.RunRunning {
		t.Fatalf("run=%+v err=%v", run, err)
	}
	config := <-entered
	if config.ThreadID != thread.ID || config.RunID != run.ID {
		t.Fatalf("config=%+v", config)
	}
	close(release)
	deadline := time.Now().Add(time.Second)
	for {
		run, err = client.GetRun(context.Background(), thread.ID, run.ID)
		if err != nil {
			t.Fatal(err)
		}
		if run.Status == remote.RunSuccess {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("run did not finish: %+v", run)
		}
		time.Sleep(time.Millisecond)
	}
	if run.Output == nil || run.Output.Value != 21 {
		t.Fatalf("run=%+v", run)
	}
	gotThread, err := client.GetThread(context.Background(), thread.ID)
	if err != nil || gotThread.ID != thread.ID {
		t.Fatalf("thread=%+v err=%v", gotThread, err)
	}
}

func TestRemoteCancelRunPropagatesContext(t *testing.T) {
	started := make(chan struct{})
	canceled := make(chan struct{})
	backend := fakeInvoker{run: func(ctx context.Context, _ invokeInput, _ graph.RunConfig) (invokeOutput, error) {
		close(started)
		<-ctx.Done()
		close(canceled)
		return invokeOutput{}, ctx.Err()
	}}
	handler, _ := remote.NewServer[invokeInput, invokeOutput](backend, remote.ServerOptions{})
	server := httptest.NewServer(handler)
	defer server.Close()
	client, _ := remote.NewClient[invokeInput, invokeOutput](server.URL, server.Client())
	thread, _ := client.CreateThread(context.Background(), "cancel-thread")
	run, _ := client.CreateRun(context.Background(), thread.ID, invokeInput{}, remote.RunConfig{})
	<-started
	run, err := client.CancelRun(context.Background(), thread.ID, run.ID)
	if err != nil || run.Status != remote.RunCanceled {
		t.Fatalf("run=%+v err=%v", run, err)
	}
	select {
	case <-canceled:
	case <-time.After(time.Second):
		t.Fatal("backend context was not canceled")
	}
}

func TestRemoteControlNotFoundIsTyped(t *testing.T) {
	handler, _ := remote.NewServer(fakeInvoker{}, remote.ServerOptions{})
	server := httptest.NewServer(handler)
	defer server.Close()
	client, _ := remote.NewClient[invokeInput, invokeOutput](server.URL, server.Client())
	_, err := client.GetThread(context.Background(), "missing")
	var remoteErr *remote.Error
	if !errors.As(err, &remoteErr) || remoteErr.Code != remote.CodeNotFound {
		t.Fatalf("err=%v", err)
	}
}
