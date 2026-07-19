package remote_test

import (
	"context"
	"errors"
	"net/http/httptest"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"github.com/wahanbo/langgraph-go/graph"
	"github.com/wahanbo/langgraph-go/remote"
)

func TestRemoteListJoinAndDeleteLifecycle(t *testing.T) {
	ids := []string{"thread-a", "thread-b", "run-a", "run-b"}
	var index atomic.Int32
	backend := fakeInvoker{run: func(_ context.Context, input invokeInput, _ graph.RunConfig) (invokeOutput, error) {
		return invokeOutput{Value: input.Value + 1}, nil
	}}
	handler, _ := remote.NewServer[invokeInput, invokeOutput](backend, remote.ServerOptions{
		IDGenerator: func() string { return ids[int(index.Add(1))-1] },
	})
	server := httptest.NewServer(handler)
	defer server.Close()
	client, _ := remote.NewClient[invokeInput, invokeOutput](server.URL, server.Client())
	threadA, _ := client.CreateThread(context.Background(), "")
	threadB, _ := client.CreateThread(context.Background(), "")
	threads, err := client.ListThreads(context.Background(), remote.ListOptions{Limit: 1, Offset: 1})
	if err != nil || len(threads) != 1 || threads[0].ID != threadB.ID {
		t.Fatalf("threads=%+v err=%v", threads, err)
	}
	runA, _ := client.CreateRun(context.Background(), threadA.ID, invokeInput{Value: 2}, remote.RunConfig{})
	runB, _ := client.CreateRun(context.Background(), threadA.ID, invokeInput{Value: 4}, remote.RunConfig{})
	joined, err := client.JoinRun(context.Background(), threadA.ID, runA.ID)
	if err != nil || joined.Status != remote.RunSuccess || joined.Output == nil || joined.Output.Value != 3 {
		t.Fatalf("joined=%+v err=%v", joined, err)
	}
	runs, err := client.ListRuns(context.Background(), threadA.ID, remote.ListOptions{})
	if err != nil || !reflect.DeepEqual([]string{runs[0].ID, runs[1].ID}, []string{runA.ID, runB.ID}) {
		t.Fatalf("runs=%+v err=%v", runs, err)
	}
	if err := client.DeleteThread(context.Background(), threadA.ID); err != nil {
		t.Fatal(err)
	}
	_, err = client.GetThread(context.Background(), threadA.ID)
	var remoteErr *remote.Error
	if !errors.As(err, &remoteErr) || remoteErr.Code != remote.CodeNotFound {
		t.Fatalf("err=%v", err)
	}
}

func TestJoinCancellationDoesNotCancelRun(t *testing.T) {
	release := make(chan struct{})
	backendCanceled := make(chan struct{}, 1)
	backend := fakeInvoker{run: func(ctx context.Context, _ invokeInput, _ graph.RunConfig) (invokeOutput, error) {
		select {
		case <-release:
			return invokeOutput{Value: 9}, nil
		case <-ctx.Done():
			backendCanceled <- struct{}{}
			return invokeOutput{}, ctx.Err()
		}
	}}
	handler, _ := remote.NewServer[invokeInput, invokeOutput](backend, remote.ServerOptions{})
	server := httptest.NewServer(handler)
	defer server.Close()
	client, _ := remote.NewClient[invokeInput, invokeOutput](server.URL, server.Client())
	thread, _ := client.CreateThread(context.Background(), "join-thread")
	run, _ := client.CreateRun(context.Background(), thread.ID, invokeInput{}, remote.RunConfig{})
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	_, err := client.JoinRun(ctx, thread.ID, run.ID)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err=%v", err)
	}
	select {
	case <-backendCanceled:
		t.Fatal("join cancellation canceled backend run")
	default:
	}
	close(release)
	joined, err := client.JoinRun(context.Background(), thread.ID, run.ID)
	if err != nil || joined.Status != remote.RunSuccess {
		t.Fatalf("joined=%+v err=%v", joined, err)
	}
}
