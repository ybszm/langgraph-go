package remote_test

import (
	"context"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"net/http/httptest"

	"github.com/wahanbo/langgraph-go/graph"
	"github.com/wahanbo/langgraph-go/remote"
)

func TestSQLiteControlStoreCrossInstanceJoinAndCancel(t *testing.T) {
	ctx := context.Background()
	store, err := remote.OpenSQLiteControlStore(ctx, filepath.Join(t.TempDir(), "control.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	started := make(chan string, 2)
	ownerCanceled := make(chan struct{}, 1)
	backend := fakeInvoker{run: func(ctx context.Context, input invokeInput, config graph.RunConfig) (invokeOutput, error) {
		started <- config.RunID
		if input.Value == 1 {
			select {
			case <-time.After(80 * time.Millisecond):
				return invokeOutput{Value: 3}, nil
			case <-ctx.Done():
				return invokeOutput{}, ctx.Err()
			}
		}
		<-ctx.Done()
		ownerCanceled <- struct{}{}
		return invokeOutput{}, ctx.Err()
	}}
	var sequence atomic.Int32
	ids := []string{"shared-thread", "join-run", "cancel-run"}
	owner, _ := remote.NewServer[invokeInput, invokeOutput](backend, remote.ServerOptions{ControlStore: store, ControlPollInterval: 5 * time.Millisecond, IDGenerator: func() string { return ids[int(sequence.Add(1))-1] }})
	observer, _ := remote.NewServer[invokeInput, invokeOutput](fakeInvoker{}, remote.ServerOptions{ControlStore: store, ControlPollInterval: 5 * time.Millisecond})
	ownerHTTP := httptest.NewServer(owner)
	defer ownerHTTP.Close()
	defer owner.Close()
	observerHTTP := httptest.NewServer(observer)
	defer observerHTTP.Close()
	defer observer.Close()
	ownerClient, _ := remote.NewClient[invokeInput, invokeOutput](ownerHTTP.URL, ownerHTTP.Client())
	observerClient, _ := remote.NewClient[invokeInput, invokeOutput](observerHTTP.URL, observerHTTP.Client())
	thread, err := ownerClient.CreateThread(ctx, "")
	if err != nil {
		t.Fatal(err)
	}
	joinRun, err := ownerClient.CreateRun(ctx, thread.ID, invokeInput{Value: 1}, remote.RunConfig{})
	if err != nil {
		t.Fatal(err)
	}
	<-started
	joined, err := observerClient.JoinRun(ctx, thread.ID, joinRun.ID)
	if err != nil || joined.Status != remote.RunSuccess || joined.Output == nil || joined.Output.Value != 3 {
		t.Fatalf("joined=%+v err=%v", joined, err)
	}
	cancelRun, err := ownerClient.CreateRun(ctx, thread.ID, invokeInput{Value: 2}, remote.RunConfig{})
	if err != nil {
		t.Fatal(err)
	}
	<-started
	canceled, err := observerClient.CancelRun(ctx, thread.ID, cancelRun.ID)
	if err != nil || canceled.Status != remote.RunCanceled {
		t.Fatalf("canceled=%+v err=%v", canceled, err)
	}
	select {
	case <-ownerCanceled:
	case <-time.After(time.Second):
		t.Fatal("owner invocation did not observe cross-instance cancel")
	}
	deadline := time.Now().Add(time.Second)
	for {
		stored, _, err := store.GetRun(ctx, thread.ID, cancelRun.ID)
		if err != nil {
			t.Fatal(err)
		}
		if stored.Status == remote.RunCanceled {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("owner did not observe cross-instance cancel")
		}
		time.Sleep(time.Millisecond)
	}
}

func TestSQLiteControlStoreConcurrentIdempotencyAcrossHandles(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "concurrent.db")
	left, err := remote.OpenSQLiteControlStore(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	defer left.Close()
	right, err := remote.OpenSQLiteControlStore(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	defer right.Close()
	now := time.Now().UTC()
	if err := left.CreateThread(ctx, remote.Thread{ID: "thread", CreatedAt: now, UpdatedAt: now}); err != nil {
		t.Fatal(err)
	}
	stores := []*remote.SQLiteControlStore{left, right}
	results := make([]remote.StoredRun, 2)
	created := make([]bool, 2)
	errs := make([]error, 2)
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := range stores {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			results[i], created[i], errs[i] = stores[i].CreateRun(ctx, remote.StoredRun{ID: []string{"left", "right"}[i], ThreadID: "thread", Status: remote.RunRunning, CreatedAt: now, UpdatedAt: now}, "key", []byte("same"))
		}(i)
	}
	close(start)
	wg.Wait()
	for _, err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	if created[0] == created[1] {
		t.Fatalf("created=%v, want exactly one winner", created)
	}
	if results[0].ID != results[1].ID {
		t.Fatalf("results=%+v", results)
	}
}

func TestSQLiteControlStoreRestartIdempotencyAndRetention(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "restart.db")
	store, err := remote.OpenSQLiteControlStore(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 7, 19, 3, 0, 0, 0, time.UTC)
	thread := remote.Thread{ID: "thread", CreatedAt: now, UpdatedAt: now, Metadata: map[string]any{"tenant": "acme"}}
	if err := store.CreateThread(ctx, thread); err != nil {
		t.Fatal(err)
	}
	original := remote.StoredRun{ID: "run-1", ThreadID: thread.ID, Status: remote.RunRunning, CreatedAt: now, UpdatedAt: now}
	if _, created, err := store.CreateRun(ctx, original, "same", []byte("hash")); err != nil || !created {
		t.Fatalf("created=%v err=%v", created, err)
	}
	original.Status = remote.RunSuccess
	original.Output = []byte(`{"value":9}`)
	original.UpdatedAt = now.Add(time.Minute)
	if _, changed, err := store.TransitionRun(ctx, thread.ID, original.ID, []remote.RunStatus{remote.RunRunning}, original); err != nil || !changed {
		t.Fatalf("changed=%v err=%v", changed, err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = remote.OpenSQLiteControlStore(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	restarted, ok, err := store.GetThread(ctx, "thread")
	if err != nil || !ok || restarted.Metadata["tenant"] != "acme" {
		t.Fatalf("restarted thread=%+v ok=%v err=%v", restarted, ok, err)
	}
	prior, created, err := store.CreateRun(ctx, remote.StoredRun{ID: "run-2", ThreadID: thread.ID, Status: remote.RunRunning, CreatedAt: now, UpdatedAt: now}, "same", []byte("hash"))
	if err != nil || created || prior.ID != "run-1" || prior.Status != remote.RunSuccess {
		t.Fatalf("prior=%+v created=%v err=%v", prior, created, err)
	}
	if _, _, err := store.CreateRun(ctx, remote.StoredRun{ID: "run-3", ThreadID: thread.ID, Status: remote.RunRunning}, "same", []byte("different")); err != remote.ErrIdempotencyConflict {
		t.Fatalf("err=%v", err)
	}
	active := remote.StoredRun{ID: "active", ThreadID: thread.ID, Status: remote.RunRunning, CreatedAt: now, UpdatedAt: now}
	if _, _, err := store.CreateRun(ctx, active, "", nil); err != nil {
		t.Fatal(err)
	}
	count, err := store.PruneRuns(ctx, now.Add(2*time.Minute))
	if err != nil || count != 1 {
		t.Fatalf("count=%d err=%v", count, err)
	}
	if _, ok, _ := store.GetRun(ctx, thread.ID, "run-1"); ok {
		t.Fatal("terminal run was not pruned")
	}
	if _, ok, _ := store.GetRun(ctx, thread.ID, "active"); !ok {
		t.Fatal("running run was pruned")
	}
}
