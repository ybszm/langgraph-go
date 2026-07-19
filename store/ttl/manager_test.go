package ttl_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/ybszm/langgraph-go/store"
	"github.com/ybszm/langgraph-go/store/ttl"
)

type recordingStore struct {
	mu         sync.Mutex
	operations []store.Operation
	sweeps     int
	sweepErrs  []error
	sweepCalls chan struct{}
}

func (s *recordingStore) Batch(_ context.Context, operations []store.Operation) ([]store.Result, error) {
	s.mu.Lock()
	s.operations = append(s.operations, operations...)
	s.mu.Unlock()
	return make([]store.Result, len(operations)), nil
}

func (s *recordingStore) Get(ctx context.Context, namespace store.Namespace, key string) (*store.Item, error) {
	results, err := s.Batch(ctx, []store.Operation{store.GetOp{Namespace: namespace, Key: key}})
	if err != nil {
		return nil, err
	}
	return results[0].Item, nil
}

func (s *recordingStore) Search(ctx context.Context, namespace store.Namespace, options store.SearchOptions) ([]store.SearchItem, error) {
	results, err := s.Batch(ctx, []store.Operation{store.SearchOp{NamespacePrefix: namespace, Query: options.Query, Filter: options.Filter, Limit: options.Limit, Offset: options.Offset}})
	if err != nil {
		return nil, err
	}
	return results[0].Items, nil
}

func (s *recordingStore) Put(ctx context.Context, namespace store.Namespace, key string, value store.Value) error {
	_, err := s.Batch(ctx, []store.Operation{store.PutOp{Namespace: namespace, Key: key, Value: value}})
	return err
}

func (s *recordingStore) Delete(ctx context.Context, namespace store.Namespace, key string) error {
	return s.Put(ctx, namespace, key, nil)
}

func (s *recordingStore) ListNamespaces(context.Context, store.ListNamespacesOptions) ([]store.Namespace, error) {
	return nil, nil
}

func (s *recordingStore) PutWithTTL(ctx context.Context, namespace store.Namespace, key string, value store.Value, duration time.Duration) error {
	_, err := s.Batch(ctx, []store.Operation{store.PutOp{Namespace: namespace, Key: key, Value: value, TTL: duration}})
	return err
}

func (s *recordingStore) GetWithTTLRefresh(ctx context.Context, namespace store.Namespace, key string, refresh bool) (*store.Item, error) {
	results, err := s.Batch(ctx, []store.Operation{store.GetOp{Namespace: namespace, Key: key, RefreshTTL: refresh}})
	if err != nil {
		return nil, err
	}
	return results[0].Item, nil
}

func (s *recordingStore) SearchWithTTLRefresh(ctx context.Context, namespace store.Namespace, options store.SearchOptions, refresh bool) ([]store.SearchItem, error) {
	results, err := s.Batch(ctx, []store.Operation{store.SearchOp{NamespacePrefix: namespace, Query: options.Query, Filter: options.Filter, Limit: options.Limit, Offset: options.Offset, RefreshTTL: refresh}})
	if err != nil {
		return nil, err
	}
	return results[0].Items, nil
}

func (s *recordingStore) SweepExpired(context.Context) (int64, error) {
	s.mu.Lock()
	s.sweeps++
	var err error
	if len(s.sweepErrs) > 0 {
		err = s.sweepErrs[0]
		s.sweepErrs = s.sweepErrs[1:]
	}
	s.mu.Unlock()
	if s.sweepCalls != nil {
		select {
		case s.sweepCalls <- struct{}{}:
		default:
		}
	}
	return 1, err
}

func TestDefaultsAndExplicitOverrides(t *testing.T) {
	base := &recordingStore{}
	refresh := false
	manager, err := ttl.New(base, ttl.Config{DefaultTTL: 2 * time.Minute, RefreshOnRead: &refresh})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	namespace := store.Namespace{"ttl"}
	if err := manager.Put(ctx, namespace, "default", store.Value{"v": 1}); err != nil {
		t.Fatal(err)
	}
	if err := manager.PutWithTTL(ctx, namespace, "explicit", store.Value{"v": 2}, time.Minute); err != nil {
		t.Fatal(err)
	}
	if err := manager.PutWithoutTTL(ctx, namespace, "clear", store.Value{"v": 3}); err != nil {
		t.Fatal(err)
	}
	if _, err := manager.Get(ctx, namespace, "default"); err != nil {
		t.Fatal(err)
	}
	if _, err := manager.Search(ctx, namespace, store.SearchOptions{Limit: 1}); err != nil {
		t.Fatal(err)
	}
	_, err = manager.Batch(ctx, []store.Operation{
		store.PutOp{Namespace: namespace, Key: "batch-default", Value: store.Value{"v": 4}},
		store.PutOp{Namespace: namespace, Key: "batch-clear", Value: store.Value{"v": 5}, TTLSet: true},
		store.GetOp{Namespace: namespace, Key: "default", RefreshTTL: true, RefreshTTLSet: true},
	})
	if err != nil {
		t.Fatal(err)
	}

	base.mu.Lock()
	operations := append([]store.Operation(nil), base.operations...)
	base.mu.Unlock()
	puts := []store.PutOp{operations[0].(store.PutOp), operations[1].(store.PutOp), operations[2].(store.PutOp), operations[5].(store.PutOp), operations[6].(store.PutOp)}
	wantTTLs := []time.Duration{2 * time.Minute, time.Minute, 0, 2 * time.Minute, 0}
	for index := range puts {
		if puts[index].TTL != wantTTLs[index] || !puts[index].TTLSet {
			t.Fatalf("put %d=%+v want TTL=%v explicit", index, puts[index], wantTTLs[index])
		}
	}
	if operation := operations[3].(store.GetOp); operation.RefreshTTL || !operation.RefreshTTLSet {
		t.Fatalf("Get op=%+v", operation)
	}
	if operation := operations[4].(store.SearchOp); operation.RefreshTTL || !operation.RefreshTTLSet {
		t.Fatalf("Search op=%+v", operation)
	}
	if operation := operations[7].(store.GetOp); !operation.RefreshTTL || !operation.RefreshTTLSet {
		t.Fatalf("explicit Get op=%+v", operation)
	}
}

func TestSweeperLifecycleReportsIterationErrorAndContinues(t *testing.T) {
	injected := errors.New("temporary sweep error")
	base := &recordingStore{sweepErrs: []error{injected}, sweepCalls: make(chan struct{}, 4)}
	reported := make(chan error, 1)
	manager, err := ttl.New(base, ttl.Config{
		SweepInterval: 2 * time.Millisecond,
		OnSweepError:  func(err error) { reported <- err },
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := manager.StartSweeper(ctx); err != nil {
		t.Fatal(err)
	}
	if err := manager.StartSweeper(ctx); !errors.Is(err, ttl.ErrSweeperRunning) {
		t.Fatalf("second StartSweeper err=%v", err)
	}
	select {
	case err := <-reported:
		if !errors.Is(err, injected) {
			t.Fatalf("reported err=%v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("sweep error not reported")
	}
	for count := 0; count < 2; count++ {
		select {
		case <-base.sweepCalls:
		case <-time.After(time.Second):
			t.Fatal("sweeper did not continue")
		}
	}
	stopCtx, stopCancel := context.WithTimeout(context.Background(), time.Second)
	defer stopCancel()
	if err := manager.StopSweeper(stopCtx); err != nil {
		t.Fatal(err)
	}
	if manager.SweeperRunning() {
		t.Fatal("sweeper still running")
	}
	if err := manager.StopSweeper(stopCtx); err != nil {
		t.Fatalf("idempotent StopSweeper: %v", err)
	}
}

func TestParentCancellationStopsSweeper(t *testing.T) {
	base := &recordingStore{}
	manager, err := ttl.New(base, ttl.Config{SweepInterval: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	if err := manager.StartSweeper(ctx); err != nil {
		t.Fatal(err)
	}
	cancel()
	deadline := time.Now().Add(time.Second)
	for manager.SweeperRunning() && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if manager.SweeperRunning() {
		t.Fatal("sweeper did not stop after parent cancellation")
	}
}

func TestConfigValidation(t *testing.T) {
	base := &recordingStore{}
	for _, config := range []ttl.Config{{DefaultTTL: -1}, {SweepInterval: -1}} {
		if _, err := ttl.New(base, config); err == nil {
			t.Fatalf("New accepted %+v", config)
		}
	}
	if _, err := ttl.New(nil, ttl.Config{}); err == nil {
		t.Fatal("New accepted nil store")
	}
}
