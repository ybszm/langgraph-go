// Package storetest provides a reusable black-box conformance suite for
// store.Store implementations.
package storetest

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"testing"

	"github.com/wahanbo/langgraph-go/store"
)

// Factory creates a fresh Store for one contract subtest. It should register
// any required cleanup with t.
type Factory func(t *testing.T) store.Store

// Run executes the Store contract against fresh instances from factory.
func Run(t *testing.T, factory Factory) {
	t.Helper()
	if factory == nil {
		t.Fatal("storetest: nil Factory")
	}
	t.Run("put_get_update_delete_and_copies", func(t *testing.T) {
		s := factory(t)
		ctx := context.Background()
		namespace := store.Namespace{"users", "123"}
		value := store.Value{"theme": "dark", "nested": map[string]any{"count": 2}, "tags": []any{"a", "b"}}
		if err := s.Put(ctx, namespace, "prefs", value); err != nil {
			t.Fatal(err)
		}
		value["theme"] = "caller mutation"
		value["nested"].(map[string]any)["count"] = 9
		first, err := s.Get(ctx, namespace, "prefs")
		if err != nil || first == nil {
			t.Fatalf("Get(): item=%+v err=%v", first, err)
		}
		if first.Value["theme"] != "dark" || first.Value["nested"].(map[string]any)["count"] != 2 {
			t.Fatalf("stored value aliases caller: %+v", first)
		}
		if first.CreatedAt.IsZero() || first.UpdatedAt.IsZero() {
			t.Fatalf("timestamps=%+v", first)
		}
		if err := s.Put(ctx, namespace, "prefs", store.Value{"theme": "light"}); err != nil {
			t.Fatal(err)
		}
		updated, err := s.Get(ctx, namespace, "prefs")
		if err != nil || updated == nil {
			t.Fatalf("updated item=%+v err=%v", updated, err)
		}
		if updated.CreatedAt != first.CreatedAt {
			t.Fatalf("update changed CreatedAt: before=%v after=%v", first.CreatedAt, updated.CreatedAt)
		}
		if updated.UpdatedAt.Before(first.UpdatedAt) {
			t.Fatalf("UpdatedAt moved backward: before=%v after=%v", first.UpdatedAt, updated.UpdatedAt)
		}
		updated.Value["theme"] = "result mutation"
		again, err := s.Get(ctx, namespace, "prefs")
		if err != nil || again == nil || again.Value["theme"] != "light" {
			t.Fatalf("Get result aliases backend: item=%+v err=%v", again, err)
		}
		if err := s.Delete(ctx, namespace, "prefs"); err != nil {
			t.Fatal(err)
		}
		missing, err := s.Get(ctx, namespace, "prefs")
		if err != nil || missing != nil {
			t.Fatalf("deleted item=%+v err=%v", missing, err)
		}
	})

	t.Run("search_filter_order_and_pagination", func(t *testing.T) {
		s := factory(t)
		ctx := context.Background()
		fixtures := []struct {
			ns    store.Namespace
			key   string
			value store.Value
		}{
			{store.Namespace{"docs", "a"}, "1", store.Value{"score": 5, "meta": map[string]any{"status": "active"}}},
			{store.Namespace{"docs", "a"}, "2", store.Value{"score": 3, "meta": map[string]any{"status": "active"}}},
			{store.Namespace{"docs", "b"}, "3", store.Value{"score": 8, "meta": map[string]any{"status": "draft"}}},
			{store.Namespace{"other"}, "4", store.Value{"score": 9}},
		}
		for _, fixture := range fixtures {
			if err := s.Put(ctx, fixture.ns, fixture.key, fixture.value); err != nil {
				t.Fatal(err)
			}
		}
		results, err := s.Search(ctx, store.Namespace{"docs"}, store.SearchOptions{
			Query:  "scoreless without embeddings",
			Filter: store.Value{"score": map[string]any{"$gte": 3, "$lt": 8}, "meta": map[string]any{"status": "active"}},
			Limit:  1, Offset: 1,
		})
		if err != nil || len(results) != 1 || results[0].Key != "2" || results[0].Score != nil {
			t.Fatalf("Search()=%+v err=%v", results, err)
		}
		_, err = s.Search(ctx, store.Namespace{"docs"}, store.SearchOptions{Filter: store.Value{"score": map[string]any{"$bad": 1}}})
		if !errors.Is(err, store.ErrUnsupportedQuery) {
			t.Fatalf("unsupported filter err=%v", err)
		}
	})

	t.Run("list_namespaces_wildcard_depth_and_pagination", func(t *testing.T) {
		s := factory(t)
		ctx := context.Background()
		namespaces := []store.Namespace{{"a", "b", "c"}, {"a", "b", "d", "e"}, {"a", "b", "d", "i"}, {"a", "c", "f"}, {"b", "a", "f"}}
		for index, namespace := range namespaces {
			if err := s.Put(ctx, namespace, fmt.Sprintf("%d", index), store.Value{"x": index}); err != nil {
				t.Fatal(err)
			}
		}
		listed, err := s.ListNamespaces(ctx, store.ListNamespacesOptions{Prefix: store.Namespace{"a", "*"}, MaxDepth: 3, Limit: 2, Offset: 1})
		want := []store.Namespace{{"a", "b", "d"}, {"a", "c", "f"}}
		if err != nil || !reflect.DeepEqual(listed, want) {
			t.Fatalf("ListNamespaces()=%v want=%v err=%v", listed, want, err)
		}
		suffix, err := s.ListNamespaces(ctx, store.ListNamespacesOptions{Suffix: store.Namespace{"*", "f"}})
		if err != nil || len(suffix) != 2 {
			t.Fatalf("suffix=%v err=%v", suffix, err)
		}
	})

	t.Run("batch_snapshot_order_and_last_write_wins", func(t *testing.T) {
		s := factory(t)
		ctx := context.Background()
		ns := store.Namespace{"batch"}
		if err := s.Put(ctx, ns, "key", store.Value{"version": 0}); err != nil {
			t.Fatal(err)
		}
		before, err := s.Get(ctx, ns, "key")
		if err != nil || before == nil {
			t.Fatalf("Get(before): item=%+v err=%v", before, err)
		}
		results, err := s.Batch(ctx, []store.Operation{
			store.PutOp{Namespace: ns, Key: "key", Value: store.Value{"version": 1}},
			store.GetOp{Namespace: ns, Key: "key"},
			store.PutOp{Namespace: ns, Key: "key", Value: store.Value{"version": 2}},
		})
		if err != nil || results[1].Item == nil || results[1].Item.Value["version"] != 0 {
			t.Fatalf("Batch()=%+v err=%v", results, err)
		}
		final, err := s.Get(ctx, ns, "key")
		if err != nil || final == nil || final.Value["version"] != 2 || final.CreatedAt != before.CreatedAt {
			t.Fatalf("final=%+v before=%+v err=%v", final, before, err)
		}
	})

	t.Run("validation_internal_namespace_and_cancellation", func(t *testing.T) {
		s := factory(t)
		ctx := context.Background()
		for _, namespace := range []store.Namespace{nil, {"thing.about"}, {"some", ""}, {"langgraph", "private"}} {
			if err := s.Put(ctx, namespace, "key", store.Value{"x": 1}); !errors.Is(err, store.ErrInvalidNamespace) {
				t.Fatalf("namespace=%v err=%v", namespace, err)
			}
		}
		if _, err := s.Batch(ctx, []store.Operation{store.PutOp{Namespace: store.Namespace{"langgraph", "internal"}, Key: "key", Value: store.Value{"x": 1}}}); err != nil {
			t.Fatalf("internal Batch: %v", err)
		}
		canceled, cancel := context.WithCancel(ctx)
		cancel()
		if _, err := s.Get(canceled, store.Namespace{"valid"}, "key"); !errors.Is(err, context.Canceled) {
			t.Fatalf("canceled Get err=%v", err)
		}
	})

	t.Run("concurrent_namespaces", func(t *testing.T) {
		s := factory(t)
		var group sync.WaitGroup
		for worker := 0; worker < 8; worker++ {
			worker := worker
			group.Add(1)
			go func() {
				defer group.Done()
				ns := store.Namespace{"concurrent", fmt.Sprintf("%02d", worker)}
				for index := 0; index < 20; index++ {
					if err := s.Put(context.Background(), ns, "key", store.Value{"worker": worker, "index": index}); err != nil {
						t.Errorf("Put: %v", err)
						return
					}
				}
			}()
		}
		group.Wait()
		items, err := s.Search(context.Background(), store.Namespace{"concurrent"}, store.SearchOptions{Limit: 20})
		if err != nil || len(items) != 8 {
			t.Fatalf("concurrent Search len=%d err=%v", len(items), err)
		}
	})
}
