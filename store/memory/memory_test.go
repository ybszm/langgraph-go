package memory_test

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/wahanbo/langgraph-go/store"
	"github.com/wahanbo/langgraph-go/store/memory"
	"github.com/wahanbo/langgraph-go/store/storetest"
)

func TestStoreContract(t *testing.T) {
	storetest.Run(t, func(t *testing.T) store.Store {
		return memory.New()
	})
}

func TestPutGetDeleteAndDefensiveCopies(t *testing.T) {
	ctx := context.Background()
	s := memory.New()
	namespace := store.Namespace{"users", "123"}
	value := store.Value{"theme": "dark", "nested": map[string]any{"count": 2}, "tags": []any{"a", "b"}}
	if err := s.Put(ctx, namespace, "prefs", value); err != nil {
		t.Fatal(err)
	}
	value["theme"] = "mutated"
	value["nested"].(map[string]any)["count"] = 9
	item, err := s.Get(ctx, namespace, "prefs")
	if err != nil {
		t.Fatal(err)
	}
	if item.Value["theme"] != "dark" || item.Value["nested"].(map[string]any)["count"] != 2 {
		t.Fatalf("stored value was aliased: %#v", item.Value)
	}
	item.Value["theme"] = "changed again"
	again, _ := s.Get(ctx, namespace, "prefs")
	if again.Value["theme"] != "dark" {
		t.Fatal("Get returned an internal mutable map")
	}
	if err := s.Delete(ctx, namespace, "prefs"); err != nil {
		t.Fatal(err)
	}
	missing, err := s.Get(ctx, namespace, "prefs")
	if err != nil || missing != nil {
		t.Fatalf("deleted item=%+v err=%v", missing, err)
	}
}

func TestSearchPrefixStructuredFiltersAndPagination(t *testing.T) {
	ctx := context.Background()
	s := memory.New()
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
		Query: "ignored without embeddings",
		Filter: store.Value{
			"score": map[string]any{"$gte": 3, "$lt": 8},
			"meta":  map[string]any{"status": "active"},
		},
		Limit: 1, Offset: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 1 || results[0].Key != "2" || results[0].Score != nil {
		t.Fatalf("Search()=%+v", results)
	}
	_, err = s.Search(ctx, store.Namespace{"docs"}, store.SearchOptions{Filter: store.Value{"score": map[string]any{"$bad": 1}}})
	if !errors.Is(err, store.ErrUnsupportedQuery) {
		t.Fatalf("unsupported operator err=%v", err)
	}
}

func TestListNamespacesWildcardsDepthAndPagination(t *testing.T) {
	ctx := context.Background()
	s := memory.New()
	namespaces := []store.Namespace{
		{"a", "b", "c"}, {"a", "b", "d", "e"}, {"a", "b", "d", "i"},
		{"a", "c", "f"}, {"b", "a", "f"}, {"users", "123"},
	}
	for index, namespace := range namespaces {
		if err := s.Put(ctx, namespace, string(rune('a'+index)), store.Value{"x": index}); err != nil {
			t.Fatal(err)
		}
	}
	listed, err := s.ListNamespaces(ctx, store.ListNamespacesOptions{
		Prefix: store.Namespace{"a", "*"}, MaxDepth: 3, Limit: 2, Offset: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	want := []store.Namespace{{"a", "b", "d"}, {"a", "c", "f"}}
	if !reflect.DeepEqual(listed, want) {
		t.Fatalf("ListNamespaces()=%v want=%v", listed, want)
	}
	suffix, err := s.ListNamespaces(ctx, store.ListNamespacesOptions{Suffix: store.Namespace{"*", "f"}})
	if err != nil {
		t.Fatal(err)
	}
	if len(suffix) != 2 {
		t.Fatalf("suffix=%v", suffix)
	}
}

func TestPublicNamespaceValidationAndBatchBypass(t *testing.T) {
	ctx := context.Background()
	s := memory.New()
	invalid := []store.Namespace{nil, {"thing.about"}, {"some", ""}, {"langgraph", "private"}}
	for _, namespace := range invalid {
		if err := s.Put(ctx, namespace, "key", store.Value{"x": 1}); !errors.Is(err, store.ErrInvalidNamespace) {
			t.Fatalf("namespace=%v err=%v", namespace, err)
		}
	}
	_, err := s.Batch(ctx, []store.Operation{store.PutOp{Namespace: store.Namespace{"langgraph", "internal"}, Key: "key", Value: store.Value{"x": 1}}})
	if err != nil {
		t.Fatalf("internal Batch should bypass public namespace reservation: %v", err)
	}
}

func TestBatchReadsPreWriteSnapshotAndLastWriteWins(t *testing.T) {
	ctx := context.Background()
	s := memory.New()
	if err := s.Put(ctx, store.Namespace{"batch"}, "key", store.Value{"version": 0}); err != nil {
		t.Fatal(err)
	}
	results, err := s.Batch(ctx, []store.Operation{
		store.PutOp{Namespace: store.Namespace{"batch"}, Key: "key", Value: store.Value{"version": 1}},
		store.GetOp{Namespace: store.Namespace{"batch"}, Key: "key"},
		store.PutOp{Namespace: store.Namespace{"batch"}, Key: "key", Value: store.Value{"version": 2}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if results[1].Item.Value["version"] != 0 {
		t.Fatalf("batch read observed an in-batch write: %+v", results[1].Item)
	}
	final, _ := s.Get(ctx, store.Namespace{"batch"}, "key")
	if final.Value["version"] != 2 {
		t.Fatalf("last write did not win: %+v", final)
	}
}

func TestConcurrentAccess(t *testing.T) {
	ctx := context.Background()
	s := memory.New()
	var group sync.WaitGroup
	for worker := 0; worker < 20; worker++ {
		worker := worker
		group.Add(1)
		go func() {
			defer group.Done()
			ns := store.Namespace{"concurrent", string(rune('a' + worker))}
			for index := 0; index < 100; index++ {
				if err := s.Put(ctx, ns, "key", store.Value{"worker": worker, "index": index}); err != nil {
					t.Errorf("Put: %v", err)
					return
				}
				if _, err := s.Get(ctx, ns, "key"); err != nil {
					t.Errorf("Get: %v", err)
					return
				}
			}
		}()
	}
	group.Wait()
}

func TestTTLIsExplicitlyUnsupported(t *testing.T) {
	s := memory.New()
	_, err := s.Batch(context.Background(), []store.Operation{store.PutOp{
		Namespace: store.Namespace{"ttl"}, Key: "key", Value: store.Value{"x": 1}, TTL: time.Minute,
	}})
	if !errors.Is(err, store.ErrUnsupportedTTL) {
		t.Fatalf("TTL err=%v", err)
	}
}
