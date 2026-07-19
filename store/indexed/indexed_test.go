package indexed_test

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"testing"

	"github.com/ybszm/langgraph-go/store"
	"github.com/ybszm/langgraph-go/store/indexed"
	storememory "github.com/ybszm/langgraph-go/store/memory"
	vectormemory "github.com/ybszm/langgraph-go/store/vector/memory"
)

type fakeEmbedder struct {
	mu         sync.Mutex
	vectors    map[string][]float32
	docCalls   [][]string
	queryCalls []string
	fail       bool
	malformed  bool
}

var errInjectedIndex = errors.New("injected index failure")

type toggleIndex struct {
	delegate store.VectorIndex
	mu       sync.Mutex
	fail     bool
}

func (i *toggleIndex) Upsert(ctx context.Context, documents []store.VectorDocument) error {
	i.mu.Lock()
	fail := i.fail
	i.mu.Unlock()
	if fail {
		return errInjectedIndex
	}
	return i.delegate.Upsert(ctx, documents)
}

func (i *toggleIndex) Delete(ctx context.Context, namespace store.Namespace, key string) error {
	return i.delegate.Delete(ctx, namespace, key)
}

func (i *toggleIndex) Search(ctx context.Context, query store.VectorQuery) ([]store.VectorMatch, error) {
	return i.delegate.Search(ctx, query)
}

func (f *fakeEmbedder) EmbedDocuments(_ context.Context, texts []string) ([][]float32, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.docCalls = append(f.docCalls, append([]string(nil), texts...))
	if f.fail {
		return nil, errors.New("embedding unavailable")
	}
	result := make([][]float32, 0, len(texts))
	for _, value := range texts {
		vector, ok := f.vectors[value]
		if !ok {
			return nil, errors.New("unknown text: " + value)
		}
		result = append(result, append([]float32(nil), vector...))
	}
	if f.malformed && len(result) > 0 {
		return result[:len(result)-1], nil
	}
	return result, nil
}

func (f *fakeEmbedder) EmbedQuery(_ context.Context, text string) ([]float32, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.queryCalls = append(f.queryCalls, text)
	if f.fail {
		return nil, errors.New("embedding unavailable")
	}
	vector, ok := f.vectors[text]
	if !ok {
		return nil, errors.New("unknown query: " + text)
	}
	return append([]float32(nil), vector...), nil
}

func newStore(t *testing.T, embedder *fakeEmbedder, fields []string) (*indexed.Store, *storememory.Store) {
	t.Helper()
	vectorIndex, err := vectormemory.New(2, store.VectorCosine)
	if err != nil {
		t.Fatal(err)
	}
	base := storememory.New()
	result, err := indexed.New(base, indexed.Config{Embedder: embedder, Index: vectorIndex, Fields: fields})
	if err != nil {
		t.Fatal(err)
	}
	return result, base
}

func TestSemanticSearchIndexOverrideUpdateAndDelete(t *testing.T) {
	embedder := &fakeEmbedder{vectors: map[string][]float32{
		"Go concurrency": {1, 0}, "Kitchen recipes": {0, 1}, "Private Go": {1, 0},
		"Rust ownership": {-1, 0}, "golang": {1, 0},
	}}
	s, _ := newStore(t, embedder, []string{"text"})
	ctx := context.Background()
	operations := []store.Operation{
		store.PutOp{Namespace: store.Namespace{"docs"}, Key: "a", Value: store.Value{"text": "Go concurrency", "kind": "tech"}},
		store.PutOp{Namespace: store.Namespace{"docs"}, Key: "b", Value: store.Value{"text": "Kitchen recipes", "kind": "food"}},
		store.PutOp{Namespace: store.Namespace{"docs"}, Key: "c", Value: store.Value{"text": "Private Go", "kind": "tech"}, Index: []string{}},
	}
	if _, err := s.Batch(ctx, operations); err != nil {
		t.Fatal(err)
	}

	items, err := s.Search(ctx, store.Namespace{"docs"}, store.SearchOptions{Query: "golang", Limit: 2})
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 2 || items[0].Key != "a" || items[1].Key != "b" || items[0].Score == nil || items[1].Score == nil {
		t.Fatalf("semantic results=%+v", items)
	}
	filtered, err := s.Search(ctx, store.Namespace{"docs"}, store.SearchOptions{
		Query: "golang", Filter: store.Value{"kind": "tech"}, Limit: 3,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(filtered) != 2 || filtered[0].Key != "a" || filtered[0].Score == nil || filtered[1].Key != "c" || filtered[1].Score != nil {
		t.Fatalf("filtered/fallback results=%+v", filtered)
	}

	if _, err := s.Batch(ctx, []store.Operation{store.PutOp{
		Namespace: store.Namespace{"docs"}, Key: "a", Value: store.Value{"text": "Rust ownership", "kind": "tech"},
	}}); err != nil {
		t.Fatal(err)
	}
	items, err = s.Search(ctx, store.Namespace{"docs"}, store.SearchOptions{Query: "golang", Limit: 3})
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 3 || items[0].Key != "b" || items[1].Key != "a" || items[2].Key != "c" {
		t.Fatalf("updated results=%+v", items)
	}
	if err := s.Delete(ctx, store.Namespace{"docs"}, "b"); err != nil {
		t.Fatal(err)
	}
	items, err = s.Search(ctx, store.Namespace{"docs"}, store.SearchOptions{Query: "golang", Limit: 3})
	if err != nil || len(items) != 2 || items[0].Key != "a" || items[1].Key != "c" {
		t.Fatalf("after delete results=%+v err=%v", items, err)
	}
}

func TestJSONPathsBatchEmbeddingAndMaxPooling(t *testing.T) {
	embedder := &fakeEmbedder{vectors: map[string][]float32{
		"Nested": {0, 1}, "first": {1, 0}, "second": {0.8, 0.2}, "last": {-1, 0}, "query": {1, 0},
	}}
	s, _ := newStore(t, embedder, []string{"metadata.title", "sections[*].text", "tags[-1]", "missing"})
	value := store.Value{
		"metadata": map[string]any{"title": "Nested"},
		"sections": []any{map[string]any{"text": "first"}, map[string]any{"text": "second"}},
		"tags":     []any{"ignored", "last"},
	}
	if err := s.Put(context.Background(), store.Namespace{"docs"}, "multi", value); err != nil {
		t.Fatal(err)
	}
	embedder.mu.Lock()
	calls := append([][]string(nil), embedder.docCalls...)
	embedder.mu.Unlock()
	if len(calls) != 1 || !reflect.DeepEqual(calls[0], []string{"Nested", "first", "second", "last"}) {
		t.Fatalf("EmbedDocuments calls=%v", calls)
	}
	items, err := s.Search(context.Background(), store.Namespace{"docs"}, store.SearchOptions{Query: "query", Limit: 1})
	if err != nil || len(items) != 1 || items[0].Score == nil || *items[0].Score != 1 {
		t.Fatalf("max pooled results=%+v err=%v", items, err)
	}
}

func TestBatchReadsSnapshotBeforeWrites(t *testing.T) {
	embedder := &fakeEmbedder{vectors: map[string][]float32{
		"old": {1, 0}, "new": {-1, 0}, "query": {1, 0},
	}}
	s, _ := newStore(t, embedder, []string{"text"})
	ctx := context.Background()
	if err := s.Put(ctx, store.Namespace{"docs"}, "one", store.Value{"text": "old"}); err != nil {
		t.Fatal(err)
	}
	results, err := s.Batch(ctx, []store.Operation{
		store.SearchOp{NamespacePrefix: store.Namespace{"docs"}, Query: "query", Limit: 10},
		store.PutOp{Namespace: store.Namespace{"docs"}, Key: "one", Value: store.Value{"text": "new"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(results[0].Items) != 1 || results[0].Items[0].Value["text"] != "old" || results[0].Items[0].Score == nil || *results[0].Items[0].Score != 1 {
		t.Fatalf("snapshot search=%+v", results[0].Items)
	}
	item, err := s.Get(ctx, store.Namespace{"docs"}, "one")
	if err != nil || item.Value["text"] != "new" {
		t.Fatalf("post-batch item=%+v err=%v", item, err)
	}
}

func TestEmbeddingFailureAndCardinalityDoNotWrite(t *testing.T) {
	for _, malformed := range []bool{false, true} {
		t.Run(map[bool]string{false: "provider error", true: "cardinality"}[malformed], func(t *testing.T) {
			embedder := &fakeEmbedder{vectors: map[string][]float32{"text": {1, 0}}, fail: !malformed, malformed: malformed}
			s, base := newStore(t, embedder, []string{"text"})
			err := s.Put(context.Background(), store.Namespace{"docs"}, "one", store.Value{"text": "text"})
			if err == nil {
				t.Fatal("Put succeeded")
			}
			item, getErr := base.Get(context.Background(), store.Namespace{"docs"}, "one")
			if getErr != nil || item != nil {
				t.Fatalf("base mutated item=%+v err=%v", item, getErr)
			}
		})
	}
}

func TestConstructorValidation(t *testing.T) {
	index, _ := vectormemory.New(2, store.VectorCosine)
	embedder := &fakeEmbedder{}
	for _, config := range []indexed.Config{
		{Index: index},
		{Embedder: embedder},
		{Embedder: embedder, Index: index, Fields: []string{""}},
	} {
		if _, err := indexed.New(storememory.New(), config); err == nil {
			t.Fatalf("New accepted config=%+v", config)
		}
	}
	if _, err := indexed.New(nil, indexed.Config{Embedder: embedder, Index: index}); err == nil {
		t.Fatal("New accepted nil base")
	}
}

func TestIndexSyncFailureIsExplicitAndRetryRepairs(t *testing.T) {
	embedder := &fakeEmbedder{vectors: map[string][]float32{"document": {1, 0}, "query": {1, 0}}}
	delegate, _ := vectormemory.New(2, store.VectorCosine)
	index := &toggleIndex{delegate: delegate, fail: true}
	base := storememory.New()
	s, err := indexed.New(base, indexed.Config{Embedder: embedder, Index: index, Fields: []string{"text"}})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	err = s.Put(ctx, store.Namespace{"docs"}, "one", store.Value{"text": "document"})
	if !errors.Is(err, indexed.ErrIndexSync) || !errors.Is(err, errInjectedIndex) {
		t.Fatalf("Put err=%v", err)
	}
	item, getErr := base.Get(ctx, store.Namespace{"docs"}, "one")
	if getErr != nil || item == nil {
		t.Fatalf("committed item=%+v err=%v", item, getErr)
	}
	index.mu.Lock()
	index.fail = false
	index.mu.Unlock()
	if err := s.Put(ctx, store.Namespace{"docs"}, "one", store.Value{"text": "document"}); err != nil {
		t.Fatal(err)
	}
	items, err := s.Search(ctx, store.Namespace{"docs"}, store.SearchOptions{Query: "query", Limit: 1})
	if err != nil || len(items) != 1 || items[0].Score == nil || *items[0].Score != 1 {
		t.Fatalf("repaired search=%+v err=%v", items, err)
	}
}

func TestConcurrentPutAndSearch(t *testing.T) {
	vectors := map[string][]float32{"query": {1, 0}}
	for index := 0; index < 20; index++ {
		vectors[fmt.Sprintf("doc-%d", index)] = []float32{1, float32(index) / 100}
	}
	embedder := &fakeEmbedder{vectors: vectors}
	s, _ := newStore(t, embedder, []string{"text"})
	ctx := context.Background()
	var group sync.WaitGroup
	for index := 0; index < 20; index++ {
		index := index
		group.Add(2)
		go func() {
			defer group.Done()
			if err := s.Put(ctx, store.Namespace{"concurrent"}, fmt.Sprintf("%02d", index), store.Value{"text": fmt.Sprintf("doc-%d", index)}); err != nil {
				t.Errorf("Put: %v", err)
			}
		}()
		go func() {
			defer group.Done()
			if _, err := s.Search(ctx, store.Namespace{"concurrent"}, store.SearchOptions{Query: "query", Limit: 5}); err != nil {
				t.Errorf("Search: %v", err)
			}
		}()
	}
	group.Wait()
	items, err := s.Search(ctx, store.Namespace{"concurrent"}, store.SearchOptions{Query: "query", Limit: 20})
	if err != nil || len(items) != 20 {
		t.Fatalf("final Search len=%d err=%v", len(items), err)
	}
}
