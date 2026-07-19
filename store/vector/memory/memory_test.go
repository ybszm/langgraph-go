package memory_test

import (
	"context"
	"errors"
	"math"
	"reflect"
	"sync"
	"testing"

	"github.com/ybszm/langgraph-go/store"
	vectormemory "github.com/ybszm/langgraph-go/store/vector/memory"
	"github.com/ybszm/langgraph-go/store/vectortest"
)

func TestVectorIndexContract(t *testing.T) {
	vectortest.Run(t, func(t *testing.T, dimensions int, metric store.VectorMetric) store.VectorIndex {
		t.Helper()
		index, err := vectormemory.New(dimensions, metric)
		if err != nil {
			t.Fatal(err)
		}
		return index
	})
}

func TestCosineSearchOrderingPaginationAndCopies(t *testing.T) {
	index, err := vectormemory.New(2, store.VectorCosine)
	if err != nil {
		t.Fatal(err)
	}
	documents := []store.VectorDocument{
		{Namespace: store.Namespace{"docs", "b"}, Key: "2", Field: "text", Vector: []float32{1, 0}},
		{Namespace: store.Namespace{"docs", "a"}, Key: "1", Field: "title", Vector: []float32{1, 0}},
		{Namespace: store.Namespace{"docs", "a"}, Key: "1", Field: "text", Vector: []float32{0.8, 0.2}},
		{Namespace: store.Namespace{"other"}, Key: "3", Field: "text", Vector: []float32{1, 0}},
	}
	if err := index.Upsert(context.Background(), documents); err != nil {
		t.Fatal(err)
	}
	documents[0].Vector[0] = -1
	matches, err := index.Search(context.Background(), store.VectorQuery{
		NamespacePrefix: store.Namespace{"docs"}, Vector: []float32{1, 0}, Limit: 2, Offset: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	wantIDs := []string{"docs/b/2/text", "docs/a/1/text"}
	gotIDs := []string{
		matches[0].Namespace[0] + "/" + matches[0].Namespace[1] + "/" + matches[0].Key + "/" + matches[0].Field,
		matches[1].Namespace[0] + "/" + matches[1].Namespace[1] + "/" + matches[1].Key + "/" + matches[1].Field,
	}
	if !reflect.DeepEqual(gotIDs, wantIDs) {
		t.Fatalf("matches=%+v", matches)
	}
	matches[0].Namespace[0] = "mutated"
	again, _ := index.Search(context.Background(), store.VectorQuery{NamespacePrefix: store.Namespace{"docs"}, Vector: []float32{1, 0}, Limit: 10})
	if again[0].Namespace[0] != "docs" {
		t.Fatal("Search returned aliased namespace")
	}
}

func TestMetricsUpsertAndDelete(t *testing.T) {
	for _, metric := range []store.VectorMetric{store.VectorL2, store.VectorInnerProduct} {
		t.Run(string(metric), func(t *testing.T) {
			index, err := vectormemory.New(2, metric)
			if err != nil {
				t.Fatal(err)
			}
			if err := index.Upsert(context.Background(), []store.VectorDocument{
				{Namespace: store.Namespace{"n"}, Key: "far", Field: "f", Vector: []float32{0, 1}},
				{Namespace: store.Namespace{"n"}, Key: "near", Field: "f", Vector: []float32{1, 0}},
			}); err != nil {
				t.Fatal(err)
			}
			if err := index.Upsert(context.Background(), []store.VectorDocument{{Namespace: store.Namespace{"n"}, Key: "far", Field: "f", Vector: []float32{2, 0}}}); err != nil {
				t.Fatal(err)
			}
			matches, err := index.Search(context.Background(), store.VectorQuery{NamespacePrefix: store.Namespace{"n"}, Vector: []float32{1, 0}, Limit: 10})
			wantFirst := "far"
			if metric == store.VectorL2 {
				wantFirst = "near"
			}
			if err != nil || len(matches) != 2 || matches[0].Key != wantFirst {
				t.Fatalf("matches=%+v err=%v", matches, err)
			}
			if err := index.Delete(context.Background(), store.Namespace{"n"}, "far"); err != nil {
				t.Fatal(err)
			}
			matches, _ = index.Search(context.Background(), store.VectorQuery{NamespacePrefix: store.Namespace{"n"}, Vector: []float32{1, 0}, Limit: 10})
			if len(matches) != 1 || matches[0].Key != "near" {
				t.Fatalf("after delete=%+v", matches)
			}
		})
	}
}

func TestValidationCancellationAndAtomicUpsert(t *testing.T) {
	if _, err := vectormemory.New(0, store.VectorCosine); !errors.Is(err, store.ErrInvalidVector) {
		t.Fatalf("zero dimensions err=%v", err)
	}
	if _, err := vectormemory.New(2, "bad"); !errors.Is(err, store.ErrInvalidVector) {
		t.Fatalf("metric err=%v", err)
	}
	index, _ := vectormemory.New(2, store.VectorCosine)
	err := index.Upsert(context.Background(), []store.VectorDocument{
		{Namespace: store.Namespace{"valid"}, Key: "one", Field: "f", Vector: []float32{1, 0}},
		{Namespace: store.Namespace{"valid"}, Key: "bad", Field: "f", Vector: []float32{1}},
	})
	if !errors.Is(err, store.ErrInvalidVector) {
		t.Fatalf("dimension err=%v", err)
	}
	matches, _ := index.Search(context.Background(), store.VectorQuery{Vector: []float32{1, 0}, Limit: 10})
	if len(matches) != 0 {
		t.Fatalf("invalid batch partially committed: %+v", matches)
	}
	invalidVectors := [][]float32{{0, 0}, {float32(math.Inf(1)), 0}, {float32(math.NaN()), 0}}
	for _, vector := range invalidVectors {
		err := index.Upsert(context.Background(), []store.VectorDocument{{Namespace: store.Namespace{"n"}, Key: "k", Field: "f", Vector: vector}})
		if !errors.Is(err, store.ErrInvalidVector) {
			t.Fatalf("vector=%v err=%v", vector, err)
		}
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := index.Search(canceled, store.VectorQuery{Vector: []float32{1, 0}, Limit: 1}); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled Search err=%v", err)
	}
}

func TestConcurrentUpsertAndSearch(t *testing.T) {
	index, _ := vectormemory.New(2, store.VectorCosine)
	var group sync.WaitGroup
	for worker := 0; worker < 16; worker++ {
		worker := worker
		group.Add(1)
		go func() {
			defer group.Done()
			for iteration := 0; iteration < 50; iteration++ {
				if err := index.Upsert(context.Background(), []store.VectorDocument{{
					Namespace: store.Namespace{"workers"}, Key: string(rune('a' + worker)), Field: "f", Vector: []float32{1, float32(worker) / 16},
				}}); err != nil {
					t.Errorf("Upsert: %v", err)
					return
				}
				if _, err := index.Search(context.Background(), store.VectorQuery{NamespacePrefix: store.Namespace{"workers"}, Vector: []float32{1, 0}, Limit: 20}); err != nil {
					t.Errorf("Search: %v", err)
					return
				}
			}
		}()
	}
	group.Wait()
	matches, err := index.Search(context.Background(), store.VectorQuery{NamespacePrefix: store.Namespace{"workers"}, Vector: []float32{1, 0}, Limit: 20})
	if err != nil || len(matches) != 16 {
		t.Fatalf("matches len=%d err=%v", len(matches), err)
	}
}
