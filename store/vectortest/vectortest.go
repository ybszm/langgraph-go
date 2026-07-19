// Package vectortest provides a reusable black-box conformance suite for
// provider-neutral VectorIndex implementations.
package vectortest

import (
	"context"
	"errors"
	"math"
	"sync"
	"testing"

	"github.com/wahanbo/langgraph-go/store"
)

// Factory constructs a fresh, empty index for one metric and dimension.
type Factory func(*testing.T, int, store.VectorMetric) store.VectorIndex

// Run registers the complete VectorIndex behavioral contract.
func Run(t *testing.T, factory Factory) {
	t.Helper()
	if factory == nil {
		t.Fatal("vectortest factory is nil")
	}
	t.Run("metrics", func(t *testing.T) { testMetrics(t, factory) })
	t.Run("identity-delete-pagination-copies", func(t *testing.T) { testIdentityDeletePaginationCopies(t, factory) })
	t.Run("validation-cancellation-atomicity", func(t *testing.T) { testValidationCancellationAtomicity(t, factory) })
	t.Run("concurrency", func(t *testing.T) { testConcurrency(t, factory) })
}

func testMetrics(t *testing.T, factory Factory) {
	tests := []struct {
		name       string
		metric     store.VectorMetric
		query      []float32
		documents  []store.VectorDocument
		wantKeys   []string
		wantScores []float64
	}{
		{
			name: "cosine", metric: store.VectorCosine, query: []float32{1, 0},
			documents: []store.VectorDocument{
				{Namespace: store.Namespace{"n"}, Key: "exact", Field: "text", Vector: []float32{1, 0}},
				{Namespace: store.Namespace{"n"}, Key: "diagonal", Field: "text", Vector: []float32{1, 1}},
				{Namespace: store.Namespace{"n"}, Key: "opposite", Field: "text", Vector: []float32{-1, 0}},
			},
			wantKeys: []string{"exact", "diagonal", "opposite"}, wantScores: []float64{1, 1 / math.Sqrt2, -1},
		},
		{
			name: "l2", metric: store.VectorL2, query: []float32{0, 0},
			documents: []store.VectorDocument{
				{Namespace: store.Namespace{"n"}, Key: "far", Field: "text", Vector: []float32{3, 0}},
				{Namespace: store.Namespace{"n"}, Key: "near", Field: "text", Vector: []float32{1, 0}},
			},
			wantKeys: []string{"near", "far"}, wantScores: []float64{-1, -3},
		},
		{
			name: "inner-product", metric: store.VectorInnerProduct, query: []float32{1, 0},
			documents: []store.VectorDocument{
				{Namespace: store.Namespace{"n"}, Key: "low", Field: "text", Vector: []float32{1, 0}},
				{Namespace: store.Namespace{"n"}, Key: "high", Field: "text", Vector: []float32{3, 0}},
			},
			wantKeys: []string{"high", "low"}, wantScores: []float64{3, 1},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			index := factory(t, 2, test.metric)
			if err := index.Upsert(context.Background(), test.documents); err != nil {
				t.Fatal(err)
			}
			matches, err := index.Search(context.Background(), store.VectorQuery{NamespacePrefix: store.Namespace{"n"}, Vector: test.query, Limit: 10})
			if err != nil || len(matches) != len(test.wantKeys) {
				t.Fatalf("Search matches=%+v err=%v", matches, err)
			}
			for position, match := range matches {
				if match.Key != test.wantKeys[position] || math.Abs(match.Score-test.wantScores[position]) > 1e-6 {
					t.Fatalf("match %d=%+v want key=%q score=%v", position, match, test.wantKeys[position], test.wantScores[position])
				}
			}
		})
	}
}

func testIdentityDeletePaginationCopies(t *testing.T, factory Factory) {
	index := factory(t, 2, store.VectorCosine)
	documents := []store.VectorDocument{
		{Namespace: store.Namespace{"docs", "b"}, Key: "same", Field: "title", Vector: []float32{1, 0}},
		{Namespace: store.Namespace{"docs", "a"}, Key: "same", Field: "title", Vector: []float32{1, 0}},
		{Namespace: store.Namespace{"docs", "a"}, Key: "same", Field: "body", Vector: []float32{0, 1}},
	}
	if err := index.Upsert(context.Background(), documents); err != nil {
		t.Fatal(err)
	}
	documents[0].Namespace[0] = "mutated"
	documents[0].Vector[0] = -1
	matches, err := index.Search(context.Background(), store.VectorQuery{
		NamespacePrefix: store.Namespace{"docs"}, Vector: []float32{1, 0}, Limit: 2, Offset: 1,
	})
	if err != nil || len(matches) != 2 || matches[0].Namespace[1] != "b" || matches[1].Field != "body" {
		t.Fatalf("paginated matches=%+v err=%v", matches, err)
	}
	matches[0].Namespace[0] = "changed"
	again, err := index.Search(context.Background(), store.VectorQuery{NamespacePrefix: store.Namespace{"docs"}, Vector: []float32{1, 0}, Limit: 10})
	if err != nil || again[0].Namespace[0] != "docs" {
		t.Fatalf("aliased Search result=%+v err=%v", again, err)
	}
	if err := index.Delete(context.Background(), store.Namespace{"docs", "a"}, "same"); err != nil {
		t.Fatal(err)
	}
	again, err = index.Search(context.Background(), store.VectorQuery{NamespacePrefix: store.Namespace{"docs"}, Vector: []float32{1, 0}, Limit: 10})
	if err != nil || len(again) != 1 || again[0].Namespace[1] != "b" {
		t.Fatalf("after Delete=%+v err=%v", again, err)
	}
}

func testValidationCancellationAtomicity(t *testing.T, factory Factory) {
	index := factory(t, 2, store.VectorCosine)
	ctx := context.Background()
	if err := index.Upsert(ctx, []store.VectorDocument{{Namespace: store.Namespace{"n"}, Key: "base", Field: "text", Vector: []float32{1, 0}}}); err != nil {
		t.Fatal(err)
	}
	err := index.Upsert(ctx, []store.VectorDocument{
		{Namespace: store.Namespace{"n"}, Key: "should-not-commit", Field: "text", Vector: []float32{1, 0}},
		{Namespace: store.Namespace{"n"}, Key: "invalid", Field: "text", Vector: []float32{float32(math.NaN()), 0}},
	})
	if !errors.Is(err, store.ErrInvalidVector) {
		t.Fatalf("invalid batch err=%v", err)
	}
	matches, err := index.Search(ctx, store.VectorQuery{NamespacePrefix: store.Namespace{"n"}, Vector: []float32{1, 0}, Limit: 10})
	if err != nil || len(matches) != 1 || matches[0].Key != "base" {
		t.Fatalf("batch was not atomic matches=%+v err=%v", matches, err)
	}
	if _, err := index.Search(ctx, store.VectorQuery{Vector: []float32{0, 0}}); !errors.Is(err, store.ErrInvalidVector) {
		t.Fatalf("zero cosine query err=%v", err)
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := index.Search(canceled, store.VectorQuery{Vector: []float32{1, 0}}); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled Search err=%v", err)
	}
}

func testConcurrency(t *testing.T, factory Factory) {
	index := factory(t, 2, store.VectorCosine)
	ctx := context.Background()
	errorsFound := make(chan error, 32)
	var group sync.WaitGroup
	for worker := 0; worker < 16; worker++ {
		worker := worker
		group.Add(2)
		go func() {
			defer group.Done()
			err := index.Upsert(ctx, []store.VectorDocument{{
				Namespace: store.Namespace{"workers"}, Key: string(rune('a' + worker)), Field: "text",
				Vector: []float32{1, float32(worker+1) / 100},
			}})
			if err != nil {
				errorsFound <- err
			}
		}()
		go func() {
			defer group.Done()
			if _, err := index.Search(ctx, store.VectorQuery{NamespacePrefix: store.Namespace{"workers"}, Vector: []float32{1, 0}, Limit: 16}); err != nil {
				errorsFound <- err
			}
		}()
	}
	group.Wait()
	close(errorsFound)
	for err := range errorsFound {
		t.Errorf("concurrent operation: %v", err)
	}
	matches, err := index.Search(ctx, store.VectorQuery{NamespacePrefix: store.Namespace{"workers"}, Vector: []float32{1, 0}, Limit: 20})
	if err != nil || len(matches) != 16 {
		t.Fatalf("final Search len=%d err=%v", len(matches), err)
	}
}
