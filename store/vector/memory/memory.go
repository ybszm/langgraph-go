// Package memory implements a deterministic, concurrency-safe VectorIndex.
package memory

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"sync"

	"github.com/wahanbo/langgraph-go/store"
)

type documentKey struct {
	namespace string
	key       string
	field     string
}

type document struct {
	namespace store.Namespace
	key       string
	field     string
	vector    []float32
	norm      float64
}

// Index stores fixed-dimension vectors in process memory.
type Index struct {
	mu         sync.RWMutex
	dimensions int
	metric     store.VectorMetric
	documents  map[documentKey]document
}

// New creates an empty vector index with a fixed positive dimension.
func New(dimensions int, metric store.VectorMetric) (*Index, error) {
	if dimensions <= 0 {
		return nil, fmt.Errorf("%w: dimensions must be positive", store.ErrInvalidVector)
	}
	switch metric {
	case store.VectorCosine, store.VectorL2, store.VectorInnerProduct:
	default:
		return nil, fmt.Errorf("%w: metric %q", store.ErrInvalidVector, metric)
	}
	return &Index{
		dimensions: dimensions,
		metric:     metric,
		documents:  make(map[documentKey]document),
	}, nil
}

// Upsert atomically validates and stores all documents. Duplicate identities
// within one call use last-write-wins.
func (i *Index) Upsert(ctx context.Context, documents []store.VectorDocument) error {
	if err := contextError(ctx); err != nil {
		return err
	}
	prepared := make([]struct {
		key documentKey
		doc document
	}, len(documents))
	for index, source := range documents {
		if err := contextError(ctx); err != nil {
			return err
		}
		if len(source.Namespace) == 0 || source.Key == "" || source.Field == "" {
			return fmt.Errorf("%w: document has an empty namespace, key, or field", store.ErrInvalidVector)
		}
		for _, label := range source.Namespace {
			if label == "" {
				return fmt.Errorf("%w: namespace label is empty", store.ErrInvalidVector)
			}
		}
		norm, err := validateVector(source.Vector, i.dimensions, i.metric == store.VectorCosine)
		if err != nil {
			return err
		}
		namespaceKey, err := encodeNamespace(source.Namespace)
		if err != nil {
			return err
		}
		prepared[index].key = documentKey{namespace: namespaceKey, key: source.Key, field: source.Field}
		prepared[index].doc = document{
			namespace: cloneNamespace(source.Namespace), key: source.Key, field: source.Field,
			vector: append([]float32(nil), source.Vector...), norm: norm,
		}
	}
	if err := contextError(ctx); err != nil {
		return err
	}
	i.mu.Lock()
	defer i.mu.Unlock()
	for _, item := range prepared {
		i.documents[item.key] = item.doc
	}
	return nil
}

// Delete removes every indexed field for one namespace/key pair.
func (i *Index) Delete(ctx context.Context, namespace store.Namespace, key string) error {
	if err := contextError(ctx); err != nil {
		return err
	}
	if len(namespace) == 0 || key == "" {
		return fmt.Errorf("%w: delete namespace or key is empty", store.ErrInvalidVector)
	}
	namespaceKey, err := encodeNamespace(namespace)
	if err != nil {
		return err
	}
	i.mu.Lock()
	defer i.mu.Unlock()
	for candidate := range i.documents {
		if candidate.namespace == namespaceKey && candidate.key == key {
			delete(i.documents, candidate)
		}
	}
	return nil
}

// Search returns deterministic nearest-field matches. Cosine and inner
// product use their natural score; L2 uses negative Euclidean distance.
func (i *Index) Search(ctx context.Context, query store.VectorQuery) ([]store.VectorMatch, error) {
	if err := contextError(ctx); err != nil {
		return nil, err
	}
	if query.Limit < 0 || query.Offset < 0 {
		return nil, fmt.Errorf("%w: negative pagination", store.ErrInvalidVector)
	}
	queryNorm, err := validateVector(query.Vector, i.dimensions, i.metric == store.VectorCosine)
	if err != nil {
		return nil, err
	}
	limit := query.Limit
	if limit == 0 {
		limit = 10
	}
	i.mu.RLock()
	matches := make([]store.VectorMatch, 0, len(i.documents))
	for _, candidate := range i.documents {
		if err := contextError(ctx); err != nil {
			i.mu.RUnlock()
			return nil, err
		}
		if !hasPrefix(candidate.namespace, query.NamespacePrefix) {
			continue
		}
		matches = append(matches, store.VectorMatch{
			Namespace: cloneNamespace(candidate.namespace), Key: candidate.key, Field: candidate.field,
			Score: i.score(query.Vector, queryNorm, candidate),
		})
	}
	i.mu.RUnlock()
	sort.Slice(matches, func(a, b int) bool {
		if matches[a].Score != matches[b].Score {
			return matches[a].Score > matches[b].Score
		}
		left, right := namespaceSortKey(matches[a].Namespace), namespaceSortKey(matches[b].Namespace)
		if left != right {
			return left < right
		}
		if matches[a].Key != matches[b].Key {
			return matches[a].Key < matches[b].Key
		}
		return matches[a].Field < matches[b].Field
	})
	start := min(query.Offset, len(matches))
	end := min(start+limit, len(matches))
	return matches[start:end], nil
}

func (i *Index) score(query []float32, queryNorm float64, candidate document) float64 {
	var dot, squared float64
	for index, value := range query {
		left, right := float64(value), float64(candidate.vector[index])
		dot += left * right
		difference := left - right
		squared += difference * difference
	}
	switch i.metric {
	case store.VectorCosine:
		return dot / (queryNorm * candidate.norm)
	case store.VectorL2:
		return -math.Sqrt(squared)
	default:
		return dot
	}
}

func validateVector(vector []float32, dimensions int, requireNonzero bool) (float64, error) {
	if len(vector) != dimensions {
		return 0, fmt.Errorf("%w: dimensions=%d want=%d", store.ErrInvalidVector, len(vector), dimensions)
	}
	var squared float64
	for _, value := range vector {
		converted := float64(value)
		if math.IsNaN(converted) || math.IsInf(converted, 0) {
			return 0, fmt.Errorf("%w: vector contains a non-finite value", store.ErrInvalidVector)
		}
		squared += converted * converted
	}
	if requireNonzero && squared == 0 {
		return 0, fmt.Errorf("%w: cosine vector has zero norm", store.ErrInvalidVector)
	}
	return math.Sqrt(squared), nil
}

func contextError(ctx context.Context) error {
	if ctx == nil {
		return fmt.Errorf("%w: context is nil", store.ErrInvalidVector)
	}
	return ctx.Err()
}

func encodeNamespace(namespace store.Namespace) (string, error) {
	data, err := json.Marshal(namespace)
	if err != nil {
		return "", fmt.Errorf("%w: encode namespace: %v", store.ErrInvalidVector, err)
	}
	return string(data), nil
}

func namespaceSortKey(namespace store.Namespace) string {
	encoded, _ := json.Marshal(namespace)
	return string(encoded)
}

func cloneNamespace(namespace store.Namespace) store.Namespace {
	return append(store.Namespace(nil), namespace...)
}

func hasPrefix(namespace, prefix store.Namespace) bool {
	if len(namespace) < len(prefix) {
		return false
	}
	for index := range prefix {
		if namespace[index] != prefix[index] {
			return false
		}
	}
	return true
}

var _ store.VectorIndex = (*Index)(nil)
