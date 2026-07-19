package store

import (
	"context"
	"errors"
)

var ErrInvalidVector = errors.New("invalid store vector")

var ErrInvalidEmbedding = errors.New("invalid store embedding")

// Embedder converts stored document text and search queries into vectors.
// Implementations may batch, cache, or call any external embedding provider.
type Embedder interface {
	EmbedDocuments(context.Context, []string) ([][]float32, error)
	EmbedQuery(context.Context, string) ([]float32, error)
}

// VectorMetric determines how vector similarity is scored. Higher returned
// scores always mean a better match, including negative L2 distance.
type VectorMetric string

const (
	VectorCosine       VectorMetric = "cosine"
	VectorL2           VectorMetric = "l2"
	VectorInnerProduct VectorMetric = "inner_product"
)

// VectorDocument is one independently indexed field of a Store item.
type VectorDocument struct {
	Namespace Namespace
	Key       string
	Field     string
	Vector    []float32
}

// VectorQuery selects nearest indexed fields under a namespace prefix.
type VectorQuery struct {
	NamespacePrefix Namespace
	Vector          []float32
	Limit           int
	Offset          int
}

// VectorMatch identifies one indexed field and its similarity score.
type VectorMatch struct {
	Namespace Namespace
	Key       string
	Field     string
	Score     float64
}

// VectorIndex is the provider-neutral boundary between Store orchestration and
// concrete in-memory, pgvector, SQLite-vector, or external indexes.
type VectorIndex interface {
	Upsert(context.Context, []VectorDocument) error
	Delete(context.Context, Namespace, string) error
	Search(context.Context, VectorQuery) ([]VectorMatch, error)
}
