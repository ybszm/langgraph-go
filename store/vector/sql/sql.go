// Package sql implements a durable, provider-neutral VectorIndex over SQLite
// or PostgreSQL. Vectors are stored as portable JSON and scored in Go, making
// this backend suitable when sqlite-vec/pgvector extensions are unavailable.
package sql

import (
	"context"
	stdsql "database/sql"
	"encoding/json"
	"fmt"
	"math"
	"sort"

	"github.com/ybszm/langgraph-go/store"
)

// Dialect selects SQL placeholder and setup behavior.
type Dialect string

const (
	SQLite   Dialect = "sqlite"
	Postgres Dialect = "postgres"
)

// Index is a durable fixed-dimension vector index.
type Index struct {
	db         *stdsql.DB
	dialect    Dialect
	dimensions int
	metric     store.VectorMetric
}

// New validates a SQL vector index. Call Setup before use.
func New(db *stdsql.DB, dialect Dialect, dimensions int, metric store.VectorMetric) (*Index, error) {
	if db == nil || dimensions <= 0 {
		return nil, fmt.Errorf("%w: SQL vector index requires DB and positive dimensions", store.ErrInvalidVector)
	}
	if dialect != SQLite && dialect != Postgres {
		return nil, fmt.Errorf("%w: SQL vector dialect %q", store.ErrInvalidVector, dialect)
	}
	switch metric {
	case store.VectorCosine, store.VectorL2, store.VectorInnerProduct:
	default:
		return nil, fmt.Errorf("%w: metric %q", store.ErrInvalidVector, metric)
	}
	return &Index{db: db, dialect: dialect, dimensions: dimensions, metric: metric}, nil
}

// Setup creates the idempotent portable vector table.
func (i *Index) Setup(ctx context.Context) error {
	if err := contextError(ctx); err != nil {
		return err
	}
	if i.dialect == SQLite {
		if _, err := i.db.ExecContext(ctx, "PRAGMA busy_timeout = 5000"); err != nil {
			return fmt.Errorf("setup SQL vector busy timeout: %w", err)
		}
		if _, err := i.db.ExecContext(ctx, "PRAGMA journal_mode = WAL"); err != nil {
			return fmt.Errorf("setup SQL vector WAL: %w", err)
		}
	}
	_, err := i.db.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS langgraph_vector_index (
namespace TEXT NOT NULL,
item_key TEXT NOT NULL,
field TEXT NOT NULL,
vector TEXT NOT NULL,
PRIMARY KEY (namespace, item_key, field)
)`)
	if err != nil {
		return fmt.Errorf("setup SQL vector table: %w", err)
	}
	return nil
}

type preparedDocument struct {
	namespace string
	key       string
	field     string
	vector    string
}

// Upsert atomically validates and writes a document batch.
func (i *Index) Upsert(ctx context.Context, documents []store.VectorDocument) error {
	if err := contextError(ctx); err != nil {
		return err
	}
	prepared := make([]preparedDocument, len(documents))
	for index, document := range documents {
		if len(document.Namespace) == 0 || document.Key == "" || document.Field == "" {
			return fmt.Errorf("%w: document has an empty namespace, key, or field", store.ErrInvalidVector)
		}
		for _, label := range document.Namespace {
			if label == "" {
				return fmt.Errorf("%w: namespace label is empty", store.ErrInvalidVector)
			}
		}
		if _, err := validateVector(document.Vector, i.dimensions, i.metric == store.VectorCosine); err != nil {
			return err
		}
		namespace, err := json.Marshal(document.Namespace)
		if err != nil {
			return fmt.Errorf("%w: encode namespace: %v", store.ErrInvalidVector, err)
		}
		vector, err := json.Marshal(document.Vector)
		if err != nil {
			return fmt.Errorf("%w: encode vector: %v", store.ErrInvalidVector, err)
		}
		prepared[index] = preparedDocument{namespace: string(namespace), key: document.Key, field: document.Field, vector: string(vector)}
	}
	tx, err := i.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin SQL vector upsert: %w", err)
	}
	defer tx.Rollback()
	query := `INSERT INTO langgraph_vector_index(namespace, item_key, field, vector)
VALUES (?, ?, ?, ?)
ON CONFLICT(namespace, item_key, field) DO UPDATE SET vector = excluded.vector`
	if i.dialect == Postgres {
		query = `INSERT INTO langgraph_vector_index(namespace, item_key, field, vector)
VALUES ($1, $2, $3, $4)
ON CONFLICT(namespace, item_key, field) DO UPDATE SET vector = excluded.vector`
	}
	for _, document := range prepared {
		if _, err := tx.ExecContext(ctx, query, document.namespace, document.key, document.field, document.vector); err != nil {
			return fmt.Errorf("upsert SQL vector document: %w", err)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit SQL vector upsert: %w", err)
	}
	return nil
}

// Delete removes all indexed fields for one item.
func (i *Index) Delete(ctx context.Context, namespace store.Namespace, key string) error {
	if err := contextError(ctx); err != nil {
		return err
	}
	if len(namespace) == 0 || key == "" {
		return fmt.Errorf("%w: delete namespace or key is empty", store.ErrInvalidVector)
	}
	encoded, err := json.Marshal(namespace)
	if err != nil {
		return fmt.Errorf("%w: encode namespace: %v", store.ErrInvalidVector, err)
	}
	query := "DELETE FROM langgraph_vector_index WHERE namespace = ? AND item_key = ?"
	if i.dialect == Postgres {
		query = "DELETE FROM langgraph_vector_index WHERE namespace = $1 AND item_key = $2"
	}
	if _, err := i.db.ExecContext(ctx, query, string(encoded), key); err != nil {
		return fmt.Errorf("delete SQL vector document: %w", err)
	}
	return nil
}

type candidate struct {
	namespace store.Namespace
	key       string
	field     string
	vector    []float32
	norm      float64
}

// Search scans durable candidates and returns deterministic nearest matches.
// This portable fallback intentionally does not require a database extension.
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
	rows, err := i.db.QueryContext(ctx, "SELECT namespace, item_key, field, vector FROM langgraph_vector_index")
	if err != nil {
		return nil, fmt.Errorf("query SQL vectors: %w", err)
	}
	defer rows.Close()
	matches := make([]store.VectorMatch, 0)
	for rows.Next() {
		if err := contextError(ctx); err != nil {
			return nil, err
		}
		var namespaceRaw, key, field, vectorRaw string
		if err := rows.Scan(&namespaceRaw, &key, &field, &vectorRaw); err != nil {
			return nil, fmt.Errorf("scan SQL vector: %w", err)
		}
		var namespace store.Namespace
		var vector []float32
		if err := json.Unmarshal([]byte(namespaceRaw), &namespace); err != nil {
			return nil, fmt.Errorf("%w: decode SQL vector namespace: %v", store.ErrInvalidVector, err)
		}
		if !hasPrefix(namespace, query.NamespacePrefix) {
			continue
		}
		if err := json.Unmarshal([]byte(vectorRaw), &vector); err != nil {
			return nil, fmt.Errorf("%w: decode SQL vector: %v", store.ErrInvalidVector, err)
		}
		norm, err := validateVector(vector, i.dimensions, i.metric == store.VectorCosine)
		if err != nil {
			return nil, fmt.Errorf("corrupt SQL vector %s/%s/%s: %w", namespaceRaw, key, field, err)
		}
		matches = append(matches, store.VectorMatch{
			Namespace: append(store.Namespace(nil), namespace...), Key: key, Field: field,
			Score: i.score(query.Vector, queryNorm, candidate{vector: vector, norm: norm}),
		})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate SQL vectors: %w", err)
	}
	sort.Slice(matches, func(left, right int) bool {
		if matches[left].Score != matches[right].Score {
			return matches[left].Score > matches[right].Score
		}
		leftNamespace, _ := json.Marshal(matches[left].Namespace)
		rightNamespace, _ := json.Marshal(matches[right].Namespace)
		if string(leftNamespace) != string(rightNamespace) {
			return string(leftNamespace) < string(rightNamespace)
		}
		if matches[left].Key != matches[right].Key {
			return matches[left].Key < matches[right].Key
		}
		return matches[left].Field < matches[right].Field
	})
	limit := query.Limit
	if limit == 0 {
		limit = 10
	}
	start := min(query.Offset, len(matches))
	end := min(start+limit, len(matches))
	return matches[start:end], nil
}

func (i *Index) score(query []float32, queryNorm float64, document candidate) float64 {
	var dot, squared float64
	for index, value := range query {
		left, right := float64(value), float64(document.vector[index])
		dot += left * right
		difference := left - right
		squared += difference * difference
	}
	switch i.metric {
	case store.VectorCosine:
		return dot / (queryNorm * document.norm)
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
