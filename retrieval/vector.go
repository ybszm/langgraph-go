package retrieval

import (
	"context"
	"fmt"
	"sync"

	"github.com/wahanbo/langgraph-go/store"
)

// VectorIndex composes the existing embedding and vector-index contracts with
// retrievable document payloads.
type VectorIndex struct {
	mu        sync.RWMutex
	embedder  store.Embedder
	index     store.VectorIndex
	namespace store.Namespace
	documents map[string]Document
}

// NewVectorIndex creates a retrieval index over an existing vector backend.
// Document payloads remain process-local and should be upserted after restart.
func NewVectorIndex(embedder store.Embedder, index store.VectorIndex, namespace store.Namespace) (*VectorIndex, error) {
	if embedder == nil || index == nil || len(namespace) == 0 {
		return nil, fmt.Errorf("%w: embedder, vector index, and namespace are required", ErrInvalidDocument)
	}
	return &VectorIndex{embedder: embedder, index: index, namespace: append(store.Namespace(nil), namespace...), documents: map[string]Document{}}, nil
}

func (index *VectorIndex) Upsert(ctx context.Context, documents []Document) error {
	if ctx == nil {
		return fmt.Errorf("%w: context is nil", ErrInvalidDocument)
	}
	texts := make([]string, len(documents))
	prepared := make([]Document, len(documents))
	for i, document := range documents {
		if err := validateDocument(document); err != nil {
			return err
		}
		texts[i] = document.Content
		prepared[i] = document.Clone()
	}
	vectors, err := index.embedder.EmbedDocuments(ctx, texts)
	if err != nil {
		return fmt.Errorf("embed retrieval documents: %w", err)
	}
	if len(vectors) != len(documents) {
		return fmt.Errorf("%w: embedder returned %d vectors for %d documents", ErrInvalidDocument, len(vectors), len(documents))
	}
	vectorDocuments := make([]store.VectorDocument, len(documents))
	for i, document := range documents {
		vectorDocuments[i] = store.VectorDocument{Namespace: append(store.Namespace(nil), index.namespace...), Key: document.ID, Field: "content", Vector: append([]float32(nil), vectors[i]...)}
	}
	if err := index.index.Upsert(ctx, vectorDocuments); err != nil {
		return fmt.Errorf("upsert retrieval vectors: %w", err)
	}
	index.mu.Lock()
	defer index.mu.Unlock()
	for _, document := range prepared {
		index.documents[document.ID] = document
	}
	return nil
}

func (index *VectorIndex) Delete(ctx context.Context, ids []string) error {
	if ctx == nil {
		return fmt.Errorf("%w: context is nil", ErrInvalidDocument)
	}
	for _, id := range ids {
		if err := index.index.Delete(ctx, index.namespace, id); err != nil {
			return fmt.Errorf("delete retrieval vector %q: %w", id, err)
		}
	}
	index.mu.Lock()
	defer index.mu.Unlock()
	for _, id := range ids {
		delete(index.documents, id)
	}
	return nil
}

func (index *VectorIndex) Retrieve(ctx context.Context, query Query) ([]Result, error) {
	if err := validateQuery(ctx, query); err != nil {
		return nil, err
	}
	vector, err := index.embedder.EmbedQuery(ctx, query.Text)
	if err != nil {
		return nil, fmt.Errorf("embed retrieval query: %w", err)
	}
	limit := query.Limit
	if limit == 0 {
		limit = 10
	}
	candidateLimit := limit
	if len(query.Filter) > 0 {
		candidateLimit = max(limit*4, 32)
	}
	matches, err := index.index.Search(ctx, store.VectorQuery{NamespacePrefix: index.namespace, Vector: vector, Limit: candidateLimit})
	if err != nil {
		return nil, fmt.Errorf("search retrieval vectors: %w", err)
	}
	index.mu.RLock()
	defer index.mu.RUnlock()
	results := make([]Result, 0, min(limit, len(matches)))
	seen := make(map[string]struct{})
	for _, match := range matches {
		if _, duplicate := seen[match.Key]; duplicate {
			continue
		}
		document, found := index.documents[match.Key]
		if !found || !metadataMatches(document.Metadata, query.Filter) {
			continue
		}
		seen[match.Key] = struct{}{}
		results = append(results, Result{Document: document.Clone(), Score: match.Score})
		if len(results) == limit {
			break
		}
	}
	return results, nil
}

var _ Index = (*VectorIndex)(nil)
