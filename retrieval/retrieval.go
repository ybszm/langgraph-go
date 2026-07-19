// Package retrieval provides small, provider-neutral document retrieval
// primitives that compose with LangGraph tools and the existing vector store.
package retrieval

import (
	"context"
	"errors"
	"fmt"
)

var ErrInvalidDocument = errors.New("invalid retrieval document")
var ErrInvalidQuery = errors.New("invalid retrieval query")

// Document is one retrievable text unit. ID must be stable within an index.
type Document struct {
	ID       string         `json:"id"`
	Content  string         `json:"content"`
	Metadata map[string]any `json:"metadata,omitempty"`
}

// Clone returns an isolated JSON-compatible document.
func (document Document) Clone() Document {
	document.Metadata = cloneMetadata(document.Metadata)
	return document
}

// Query controls one retrieval operation. Filter uses exact metadata matches.
type Query struct {
	Text   string
	Limit  int
	Filter map[string]any
}

// Result couples a document to a retriever-specific higher-is-better score.
type Result struct {
	Document Document `json:"document"`
	Score    float64  `json:"score"`
}

// Retriever is the provider-neutral search boundary.
type Retriever interface {
	Retrieve(context.Context, Query) ([]Result, error)
}

// RetrieverFunc adapts a function to Retriever.
type RetrieverFunc func(context.Context, Query) ([]Result, error)

func (function RetrieverFunc) Retrieve(ctx context.Context, query Query) ([]Result, error) {
	return function(ctx, query)
}

// Index is a mutable Retriever used by ingestion pipelines.
type Index interface {
	Retriever
	Upsert(context.Context, []Document) error
	Delete(context.Context, []string) error
}

// Loader produces source documents without coupling retrieval to filesystem,
// HTTP, object-store, or database SDKs.
type Loader interface {
	Load(context.Context) ([]Document, error)
}

// LoaderFunc adapts a function to Loader.
type LoaderFunc func(context.Context) ([]Document, error)

func (function LoaderFunc) Load(ctx context.Context) ([]Document, error) { return function(ctx) }

// Splitter turns source documents into independently retrievable chunks.
type Splitter interface {
	Split(context.Context, []Document) ([]Document, error)
}

// Ingest loads, optionally splits, and atomically hands the resulting batch to
// an Index. Individual indexes define their own commit behavior.
func Ingest(ctx context.Context, loader Loader, splitter Splitter, index Index) (int, error) {
	if ctx == nil {
		return 0, fmt.Errorf("%w: context is nil", ErrInvalidDocument)
	}
	if loader == nil || index == nil {
		return 0, fmt.Errorf("%w: loader and index are required", ErrInvalidDocument)
	}
	documents, err := loader.Load(ctx)
	if err != nil {
		return 0, fmt.Errorf("load retrieval documents: %w", err)
	}
	if splitter != nil {
		documents, err = splitter.Split(ctx, documents)
		if err != nil {
			return 0, fmt.Errorf("split retrieval documents: %w", err)
		}
	}
	if err := index.Upsert(ctx, documents); err != nil {
		return 0, fmt.Errorf("index retrieval documents: %w", err)
	}
	return len(documents), nil
}

func validateDocument(document Document) error {
	if document.ID == "" || document.Content == "" {
		return fmt.Errorf("%w: ID and content are required", ErrInvalidDocument)
	}
	return nil
}

func validateQuery(ctx context.Context, query Query) error {
	if ctx == nil {
		return fmt.Errorf("%w: context is nil", ErrInvalidQuery)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if query.Text == "" {
		return fmt.Errorf("%w: text is empty", ErrInvalidQuery)
	}
	if query.Limit < 0 {
		return fmt.Errorf("%w: limit is negative", ErrInvalidQuery)
	}
	return nil
}

func cloneMetadata(source map[string]any) map[string]any {
	if source == nil {
		return nil
	}
	result := make(map[string]any, len(source))
	for key, value := range source {
		result[key] = cloneValue(value)
	}
	return result
}

func cloneValue(value any) any {
	switch typed := value.(type) {
	case map[string]any:
		return cloneMetadata(typed)
	case []any:
		result := make([]any, len(typed))
		for i, item := range typed {
			result[i] = cloneValue(item)
		}
		return result
	case []string:
		return append([]string(nil), typed...)
	default:
		return value
	}
}
