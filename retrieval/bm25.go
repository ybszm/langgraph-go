package retrieval

import (
	"context"
	"fmt"
	"math"
	"reflect"
	"sort"
	"strings"
	"sync"
	"unicode"
)

// BM25Options configures lexical ranking. Zero K1 and B select the defaults
// 1.5 and 0.75 respectively.
type BM25Options struct {
	K1, B    float64
	Tokenize func(string) []string
}

type bm25Document struct {
	document    Document
	frequencies map[string]int
	length      int
}

// BM25 is a deterministic in-memory lexical index.
type BM25 struct {
	mu                sync.RWMutex
	k1, b             float64
	tokenize          func(string) []string
	documents         map[string]bm25Document
	documentFrequency map[string]int
	averageLength     float64
}

// NewBM25 creates an in-memory lexical index and optionally seeds documents.
func NewBM25(documents []Document, options BM25Options) (*BM25, error) {
	if options.K1 == 0 {
		options.K1 = 1.5
	}
	if options.B == 0 {
		options.B = 0.75
	}
	if options.K1 <= 0 || options.B < 0 || options.B > 1 {
		return nil, fmt.Errorf("%w: invalid BM25 K1 or B", ErrInvalidDocument)
	}
	if options.Tokenize == nil {
		options.Tokenize = DefaultTokenizer
	}
	index := &BM25{k1: options.K1, b: options.B, tokenize: options.Tokenize, documents: map[string]bm25Document{}, documentFrequency: map[string]int{}}
	if err := index.Upsert(context.Background(), documents); err != nil {
		return nil, err
	}
	return index, nil
}

func (index *BM25) Upsert(ctx context.Context, documents []Document) error {
	if ctx == nil {
		return fmt.Errorf("%w: context is nil", ErrInvalidDocument)
	}
	prepared := make([]bm25Document, len(documents))
	for i, document := range documents {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := validateDocument(document); err != nil {
			return err
		}
		tokens := index.tokenize(document.Content)
		frequencies := make(map[string]int)
		for _, token := range tokens {
			if token != "" {
				frequencies[token]++
			}
		}
		prepared[i] = bm25Document{document: document.Clone(), frequencies: frequencies, length: len(tokens)}
	}
	index.mu.Lock()
	defer index.mu.Unlock()
	for _, document := range prepared {
		index.documents[document.document.ID] = document
	}
	index.rebuildLocked()
	return nil
}

func (index *BM25) Delete(ctx context.Context, ids []string) error {
	if ctx == nil {
		return fmt.Errorf("%w: context is nil", ErrInvalidDocument)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	index.mu.Lock()
	defer index.mu.Unlock()
	for _, id := range ids {
		delete(index.documents, id)
	}
	index.rebuildLocked()
	return nil
}

func (index *BM25) Retrieve(ctx context.Context, query Query) ([]Result, error) {
	if err := validateQuery(ctx, query); err != nil {
		return nil, err
	}
	tokens := index.tokenize(query.Text)
	if len(tokens) == 0 {
		return nil, nil
	}
	index.mu.RLock()
	defer index.mu.RUnlock()
	result := make([]Result, 0)
	for _, candidate := range index.documents {
		if !metadataMatches(candidate.document.Metadata, query.Filter) {
			continue
		}
		var score float64
		for _, token := range tokens {
			frequency := candidate.frequencies[token]
			if frequency == 0 {
				continue
			}
			df := index.documentFrequency[token]
			idf := math.Log(1 + (float64(len(index.documents)-df)+0.5)/(float64(df)+0.5))
			lengthRatio := 1.0
			if index.averageLength > 0 {
				lengthRatio = float64(candidate.length) / index.averageLength
			}
			score += idf * (float64(frequency) * (index.k1 + 1)) / (float64(frequency) + index.k1*(1-index.b+index.b*lengthRatio))
		}
		if score > 0 {
			result = append(result, Result{Document: candidate.document.Clone(), Score: score})
		}
	}
	sortResults(result)
	limit := query.Limit
	if limit == 0 {
		limit = 10
	}
	if len(result) > limit {
		result = result[:limit]
	}
	return result, nil
}

func (index *BM25) rebuildLocked() {
	index.documentFrequency = make(map[string]int)
	total := 0
	for _, document := range index.documents {
		total += document.length
		for token := range document.frequencies {
			index.documentFrequency[token]++
		}
	}
	index.averageLength = 0
	if len(index.documents) > 0 {
		index.averageLength = float64(total) / float64(len(index.documents))
	}
}

// DefaultTokenizer lowercases Unicode words and indexes each Han character as
// a token so basic English and Chinese retrieval work without dependencies.
func DefaultTokenizer(text string) []string {
	var tokens []string
	var word strings.Builder
	flush := func() {
		if word.Len() > 0 {
			tokens = append(tokens, strings.ToLower(word.String()))
			word.Reset()
		}
	}
	for _, character := range text {
		switch {
		case unicode.In(character, unicode.Han):
			flush()
			tokens = append(tokens, string(character))
		case unicode.IsLetter(character) || unicode.IsNumber(character):
			word.WriteRune(unicode.ToLower(character))
		default:
			flush()
		}
	}
	flush()
	return tokens
}

func metadataMatches(metadata, filter map[string]any) bool {
	for key, expected := range filter {
		if !reflect.DeepEqual(metadata[key], expected) {
			return false
		}
	}
	return true
}
func sortResults(results []Result) {
	sort.Slice(results, func(i, j int) bool {
		if results[i].Score != results[j].Score {
			return results[i].Score > results[j].Score
		}
		return results[i].Document.ID < results[j].Document.ID
	})
}

var _ Index = (*BM25)(nil)
