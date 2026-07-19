package retrieval

import (
	"context"
	"fmt"
	"strings"
)

// TextSplitter splits UTF-8 text by rune count with overlap. It prefers a
// nearby newline or whitespace boundary without producing empty chunks.
type TextSplitter struct {
	ChunkSize    int
	ChunkOverlap int
}

func (splitter TextSplitter) Split(ctx context.Context, documents []Document) ([]Document, error) {
	if ctx == nil {
		return nil, fmt.Errorf("%w: context is nil", ErrInvalidDocument)
	}
	if splitter.ChunkSize <= 0 || splitter.ChunkOverlap < 0 || splitter.ChunkOverlap >= splitter.ChunkSize {
		return nil, fmt.Errorf("%w: chunk size must be positive and overlap smaller than size", ErrInvalidDocument)
	}
	result := make([]Document, 0)
	for _, document := range documents {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if err := validateDocument(document); err != nil {
			return nil, err
		}
		runes := []rune(document.Content)
		if len(runes) <= splitter.ChunkSize {
			result = append(result, document.Clone())
			continue
		}
		start := 0
		for chunkIndex := 0; start < len(runes); chunkIndex++ {
			end := min(start+splitter.ChunkSize, len(runes))
			if end < len(runes) {
				end = preferredBoundary(runes, start, end)
			}
			content := strings.TrimSpace(string(runes[start:end]))
			if content != "" {
				chunk := document.Clone()
				chunk.ID = fmt.Sprintf("%s#%d", document.ID, chunkIndex)
				chunk.Content = content
				if chunk.Metadata == nil {
					chunk.Metadata = map[string]any{}
				}
				chunk.Metadata["source_id"] = document.ID
				chunk.Metadata["chunk_index"] = chunkIndex
				chunk.Metadata["start_rune"] = start
				chunk.Metadata["end_rune"] = end
				result = append(result, chunk)
			}
			if end == len(runes) {
				break
			}
			next := end - splitter.ChunkOverlap
			if next <= start {
				next = end
			}
			start = next
		}
	}
	return result, nil
}

func preferredBoundary(text []rune, start, end int) int {
	minimum := start + (end-start)*4/5
	for index := end; index > minimum; index-- {
		if text[index-1] == '\n' {
			return index
		}
	}
	for index := end; index > minimum; index-- {
		if text[index-1] == ' ' || text[index-1] == '\t' {
			return index
		}
	}
	return end
}
