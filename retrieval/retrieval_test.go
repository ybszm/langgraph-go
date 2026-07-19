package retrieval_test

import (
	"context"
	"encoding/json"
	"math"
	"reflect"
	"strings"
	"testing"

	"github.com/wahanbo/langgraph-go/prebuilt"
	"github.com/wahanbo/langgraph-go/retrieval"
	"github.com/wahanbo/langgraph-go/store"
	vectorMemory "github.com/wahanbo/langgraph-go/store/vector/memory"
)

func TestTextSplitterPreservesUnicodeOverlapAndMetadata(t *testing.T) {
	documents, err := (retrieval.TextSplitter{ChunkSize: 6, ChunkOverlap: 2}).Split(context.Background(), []retrieval.Document{{ID: "doc", Content: "你好世界 hello world", Metadata: map[string]any{"source": "test"}}})
	if err != nil || len(documents) < 2 {
		t.Fatalf("documents=%+v err=%v", documents, err)
	}
	if documents[0].ID != "doc#0" || documents[0].Metadata["source_id"] != "doc" || documents[0].Metadata["source"] != "test" {
		t.Fatalf("first=%+v", documents[0])
	}
	for _, document := range documents {
		if !strings.HasPrefix(document.ID, "doc#") || document.Content == "" {
			t.Fatalf("chunk=%+v", document)
		}
	}
}

func TestBM25UpsertFilterDeleteAndChineseTokenization(t *testing.T) {
	index, err := retrieval.NewBM25([]retrieval.Document{{ID: "go", Content: "Go concurrency channels", Metadata: map[string]any{"lang": "en"}}, {ID: "python", Content: "Python concurrency asyncio", Metadata: map[string]any{"lang": "en"}}, {ID: "zh", Content: "并发工作流和状态图", Metadata: map[string]any{"lang": "zh"}}}, retrieval.BM25Options{})
	if err != nil {
		t.Fatal(err)
	}
	results, err := index.Retrieve(context.Background(), retrieval.Query{Text: "Go channels", Limit: 2, Filter: map[string]any{"lang": "en"}})
	if err != nil || len(results) == 0 || results[0].Document.ID != "go" {
		t.Fatalf("results=%+v err=%v", results, err)
	}
	results, _ = index.Retrieve(context.Background(), retrieval.Query{Text: "并发状态"})
	if len(results) == 0 || results[0].Document.ID != "zh" {
		t.Fatalf("Chinese results=%+v", results)
	}
	if err := index.Delete(context.Background(), []string{"go"}); err != nil {
		t.Fatal(err)
	}
	results, _ = index.Retrieve(context.Background(), retrieval.Query{Text: "Go channels"})
	if len(results) != 0 {
		t.Fatalf("deleted results=%+v", results)
	}
}

type testEmbedder struct{}

func (testEmbedder) EmbedDocuments(_ context.Context, texts []string) ([][]float32, error) {
	result := make([][]float32, len(texts))
	for i, text := range texts {
		result[i] = embed(text)
	}
	return result, nil
}
func (testEmbedder) EmbedQuery(_ context.Context, text string) ([]float32, error) {
	return embed(text), nil
}
func embed(text string) []float32 {
	lower := strings.ToLower(text)
	return []float32{float32(strings.Count(lower, "go")), float32(strings.Count(lower, "python")) + 0.01}
}

func TestVectorAndHybridRetrieval(t *testing.T) {
	vectorStore, _ := vectorMemory.New(2, store.VectorCosine)
	vector, _ := retrieval.NewVectorIndex(testEmbedder{}, vectorStore, store.Namespace{"docs"})
	documents := []retrieval.Document{{ID: "go", Content: "Go graph"}, {ID: "python", Content: "Python graph"}}
	if err := vector.Upsert(context.Background(), documents); err != nil {
		t.Fatal(err)
	}
	vectorResults, err := vector.Retrieve(context.Background(), retrieval.Query{Text: "Go", Limit: 1})
	if err != nil || vectorResults[0].Document.ID != "go" {
		t.Fatalf("vector=%+v err=%v", vectorResults, err)
	}
	lexical, _ := retrieval.NewBM25(documents, retrieval.BM25Options{})
	hybrid, err := retrieval.NewHybrid([]retrieval.WeightedRetriever{{Retriever: vector, Weight: 1}, {Retriever: lexical, Weight: 1}}, 60)
	if err != nil {
		t.Fatal(err)
	}
	results, err := hybrid.Retrieve(context.Background(), retrieval.Query{Text: "Go graph", Limit: 2})
	if err != nil || len(results) != 2 || results[0].Document.ID != "go" || math.IsNaN(results[0].Score) {
		t.Fatalf("hybrid=%+v err=%v", results, err)
	}
}

func TestIngestAndRetrieverTool(t *testing.T) {
	index, _ := retrieval.NewBM25(nil, retrieval.BM25Options{})
	count, err := retrieval.Ingest(context.Background(), retrieval.LoaderFunc(func(context.Context) ([]retrieval.Document, error) {
		return []retrieval.Document{{ID: "one", Content: "durable graph runtime"}}, nil
	}), nil, index)
	if err != nil || count != 1 {
		t.Fatalf("count=%d err=%v", count, err)
	}
	tool, err := retrieval.AsTool[string, string](index, retrieval.ToolOptions{DefaultLimit: 2})
	if err != nil {
		t.Fatal(err)
	}
	definitions, err := prebuilt.ToolDefinitions([]prebuilt.Tool[string, string]{tool})
	if err != nil || len(definitions) != 1 || definitions[0].Name != "search_documents" || !strings.Contains(string(definitions[0].InputSchema), `"query"`) {
		t.Fatalf("definitions=%+v err=%v", definitions, err)
	}
	result, err := tool.Invoke(context.Background(), prebuilt.ToolCall{ID: "call", Name: "search_documents", Arguments: json.RawMessage(`{"query":"durable"}`)}, prebuilt.ToolRuntime[string]{})
	if err != nil || result.Message == nil || !strings.Contains(result.Message.Content, "durable graph") {
		t.Fatalf("result=%+v err=%v", result, err)
	}
	if !reflect.DeepEqual(retrieval.DefaultTokenizer("Go并发"), []string{"go", "并", "发"}) {
		t.Fatalf("tokens=%v", retrieval.DefaultTokenizer("Go并发"))
	}
}
