package remote_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/ybszm/langgraph-go/graph"
	"github.com/ybszm/langgraph-go/remote"
)

func TestMemoryTraceStoreAndUI(t *testing.T) {
	store := remote.NewMemoryTraceStore(2)
	store.OnGraphStart(context.Background(), graph.GraphRunStartEvent{RunID: "graph", Name: "agent"})
	store.OnGraphEnd(context.Background(), graph.GraphRunEndEvent{RunID: "graph", Name: "agent"})
	now := time.Now()
	if err := store.Record(context.Background(), remote.HTTPTrace{TraceID: "http-1", Method: "POST", Path: "/runs", Status: 200, StartedAt: now, FinishedAt: now.Add(time.Millisecond)}); err != nil {
		t.Fatal(err)
	}
	store.OnResume(context.Background(), graph.GraphResumeEvent{RunID: "graph", Status: graph.LifecyclePending})

	request := httptest.NewRequest(http.MethodGet, "/api/traces", nil)
	response := httptest.NewRecorder()
	remote.NewTraceUI(store).ServeHTTP(response, request)
	var records []remote.TraceRecord
	if err := json.Unmarshal(response.Body.Bytes(), &records); err != nil {
		t.Fatal(err)
	}
	if len(records) != 2 || records[0].Name != "resume" || records[1].TraceID != "http-1" {
		t.Fatalf("unexpected bounded records: %#v", records)
	}

	request = httptest.NewRequest(http.MethodGet, "/", nil)
	response = httptest.NewRecorder()
	remote.NewTraceUI(store).ServeHTTP(response, request)
	if response.Code != http.StatusOK || response.Header().Get("Content-Security-Policy") == "" {
		t.Fatalf("unexpected UI response: status=%d headers=%v", response.Code, response.Header())
	}
}
