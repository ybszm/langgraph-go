package remote

import (
	"context"
	"encoding/json"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/ybszm/langgraph-go/graph"
)

// TraceRecord is a completed HTTP, graph, node, or lifecycle trace entry.
type TraceRecord struct {
	TraceID     string    `json:"trace_id,omitempty"`
	RunID       string    `json:"run_id,omitempty"`
	ParentRunID string    `json:"parent_run_id,omitempty"`
	Kind        string    `json:"kind"`
	Name        string    `json:"name"`
	Status      string    `json:"status"`
	ThreadID    string    `json:"thread_id,omitempty"`
	Step        int       `json:"step,omitempty"`
	Attempt     int       `json:"attempt,omitempty"`
	HTTPStatus  int       `json:"http_status,omitempty"`
	StartedAt   time.Time `json:"started_at"`
	FinishedAt  time.Time `json:"finished_at"`
	DurationMS  float64   `json:"duration_ms"`
	Error       string    `json:"error,omitempty"`
}

// MemoryTraceStore is a bounded, concurrency-safe development trace store.
// Attach it both as ServerOptions.TraceSink and as a graph callback to correlate
// transport, graph, and node activity in the local UI.
type MemoryTraceStore struct {
	mu      sync.Mutex
	limit   int
	records []TraceRecord
	active  map[string]TraceRecord
}

// NewMemoryTraceStore creates a trace store retaining at most limit completed
// records. Non-positive limits use 512.
func NewMemoryTraceStore(limit int) *MemoryTraceStore {
	if limit <= 0 {
		limit = 512
	}
	return &MemoryTraceStore{limit: limit, active: make(map[string]TraceRecord)}
}

// Record implements TraceSink.
func (store *MemoryTraceStore) Record(_ context.Context, span HTTPTrace) error {
	status := "ok"
	if span.Status >= http.StatusBadRequest {
		status = "error"
	}
	store.append(TraceRecord{
		TraceID: span.TraceID, RunID: span.TraceID, Kind: "http", Name: span.Method + " " + span.Path,
		Status: status, HTTPStatus: span.Status, StartedAt: span.StartedAt, FinishedAt: span.FinishedAt,
		DurationMS: milliseconds(span.StartedAt, span.FinishedAt),
	})
	return nil
}

func (store *MemoryTraceStore) OnGraphStart(_ context.Context, event graph.GraphRunStartEvent) {
	store.start(TraceRecord{TraceID: metadataString(event.Metadata, TraceMetadataKey), RunID: event.RunID, ParentRunID: event.ParentRunID, Kind: "graph", Name: event.Name, Status: "running", ThreadID: event.ThreadID, StartedAt: time.Now()})
}

func (store *MemoryTraceStore) OnGraphEnd(_ context.Context, event graph.GraphRunEndEvent) {
	store.finish(event.RunID, "graph", event.Name, "ok", "")
}

func (store *MemoryTraceStore) OnGraphError(_ context.Context, event graph.GraphRunErrorEvent) {
	store.finish(event.RunID, "graph", event.Name, "error", errorText(event.Err))
}

func (store *MemoryTraceStore) OnNodeStart(_ context.Context, event graph.NodeRunStartEvent) {
	store.start(TraceRecord{TraceID: metadataString(event.Metadata, TraceMetadataKey), RunID: event.RunID, ParentRunID: event.ParentRunID, Kind: "node", Name: event.Name, Status: "running", ThreadID: event.ThreadID, Step: event.Step, Attempt: event.Attempt, StartedAt: time.Now()})
}

func (store *MemoryTraceStore) OnNodeEnd(_ context.Context, event graph.NodeRunEndEvent) {
	store.finish(event.RunID, "node", event.Name, "ok", "")
}

func (store *MemoryTraceStore) OnNodeError(_ context.Context, event graph.NodeRunErrorEvent) {
	store.finish(event.RunID, "node", event.Name, "error", errorText(event.Err))
}

func (store *MemoryTraceStore) OnInterrupt(_ context.Context, event graph.GraphInterruptEvent) {
	now := time.Now()
	store.append(TraceRecord{RunID: event.RunID, Kind: "lifecycle", Name: "interrupt", Status: string(event.Status), StartedAt: now, FinishedAt: now})
}

func (store *MemoryTraceStore) OnResume(_ context.Context, event graph.GraphResumeEvent) {
	now := time.Now()
	store.append(TraceRecord{RunID: event.RunID, Kind: "lifecycle", Name: "resume", Status: string(event.Status), StartedAt: now, FinishedAt: now})
}

// Snapshot returns completed records in newest-first order.
func (store *MemoryTraceStore) Snapshot() []TraceRecord {
	store.mu.Lock()
	defer store.mu.Unlock()
	result := make([]TraceRecord, len(store.records))
	for i := range store.records {
		result[len(store.records)-1-i] = store.records[i]
	}
	return result
}

func (store *MemoryTraceStore) start(record TraceRecord) {
	store.mu.Lock()
	defer store.mu.Unlock()
	store.active[record.RunID] = record
}

func (store *MemoryTraceStore) finish(runID, kind, name, status, failure string) {
	now := time.Now()
	store.mu.Lock()
	record, found := store.active[runID]
	delete(store.active, runID)
	if !found {
		record = TraceRecord{RunID: runID, Kind: kind, Name: name, StartedAt: now}
	}
	record.Status, record.Error, record.FinishedAt = status, failure, now
	record.DurationMS = milliseconds(record.StartedAt, now)
	store.appendLocked(record)
	store.mu.Unlock()
}

func (store *MemoryTraceStore) append(record TraceRecord) {
	store.mu.Lock()
	defer store.mu.Unlock()
	store.appendLocked(record)
}

func (store *MemoryTraceStore) appendLocked(record TraceRecord) {
	store.records = append(store.records, record)
	if excess := len(store.records) - store.limit; excess > 0 {
		copy(store.records, store.records[excess:])
		store.records = store.records[:store.limit]
	}
}

func milliseconds(start, end time.Time) float64 { return float64(end.Sub(start).Microseconds()) / 1000 }

func errorText(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

func metadataString(metadata map[string]any, key string) string {
	value, _ := metadata[key].(string)
	return value
}

// NewTraceUI returns a dependency-free development UI and JSON endpoint. Mount
// it behind authentication in shared environments, for example at /debug/traces/.
func NewTraceUI(store *MemoryTraceStore) http.Handler {
	return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("Cache-Control", "no-store")
		writer.Header().Set("X-Content-Type-Options", "nosniff")
		if request.Method != http.MethodGet {
			writer.Header().Set("Allow", http.MethodGet)
			http.Error(writer, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		if store == nil {
			http.Error(writer, "trace store is nil", http.StatusInternalServerError)
			return
		}
		if request.URL.Path == "/api/traces" {
			writer.Header().Set("Content-Type", "application/json; charset=utf-8")
			_ = json.NewEncoder(writer).Encode(store.Snapshot())
			return
		}
		if request.URL.Path != "/" && request.URL.Path != "" {
			http.NotFound(writer, request)
			return
		}
		writer.Header().Set("Content-Type", "text/html; charset=utf-8")
		writer.Header().Set("Content-Security-Policy", "default-src 'none'; script-src 'unsafe-inline'; style-src 'unsafe-inline'; connect-src 'self'")
		writer.Header().Set("Content-Length", strconv.Itoa(len(traceUIHTML)))
		_, _ = writer.Write([]byte(traceUIHTML))
	})
}

const traceUIHTML = `<!doctype html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width"><title>LangGraph Go traces</title><style>
:root{color-scheme:dark;font:14px system-ui;background:#0b1020;color:#dbe5ff}body{margin:0;padding:28px}h1{margin:0 0 6px;font-size:22px}p{color:#91a0c0;margin:0 0 22px}.card{overflow:auto;border:1px solid #273454;border-radius:12px;background:#111932}table{width:100%;border-collapse:collapse}th,td{text-align:left;padding:10px 12px;border-bottom:1px solid #202c49;white-space:nowrap}th{color:#91a0c0;font-size:12px}.ok{color:#5ee6a8}.error{color:#ff7b88}.running{color:#ffd166}code{color:#b9c9ef}</style></head><body><h1>LangGraph Go traces</h1><p>Local bounded trace buffer · refreshes every 2 seconds</p><div class="card"><table><thead><tr><th>TIME</th><th>KIND</th><th>NAME</th><th>STATUS</th><th>DURATION</th><th>RUN</th><th>PARENT</th><th>ERROR</th></tr></thead><tbody id="rows"></tbody></table></div><script>
const cell=(row,value,cls='')=>{const td=document.createElement('td');td.textContent=value??'';if(cls)td.className=cls;row.append(td)};async function load(){const response=await fetch('./api/traces');const traces=await response.json();const body=document.querySelector('#rows');body.replaceChildren();for(const t of traces){const row=document.createElement('tr');cell(row,new Date(t.started_at).toLocaleTimeString());cell(row,t.kind);cell(row,t.name);cell(row,t.status,t.status);cell(row,(t.duration_ms||0).toFixed(2)+' ms');cell(row,t.run_id);cell(row,t.parent_run_id);cell(row,t.error);body.append(row)}}load();setInterval(load,2000);
</script></body></html>`

var _ TraceSink = (*MemoryTraceStore)(nil)
var _ graph.GraphCallback = (*MemoryTraceStore)(nil)
var _ graph.GraphRunCallback = (*MemoryTraceStore)(nil)
var _ graph.NodeRunCallback = (*MemoryTraceStore)(nil)
