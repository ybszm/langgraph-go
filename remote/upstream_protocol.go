package remote

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"strings"

	"github.com/wahanbo/langgraph-go/checkpoint"
	"github.com/wahanbo/langgraph-go/graph"
)

// langGraphProtocolRun is the pinned Python SDK's raw run resource shape.
type langGraphProtocolRun struct {
	RunID             string         `json:"run_id"`
	ThreadID          string         `json:"thread_id"`
	AssistantID       string         `json:"assistant_id"`
	CreatedAt         any            `json:"created_at"`
	UpdatedAt         any            `json:"updated_at"`
	Status            string         `json:"status"`
	Metadata          map[string]any `json:"metadata"`
	MultitaskStrategy string         `json:"multitask_strategy"`
}
type langGraphProtocolThread struct {
	ThreadID   string         `json:"thread_id"`
	CreatedAt  any            `json:"created_at"`
	UpdatedAt  any            `json:"updated_at"`
	Metadata   map[string]any `json:"metadata"`
	Status     string         `json:"status"`
	Values     any            `json:"values"`
	Interrupts map[string]any `json:"interrupts"`
}

func (s *Server[I, O]) serveLangGraphProtocol(writer http.ResponseWriter, request *http.Request) {
	writer.Header().Set("Content-Type", "application/json")
	parts := strings.Split(strings.Trim(strings.TrimPrefix(request.URL.Path, "/threads"), "/"), "/")
	if len(parts) == 1 && parts[0] == "" {
		parts = nil
	}
	switch {
	case len(parts) == 0 && request.Method == http.MethodPost:
		s.createLangGraphThread(writer, request)
	case len(parts) == 1 && request.Method == http.MethodGet:
		s.getLangGraphThread(writer, request, parts[0])
	case len(parts) == 1 && request.Method == http.MethodDelete:
		s.deleteThread(writer, request, parts[0])
	case len(parts) == 2 && parts[1] == "runs" && request.Method == http.MethodPost:
		s.createLangGraphRun(writer, request, parts[0])
	case len(parts) == 2 && parts[1] == "runs" && request.Method == http.MethodGet:
		s.listLangGraphRuns(writer, request, parts[0])
	case len(parts) == 3 && parts[1] == "runs" && request.Method == http.MethodGet:
		s.getLangGraphRun(writer, request, parts[0], parts[2])
	case len(parts) == 4 && parts[1] == "runs" && parts[3] == "cancel" && request.Method == http.MethodPost:
		s.cancelLangGraphRun(writer, request, parts[0], parts[2])
	case len(parts) == 4 && parts[1] == "runs" && parts[3] == "join" && request.Method == http.MethodGet:
		s.joinLangGraphRun(writer, request, parts[0], parts[2])
	case len(parts) == 2 && parts[1] == "state" && request.Method == http.MethodGet:
		s.getLangGraphState(writer, request, parts[0], "")
	case len(parts) == 3 && parts[1] == "state" && request.Method == http.MethodGet:
		s.getLangGraphState(writer, request, parts[0], parts[2])
	case len(parts) == 2 && parts[1] == "state" && request.Method == http.MethodPost:
		s.updateLangGraphState(writer, request, parts[0])
	case len(parts) == 2 && parts[1] == "history" && request.Method == http.MethodPost:
		s.historyLangGraphState(writer, request, parts[0])
	default:
		s.writeControlError(writer, http.StatusNotFound, CodeNotFound, "LangGraph protocol endpoint not found")
	}
}

type langGraphThreadCreate struct {
	ThreadID string         `json:"thread_id"`
	Metadata map[string]any `json:"metadata"`
	IfExists string         `json:"if_exists"`
}

func (s *Server[I, O]) createLangGraphThread(w http.ResponseWriter, r *http.Request) {
	var p langGraphThreadCreate
	if err := s.decodeLoose(r, &p); err != nil {
		s.writeControlError(w, 400, CodeInvalidRequest, err.Error())
		return
	}
	body, _ := json.Marshal(createThreadRequest{ThreadID: p.ThreadID, Metadata: p.Metadata})
	capture := newProtocolCapture()
	s.createThread(capture, cloneProtocolRequest(r, http.MethodPost, "/v1/threads", nil, body))
	if capture.status >= 300 {
		capture.copyTo(w)
		return
	}
	var envelope controlEnvelope[O]
	if json.Unmarshal(capture.body.Bytes(), &envelope) != nil || envelope.Thread == nil {
		s.writeControlError(w, 500, CodeProtocol, "invalid native thread response")
		return
	}
	s.writeLangGraphThread(w, r, *envelope.Thread, envelope.Thread.Metadata)
}
func (s *Server[I, O]) getLangGraphThread(w http.ResponseWriter, r *http.Request, id string) {
	capture := newProtocolCapture()
	s.getThread(capture, r, id)
	if capture.status >= 300 {
		capture.copyTo(w)
		return
	}
	var envelope controlEnvelope[O]
	if json.Unmarshal(capture.body.Bytes(), &envelope) != nil || envelope.Thread == nil {
		s.writeControlError(w, 500, CodeProtocol, "invalid native thread response")
		return
	}
	s.writeLangGraphThread(w, r, *envelope.Thread, envelope.Thread.Metadata)
}
func (s *Server[I, O]) writeLangGraphThread(w http.ResponseWriter, r *http.Request, thread Thread, metadata map[string]any) {
	status := "idle"
	runs, err := s.protocolRuns(r, thread.ID, ListOptions{})
	if err == nil {
		for _, run := range runs {
			if run.Status == RunRunning {
				status = "busy"
				break
			}
		}
	}
	_ = json.NewEncoder(w).Encode(langGraphProtocolThread{ThreadID: thread.ID, CreatedAt: thread.CreatedAt, UpdatedAt: thread.UpdatedAt, Metadata: nonnullMap(metadata), Status: status, Values: map[string]any{}, Interrupts: map[string]any{}})
}

type langGraphRunCreate struct {
	Input       json.RawMessage `json:"input"`
	AssistantID string          `json:"assistant_id"`
	Config      struct {
		Configurable map[string]any `json:"configurable"`
		Metadata     map[string]any `json:"metadata"`
	} `json:"config"`
}

func (s *Server[I, O]) createLangGraphRun(w http.ResponseWriter, r *http.Request, threadID string) {
	var p langGraphRunCreate
	if err := s.decodeLoose(r, &p); err != nil {
		s.writeControlError(w, 400, CodeInvalidRequest, err.Error())
		return
	}
	if p.AssistantID != "" && p.AssistantID != s.options.AssistantID {
		s.writeControlError(w, 404, CodeNotFound, "assistant not found")
		return
	}
	var input I
	if len(p.Input) > 0 && string(p.Input) != "null" {
		if err := json.Unmarshal(p.Input, &input); err != nil {
			s.writeControlError(w, 400, CodeInvalidRequest, err.Error())
			return
		}
	}
	config := RunConfig{Metadata: p.Config.Metadata}
	if value, ok := p.Config.Configurable["checkpoint_id"].(string); ok {
		config.CheckpointID = value
	}
	body, _ := json.Marshal(invokeRequest[I]{Input: input, Config: config})
	capture := newProtocolCapture()
	s.createRun(capture, cloneProtocolRequest(r, http.MethodPost, "/v1/threads/"+threadID+"/runs", nil, body), threadID)
	if capture.status >= 300 {
		capture.copyTo(w)
		return
	}
	var envelope controlEnvelope[O]
	if json.Unmarshal(capture.body.Bytes(), &envelope) != nil || envelope.Run == nil {
		s.writeControlError(w, 500, CodeProtocol, "invalid native run response")
		return
	}
	w.WriteHeader(capture.status)
	_ = json.NewEncoder(w).Encode(s.langGraphRun(*envelope.Run))
}
func (s *Server[I, O]) listLangGraphRuns(w http.ResponseWriter, r *http.Request, threadID string) {
	options, err := listOptionsFromRequest(r)
	if err != nil {
		s.writeControlError(w, 400, CodeInvalidRequest, err.Error())
		return
	}
	runs, err := s.protocolRuns(r, threadID, options)
	if err != nil {
		s.writeStoreError(w, err, "list runs")
		return
	}
	result := make([]langGraphProtocolRun, len(runs))
	for i, run := range runs {
		result[i] = s.langGraphRun(run)
	}
	_ = json.NewEncoder(w).Encode(result)
}
func (s *Server[I, O]) getLangGraphRun(w http.ResponseWriter, r *http.Request, threadID, runID string) {
	run, ok, err := s.protocolRun(r, threadID, runID)
	if err != nil {
		s.writeStoreError(w, err, "get run")
		return
	}
	if !ok {
		s.writeControlError(w, 404, CodeNotFound, "run not found")
		return
	}
	_ = json.NewEncoder(w).Encode(s.langGraphRun(run))
}
func (s *Server[I, O]) cancelLangGraphRun(w http.ResponseWriter, r *http.Request, threadID, runID string) {
	capture := newProtocolCapture()
	s.cancelRun(capture, r, threadID, runID)
	if capture.status >= 300 {
		capture.copyTo(w)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}
func (s *Server[I, O]) joinLangGraphRun(w http.ResponseWriter, r *http.Request, threadID, runID string) {
	capture := newProtocolCapture()
	s.joinRun(capture, r, threadID, runID)
	if capture.status >= 300 {
		capture.copyTo(w)
		return
	}
	var envelope controlEnvelope[O]
	if json.Unmarshal(capture.body.Bytes(), &envelope) != nil || envelope.Run == nil {
		s.writeControlError(w, 500, CodeProtocol, "invalid native join response")
		return
	}
	if envelope.Run.Error != nil {
		s.writeControlError(w, 500, envelope.Run.Error.Code, envelope.Run.Error.Message)
		return
	}
	if envelope.Run.Output == nil {
		_ = json.NewEncoder(w).Encode(map[string]any{})
		return
	}
	_ = json.NewEncoder(w).Encode(envelope.Run.Output)
}
func (s *Server[I, O]) langGraphRun(run Run[O]) langGraphProtocolRun {
	status := string(run.Status)
	if run.Status == RunCanceled {
		status = "interrupted"
	}
	return langGraphProtocolRun{RunID: run.ID, ThreadID: run.ThreadID, AssistantID: s.options.AssistantID, CreatedAt: run.CreatedAt, UpdatedAt: run.UpdatedAt, Status: status, Metadata: map[string]any{}, MultitaskStrategy: "reject"}
}

func (s *Server[I, O]) protocolRun(r *http.Request, threadID, runID string) (Run[O], bool, error) {
	if !isNil(s.options.ControlStore) {
		stored, ok, err := s.options.ControlStore.GetRun(r.Context(), threadID, runID)
		if err != nil || !ok {
			return Run[O]{}, ok, err
		}
		run, err := typedRunFromStored[O](stored)
		return run, err == nil, err
	}
	s.control.mu.RLock()
	defer s.control.mu.RUnlock()
	record := s.lookupRunLocked(threadID, runID)
	if record == nil {
		return Run[O]{}, false, nil
	}
	return record.run, true, nil
}
func (s *Server[I, O]) protocolRuns(r *http.Request, threadID string, options ListOptions) ([]Run[O], error) {
	if !isNil(s.options.ControlStore) {
		stored, err := s.options.ControlStore.ListRuns(r.Context(), threadID, options)
		if err != nil {
			return nil, err
		}
		runs := make([]Run[O], len(stored))
		for i, item := range stored {
			runs[i], err = typedRunFromStored[O](item)
			if err != nil {
				return nil, err
			}
		}
		return runs, nil
	}
	s.control.mu.RLock()
	if _, ok := s.control.threads[threadID]; !ok {
		s.control.mu.RUnlock()
		return nil, ErrControlNotFound
	}
	runs := make([]Run[O], 0, len(s.control.runs[threadID]))
	for _, record := range s.control.runs[threadID] {
		runs = append(runs, record.run)
	}
	s.control.mu.RUnlock()
	sortRuns(runs)
	return paginate(runs, options), nil
}

func (s *Server[I, O]) getLangGraphState(w http.ResponseWriter, r *http.Request, threadID, checkpointID string) {
	if !s.requireState(r.Context(), w, threadID) {
		return
	}
	snapshot, err := s.state.get(r.Context(), graph.RunConfig{ThreadID: threadID, CheckpointID: checkpointID}, r.URL.Query().Get("subgraphs") == "true")
	if err != nil {
		s.writeControlError(w, 500, CodeExecution, err.Error())
		return
	}
	writeNormalizedState(w, snapshot)
}
func (s *Server[I, O]) updateLangGraphState(w http.ResponseWriter, r *http.Request, threadID string) {
	if !s.requireState(r.Context(), w, threadID) {
		return
	}
	var payload struct {
		Values     json.RawMessage `json:"values"`
		AsNode     string          `json:"as_node"`
		Checkpoint *struct {
			CheckpointID string `json:"checkpoint_id"`
			CheckpointNS string `json:"checkpoint_ns"`
		} `json:"checkpoint"`
		CheckpointID string `json:"checkpoint_id"`
	}
	if err := s.decodeLoose(r, &payload); err != nil {
		s.writeControlError(w, 400, CodeInvalidRequest, err.Error())
		return
	}
	config := graph.RunConfig{ThreadID: threadID, CheckpointID: payload.CheckpointID}
	if payload.Checkpoint != nil {
		config.CheckpointID = payload.Checkpoint.CheckpointID
		config.CheckpointNamespace = payload.Checkpoint.CheckpointNS
	}
	update, _ := json.Marshal(map[string]any{"Delta": payload.Values, "AsNode": payload.AsNode})
	var normalized map[string]json.RawMessage
	_ = json.Unmarshal(update, &normalized)
	update, _ = json.Marshal(map[string]any{"Delta": normalized["Delta"], "AsNode": payload.AsNode})
	saved, err := s.state.update(r.Context(), config, update)
	if err != nil {
		s.writeControlError(w, 500, CodeExecution, err.Error())
		return
	}
	_ = json.NewEncoder(w).Encode(map[string]any{"checkpoint": langGraphCheckpoint(saved)})
}
func (s *Server[I, O]) historyLangGraphState(w http.ResponseWriter, r *http.Request, threadID string) {
	if !s.requireState(r.Context(), w, threadID) {
		return
	}
	var payload struct {
		Limit    int                 `json:"limit"`
		Before   json.RawMessage     `json:"before"`
		Metadata checkpoint.Metadata `json:"metadata"`
	}
	if err := s.decodeLoose(r, &payload); err != nil {
		s.writeControlError(w, 400, CodeInvalidRequest, err.Error())
		return
	}
	before := ""
	_ = json.Unmarshal(payload.Before, &before)
	history, err := s.state.history(r.Context(), graph.RunConfig{ThreadID: threadID}, graph.StateHistoryOptions{Limit: payload.Limit, BeforeCheckpointID: before, Filter: payload.Metadata})
	if err != nil {
		s.writeControlError(w, 500, CodeExecution, err.Error())
		return
	}
	writeNormalizedHistory(w, history)
}

func (s *Server[I, O]) decodeLoose(r *http.Request, target any) error {
	decoder := json.NewDecoder(r.Body)
	if err := decoder.Decode(target); err != nil {
		return err
	}
	return ensureJSONEOF(decoder)
}
func langGraphCheckpoint(c checkpoint.Config) map[string]any {
	return map[string]any{"thread_id": c.ThreadID, "checkpoint_ns": c.Namespace, "checkpoint_id": c.CheckpointID}
}
func writeNormalizedState(w http.ResponseWriter, value any) {
	encoded, _ := json.Marshal(value)
	var source map[string]any
	_ = json.Unmarshal(encoded, &source)
	result := map[string]any{"values": source["Values"], "next": source["Next"], "metadata": source["Metadata"], "created_at": source["CreatedAt"], "tasks": source["Tasks"], "interrupts": source["Interrupts"]}
	result["checkpoint"] = normalizeCheckpoint(source["Config"])
	result["parent_checkpoint"] = normalizeCheckpoint(source["ParentConfig"])
	_ = json.NewEncoder(w).Encode(result)
}
func writeNormalizedHistory(w http.ResponseWriter, value any) {
	encoded, _ := json.Marshal(value)
	var items []map[string]any
	_ = json.Unmarshal(encoded, &items)
	result := make([]map[string]any, len(items))
	for i, item := range items {
		result[i] = map[string]any{"values": item["Values"], "next": item["Next"], "metadata": item["Metadata"], "created_at": item["CreatedAt"], "tasks": item["Tasks"], "interrupts": item["Interrupts"], "checkpoint": normalizeCheckpoint(item["Config"]), "parent_checkpoint": normalizeCheckpoint(item["ParentConfig"])}
	}
	_ = json.NewEncoder(w).Encode(result)
}
func normalizeCheckpoint(value any) any {
	item, ok := value.(map[string]any)
	if !ok || item == nil {
		return nil
	}
	return map[string]any{"thread_id": item["ThreadID"], "checkpoint_ns": item["Namespace"], "checkpoint_id": item["CheckpointID"]}
}
func nonnullMap(value map[string]any) map[string]any {
	if value == nil {
		return map[string]any{}
	}
	return value
}
func sortRuns[O any](runs []Run[O]) {
	for i := 1; i < len(runs); i++ {
		for j := i; j > 0 && (runs[j].CreatedAt.Before(runs[j-1].CreatedAt) || (runs[j].CreatedAt.Equal(runs[j-1].CreatedAt) && runs[j].ID < runs[j-1].ID)); j-- {
			runs[j], runs[j-1] = runs[j-1], runs[j]
		}
	}
}

type protocolCapture struct {
	header http.Header
	body   bytes.Buffer
	status int
}

func newProtocolCapture() *protocolCapture {
	return &protocolCapture{header: http.Header{}, status: 200}
}
func (c *protocolCapture) Header() http.Header            { return c.header }
func (c *protocolCapture) WriteHeader(status int)         { c.status = status }
func (c *protocolCapture) Write(data []byte) (int, error) { return c.body.Write(data) }
func (c *protocolCapture) copyTo(w http.ResponseWriter) {
	for key, values := range c.header {
		for _, value := range values {
			w.Header().Add(key, value)
		}
	}
	w.WriteHeader(c.status)
	_, _ = w.Write(c.body.Bytes())
}
func cloneProtocolRequest(source *http.Request, method, path string, query url.Values, body []byte) *http.Request {
	clone := source.Clone(source.Context())
	clone.Method = method
	clone.URL = &url.URL{Path: path, RawQuery: query.Encode()}
	clone.Body = http.NoBody
	if body != nil {
		clone.Body = io.NopCloser(bytes.NewReader(body))
	}
	return clone
}
