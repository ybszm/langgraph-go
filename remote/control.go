package remote

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/ybszm/langgraph-go/graph"
)

// Thread is a remotely managed execution namespace.
type Thread struct {
	ID        string         `json:"id"`
	CreatedAt time.Time      `json:"created_at"`
	UpdatedAt time.Time      `json:"updated_at"`
	Metadata  map[string]any `json:"metadata,omitempty"`
}

// RunStatus is the lifecycle state of an asynchronous remote run.
type RunStatus string

const (
	// RunRunning means the backend invocation is executing.
	RunRunning RunStatus = "running"
	// RunSuccess means the invocation completed successfully.
	RunSuccess RunStatus = "success"
	// RunError means the invocation returned an error.
	RunError RunStatus = "error"
	// RunCanceled means cancellation was requested before completion.
	RunCanceled RunStatus = "canceled"
)

// Run is a snapshot of one asynchronous remote invocation.
type Run[O any] struct {
	ID        string    `json:"id"`
	ThreadID  string    `json:"thread_id"`
	Status    RunStatus `json:"status"`
	Output    *O        `json:"output,omitempty"`
	Error     *Error    `json:"error,omitempty"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

type runRecord[O any] struct {
	run    Run[O]
	cancel context.CancelFunc
	done   chan struct{}
}

type controlState[O any] struct {
	mu          sync.RWMutex
	threads     map[string]Thread
	runs        map[string]map[string]*runRecord[O]
	ctx         context.Context
	cancel      context.CancelFunc
	wg          sync.WaitGroup
	closed      bool
	idempotency map[string]idempotencyRecord
}

type idempotencyRecord struct {
	hash  [32]byte
	runID string
}

func newControlState[O any](ctx context.Context, cancel context.CancelFunc) *controlState[O] {
	return &controlState[O]{
		threads: make(map[string]Thread), runs: make(map[string]map[string]*runRecord[O]),
		ctx: ctx, cancel: cancel, idempotency: make(map[string]idempotencyRecord),
	}
}

// Close cancels active asynchronous runs and waits for their workers.
func (s *Server[I, O]) Close() error {
	s.control.mu.Lock()
	if !s.control.closed {
		s.control.closed = true
		s.control.cancel()
		for _, threadRuns := range s.control.runs {
			for _, record := range threadRuns {
				record.cancel()
			}
		}
	}
	s.control.mu.Unlock()
	s.control.wg.Wait()
	return nil
}

type controlEnvelope[O any] struct {
	Thread  *Thread  `json:"thread,omitempty"`
	Threads []Thread `json:"threads,omitempty"`
	Run     *Run[O]  `json:"run,omitempty"`
	Runs    []Run[O] `json:"runs,omitempty"`
	Error   *Error   `json:"error,omitempty"`
}

// ListOptions controls stable offset pagination.
type ListOptions struct {
	Limit  int
	Offset int
}

// RunCreateOptions controls asynchronous run creation.
type RunCreateOptions struct {
	// IdempotencyKey deduplicates equal requests within one thread.
	IdempotencyKey string
}

type createThreadRequest struct {
	ThreadID string         `json:"thread_id,omitempty"`
	Metadata map[string]any `json:"metadata,omitempty"`
}

func (s *Server[I, O]) serveControl(writer http.ResponseWriter, request *http.Request) {
	writer.Header().Set("Content-Type", "application/json")
	parts := splitControlPath(request.URL.Path)
	switch {
	case len(parts) == 0 && request.Method == http.MethodPost:
		s.createThread(writer, request)
	case len(parts) == 0 && request.Method == http.MethodGet:
		s.listThreads(writer, request)
	case len(parts) == 1 && request.Method == http.MethodGet:
		s.getThread(writer, request, parts[0])
	case len(parts) == 1 && request.Method == http.MethodDelete:
		s.deleteThread(writer, request, parts[0])
	case len(parts) == 2 && parts[1] == "state" && request.Method == http.MethodGet:
		s.getRemoteState(writer, request, parts[0])
	case len(parts) == 2 && parts[1] == "interrupts" && request.Method == http.MethodGet:
		s.listRemoteInterrupts(writer, request, parts[0])
	case len(parts) == 3 && parts[1] == "state" && parts[2] == "history" && request.Method == http.MethodPost:
		s.getRemoteStateHistory(writer, request, parts[0])
	case len(parts) == 3 && parts[1] == "state" && parts[2] == "update" && request.Method == http.MethodPost:
		s.updateRemoteState(writer, request, parts[0])
	case len(parts) == 2 && parts[1] == "runs" && request.Method == http.MethodPost:
		s.createRun(writer, request, parts[0])
	case len(parts) == 2 && parts[1] == "runs" && request.Method == http.MethodGet:
		s.listRuns(writer, request, parts[0])
	case len(parts) == 3 && parts[1] == "runs" && request.Method == http.MethodGet:
		s.getRun(writer, request, parts[0], parts[2])
	case len(parts) == 4 && parts[1] == "runs" && parts[3] == "cancel" && request.Method == http.MethodPost:
		s.cancelRun(writer, request, parts[0], parts[2])
	case len(parts) == 4 && parts[1] == "runs" && parts[3] == "join" && request.Method == http.MethodGet:
		s.joinRun(writer, request, parts[0], parts[2])
	case len(parts) == 4 && parts[1] == "runs" && parts[3] == "stream" && request.Method == http.MethodGet:
		s.streamRemoteRun(writer, request, parts[0], parts[2])
	case len(parts) == 4 && parts[1] == "interrupts" && parts[3] == "resume" && request.Method == http.MethodPost:
		s.resumeRemoteInterrupt(writer, request, parts[0], parts[2])
	default:
		s.writeControlError(writer, http.StatusNotFound, CodeNotFound, "control endpoint not found")
	}
}

func listOptionsFromRequest(request *http.Request) (ListOptions, error) {
	options := ListOptions{}
	for key, target := range map[string]*int{"limit": &options.Limit, "offset": &options.Offset} {
		value := request.URL.Query().Get(key)
		if value == "" {
			continue
		}
		parsed, err := strconv.Atoi(value)
		if err != nil || parsed < 0 {
			return ListOptions{}, fmt.Errorf("%s must be a non-negative integer", key)
		}
		*target = parsed
	}
	return options, nil
}

func paginate[T any](items []T, options ListOptions) []T {
	if options.Offset >= len(items) {
		return []T{}
	}
	items = items[options.Offset:]
	if options.Limit > 0 && options.Limit < len(items) {
		items = items[:options.Limit]
	}
	return items
}

func splitControlPath(path string) []string {
	remainder := strings.TrimPrefix(path, "/v1/threads")
	remainder = strings.Trim(remainder, "/")
	if remainder == "" {
		return nil
	}
	return strings.Split(remainder, "/")
}

func (s *Server[I, O]) decodeControl(writer http.ResponseWriter, request *http.Request, target any) error {
	request.Body = http.MaxBytesReader(writer, request.Body, s.options.MaxBodyBytes)
	decoder := json.NewDecoder(request.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	return ensureJSONEOF(decoder)
}

func (s *Server[I, O]) createThread(writer http.ResponseWriter, request *http.Request) {
	var payload createThreadRequest
	if err := s.decodeControl(writer, request, &payload); err != nil {
		s.writeControlError(writer, http.StatusBadRequest, CodeInvalidRequest, err.Error())
		return
	}
	s.control.mu.RLock()
	if s.control.closed {
		s.control.mu.RUnlock()
		s.writeControlError(writer, http.StatusServiceUnavailable, CodeConflict, "remote server is closed")
		return
	}
	s.control.mu.RUnlock()
	id := payload.ThreadID
	if id == "" {
		id = s.options.IDGenerator()
	}
	if !validResourceID(id) {
		s.writeControlError(writer, http.StatusBadRequest, CodeInvalidRequest, "thread ID is invalid")
		return
	}
	now := s.options.Clock()
	thread := Thread{ID: id, CreatedAt: now, UpdatedAt: now, Metadata: cloneMetadata(payload.Metadata)}
	if !isNil(s.options.ControlStore) {
		if err := s.options.ControlStore.CreateThread(request.Context(), thread); err != nil {
			s.writeStoreError(writer, err, "create thread")
			return
		}
		writer.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(writer).Encode(controlEnvelope[O]{Thread: &thread})
		return
	}
	s.control.mu.Lock()
	defer s.control.mu.Unlock()
	if _, exists := s.control.threads[id]; exists {
		s.writeControlError(writer, http.StatusConflict, CodeConflict, "thread already exists")
		return
	}
	s.control.threads[id] = thread
	s.control.runs[id] = make(map[string]*runRecord[O])
	writer.WriteHeader(http.StatusCreated)
	_ = json.NewEncoder(writer).Encode(controlEnvelope[O]{Thread: &thread})
}

func (s *Server[I, O]) getThread(writer http.ResponseWriter, request *http.Request, threadID string) {
	if !isNil(s.options.ControlStore) {
		thread, exists, err := s.options.ControlStore.GetThread(request.Context(), threadID)
		if err != nil {
			s.writeStoreError(writer, err, "get thread")
			return
		}
		if !exists {
			s.writeControlError(writer, http.StatusNotFound, CodeNotFound, "thread not found")
			return
		}
		_ = json.NewEncoder(writer).Encode(controlEnvelope[O]{Thread: &thread})
		return
	}
	s.control.mu.RLock()
	thread, exists := s.control.threads[threadID]
	s.control.mu.RUnlock()
	if !exists {
		s.writeControlError(writer, http.StatusNotFound, CodeNotFound, "thread not found")
		return
	}
	_ = json.NewEncoder(writer).Encode(controlEnvelope[O]{Thread: &thread})
}

func (s *Server[I, O]) listThreads(writer http.ResponseWriter, request *http.Request) {
	options, err := listOptionsFromRequest(request)
	if err != nil {
		s.writeControlError(writer, http.StatusBadRequest, CodeInvalidRequest, err.Error())
		return
	}
	if !isNil(s.options.ControlStore) {
		threads, err := s.options.ControlStore.ListThreads(request.Context(), options)
		if err != nil {
			s.writeStoreError(writer, err, "list threads")
			return
		}
		_ = json.NewEncoder(writer).Encode(controlEnvelope[O]{Threads: threads})
		return
	}
	s.control.mu.RLock()
	threads := make([]Thread, 0, len(s.control.threads))
	for _, thread := range s.control.threads {
		threads = append(threads, thread)
	}
	s.control.mu.RUnlock()
	sort.Slice(threads, func(i, j int) bool {
		if threads[i].CreatedAt.Equal(threads[j].CreatedAt) {
			return threads[i].ID < threads[j].ID
		}
		return threads[i].CreatedAt.Before(threads[j].CreatedAt)
	})
	threads = paginate(threads, options)
	_ = json.NewEncoder(writer).Encode(controlEnvelope[O]{Threads: threads})
}

func (s *Server[I, O]) deleteThread(writer http.ResponseWriter, request *http.Request, threadID string) {
	if !isNil(s.options.ControlStore) {
		runs, err := s.options.ControlStore.ListRuns(request.Context(), threadID, ListOptions{})
		if err != nil {
			s.writeStoreError(writer, err, "delete thread")
			return
		}
		for _, run := range runs {
			if run.Status == RunRunning {
				next := run
				next.Status = RunCanceled
				next.UpdatedAt = s.options.Clock()
				_, _, err = s.options.ControlStore.TransitionRun(request.Context(), threadID, run.ID, []RunStatus{RunRunning}, next)
				if err != nil && !errors.Is(err, ErrControlNotFound) {
					s.writeStoreError(writer, err, "cancel thread runs")
					return
				}
			}
		}
		s.control.mu.Lock()
		local := s.control.runs[threadID]
		for _, record := range local {
			record.cancel()
		}
		s.control.mu.Unlock()
		if err := s.options.ControlStore.DeleteThread(request.Context(), threadID); err != nil {
			s.writeStoreError(writer, err, "delete thread")
			return
		}
		writer.WriteHeader(http.StatusNoContent)
		return
	}
	s.control.mu.Lock()
	if _, exists := s.control.threads[threadID]; !exists {
		s.control.mu.Unlock()
		s.writeControlError(writer, http.StatusNotFound, CodeNotFound, "thread not found")
		return
	}
	records := make([]*runRecord[O], 0, len(s.control.runs[threadID]))
	for _, record := range s.control.runs[threadID] {
		record.cancel()
		records = append(records, record)
	}
	s.control.mu.Unlock()
	for _, record := range records {
		select {
		case <-record.done:
		case <-request.Context().Done():
			s.writeControlError(writer, http.StatusRequestTimeout, CodeConflict, request.Context().Err().Error())
			return
		}
	}
	s.control.mu.Lock()
	delete(s.control.threads, threadID)
	delete(s.control.runs, threadID)
	for key := range s.control.idempotency {
		if strings.HasPrefix(key, threadID+"\x00") {
			delete(s.control.idempotency, key)
		}
	}
	s.control.mu.Unlock()
	writer.WriteHeader(http.StatusNoContent)
}

func (s *Server[I, O]) createRun(writer http.ResponseWriter, request *http.Request, threadID string) {
	var payload invokeRequest[I]
	if err := s.decodeControl(writer, request, &payload); err != nil {
		s.writeControlError(writer, http.StatusBadRequest, CodeInvalidRequest, err.Error())
		return
	}
	idempotencyKey := request.Header.Get(IdempotencyHeader)
	if idempotencyKey != "" && !validIdempotencyKey(idempotencyKey) {
		s.writeControlError(writer, http.StatusBadRequest, CodeInvalidRequest, "idempotency key is invalid")
		return
	}
	encodedRequest, err := json.Marshal(payload)
	if err != nil {
		s.writeControlError(writer, http.StatusBadRequest, CodeInvalidRequest, err.Error())
		return
	}
	requestHash := sha256.Sum256(encodedRequest)
	if !isNil(s.options.ControlStore) {
		s.createStoredRun(writer, request, threadID, payload, idempotencyKey, requestHash[:])
		return
	}
	s.control.mu.Lock()
	if s.control.closed {
		s.control.mu.Unlock()
		s.writeControlError(writer, http.StatusServiceUnavailable, CodeConflict, "remote server is closed")
		return
	}
	if idempotencyKey != "" {
		lookupKey := threadID + "\x00" + idempotencyKey
		if prior, exists := s.control.idempotency[lookupKey]; exists {
			if prior.hash != requestHash {
				s.control.mu.Unlock()
				s.writeControlError(writer, http.StatusConflict, CodeConflict, "idempotency key was reused with a different request")
				return
			}
			if record := s.control.runs[threadID][prior.runID]; record != nil {
				run := record.run
				s.control.mu.Unlock()
				writer.WriteHeader(http.StatusCreated)
				_ = json.NewEncoder(writer).Encode(controlEnvelope[O]{Run: &run})
				return
			}
		}
	}
	thread, exists := s.control.threads[threadID]
	if !exists {
		s.control.mu.Unlock()
		s.writeControlError(writer, http.StatusNotFound, CodeNotFound, "thread not found")
		return
	}
	runID := s.options.IDGenerator()
	if !validResourceID(runID) {
		s.control.mu.Unlock()
		s.writeControlError(writer, http.StatusInternalServerError, CodeProtocol, "ID generator returned an invalid run ID")
		return
	}
	if _, duplicate := s.control.runs[threadID][runID]; duplicate {
		s.control.mu.Unlock()
		s.writeControlError(writer, http.StatusConflict, CodeConflict, "run already exists")
		return
	}
	now := s.options.Clock()
	run := Run[O]{ID: runID, ThreadID: threadID, Status: RunRunning, CreatedAt: now, UpdatedAt: now}
	runContext, cancel := context.WithCancel(s.control.ctx)
	record := &runRecord[O]{run: run, cancel: cancel, done: make(chan struct{})}
	s.control.runs[threadID][runID] = record
	if idempotencyKey != "" {
		s.control.idempotency[threadID+"\x00"+idempotencyKey] = idempotencyRecord{hash: requestHash, runID: runID}
	}
	thread.UpdatedAt = now
	s.control.threads[threadID] = thread
	s.control.wg.Add(1)
	s.control.mu.Unlock()

	config := payload.Config.graphConfig()
	config.ThreadID = threadID
	config.RunID = runID
	config = tracedGraphConfig(request.Context(), config)
	runContext = withTraceID(runContext, TraceIDFromContext(request.Context()))
	go s.executeRun(runContext, record, payload.Input, config)
	writer.WriteHeader(http.StatusCreated)
	_ = json.NewEncoder(writer).Encode(controlEnvelope[O]{Run: &run})
}

func (s *Server[I, O]) createStoredRun(writer http.ResponseWriter, request *http.Request, threadID string, payload invokeRequest[I], idempotencyKey string, requestHash []byte) {
	s.control.mu.RLock()
	closed := s.control.closed
	s.control.mu.RUnlock()
	if closed {
		s.writeControlError(writer, http.StatusServiceUnavailable, CodeConflict, "remote server is closed")
		return
	}
	runID := s.options.IDGenerator()
	if !validResourceID(runID) {
		s.writeControlError(writer, http.StatusInternalServerError, CodeProtocol, "ID generator returned an invalid run ID")
		return
	}
	now := s.options.Clock()
	candidate := StoredRun{ID: runID, ThreadID: threadID, Status: RunRunning, CreatedAt: now, UpdatedAt: now}
	stored, created, err := s.options.ControlStore.CreateRun(request.Context(), candidate, idempotencyKey, requestHash)
	if err != nil {
		s.writeStoreError(writer, err, "create run")
		return
	}
	run, err := typedRunFromStored[O](stored)
	if err != nil {
		s.writeStoreError(writer, err, "decode run")
		return
	}
	if created {
		runContext, cancel := context.WithCancel(s.control.ctx)
		record := &runRecord[O]{run: run, cancel: cancel, done: make(chan struct{})}
		s.control.mu.Lock()
		if s.control.runs[threadID] == nil {
			s.control.runs[threadID] = map[string]*runRecord[O]{}
		}
		s.control.runs[threadID][run.ID] = record
		s.control.wg.Add(1)
		s.control.mu.Unlock()
		config := payload.Config.graphConfig()
		config.ThreadID = threadID
		config.RunID = run.ID
		config = tracedGraphConfig(request.Context(), config)
		runContext = withTraceID(runContext, TraceIDFromContext(request.Context()))
		go s.executeRun(runContext, record, payload.Input, config)
	}
	writer.WriteHeader(http.StatusCreated)
	_ = json.NewEncoder(writer).Encode(controlEnvelope[O]{Run: &run})
}

func (s *Server[I, O]) executeRun(ctx context.Context, record *runRecord[O], input I, config graph.RunConfig) {
	defer s.control.wg.Done()
	defer record.cancel()
	defer close(record.done)
	if !isNil(s.options.ControlStore) {
		defer func() {
			s.control.mu.Lock()
			if runs := s.control.runs[record.run.ThreadID]; runs != nil {
				delete(runs, record.run.ID)
				if len(runs) == 0 {
					delete(s.control.runs, record.run.ThreadID)
				}
			}
			s.control.mu.Unlock()
		}()
	}
	watchDone := make(chan struct{})
	if !isNil(s.options.ControlStore) {
		go s.watchStoredRun(ctx, record, watchDone)
		defer close(watchDone)
	}
	output, err := s.invoker.Invoke(ctx, input, config)
	if !isNil(s.options.ControlStore) {
		s.finishStoredRun(record, output, err)
		return
	}
	s.control.mu.Lock()
	defer s.control.mu.Unlock()
	if record.run.Status == RunCanceled {
		return
	}
	record.run.UpdatedAt = s.options.Clock()
	thread := s.control.threads[record.run.ThreadID]
	thread.UpdatedAt = record.run.UpdatedAt
	s.control.threads[record.run.ThreadID] = thread
	if err != nil {
		record.run.Status = RunError
		record.run.Error = &Error{Code: CodeExecution, Message: err.Error()}
		return
	}
	record.run.Status = RunSuccess
	record.run.Output = &output
}

func (s *Server[I, O]) watchStoredRun(ctx context.Context, record *runRecord[O], done <-chan struct{}) {
	ticker := time.NewTicker(s.options.ControlPollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-done:
			return
		case <-ctx.Done():
			return
		case <-ticker.C:
			stored, ok, err := s.options.ControlStore.GetRun(ctx, record.run.ThreadID, record.run.ID)
			if err != nil {
				continue
			}
			if !ok || stored.Status == RunCanceled {
				record.cancel()
				return
			}
		}
	}
}

func (s *Server[I, O]) finishStoredRun(record *runRecord[O], output O, invokeErr error) {
	s.control.mu.RLock()
	current := record.run
	s.control.mu.RUnlock()
	current.UpdatedAt = s.options.Clock()
	if invokeErr != nil {
		current.Status = RunError
		current.Error = &Error{Code: CodeExecution, Message: invokeErr.Error()}
	} else {
		current.Status = RunSuccess
		current.Output = &output
	}
	next, err := storedRunFromTyped(current)
	if err == nil {
		var stored StoredRun
		stored, _, err = s.options.ControlStore.TransitionRun(context.Background(), current.ThreadID, current.ID, []RunStatus{RunRunning}, next)
		if err == nil {
			current, _ = typedRunFromStored[O](stored)
		}
	}
	s.control.mu.Lock()
	record.run = current
	s.control.mu.Unlock()
	if s.options.RunRetention > 0 {
		_, _ = s.options.ControlStore.PruneRuns(context.Background(), s.options.Clock().Add(-s.options.RunRetention))
	}
}

func (s *Server[I, O]) listRuns(writer http.ResponseWriter, request *http.Request, threadID string) {
	options, err := listOptionsFromRequest(request)
	if err != nil {
		s.writeControlError(writer, http.StatusBadRequest, CodeInvalidRequest, err.Error())
		return
	}
	if !isNil(s.options.ControlStore) {
		stored, err := s.options.ControlStore.ListRuns(request.Context(), threadID, options)
		if err != nil {
			s.writeStoreError(writer, err, "list runs")
			return
		}
		runs := make([]Run[O], 0, len(stored))
		for _, item := range stored {
			run, err := typedRunFromStored[O](item)
			if err != nil {
				s.writeStoreError(writer, err, "decode run")
				return
			}
			runs = append(runs, run)
		}
		_ = json.NewEncoder(writer).Encode(controlEnvelope[O]{Runs: runs})
		return
	}
	s.control.mu.RLock()
	if _, exists := s.control.threads[threadID]; !exists {
		s.control.mu.RUnlock()
		s.writeControlError(writer, http.StatusNotFound, CodeNotFound, "thread not found")
		return
	}
	runs := make([]Run[O], 0, len(s.control.runs[threadID]))
	for _, record := range s.control.runs[threadID] {
		runs = append(runs, record.run)
	}
	s.control.mu.RUnlock()
	sort.Slice(runs, func(i, j int) bool {
		if runs[i].CreatedAt.Equal(runs[j].CreatedAt) {
			return runs[i].ID < runs[j].ID
		}
		return runs[i].CreatedAt.Before(runs[j].CreatedAt)
	})
	runs = paginate(runs, options)
	_ = json.NewEncoder(writer).Encode(controlEnvelope[O]{Runs: runs})
}

func (s *Server[I, O]) joinRun(writer http.ResponseWriter, request *http.Request, threadID, runID string) {
	if !isNil(s.options.ControlStore) {
		ticker := time.NewTicker(s.options.ControlPollInterval)
		defer ticker.Stop()
		for {
			stored, exists, err := s.options.ControlStore.GetRun(request.Context(), threadID, runID)
			if err != nil {
				s.writeStoreError(writer, err, "join run")
				return
			}
			if !exists {
				s.writeControlError(writer, http.StatusNotFound, CodeNotFound, "run not found")
				return
			}
			if terminalRun(stored.Status) {
				run, err := typedRunFromStored[O](stored)
				if err != nil {
					s.writeStoreError(writer, err, "decode run")
					return
				}
				_ = json.NewEncoder(writer).Encode(controlEnvelope[O]{Run: &run})
				return
			}
			select {
			case <-request.Context().Done():
				return
			case <-ticker.C:
			}
		}
	}
	s.control.mu.RLock()
	record := s.lookupRunLocked(threadID, runID)
	s.control.mu.RUnlock()
	if record == nil {
		s.writeControlError(writer, http.StatusNotFound, CodeNotFound, "run not found")
		return
	}
	select {
	case <-record.done:
	case <-request.Context().Done():
		return
	}
	s.getRun(writer, request, threadID, runID)
}

func (s *Server[I, O]) getRun(writer http.ResponseWriter, request *http.Request, threadID, runID string) {
	if !isNil(s.options.ControlStore) {
		stored, exists, err := s.options.ControlStore.GetRun(request.Context(), threadID, runID)
		if err != nil {
			s.writeStoreError(writer, err, "get run")
			return
		}
		if !exists {
			s.writeControlError(writer, http.StatusNotFound, CodeNotFound, "run not found")
			return
		}
		run, err := typedRunFromStored[O](stored)
		if err != nil {
			s.writeStoreError(writer, err, "decode run")
			return
		}
		_ = json.NewEncoder(writer).Encode(controlEnvelope[O]{Run: &run})
		return
	}
	s.control.mu.RLock()
	record := s.lookupRunLocked(threadID, runID)
	if record != nil {
		run := record.run
		s.control.mu.RUnlock()
		_ = json.NewEncoder(writer).Encode(controlEnvelope[O]{Run: &run})
		return
	}
	s.control.mu.RUnlock()
	s.writeControlError(writer, http.StatusNotFound, CodeNotFound, "run not found")
}

func (s *Server[I, O]) cancelRun(writer http.ResponseWriter, request *http.Request, threadID, runID string) {
	if !isNil(s.options.ControlStore) {
		stored, exists, err := s.options.ControlStore.GetRun(request.Context(), threadID, runID)
		if err != nil {
			s.writeStoreError(writer, err, "cancel run")
			return
		}
		if !exists {
			s.writeControlError(writer, http.StatusNotFound, CodeNotFound, "run not found")
			return
		}
		if stored.Status == RunRunning {
			stored.Status = RunCanceled
			stored.UpdatedAt = s.options.Clock()
			stored, _, err = s.options.ControlStore.TransitionRun(request.Context(), threadID, runID, []RunStatus{RunRunning}, stored)
			if err != nil {
				s.writeStoreError(writer, err, "cancel run")
				return
			}
		}
		s.control.mu.RLock()
		record := s.lookupRunLocked(threadID, runID)
		s.control.mu.RUnlock()
		if record != nil {
			record.cancel()
		}
		run, err := typedRunFromStored[O](stored)
		if err != nil {
			s.writeStoreError(writer, err, "decode run")
			return
		}
		_ = json.NewEncoder(writer).Encode(controlEnvelope[O]{Run: &run})
		return
	}
	s.control.mu.Lock()
	record := s.lookupRunLocked(threadID, runID)
	if record == nil {
		s.control.mu.Unlock()
		s.writeControlError(writer, http.StatusNotFound, CodeNotFound, "run not found")
		return
	}
	if record.run.Status == RunRunning {
		record.run.Status = RunCanceled
		record.run.UpdatedAt = s.options.Clock()
		thread := s.control.threads[threadID]
		thread.UpdatedAt = record.run.UpdatedAt
		s.control.threads[threadID] = thread
		record.cancel()
	}
	run := record.run
	s.control.mu.Unlock()
	_ = json.NewEncoder(writer).Encode(controlEnvelope[O]{Run: &run})
}

func (s *Server[I, O]) lookupRunLocked(threadID, runID string) *runRecord[O] {
	threadRuns := s.control.runs[threadID]
	if threadRuns == nil {
		return nil
	}
	return threadRuns[runID]
}

func (s *Server[I, O]) writeControlError(writer http.ResponseWriter, status int, code ErrorCode, message string) {
	writer.WriteHeader(status)
	_ = json.NewEncoder(writer).Encode(controlEnvelope[O]{Error: &Error{Code: code, Message: message}})
}

func validResourceID(id string) bool {
	return id != "" && strings.TrimSpace(id) == id && !strings.ContainsAny(id, "/\\?#")
}

func validIdempotencyKey(key string) bool {
	if len(key) == 0 || len(key) > 256 || strings.TrimSpace(key) != key {
		return false
	}
	for _, character := range key {
		if character < 0x21 || character == 0x7f {
			return false
		}
	}
	return true
}

func randomID() string {
	buffer := make([]byte, 16)
	if _, err := rand.Read(buffer); err != nil {
		panic(fmt.Sprintf("remote ID generation failed: %v", err))
	}
	return hex.EncodeToString(buffer)
}

// CreateThread creates a remote execution namespace. An empty ID asks the server to generate one.
func (c *Client[I, O]) CreateThread(ctx context.Context, threadID string) (Thread, error) {
	var result controlEnvelope[O]
	err := c.controlRequest(ctx, http.MethodPost, "/v1/threads", createThreadRequest{ThreadID: threadID}, &result, http.StatusCreated)
	if err != nil || result.Thread == nil {
		if err == nil {
			err = &Error{Code: CodeProtocol, Message: "thread response is missing thread"}
		}
		return Thread{}, err
	}
	return *result.Thread, nil
}

// GetThread returns a remote thread snapshot.
func (c *Client[I, O]) GetThread(ctx context.Context, threadID string) (Thread, error) {
	var result controlEnvelope[O]
	err := c.controlRequest(ctx, http.MethodGet, "/v1/threads/"+url.PathEscape(threadID), nil, &result, http.StatusOK)
	if err != nil || result.Thread == nil {
		if err == nil {
			err = &Error{Code: CodeProtocol, Message: "thread response is missing thread"}
		}
		return Thread{}, err
	}
	return *result.Thread, nil
}

// ListThreads returns threads in stable creation-time and ID order.
func (c *Client[I, O]) ListThreads(ctx context.Context, options ListOptions) ([]Thread, error) {
	var result controlEnvelope[O]
	err := c.controlRequest(ctx, http.MethodGet, "/v1/threads?"+listQuery(options), nil, &result, http.StatusOK)
	if err != nil {
		return nil, err
	}
	if result.Threads == nil {
		return []Thread{}, nil
	}
	return result.Threads, nil
}

// DeleteThread cancels its runs, waits for their workers, and removes the resource.
func (c *Client[I, O]) DeleteThread(ctx context.Context, threadID string) error {
	request, err := http.NewRequestWithContext(ctx, http.MethodDelete, c.baseURL+"/v1/threads/"+url.PathEscape(threadID), nil)
	if err != nil {
		return err
	}
	request.Header.Set(ProtocolHeader, ProtocolVersion)
	c.applyHeaders(request)
	response, err := c.http.Do(request)
	if err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return err
	}
	defer response.Body.Close()
	if response.StatusCode == http.StatusNoContent {
		return nil
	}
	var envelope controlEnvelope[O]
	if err := json.NewDecoder(io.LimitReader(response.Body, 2<<20)).Decode(&envelope); err == nil && envelope.Error != nil {
		envelope.Error.Status = response.StatusCode
		return envelope.Error
	}
	return &Error{Code: CodeProtocol, Message: fmt.Sprintf("unexpected HTTP status %d", response.StatusCode), Status: response.StatusCode}
}

// CreateRun starts an asynchronous invocation in a thread.
func (c *Client[I, O]) CreateRun(ctx context.Context, threadID string, input I, config RunConfig) (Run[O], error) {
	return c.CreateRunWithOptions(ctx, threadID, input, config, RunCreateOptions{})
}

// CreateRunWithOptions starts an asynchronous invocation with optional idempotency.
func (c *Client[I, O]) CreateRunWithOptions(ctx context.Context, threadID string, input I, config RunConfig, options RunCreateOptions) (Run[O], error) {
	var result controlEnvelope[O]
	path := "/v1/threads/" + url.PathEscape(threadID) + "/runs"
	headers := make(http.Header)
	if options.IdempotencyKey != "" {
		headers.Set(IdempotencyHeader, options.IdempotencyKey)
	}
	err := c.controlRequestHeaders(ctx, http.MethodPost, path, invokeRequest[I]{Input: input, Config: config}, &result, http.StatusCreated, headers)
	if err != nil || result.Run == nil {
		if err == nil {
			err = &Error{Code: CodeProtocol, Message: "run response is missing run"}
		}
		return Run[O]{}, err
	}
	return *result.Run, nil
}

// GetRun returns an asynchronous run snapshot.
func (c *Client[I, O]) GetRun(ctx context.Context, threadID, runID string) (Run[O], error) {
	return c.runControlRequest(ctx, http.MethodGet, threadID, runID, "")
}

// ListRuns returns thread runs in stable creation-time and ID order.
func (c *Client[I, O]) ListRuns(ctx context.Context, threadID string, options ListOptions) ([]Run[O], error) {
	var result controlEnvelope[O]
	path := "/v1/threads/" + url.PathEscape(threadID) + "/runs?" + listQuery(options)
	if err := c.controlRequest(ctx, http.MethodGet, path, nil, &result, http.StatusOK); err != nil {
		return nil, err
	}
	if result.Runs == nil {
		return []Run[O]{}, nil
	}
	return result.Runs, nil
}

// JoinRun waits for backend completion without transferring ownership to the request context.
func (c *Client[I, O]) JoinRun(ctx context.Context, threadID, runID string) (Run[O], error) {
	return c.runControlRequest(ctx, http.MethodGet, threadID, runID, "/join")
}

// CancelRun requests cancellation and returns the resulting run snapshot.
func (c *Client[I, O]) CancelRun(ctx context.Context, threadID, runID string) (Run[O], error) {
	return c.runControlRequest(ctx, http.MethodPost, threadID, runID, "/cancel")
}

func (c *Client[I, O]) runControlRequest(ctx context.Context, method, threadID, runID, suffix string) (Run[O], error) {
	var result controlEnvelope[O]
	path := "/v1/threads/" + url.PathEscape(threadID) + "/runs/" + url.PathEscape(runID) + suffix
	err := c.controlRequest(ctx, method, path, map[string]any{}, &result, http.StatusOK)
	if err != nil || result.Run == nil {
		if err == nil {
			err = &Error{Code: CodeProtocol, Message: "run response is missing run"}
		}
		return Run[O]{}, err
	}
	return *result.Run, nil
}

func (c *Client[I, O]) controlRequest(ctx context.Context, method, path string, payload any, target *controlEnvelope[O], expectedStatus int) error {
	return c.controlRequestHeaders(ctx, method, path, payload, target, expectedStatus, nil)
}

func (c *Client[I, O]) controlRequestHeaders(ctx context.Context, method, path string, payload any, target *controlEnvelope[O], expectedStatus int, headers http.Header) error {
	var body io.Reader
	if payload != nil {
		encoded, err := json.Marshal(payload)
		if err != nil {
			return err
		}
		body = bytes.NewReader(encoded)
	}
	request, err := http.NewRequestWithContext(ctx, method, c.baseURL+path, body)
	if err != nil {
		return err
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set(ProtocolHeader, ProtocolVersion)
	c.applyHeaders(request)
	for key, values := range headers {
		request.Header[key] = append([]string(nil), values...)
	}
	response, err := c.http.Do(request)
	if err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return err
	}
	defer response.Body.Close()
	if version := response.Header.Get(ProtocolHeader); version != ProtocolVersion {
		return &Error{Code: CodeProtocol, Message: fmt.Sprintf("protocol version %q", version), Status: response.StatusCode}
	}
	if err := json.NewDecoder(io.LimitReader(response.Body, 2<<20)).Decode(target); err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return &Error{Code: CodeProtocol, Message: err.Error(), Status: response.StatusCode, cause: err}
	}
	if target.Error != nil {
		target.Error.Status = response.StatusCode
		return target.Error
	}
	if response.StatusCode != expectedStatus {
		return &Error{Code: CodeProtocol, Message: fmt.Sprintf("unexpected HTTP status %d", response.StatusCode), Status: response.StatusCode}
	}
	return nil
}

func listQuery(options ListOptions) string {
	query := url.Values{}
	if options.Limit != 0 {
		query.Set("limit", strconv.Itoa(options.Limit))
	}
	if options.Offset != 0 {
		query.Set("offset", strconv.Itoa(options.Offset))
	}
	return query.Encode()
}
