package distributed

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"
)

var (
	// ErrEventConflict identifies idempotency reuse with different event data.
	ErrEventConflict = errors.New("distributed event idempotency conflict")
	// ErrEventCursorNotFound identifies a cursor absent from the selected run stream.
	ErrEventCursorNotFound = errors.New("distributed event cursor not found")
)

// Event is one immutable, ordered run stream record.
type Event struct {
	ID        string          `json:"id"`
	ThreadID  string          `json:"thread_id"`
	RunID     string          `json:"run_id"`
	TaskID    string          `json:"task_id,omitempty"`
	Mode      string          `json:"mode"`
	Sequence  uint64          `json:"sequence"`
	Data      json.RawMessage `json:"data"`
	Terminal  bool            `json:"terminal,omitempty"`
	CreatedAt time.Time       `json:"created_at"`
}

// AppendEvent creates one record. IdempotencyKey is scoped to thread and run.
type AppendEvent struct {
	ThreadID       string
	RunID          string
	TaskID         string
	Mode           string
	Data           json.RawMessage
	Terminal       bool
	IdempotencyKey string
}

// EventQuery selects an ordered run slice after an optional exact cursor.
type EventQuery struct {
	ThreadID string
	RunID    string
	AfterID  string
	Limit    int
	Buffer   int
}

// EventLog is the storage-neutral durable distributed stream contract.
type EventLog interface {
	Append(context.Context, AppendEvent) (Event, error)
	List(context.Context, EventQuery) ([]Event, error)
	Tail(context.Context, EventQuery) <-chan EventResult
}

// EventResult carries one tailed event or a terminal cursor/storage error.
type EventResult struct {
	Event Event
	Error error
}

// EventLogOptions supplies deterministic record time and identity.
type EventLogOptions struct {
	Clock       func() time.Time
	IDGenerator func() string
}

type eventBinding struct {
	hash [32]byte
	id   string
}

// MemoryEventLog is a concurrency-safe in-memory EventLog conformance store.
type MemoryEventLog struct {
	mu       sync.RWMutex
	clock    func() time.Time
	ids      func() string
	streams  map[string][]Event
	bindings map[string]eventBinding
	usedIDs  map[string]struct{}
	notify   map[string]chan struct{}
}

// NewMemoryEventLog validates and constructs a durable event reference store.
func NewMemoryEventLog(options EventLogOptions) (*MemoryEventLog, error) {
	if options.Clock == nil || options.IDGenerator == nil {
		return nil, fmt.Errorf("%w: event clock and ID generator are required", ErrInvalidRequest)
	}
	return &MemoryEventLog{
		clock: options.Clock, ids: options.IDGenerator, streams: make(map[string][]Event),
		bindings: make(map[string]eventBinding), usedIDs: make(map[string]struct{}), notify: make(map[string]chan struct{}),
	}, nil
}

// Append atomically allocates sequence and event ID after idempotency checks.
func (l *MemoryEventLog) Append(ctx context.Context, request AppendEvent) (Event, error) {
	if err := validContext(ctx); err != nil {
		return Event{}, err
	}
	if !validID(request.ThreadID) || !validID(request.RunID) || !validID(request.Mode) ||
		request.IdempotencyKey != "" && !validID(request.IdempotencyKey) || !json.Valid(request.Data) {
		return Event{}, fmt.Errorf("%w: invalid event identity, mode, idempotency key, or JSON data", ErrInvalidRequest)
	}
	streamKey := eventStreamKey(request.ThreadID, request.RunID)
	hash := eventFingerprint(request)
	l.mu.Lock()
	defer l.mu.Unlock()
	if request.IdempotencyKey != "" {
		key := streamKey + "\x00" + request.IdempotencyKey
		if binding, exists := l.bindings[key]; exists {
			if binding.hash != hash {
				return Event{}, ErrEventConflict
			}
			for _, event := range l.streams[streamKey] {
				if event.ID == binding.id {
					return cloneEvent(event), nil
				}
			}
			return Event{}, fmt.Errorf("%w: idempotency binding event is missing", ErrEventConflict)
		}
	}
	if stream := l.streams[streamKey]; len(stream) > 0 && stream[len(stream)-1].Terminal {
		return Event{}, fmt.Errorf("%w: cannot append after terminal event", ErrEventConflict)
	}
	id := l.ids()
	if !validID(id) {
		return Event{}, fmt.Errorf("%w: generated event ID is invalid", ErrInvalidRequest)
	}
	if _, duplicate := l.usedIDs[id]; duplicate {
		return Event{}, fmt.Errorf("%w: duplicate event ID %q", ErrInvalidRequest, id)
	}
	now := l.clock()
	if now.IsZero() {
		return Event{}, fmt.Errorf("%w: event clock returned zero time", ErrInvalidRequest)
	}
	event := Event{
		ID: id, ThreadID: request.ThreadID, RunID: request.RunID, TaskID: request.TaskID,
		Mode: request.Mode, Sequence: uint64(len(l.streams[streamKey]) + 1),
		Data: append(json.RawMessage(nil), request.Data...), Terminal: request.Terminal, CreatedAt: now,
	}
	l.streams[streamKey] = append(l.streams[streamKey], event)
	l.usedIDs[id] = struct{}{}
	if request.IdempotencyKey != "" {
		l.bindings[streamKey+"\x00"+request.IdempotencyKey] = eventBinding{hash: hash, id: id}
	}
	watch := l.notify[streamKey]
	if watch != nil {
		close(watch)
	}
	l.notify[streamKey] = make(chan struct{})
	return cloneEvent(event), nil
}

// List returns isolated events in sequence order.
func (l *MemoryEventLog) List(ctx context.Context, query EventQuery) ([]Event, error) {
	if err := validContext(ctx); err != nil {
		return nil, err
	}
	if !validID(query.ThreadID) || !validID(query.RunID) || query.Limit < 0 || query.Buffer < 0 {
		return nil, fmt.Errorf("%w: invalid event query", ErrInvalidRequest)
	}
	l.mu.RLock()
	source := l.streams[eventStreamKey(query.ThreadID, query.RunID)]
	start := 0
	if query.AfterID != "" {
		start = -1
		for index, event := range source {
			if event.ID == query.AfterID {
				start = index + 1
				break
			}
		}
		if start < 0 {
			l.mu.RUnlock()
			return nil, ErrEventCursorNotFound
		}
	}
	end := len(source)
	if query.Limit > 0 && start+query.Limit < end {
		end = start + query.Limit
	}
	result := make([]Event, end-start)
	for index, event := range source[start:end] {
		result[index] = cloneEvent(event)
	}
	l.mu.RUnlock()
	return result, nil
}

// Tail replays from AfterID and blocks for future events with caller-selected
// channel backpressure. It closes after a terminal event or context cancellation.
func (l *MemoryEventLog) Tail(ctx context.Context, query EventQuery) <-chan EventResult {
	buffer := query.Buffer
	if buffer < 0 {
		buffer = 0
	}
	output := make(chan EventResult, buffer)
	go func() {
		defer close(output)
		if err := validContext(ctx); err != nil {
			return
		}
		if !validID(query.ThreadID) || !validID(query.RunID) || query.Limit < 0 || query.Buffer < 0 {
			output <- EventResult{Error: fmt.Errorf("%w: invalid event query", ErrInvalidRequest)}
			return
		}
		afterID := query.AfterID
		for {
			events, watch, completed, err := l.tailBatch(query, afterID)
			if err != nil {
				select {
				case output <- EventResult{Error: err}:
				case <-ctx.Done():
				}
				return
			}
			for _, event := range events {
				select {
				case output <- EventResult{Event: event}:
					afterID = event.ID
				case <-ctx.Done():
					return
				}
				if event.Terminal {
					return
				}
			}
			if completed {
				return
			}
			if len(events) > 0 {
				continue
			}
			select {
			case <-watch:
			case <-ctx.Done():
				return
			}
		}
	}()
	return output
}

func (l *MemoryEventLog) tailBatch(query EventQuery, afterID string) ([]Event, <-chan struct{}, bool, error) {
	key := eventStreamKey(query.ThreadID, query.RunID)
	l.mu.Lock()
	defer l.mu.Unlock()
	source := l.streams[key]
	start := 0
	completed := false
	if afterID != "" {
		start = -1
		for index, event := range source {
			if event.ID == afterID {
				start = index + 1
				completed = event.Terminal
				break
			}
		}
		if start < 0 {
			return nil, nil, false, ErrEventCursorNotFound
		}
	}
	end := len(source)
	if query.Limit > 0 && start+query.Limit < end {
		end = start + query.Limit
	}
	events := make([]Event, end-start)
	for index, event := range source[start:end] {
		events[index] = cloneEvent(event)
	}
	watch := l.notify[key]
	if watch == nil {
		watch = make(chan struct{})
		l.notify[key] = watch
	}
	return events, watch, completed, nil
}

func eventStreamKey(threadID, runID string) string { return threadID + "\x00" + runID }

func cloneEvent(event Event) Event {
	event.Data = append(json.RawMessage(nil), event.Data...)
	return event
}

func eventFingerprint(request AppendEvent) [32]byte {
	fingerprint := make([]byte, 0, len(request.TaskID)+len(request.Mode)+len(request.Data)+3)
	fingerprint = append(fingerprint, request.TaskID...)
	fingerprint = append(fingerprint, 0)
	fingerprint = append(fingerprint, request.Mode...)
	fingerprint = append(fingerprint, 0)
	fingerprint = append(fingerprint, request.Data...)
	if request.Terminal {
		fingerprint = append(fingerprint, 1)
	} else {
		fingerprint = append(fingerprint, 0)
	}
	return sha256.Sum256(fingerprint)
}
