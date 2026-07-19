package distributed

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/ybszm/langgraph-go/graph"
)

var (
	// ErrInterruptNotFound identifies an unknown thread-scoped interrupt ID.
	ErrInterruptNotFound = errors.New("distributed interrupt not found")
	// ErrInterruptConflict identifies prompt identity or resume value reuse conflict.
	ErrInterruptConflict = errors.New("distributed interrupt conflict")
)

// InterruptStatus is the durable external-input lifecycle state.
type InterruptStatus string

const (
	// InterruptPending means no resume value has been committed.
	InterruptPending InterruptStatus = "pending"
	// InterruptResumed means Await can return the committed encoded value.
	InterruptResumed InterruptStatus = "resumed"
)

// InterruptRecord is one thread-scoped durable interrupt snapshot.
type InterruptRecord struct {
	ThreadID     string
	TaskID       string
	CheckpointID string
	Index        int
	Interrupt    graph.Interrupt
	Status       InterruptStatus
	Resume       json.RawMessage
	CreatedAt    time.Time
	ResumedAt    *time.Time
}

// InterruptPendingError pauses graph node execution while preserving the prompt.
type InterruptPendingError struct{ Interrupt graph.Interrupt }

func (e *InterruptPendingError) Error() string {
	return fmt.Sprintf("distributed interrupt %s is pending", e.Interrupt.ID)
}

// Unwrap makes pending errors detectable as graph.ErrGraphInterrupt.
func (e *InterruptPendingError) Unwrap() error { return graph.ErrGraphInterrupt }

// InterruptStore is the storage-neutral distributed interrupt contract.
type InterruptStore interface {
	Await(context.Context, graph.InterruptRequest) (json.RawMessage, error)
	Resume(context.Context, string, string, any) error
	List(context.Context, string) ([]InterruptRecord, error)
}

// MemoryInterruptStore is a concurrency-safe in-memory resume conformance store.
type MemoryInterruptStore struct {
	mu      sync.RWMutex
	clock   func() time.Time
	records map[string]*InterruptRecord
}

// NewMemoryInterruptStore validates and constructs an interrupt store.
func NewMemoryInterruptStore(clock func() time.Time) (*MemoryInterruptStore, error) {
	if clock == nil {
		return nil, fmt.Errorf("%w: interrupt clock is required", ErrInvalidRequest)
	}
	return &MemoryInterruptStore{clock: clock, records: make(map[string]*InterruptRecord)}, nil
}

// Provider exposes Await as graph.ResumeProvider.
func (s *MemoryInterruptStore) Provider() graph.ResumeProvider { return s.Await }

// Await first-writes a stable prompt and returns ErrGraphInterrupt until Resume.
func (s *MemoryInterruptStore) Await(ctx context.Context, request graph.InterruptRequest) (json.RawMessage, error) {
	if err := validContext(ctx); err != nil {
		return nil, err
	}
	if !validID(request.ThreadID) || !validID(request.TaskID) || !validID(request.Interrupt.ID) ||
		request.Index < 0 || !json.Valid(request.Interrupt.Value) {
		return nil, fmt.Errorf("%w: invalid interrupt request", ErrInvalidRequest)
	}
	key := interruptKey(request.ThreadID, request.Interrupt.ID)
	s.mu.Lock()
	defer s.mu.Unlock()
	if existing := s.records[key]; existing != nil {
		if !sameInterrupt(existing, request) {
			return nil, ErrInterruptConflict
		}
		if existing.Status == InterruptResumed {
			return append(json.RawMessage(nil), existing.Resume...), nil
		}
		return nil, &InterruptPendingError{Interrupt: cloneInterrupt(existing.Interrupt)}
	}
	now := s.clock()
	if now.IsZero() {
		return nil, fmt.Errorf("%w: interrupt clock returned zero time", ErrInvalidRequest)
	}
	record := &InterruptRecord{
		ThreadID: request.ThreadID, TaskID: request.TaskID, CheckpointID: request.CheckpointID,
		Index: request.Index, Interrupt: cloneInterrupt(request.Interrupt), Status: InterruptPending, CreatedAt: now,
	}
	s.records[key] = record
	return nil, &InterruptPendingError{Interrupt: cloneInterrupt(record.Interrupt)}
}

// Resume commits one JSON-serializable value. Equal replay is idempotent.
func (s *MemoryInterruptStore) Resume(ctx context.Context, threadID, interruptID string, value any) error {
	if err := validContext(ctx); err != nil {
		return err
	}
	if !validID(threadID) || !validID(interruptID) {
		return fmt.Errorf("%w: thread and interrupt ID are required", ErrInvalidRequest)
	}
	encoded, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("%w: encode resume: %v", ErrInvalidRequest, err)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	record := s.records[interruptKey(threadID, interruptID)]
	if record == nil {
		return ErrInterruptNotFound
	}
	if record.Status == InterruptResumed {
		if bytes.Equal(record.Resume, encoded) {
			return nil
		}
		return ErrInterruptConflict
	}
	now := s.clock()
	if now.IsZero() {
		return fmt.Errorf("%w: interrupt clock returned zero time", ErrInvalidRequest)
	}
	record.Status = InterruptResumed
	record.Resume = append(json.RawMessage(nil), encoded...)
	record.ResumedAt = &now
	return nil
}

// List returns one thread's interrupts in creation-time and ID order.
func (s *MemoryInterruptStore) List(ctx context.Context, threadID string) ([]InterruptRecord, error) {
	if err := validContext(ctx); err != nil {
		return nil, err
	}
	if !validID(threadID) {
		return nil, fmt.Errorf("%w: thread ID is required", ErrInvalidRequest)
	}
	s.mu.RLock()
	result := make([]InterruptRecord, 0)
	for _, record := range s.records {
		if record.ThreadID == threadID {
			result = append(result, cloneInterruptRecord(*record))
		}
	}
	s.mu.RUnlock()
	sort.Slice(result, func(i, j int) bool {
		if result[i].CreatedAt.Equal(result[j].CreatedAt) {
			return result[i].Interrupt.ID < result[j].Interrupt.ID
		}
		return result[i].CreatedAt.Before(result[j].CreatedAt)
	})
	return result, nil
}

func interruptKey(threadID, interruptID string) string { return threadID + "\x00" + interruptID }

func sameInterrupt(record *InterruptRecord, request graph.InterruptRequest) bool {
	return record.TaskID == request.TaskID && record.CheckpointID == request.CheckpointID && record.Index == request.Index &&
		record.Interrupt.Namespace == request.Interrupt.Namespace && bytes.Equal(record.Interrupt.Value, request.Interrupt.Value)
}

func cloneInterrupt(source graph.Interrupt) graph.Interrupt {
	source.Value = append(json.RawMessage(nil), source.Value...)
	return source
}

func cloneInterruptRecord(source InterruptRecord) InterruptRecord {
	source.Interrupt = cloneInterrupt(source.Interrupt)
	source.Resume = append(json.RawMessage(nil), source.Resume...)
	if source.ResumedAt != nil {
		copy := *source.ResumedAt
		source.ResumedAt = &copy
	}
	return source
}
