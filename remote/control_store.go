package remote

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"sort"
	"sync"
	"time"
)

var (
	// ErrControlConflict reports a duplicate control resource or invalid transition.
	ErrControlConflict = errors.New("remote control resource conflict")
	// ErrControlNotFound reports a missing control resource.
	ErrControlNotFound = errors.New("remote control resource not found")
	// ErrIdempotencyConflict reports reuse of an idempotency key with different input.
	ErrIdempotencyConflict = errors.New("remote idempotency key conflict")
)

// StoredRun is the serialization-neutral representation persisted by ControlStore.
type StoredRun struct {
	ID, ThreadID         string
	Status               RunStatus
	Output               json.RawMessage
	Error                *Error
	CreatedAt, UpdatedAt time.Time
}

// ControlStore is the atomic shared control plane for remote threads and runs.
// Implementations must make CreateRun's idempotency claim and run insert atomic.
type ControlStore interface {
	CreateThread(context.Context, Thread) error
	GetThread(context.Context, string) (Thread, bool, error)
	ListThreads(context.Context, ListOptions) ([]Thread, error)
	DeleteThread(context.Context, string) error
	CreateRun(context.Context, StoredRun, string, []byte) (StoredRun, bool, error)
	GetRun(context.Context, string, string) (StoredRun, bool, error)
	ListRuns(context.Context, string, ListOptions) ([]StoredRun, error)
	TransitionRun(context.Context, string, string, []RunStatus, StoredRun) (StoredRun, bool, error)
	PruneRuns(context.Context, time.Time) (int64, error)
}

type memoryIdempotency struct {
	hash  []byte
	runID string
}

// MemoryControlStore is a process-shared atomic ControlStore. Use SQLiteControlStore
// when resources must survive process restarts.
type MemoryControlStore struct {
	mu          sync.RWMutex
	threads     map[string]Thread
	runs        map[string]map[string]StoredRun
	idempotency map[string]memoryIdempotency
}

func NewMemoryControlStore() *MemoryControlStore {
	return &MemoryControlStore{threads: map[string]Thread{}, runs: map[string]map[string]StoredRun{}, idempotency: map[string]memoryIdempotency{}}
}

func (m *MemoryControlStore) CreateThread(_ context.Context, thread Thread) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.threads[thread.ID]; ok {
		return ErrControlConflict
	}
	thread.Metadata = cloneMetadata(thread.Metadata)
	m.threads[thread.ID] = thread
	m.runs[thread.ID] = map[string]StoredRun{}
	return nil
}
func (m *MemoryControlStore) GetThread(_ context.Context, id string) (Thread, bool, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	value, ok := m.threads[id]
	value.Metadata = cloneMetadata(value.Metadata)
	return value, ok, nil
}
func (m *MemoryControlStore) ListThreads(_ context.Context, options ListOptions) ([]Thread, error) {
	m.mu.RLock()
	items := make([]Thread, 0, len(m.threads))
	for _, item := range m.threads {
		item.Metadata = cloneMetadata(item.Metadata)
		items = append(items, item)
	}
	m.mu.RUnlock()
	sort.Slice(items, func(i, j int) bool {
		if items[i].CreatedAt.Equal(items[j].CreatedAt) {
			return items[i].ID < items[j].ID
		}
		return items[i].CreatedAt.Before(items[j].CreatedAt)
	})
	return paginate(items, options), nil
}
func (m *MemoryControlStore) DeleteThread(_ context.Context, id string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.threads[id]; !ok {
		return ErrControlNotFound
	}
	delete(m.threads, id)
	delete(m.runs, id)
	for key := range m.idempotency {
		if len(key) > len(id) && key[:len(id)+1] == id+"\x00" {
			delete(m.idempotency, key)
		}
	}
	return nil
}
func (m *MemoryControlStore) CreateRun(_ context.Context, run StoredRun, key string, hash []byte) (StoredRun, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	thread, ok := m.threads[run.ThreadID]
	if !ok {
		return StoredRun{}, false, ErrControlNotFound
	}
	claim := run.ThreadID + "\x00" + key
	if key != "" {
		if prior, ok := m.idempotency[claim]; ok {
			if !bytes.Equal(prior.hash, hash) {
				return StoredRun{}, false, ErrIdempotencyConflict
			}
			return cloneStoredRun(m.runs[run.ThreadID][prior.runID]), false, nil
		}
	}
	if _, ok := m.runs[run.ThreadID][run.ID]; ok {
		return StoredRun{}, false, ErrControlConflict
	}
	m.runs[run.ThreadID][run.ID] = cloneStoredRun(run)
	if key != "" {
		m.idempotency[claim] = memoryIdempotency{append([]byte(nil), hash...), run.ID}
	}
	thread.UpdatedAt = run.UpdatedAt
	m.threads[run.ThreadID] = thread
	return cloneStoredRun(run), true, nil
}
func (m *MemoryControlStore) GetRun(_ context.Context, threadID, id string) (StoredRun, bool, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	run, ok := m.runs[threadID][id]
	return cloneStoredRun(run), ok, nil
}
func (m *MemoryControlStore) ListRuns(_ context.Context, threadID string, options ListOptions) ([]StoredRun, error) {
	m.mu.RLock()
	values, ok := m.runs[threadID]
	if !ok {
		m.mu.RUnlock()
		return nil, ErrControlNotFound
	}
	items := make([]StoredRun, 0, len(values))
	for _, item := range values {
		items = append(items, cloneStoredRun(item))
	}
	m.mu.RUnlock()
	sort.Slice(items, func(i, j int) bool {
		if items[i].CreatedAt.Equal(items[j].CreatedAt) {
			return items[i].ID < items[j].ID
		}
		return items[i].CreatedAt.Before(items[j].CreatedAt)
	})
	return paginate(items, options), nil
}
func (m *MemoryControlStore) TransitionRun(_ context.Context, threadID, id string, expected []RunStatus, next StoredRun) (StoredRun, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	current, ok := m.runs[threadID][id]
	if !ok {
		return StoredRun{}, false, ErrControlNotFound
	}
	allowed := false
	for _, status := range expected {
		if current.Status == status {
			allowed = true
			break
		}
	}
	if !allowed {
		return cloneStoredRun(current), false, nil
	}
	next.ID = id
	next.ThreadID = threadID
	next.CreatedAt = current.CreatedAt
	m.runs[threadID][id] = cloneStoredRun(next)
	thread := m.threads[threadID]
	thread.UpdatedAt = next.UpdatedAt
	m.threads[threadID] = thread
	return cloneStoredRun(next), true, nil
}
func (m *MemoryControlStore) PruneRuns(_ context.Context, before time.Time) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var count int64
	for threadID, runs := range m.runs {
		for id, run := range runs {
			if terminalRun(run.Status) && run.UpdatedAt.Before(before) {
				delete(runs, id)
				count++
				for key, claim := range m.idempotency {
					if claim.runID == id && len(key) > len(threadID) && key[:len(threadID)+1] == threadID+"\x00" {
						delete(m.idempotency, key)
					}
				}
			}
		}
	}
	return count, nil
}

func terminalRun(status RunStatus) bool {
	return status == RunSuccess || status == RunError || status == RunCanceled
}
func cloneStoredRun(run StoredRun) StoredRun {
	run.Output = append(json.RawMessage(nil), run.Output...)
	if run.Error != nil {
		value := *run.Error
		run.Error = &value
	}
	return run
}
