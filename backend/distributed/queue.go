// Package distributed defines provider-neutral leased task scheduling contracts
// and an in-memory reference implementation for conformance tests.
package distributed

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"
)

var (
	// ErrInvalidRequest identifies invalid queue arguments or generated IDs.
	ErrInvalidRequest = errors.New("invalid distributed queue request")
	// ErrTaskNotFound identifies an unknown or already acknowledged task.
	ErrTaskNotFound = errors.New("distributed task not found")
	// ErrLeaseLost identifies expired, replaced, or owner-mismatched leases.
	ErrLeaseLost = errors.New("distributed task lease lost")
	// ErrIdempotencyConflict identifies key reuse with a different request.
	ErrIdempotencyConflict = errors.New("distributed enqueue idempotency conflict")
)

// Lease is an ownership fencing token for one claimed task.
type Lease struct {
	TaskID    string
	Token     string
	WorkerID  string
	ExpiresAt time.Time
}

// Task is one claimed at-least-once work item.
type Task[P any] struct {
	ID          string
	Payload     P
	Attempt     int
	EnqueuedAt  time.Time
	AvailableAt time.Time
	Lease       Lease
}

// EnqueueRequest creates one task, optionally delayed until AvailableAt.
type EnqueueRequest[P any] struct {
	TaskID         string
	Payload        P
	AvailableAt    time.Time
	IdempotencyKey string
}

// PayloadCodec provides persistence-grade payload isolation and fingerprints.
type PayloadCodec[P any] interface {
	Encode(P) ([]byte, error)
	Decode([]byte) (P, error)
}

// Queue is the storage-neutral leased scheduler contract.
type Queue[P any] interface {
	Enqueue(context.Context, EnqueueRequest[P]) (string, error)
	Claim(context.Context, string, int, time.Duration) ([]Task[P], error)
	Ack(context.Context, Lease) error
	Nack(context.Context, Lease, time.Duration) error
	Heartbeat(context.Context, Lease, time.Duration) error
}

// QueueOptions supplies deterministic time and identity dependencies.
type QueueOptions struct {
	Clock       func() time.Time
	IDGenerator func() string
}

type taskRecord[P any] struct {
	task    Task[P]
	encoded []byte
	leased  bool
}

type enqueueBinding struct {
	taskID string
	hash   [32]byte
}

// MemoryQueue is a concurrency-safe in-memory reference leased queue.
type MemoryQueue[P any] struct {
	mu       sync.Mutex
	clock    func() time.Time
	ids      func() string
	records  map[string]*taskRecord[P]
	codec    PayloadCodec[P]
	bindings map[string]enqueueBinding
}

// NewMemoryQueue validates and constructs an in-memory leased queue.
func NewMemoryQueue[P any](options QueueOptions) (*MemoryQueue[P], error) {
	if options.Clock == nil || options.IDGenerator == nil {
		return nil, fmt.Errorf("%w: clock and ID generator are required", ErrInvalidRequest)
	}
	return &MemoryQueue[P]{
		clock: options.Clock, ids: options.IDGenerator, records: make(map[string]*taskRecord[P]),
		bindings: make(map[string]enqueueBinding),
	}, nil
}

// NewCodecMemoryQueue adds codec round-trip isolation and idempotent enqueue support.
func NewCodecMemoryQueue[P any](options QueueOptions, codec PayloadCodec[P]) (*MemoryQueue[P], error) {
	if codec == nil {
		return nil, fmt.Errorf("%w: payload codec is required", ErrInvalidRequest)
	}
	queue, err := NewMemoryQueue[P](options)
	if err != nil {
		return nil, err
	}
	queue.codec = codec
	return queue, nil
}

// Enqueue stores one task and returns its stable ID.
func (q *MemoryQueue[P]) Enqueue(ctx context.Context, request EnqueueRequest[P]) (string, error) {
	if err := validContext(ctx); err != nil {
		return "", err
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	if request.IdempotencyKey != "" && !validID(request.IdempotencyKey) {
		return "", fmt.Errorf("%w: idempotency key is invalid", ErrInvalidRequest)
	}
	storedPayload := request.Payload
	var encoded []byte
	var requestHash [32]byte
	if q.codec != nil {
		var err error
		encoded, err = q.codec.Encode(request.Payload)
		if err != nil {
			return "", fmt.Errorf("%w: encode payload: %v", ErrInvalidRequest, err)
		}
		storedPayload, err = q.codec.Decode(encoded)
		if err != nil {
			return "", fmt.Errorf("%w: decode payload clone: %v", ErrInvalidRequest, err)
		}
		requestHash = queueFingerprint(encoded, request.TaskID, request.AvailableAt)
	} else if request.IdempotencyKey != "" {
		return "", fmt.Errorf("%w: idempotent enqueue requires payload codec", ErrInvalidRequest)
	}
	if request.IdempotencyKey != "" {
		if binding, exists := q.bindings[request.IdempotencyKey]; exists {
			if binding.hash != requestHash {
				return "", ErrIdempotencyConflict
			}
			return binding.taskID, nil
		}
	}
	id := request.TaskID
	if id == "" {
		id = q.ids()
	}
	if !validID(id) {
		return "", fmt.Errorf("%w: generated task ID is invalid", ErrInvalidRequest)
	}
	if _, exists := q.records[id]; exists {
		return "", fmt.Errorf("%w: duplicate task ID %q", ErrInvalidRequest, id)
	}
	now := q.clock()
	available := request.AvailableAt
	if available.IsZero() {
		available = now
	}
	q.records[id] = &taskRecord[P]{task: Task[P]{
		ID: id, Payload: storedPayload, EnqueuedAt: now, AvailableAt: available,
	}, encoded: append([]byte(nil), encoded...)}
	if request.IdempotencyKey != "" {
		q.bindings[request.IdempotencyKey] = enqueueBinding{taskID: id, hash: requestHash}
	}
	return id, nil
}

// Claim returns up to limit tasks in deterministic availability/enqueue/ID
// order and fences each claim with a fresh token.
func (q *MemoryQueue[P]) Claim(ctx context.Context, workerID string, limit int, duration time.Duration) ([]Task[P], error) {
	if err := validContext(ctx); err != nil {
		return nil, err
	}
	if !validID(workerID) || limit <= 0 || duration <= 0 {
		return nil, fmt.Errorf("%w: worker, positive limit, and lease duration are required", ErrInvalidRequest)
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	now := q.clock()
	eligible := make([]*taskRecord[P], 0)
	for _, record := range q.records {
		if record.leased && now.Before(record.task.Lease.ExpiresAt) {
			continue
		}
		if record.task.AvailableAt.After(now) {
			continue
		}
		eligible = append(eligible, record)
	}
	sort.Slice(eligible, func(i, j int) bool {
		left, right := eligible[i].task, eligible[j].task
		if !left.AvailableAt.Equal(right.AvailableAt) {
			return left.AvailableAt.Before(right.AvailableAt)
		}
		if !left.EnqueuedAt.Equal(right.EnqueuedAt) {
			return left.EnqueuedAt.Before(right.EnqueuedAt)
		}
		return left.ID < right.ID
	})
	if len(eligible) > limit {
		eligible = eligible[:limit]
	}
	tokens := make([]string, len(eligible))
	payloads := make([]P, len(eligible))
	for index := range eligible {
		payloads[index] = eligible[index].task.Payload
		if q.codec != nil {
			var err error
			payloads[index], err = q.codec.Decode(eligible[index].encoded)
			if err != nil {
				return nil, fmt.Errorf("%w: decode claimed payload: %v", ErrInvalidRequest, err)
			}
		}
		tokens[index] = q.ids()
		if !validID(tokens[index]) {
			return nil, fmt.Errorf("%w: generated lease token is invalid", ErrInvalidRequest)
		}
	}
	result := make([]Task[P], 0, len(eligible))
	for index, record := range eligible {
		record.leased = true
		record.task.Attempt++
		record.task.Lease = Lease{TaskID: record.task.ID, Token: tokens[index], WorkerID: workerID, ExpiresAt: now.Add(duration)}
		claimed := record.task
		claimed.Payload = payloads[index]
		result = append(result, claimed)
	}
	return result, nil
}

// Ack permanently removes a task if lease ownership is still current.
func (q *MemoryQueue[P]) Ack(ctx context.Context, lease Lease) error {
	if err := validContext(ctx); err != nil {
		return err
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	record, err := q.ownedRecord(lease, q.clock())
	if err != nil {
		return err
	}
	delete(q.records, record.task.ID)
	return nil
}

// Nack releases a current lease and makes the task available after delay.
func (q *MemoryQueue[P]) Nack(ctx context.Context, lease Lease, delay time.Duration) error {
	if err := validContext(ctx); err != nil {
		return err
	}
	if delay < 0 {
		return fmt.Errorf("%w: nack delay cannot be negative", ErrInvalidRequest)
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	now := q.clock()
	record, err := q.ownedRecord(lease, now)
	if err != nil {
		return err
	}
	record.leased = false
	record.task.Lease = Lease{}
	record.task.AvailableAt = now.Add(delay)
	return nil
}

// Heartbeat replaces the expiry of a current lease with now plus extension.
func (q *MemoryQueue[P]) Heartbeat(ctx context.Context, lease Lease, extension time.Duration) error {
	if err := validContext(ctx); err != nil {
		return err
	}
	if extension <= 0 {
		return fmt.Errorf("%w: heartbeat extension must be positive", ErrInvalidRequest)
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	now := q.clock()
	record, err := q.ownedRecord(lease, now)
	if err != nil {
		return err
	}
	record.task.Lease.ExpiresAt = now.Add(extension)
	return nil
}

func (q *MemoryQueue[P]) ownedRecord(lease Lease, now time.Time) (*taskRecord[P], error) {
	record := q.records[lease.TaskID]
	if record == nil {
		return nil, ErrTaskNotFound
	}
	current := record.task.Lease
	if !record.leased || current.Token != lease.Token || current.WorkerID != lease.WorkerID || !now.Before(current.ExpiresAt) {
		return nil, ErrLeaseLost
	}
	return record, nil
}

func validContext(ctx context.Context) error {
	if ctx == nil {
		return fmt.Errorf("%w: context is nil", ErrInvalidRequest)
	}
	return ctx.Err()
}

func validID(id string) bool {
	return id != "" && strings.TrimSpace(id) == id && !strings.ContainsAny(id, "\r\n\x00")
}

func queueFingerprint(encoded []byte, taskID string, availableAt time.Time) [32]byte {
	fingerprint := append([]byte(nil), encoded...)
	fingerprint = append(fingerprint, 0)
	fingerprint = append(fingerprint, taskID...)
	fingerprint = append(fingerprint, 0)
	fingerprint = append(fingerprint, availableAt.UTC().Format(time.RFC3339Nano)...)
	return sha256.Sum256(fingerprint)
}
