package distributed

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"reflect"
	"sync"
)

// ErrCompletionConflict identifies nondeterministic results for one task ID.
var ErrCompletionConflict = errors.New("distributed task completion conflict")

// Completion is the durable result produced before a task lease is acknowledged.
type Completion[R any] struct {
	TaskID     string
	Attempt    int
	LeaseToken string
	WorkerID   string
	Value      R
}

// ResultHandler computes a typed task result.
type ResultHandler[P, R any] func(context.Context, Task[P]) (R, error)

// Outbox durably and idempotently commits task results.
type Outbox[R any] interface {
	Commit(context.Context, Completion[R]) error
}

// LeaseAcknowledgingOutbox atomically commits a result and acknowledges its
// queue lease. WithOutbox and Worker use this marker to avoid a second Ack.
type LeaseAcknowledgingOutbox interface{ AcknowledgesLease() bool }

var errLeaseAcknowledged = errors.New("distributed lease acknowledged with completion")

// WithOutbox adapts a result handler to Worker Handler. The worker can Ack only
// after Commit succeeds; handler or commit failures therefore follow Nack.
func WithOutbox[P, R any](handler ResultHandler[P, R], outbox Outbox[R]) (Handler[P], error) {
	if handler == nil || nilLike(outbox) {
		return nil, fmt.Errorf("%w: result handler and outbox are required", ErrInvalidRequest)
	}
	return func(ctx context.Context, task Task[P]) error {
		value, err := handler(ctx, task)
		if err != nil {
			return err
		}
		if err := outbox.Commit(ctx, Completion[R]{
			TaskID: task.ID, Attempt: task.Attempt, LeaseToken: task.Lease.Token, WorkerID: task.Lease.WorkerID, Value: value,
		}); err != nil {
			return &WorkerError{Operation: "commit-outbox", TaskID: task.ID, Err: err}
		}
		if atomic, ok := outbox.(LeaseAcknowledgingOutbox); ok && atomic.AcknowledgesLease() {
			return errLeaseAcknowledged
		}
		return nil
	}, nil
}

type outboxRecord struct {
	attempt    int
	leaseToken string
	encoded    []byte
	hash       [32]byte
}

// MemoryOutbox is a codec-isolated in-memory result conformance store.
type MemoryOutbox[R any] struct {
	mu      sync.RWMutex
	codec   PayloadCodec[R]
	records map[string]outboxRecord
}

// NewMemoryOutbox validates and constructs an in-memory outbox.
func NewMemoryOutbox[R any](codec PayloadCodec[R]) (*MemoryOutbox[R], error) {
	if nilLike(codec) {
		return nil, fmt.Errorf("%w: result codec is required", ErrInvalidRequest)
	}
	return &MemoryOutbox[R]{codec: codec, records: make(map[string]outboxRecord)}, nil
}

// Commit writes a result once. Redelivery with another lease/attempt and the
// same encoded value is idempotent; a different value is a consistency error.
func (o *MemoryOutbox[R]) Commit(ctx context.Context, completion Completion[R]) error {
	if err := validContext(ctx); err != nil {
		return err
	}
	if !validID(completion.TaskID) || !validID(completion.LeaseToken) || completion.Attempt < 1 {
		return fmt.Errorf("%w: completion task, attempt, and lease are required", ErrInvalidRequest)
	}
	encoded, err := o.codec.Encode(completion.Value)
	if err != nil {
		return fmt.Errorf("%w: encode completion: %v", ErrInvalidRequest, err)
	}
	// Decode before commit proves the stored representation can be restored.
	if _, err := o.codec.Decode(encoded); err != nil {
		return fmt.Errorf("%w: decode completion: %v", ErrInvalidRequest, err)
	}
	hash := sha256.Sum256(encoded)
	o.mu.Lock()
	defer o.mu.Unlock()
	if existing, found := o.records[completion.TaskID]; found {
		if existing.hash != hash {
			return ErrCompletionConflict
		}
		return nil
	}
	o.records[completion.TaskID] = outboxRecord{
		attempt: completion.Attempt, leaseToken: completion.LeaseToken,
		encoded: append([]byte(nil), encoded...), hash: hash,
	}
	return nil
}

// Get returns an isolated committed result.
func (o *MemoryOutbox[R]) Get(ctx context.Context, taskID string) (Completion[R], bool, error) {
	var zero Completion[R]
	if err := validContext(ctx); err != nil {
		return zero, false, err
	}
	o.mu.RLock()
	record, found := o.records[taskID]
	encoded := append([]byte(nil), record.encoded...)
	o.mu.RUnlock()
	if !found {
		return zero, false, nil
	}
	value, err := o.codec.Decode(encoded)
	if err != nil {
		return zero, false, fmt.Errorf("%w: decode committed completion: %v", ErrInvalidRequest, err)
	}
	return Completion[R]{TaskID: taskID, Attempt: record.attempt, LeaseToken: record.leaseToken, Value: value}, true, nil
}

func nilLike(value any) bool {
	if value == nil {
		return true
	}
	reflected := reflect.ValueOf(value)
	switch reflected.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return reflected.IsNil()
	default:
		return false
	}
}
