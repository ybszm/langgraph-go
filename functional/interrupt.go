package functional

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync/atomic"

	"github.com/wahanbo/langgraph-go/checkpoint"
)

var (
	// ErrInterrupted indicates a durable Functional invocation awaits input.
	ErrInterrupted = errors.New("functional entrypoint interrupted")
	// ErrInvalidResume indicates unknown or malformed resume values.
	ErrInvalidResume = errors.New("invalid functional resume")
	// ErrInterruptRequiresDurability indicates AwaitResume lacks a durable attempt.
	ErrInterruptRequiresDurability = errors.New("functional interrupt requires durable entrypoint")
)

// Interrupt contains one stable resume ID and JSON prompt payload.
type Interrupt struct {
	ID    string
	Value json.RawMessage
}

// InterruptError reports durable inputs required to continue a workflow.
type InterruptError struct {
	Interrupts []Interrupt
}

func (e *InterruptError) Error() string {
	return fmt.Sprintf("%v: %d pending interrupt(s)", ErrInterrupted, len(e.Interrupts))
}

func (e *InterruptError) Unwrap() error { return ErrInterrupted }

type interruptScopeKey struct{}

type interruptScope struct {
	id   string
	next atomic.Uint64
}

type persistedInterrupt struct {
	ID     string          `json:"id"`
	Value  json.RawMessage `json:"value"`
	Resume json.RawMessage `json:"resume,omitempty"`
}

// AwaitResume returns a previously supplied typed value or durably interrupts
// the current invocation with prompt.
func AwaitResume[T any](ctx context.Context, prompt any) (T, error) {
	manager, ok := ctx.Value(runtimeContextKey{}).(*taskManager)
	if !ok || manager == nil || manager.durable == nil {
		return zero[T](), ErrInterruptRequiresDurability
	}
	scope, ok := ctx.Value(interruptScopeKey{}).(*interruptScope)
	if !ok || scope == nil {
		return zero[T](), ErrInterruptRequiresDurability
	}
	id := fmt.Sprintf("%s:interrupt:%d", scope.id, scope.next.Add(1)-1)
	if write, exists := manager.durable.recovered[id]; exists {
		stored, err := decodePersistedInterrupt(write)
		if err != nil {
			return zero[T](), err
		}
		if stored.ID != id {
			return zero[T](), fmt.Errorf("%w: interrupt ID mismatch %q", ErrInvalidResume, stored.ID)
		}
		if len(stored.Resume) > 0 {
			var value T
			if err := json.Unmarshal(stored.Resume, &value); err != nil {
				return zero[T](), fmt.Errorf("%w: decode interrupt %q response: %v", ErrInvalidResume, id, err)
			}
			return value, nil
		}
		return zero[T](), &InterruptError{Interrupts: []Interrupt{{ID: id, Value: append(json.RawMessage(nil), stored.Value...)}}}
	}
	raw, err := json.Marshal(prompt)
	if err != nil {
		return zero[T](), fmt.Errorf("encode functional interrupt prompt: %w", err)
	}
	stored := persistedInterrupt{ID: id, Value: raw}
	encoded, err := encodePersistedInterrupt(stored)
	if err != nil {
		return zero[T](), err
	}
	if err := manager.durable.saver.PutWrites(ctx, manager.durable.config, []checkpoint.PendingWrite{{
		TaskID: id, TaskPath: scope.id, Index: -1, Channel: checkpoint.InterruptChannel, Value: encoded,
	}}); err != nil {
		return zero[T](), fmt.Errorf("persist functional interrupt: %w", err)
	}
	return zero[T](), &InterruptError{Interrupts: []Interrupt{{ID: id, Value: append(json.RawMessage(nil), raw...)}}}
}

// Resume supplies values for pending interrupt IDs, then replays the same input.
func (e *DurableEntrypoint[I, O, S]) Resume(
	ctx context.Context,
	input I,
	runConfig DurableRunConfig,
	resumes map[string]any,
) (O, error) {
	var output O
	if e == nil || e.run == nil {
		return output, fmt.Errorf("%w: nil durable entrypoint", ErrInvalidDefinition)
	}
	config := checkpoint.Config{ThreadID: runConfig.ThreadID, Namespace: e.config.Namespace, CheckpointID: runConfig.CheckpointID}
	if err := config.Validate(); err != nil {
		return output, err
	}
	if len(resumes) == 0 {
		return output, fmt.Errorf("%w: no resume values supplied", ErrInvalidResume)
	}
	unlock := e.lockThread(config.ThreadID, config.Namespace)
	tuple, found, err := e.config.Saver.GetTuple(ctx, config)
	if err != nil {
		unlock()
		return output, fmt.Errorf("functional checkpoint get for resume: %w", err)
	}
	if !found || tuple.Metadata["status"] != "running" {
		unlock()
		return output, fmt.Errorf("%w: thread has no running attempt", ErrInvalidResume)
	}
	if storedName, ok := tuple.Metadata["entrypoint"].(string); ok && storedName != "" && storedName != e.name {
		unlock()
		return output, fmt.Errorf("%w: checkpoint belongs to entrypoint %q, current %q", ErrIncompatibleRevision, storedName, e.name)
	}
	if err := e.validateRunningRevision(tuple); err != nil {
		unlock()
		return output, err
	}
	pending := make(map[string]checkpoint.PendingWrite)
	for _, write := range tuple.PendingWrites {
		if write.Channel == checkpoint.InterruptChannel {
			pending[write.TaskID] = write
		}
	}
	writes := make([]checkpoint.PendingWrite, 0, len(resumes))
	for id, response := range resumes {
		write, exists := pending[id]
		if !exists {
			unlock()
			return output, fmt.Errorf("%w: unknown interrupt ID %q", ErrInvalidResume, id)
		}
		stored, err := decodePersistedInterrupt(write)
		if err != nil {
			unlock()
			return output, err
		}
		stored.Resume, err = json.Marshal(response)
		if err != nil {
			unlock()
			return output, fmt.Errorf("%w: encode interrupt %q response: %v", ErrInvalidResume, id, err)
		}
		write.Value, err = encodePersistedInterrupt(stored)
		if err != nil {
			unlock()
			return output, err
		}
		writes = append(writes, write)
	}
	if err := e.config.Saver.PutWrites(ctx, tuple.Config, writes); err != nil {
		unlock()
		return output, fmt.Errorf("persist functional resume: %w", err)
	}
	unlock()
	return e.Invoke(ctx, input, runConfig)
}

func encodePersistedInterrupt(value persistedInterrupt) (checkpoint.EncodedValue, error) {
	data, err := json.Marshal(value)
	if err != nil {
		return checkpoint.EncodedValue{}, err
	}
	return checkpoint.EncodedValue{Type: "functional_interrupt", Version: 1, Data: data}, nil
}

func decodePersistedInterrupt(write checkpoint.PendingWrite) (persistedInterrupt, error) {
	if write.Value.Type != "functional_interrupt" || write.Value.Version != 1 {
		return persistedInterrupt{}, fmt.Errorf("%w: invalid interrupt envelope", ErrInvalidResume)
	}
	var result persistedInterrupt
	if err := json.Unmarshal(write.Value.Data, &result); err != nil {
		return result, fmt.Errorf("%w: decode interrupt envelope: %v", ErrInvalidResume, err)
	}
	return result, nil
}
