// Package channel contains typed Pregel channel primitives.
package channel

import "errors"

var (
	// ErrEmpty indicates that a channel currently has no readable value.
	ErrEmpty = errors.New("channel is empty")
	// ErrInvalidUpdate indicates a write that violates channel semantics.
	ErrInvalidUpdate = errors.New("invalid channel update")
)

// Snapshot distinguishes an absent checkpoint value from T's zero value.
type Snapshot[T any] struct {
	Value   T
	Present bool
}

// Overwrite replaces an aggregate instead of applying its operator. At most
// one overwrite may occur in one BinaryOperatorAggregate update call.
type Overwrite[T any] struct {
	Value T
}
