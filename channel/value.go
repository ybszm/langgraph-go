package channel

import "fmt"

// LastValue stores at most one update per super-step.
type LastValue[T any] struct {
	value   T
	present bool
}

func NewLastValue[T any](snapshot Snapshot[T]) *LastValue[T] {
	return &LastValue[T]{value: snapshot.Value, present: snapshot.Present}
}

func (c *LastValue[T]) Update(values []T) (bool, error) {
	if len(values) == 0 {
		return false, nil
	}
	if len(values) != 1 {
		return false, fmt.Errorf("%w: LastValue accepts one value per step", ErrInvalidUpdate)
	}
	c.value, c.present = values[0], true
	return true, nil
}

func (c *LastValue[T]) Get() (T, error) {
	if !c.present {
		var zero T
		return zero, ErrEmpty
	}
	return c.value, nil
}

func (c *LastValue[T]) Available() bool { return c.present }
func (c *LastValue[T]) Checkpoint() Snapshot[T] {
	return Snapshot[T]{Value: c.value, Present: c.present}
}

// Copy returns an independent channel envelope. T itself is shallow-copied,
// matching the upstream channel copy contract.
func (c *LastValue[T]) Copy() *LastValue[T] {
	return NewLastValue(c.Checkpoint())
}

// AnyValue stores the last update and clears on an empty super-step.
type AnyValue[T any] struct{ LastValue[T] }

func NewAnyValue[T any](snapshot Snapshot[T]) *AnyValue[T] {
	return &AnyValue[T]{LastValue: *NewLastValue(snapshot)}
}

func (c *AnyValue[T]) Update(values []T) (bool, error) {
	if len(values) == 0 {
		changed := c.present
		var zero T
		c.value, c.present = zero, false
		return changed, nil
	}
	c.value, c.present = values[len(values)-1], true
	return true, nil
}

// EphemeralValue stores the previous super-step's value and clears it when a
// step supplies no updates. Guard rejects concurrent writes when true.
type EphemeralValue[T any] struct {
	value   T
	present bool
	guard   bool
}

func NewEphemeralValue[T any](snapshot Snapshot[T], guard bool) *EphemeralValue[T] {
	return &EphemeralValue[T]{value: snapshot.Value, present: snapshot.Present, guard: guard}
}

func (c *EphemeralValue[T]) Update(values []T) (bool, error) {
	if len(values) == 0 {
		changed := c.present
		var zero T
		c.value, c.present = zero, false
		return changed, nil
	}
	if c.guard && len(values) != 1 {
		return false, fmt.Errorf("%w: guarded EphemeralValue accepts one value per step", ErrInvalidUpdate)
	}
	c.value, c.present = values[len(values)-1], true
	return true, nil
}

func (c *EphemeralValue[T]) Get() (T, error) {
	if !c.present {
		var zero T
		return zero, ErrEmpty
	}
	return c.value, nil
}

func (c *EphemeralValue[T]) Available() bool { return c.present }
func (c *EphemeralValue[T]) Checkpoint() Snapshot[T] {
	return Snapshot[T]{Value: c.value, Present: c.present}
}

// UntrackedValue stores the latest update but is always absent from durable
// checkpoints.
type UntrackedValue[T any] struct {
	value   T
	present bool
	guard   bool
}

func NewUntrackedValue[T any](guard bool) *UntrackedValue[T] {
	return &UntrackedValue[T]{guard: guard}
}

func (c *UntrackedValue[T]) Update(values []T) (bool, error) {
	if len(values) == 0 {
		return false, nil
	}
	if c.guard && len(values) != 1 {
		return false, fmt.Errorf("%w: guarded UntrackedValue accepts one value per step", ErrInvalidUpdate)
	}
	c.value, c.present = values[len(values)-1], true
	return true, nil
}

func (c *UntrackedValue[T]) Get() (T, error) {
	if !c.present {
		var zero T
		return zero, ErrEmpty
	}
	return c.value, nil
}

func (c *UntrackedValue[T]) Available() bool         { return c.present }
func (c *UntrackedValue[T]) Checkpoint() Snapshot[T] { return Snapshot[T]{} }
