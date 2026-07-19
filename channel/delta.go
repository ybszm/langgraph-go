package channel

import "fmt"

// DeltaReducer folds ordered writes into a channel value.
type DeltaReducer[T, U any] func(current T, updates []U) (T, error)

// DeltaWrite contains either a regular update or an overwrite value.
type DeltaWrite[T, U any] struct {
	Update      U
	Overwrite   T
	IsOverwrite bool
}

func DeltaValue[T, U any](value U) DeltaWrite[T, U] { return DeltaWrite[T, U]{Update: value} }
func DeltaOverwrite[T, U any](value T) DeltaWrite[T, U] {
	return DeltaWrite[T, U]{Overwrite: value, IsOverwrite: true}
}

// DeltaChannel normally checkpoints only its writes. SnapshotFrequency tells
// a saver how often to materialize the current value to bound ancestor walks.
type DeltaChannel[T, U any] struct {
	value             T
	present           bool
	initial           func() T
	reducer           DeltaReducer[T, U]
	snapshotFrequency int
}

func NewDeltaChannel[T, U any](
	initial func() T,
	reducer DeltaReducer[T, U],
	snapshotFrequency int,
) (*DeltaChannel[T, U], error) {
	if reducer == nil {
		return nil, fmt.Errorf("%w: delta reducer is nil", ErrInvalidUpdate)
	}
	if snapshotFrequency <= 0 {
		return nil, fmt.Errorf("%w: snapshot frequency must be positive", ErrInvalidUpdate)
	}
	if initial == nil {
		initial = func() T { var zero T; return zero }
	}
	return &DeltaChannel[T, U]{
		value: initial(), present: true, initial: initial,
		reducer: reducer, snapshotFrequency: snapshotFrequency,
	}, nil
}

func RestoreDeltaChannel[T, U any](
	seed Snapshot[T],
	initial func() T,
	reducer DeltaReducer[T, U],
	snapshotFrequency int,
) (*DeltaChannel[T, U], error) {
	channel, err := NewDeltaChannel(initial, reducer, snapshotFrequency)
	if err != nil {
		return nil, err
	}
	if seed.Present {
		channel.value = seed.Value
		channel.present = true
	}
	return channel, nil
}

func (c *DeltaChannel[T, U]) Update(writes []DeltaWrite[T, U]) (bool, error) {
	if len(writes) == 0 {
		return false, nil
	}
	overwrite := -1
	for index, write := range writes {
		if write.IsOverwrite {
			if overwrite >= 0 {
				return false, fmt.Errorf("%w: delta channel accepts one overwrite per step", ErrInvalidUpdate)
			}
			overwrite = index
		}
	}
	if overwrite >= 0 {
		c.value, c.present = writes[overwrite].Overwrite, true
		return true, nil
	}
	updates := make([]U, len(writes))
	for index, write := range writes {
		updates[index] = write.Update
	}
	next, err := c.reducer(c.value, updates)
	if err != nil {
		return false, err
	}
	c.value, c.present = next, true
	return true, nil
}

// ReplayWrites reconstructs state from ancestor writes. The last overwrite is
// the reset point; only regular writes after it reach the reducer.
func (c *DeltaChannel[T, U]) ReplayWrites(writes []DeltaWrite[T, U]) error {
	if len(writes) == 0 {
		return nil
	}
	start := 0
	base := c.value
	for index, write := range writes {
		if write.IsOverwrite {
			base = write.Overwrite
			start = index + 1
		}
	}
	updates := make([]U, 0, len(writes)-start)
	for _, write := range writes[start:] {
		if !write.IsOverwrite {
			updates = append(updates, write.Update)
		}
	}
	if len(updates) > 0 {
		next, err := c.reducer(base, updates)
		if err != nil {
			return err
		}
		base = next
	}
	c.value, c.present = base, true
	return nil
}

func (c *DeltaChannel[T, U]) Get() (T, error) {
	if !c.present {
		var zero T
		return zero, ErrEmpty
	}
	return c.value, nil
}
func (c *DeltaChannel[T, U]) Available() bool { return c.present }

// Checkpoint is intentionally absent; savers materialize snapshots separately.
func (c *DeltaChannel[T, U]) Checkpoint() Snapshot[T] { return Snapshot[T]{} }
func (c *DeltaChannel[T, U]) ShouldSnapshot(updatesSinceSnapshot int) bool {
	return updatesSinceSnapshot >= c.snapshotFrequency
}
