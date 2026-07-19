package channel

import "fmt"

// AggregateUpdate represents either a regular value or an explicit overwrite.
type AggregateUpdate[T any] struct {
	Value       T
	IsOverwrite bool
}

func AggregateValue[T any](value T) AggregateUpdate[T] {
	return AggregateUpdate[T]{Value: value}
}

func AggregateOverwrite[T any](value T) AggregateUpdate[T] {
	return AggregateUpdate[T]{Value: value, IsOverwrite: true}
}

// BinaryOperatorAggregate folds values in stable update order. Go has a
// usable zero value for every T, so a fresh aggregate begins with zero T.
type BinaryOperatorAggregate[T any] struct {
	value    T
	operator func(T, T) T
}

func NewBinaryOperatorAggregate[T any](operator func(T, T) T) *BinaryOperatorAggregate[T] {
	if operator == nil {
		panic("channel: nil binary operator")
	}
	return &BinaryOperatorAggregate[T]{operator: operator}
}

func RestoreBinaryOperatorAggregate[T any](snapshot Snapshot[T], operator func(T, T) T) *BinaryOperatorAggregate[T] {
	channel := NewBinaryOperatorAggregate(operator)
	if snapshot.Present {
		channel.value = snapshot.Value
	}
	return channel
}

func (c *BinaryOperatorAggregate[T]) Update(values []AggregateUpdate[T]) (bool, error) {
	if len(values) == 0 {
		return false, nil
	}
	overwriteSeen := false
	for _, update := range values {
		if update.IsOverwrite {
			if overwriteSeen {
				return false, fmt.Errorf("%w: aggregate accepts one overwrite per step", ErrInvalidUpdate)
			}
			c.value = update.Value
			overwriteSeen = true
			continue
		}
		if !overwriteSeen {
			c.value = c.operator(c.value, update.Value)
		}
	}
	return true, nil
}

func (c *BinaryOperatorAggregate[T]) Get() (T, error) { return c.value, nil }
func (c *BinaryOperatorAggregate[T]) Available() bool { return true }
func (c *BinaryOperatorAggregate[T]) Checkpoint() Snapshot[T] {
	return Snapshot[T]{Value: c.value, Present: true}
}
