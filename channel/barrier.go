package channel

import "fmt"

// NamedBarrier becomes available after every configured comparable name has
// been observed. Consume resets a completed barrier.
type NamedBarrier[T comparable] struct {
	names map[T]struct{}
	seen  map[T]struct{}
	order []T
}

func NewNamedBarrier[T comparable](names []T, checkpoint []T) (*NamedBarrier[T], error) {
	barrier := &NamedBarrier[T]{names: make(map[T]struct{}), seen: make(map[T]struct{})}
	for _, name := range names {
		if _, exists := barrier.names[name]; !exists {
			barrier.order = append(barrier.order, name)
		}
		barrier.names[name] = struct{}{}
	}
	if len(barrier.names) == 0 {
		return nil, fmt.Errorf("%w: barrier names are empty", ErrInvalidUpdate)
	}
	for _, value := range checkpoint {
		if _, exists := barrier.names[value]; !exists {
			return nil, fmt.Errorf("%w: checkpoint value is not a barrier name", ErrInvalidUpdate)
		}
		barrier.seen[value] = struct{}{}
	}
	return barrier, nil
}

func (c *NamedBarrier[T]) Update(values []T) (bool, error) {
	updated := false
	for _, value := range values {
		if _, exists := c.names[value]; !exists {
			return false, fmt.Errorf("%w: value is not a barrier name", ErrInvalidUpdate)
		}
		if _, exists := c.seen[value]; !exists {
			c.seen[value] = struct{}{}
			updated = true
		}
	}
	return updated, nil
}

func (c *NamedBarrier[T]) Available() bool { return len(c.seen) == len(c.names) }
func (c *NamedBarrier[T]) Get() (struct{}, error) {
	if !c.Available() {
		return struct{}{}, ErrEmpty
	}
	return struct{}{}, nil
}
func (c *NamedBarrier[T]) Consume() bool {
	if !c.Available() {
		return false
	}
	clear(c.seen)
	return true
}
func (c *NamedBarrier[T]) Checkpoint() []T {
	result := make([]T, 0, len(c.seen))
	for _, name := range c.order {
		if _, exists := c.seen[name]; exists {
			result = append(result, name)
		}
	}
	return result
}
