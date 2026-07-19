package channel

// TopicUpdate is one scalar or batch publication.
type TopicUpdate[T any] struct{ Values []T }

func TopicValue[T any](value T) TopicUpdate[T] { return TopicUpdate[T]{Values: []T{value}} }
func TopicValues[T any](values ...T) TopicUpdate[T] {
	return TopicUpdate[T]{Values: append([]T(nil), values...)}
}

// Topic is a typed pub/sub collection. Non-accumulating topics clear their
// previous step before accepting the current publications.
type Topic[T any] struct {
	values     []T
	accumulate bool
}

func NewTopic[T any](checkpoint []T, accumulate bool) *Topic[T] {
	return &Topic[T]{values: append([]T(nil), checkpoint...), accumulate: accumulate}
}

func (c *Topic[T]) Update(updates []TopicUpdate[T]) (bool, error) {
	updated := false
	if !c.accumulate {
		updated = len(c.values) > 0
		c.values = nil
	}
	for _, update := range updates {
		if len(update.Values) > 0 {
			updated = true
			c.values = append(c.values, update.Values...)
		}
	}
	return updated, nil
}

func (c *Topic[T]) Get() ([]T, error) {
	if len(c.values) == 0 {
		return nil, ErrEmpty
	}
	return append([]T(nil), c.values...), nil
}

func (c *Topic[T]) Available() bool { return len(c.values) > 0 }
func (c *Topic[T]) Checkpoint() []T { return append([]T(nil), c.values...) }
