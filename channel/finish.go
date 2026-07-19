package channel

// FinishSnapshot is the durable state of a finish-gated value.
type FinishSnapshot[T any] struct {
	Value    T
	Present  bool
	Finished bool
}

// LastValueAfterFinish exposes its latest update only after Finish. Consuming
// a finished value clears it.
type LastValueAfterFinish[T any] struct {
	value    T
	present  bool
	finished bool
}

func NewLastValueAfterFinish[T any](snapshot FinishSnapshot[T]) *LastValueAfterFinish[T] {
	return &LastValueAfterFinish[T]{
		value: snapshot.Value, present: snapshot.Present, finished: snapshot.Finished,
	}
}

func (c *LastValueAfterFinish[T]) Update(values []T) (bool, error) {
	if len(values) == 0 {
		return false, nil
	}
	c.value, c.present, c.finished = values[len(values)-1], true, false
	return true, nil
}
func (c *LastValueAfterFinish[T]) Finish() bool {
	if !c.present || c.finished {
		return false
	}
	c.finished = true
	return true
}
func (c *LastValueAfterFinish[T]) Consume() bool {
	if !c.finished {
		return false
	}
	var zero T
	c.value, c.present, c.finished = zero, false, false
	return true
}
func (c *LastValueAfterFinish[T]) Available() bool { return c.present && c.finished }
func (c *LastValueAfterFinish[T]) Get() (T, error) {
	if !c.Available() {
		var zero T
		return zero, ErrEmpty
	}
	return c.value, nil
}
func (c *LastValueAfterFinish[T]) Checkpoint() FinishSnapshot[T] {
	return FinishSnapshot[T]{Value: c.value, Present: c.present, Finished: c.finished}
}

// BarrierFinishSnapshot is the durable state of a finish-gated barrier.
type BarrierFinishSnapshot[T comparable] struct {
	Seen     []T
	Finished bool
}

// NamedBarrierAfterFinish requires all names and an explicit Finish.
type NamedBarrierAfterFinish[T comparable] struct {
	barrier  *NamedBarrier[T]
	finished bool
}

func NewNamedBarrierAfterFinish[T comparable](names []T, snapshot BarrierFinishSnapshot[T]) (*NamedBarrierAfterFinish[T], error) {
	barrier, err := NewNamedBarrier(names, snapshot.Seen)
	if err != nil {
		return nil, err
	}
	return &NamedBarrierAfterFinish[T]{barrier: barrier, finished: snapshot.Finished}, nil
}
func (c *NamedBarrierAfterFinish[T]) Update(values []T) (bool, error) {
	return c.barrier.Update(values)
}
func (c *NamedBarrierAfterFinish[T]) Finish() bool {
	if c.finished || !c.barrier.Available() {
		return false
	}
	c.finished = true
	return true
}
func (c *NamedBarrierAfterFinish[T]) Available() bool { return c.finished && c.barrier.Available() }
func (c *NamedBarrierAfterFinish[T]) Get() (struct{}, error) {
	if !c.Available() {
		return struct{}{}, ErrEmpty
	}
	return struct{}{}, nil
}
func (c *NamedBarrierAfterFinish[T]) Consume() bool {
	if !c.Available() {
		return false
	}
	c.finished = false
	return c.barrier.Consume()
}
func (c *NamedBarrierAfterFinish[T]) Checkpoint() BarrierFinishSnapshot[T] {
	return BarrierFinishSnapshot[T]{Seen: c.barrier.Checkpoint(), Finished: c.finished}
}
