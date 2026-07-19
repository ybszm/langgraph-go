package distributed

import (
	"context"
	"errors"
	"fmt"
	"time"

	"golang.org/x/sync/errgroup"
)

// Handler executes one leased task. Returning an error requests delayed nack.
type Handler[P any] func(context.Context, Task[P]) error

// WorkerOptions controls bounded claiming and lease maintenance.
type WorkerOptions struct {
	WorkerID          string
	BatchSize         int
	MaxConcurrency    int
	LeaseDuration     time.Duration
	HeartbeatInterval time.Duration
	PollInterval      time.Duration
	NackDelay         time.Duration
}

// WorkerError adds task and queue operation context while preserving causes.
type WorkerError struct {
	Operation string
	TaskID    string
	Err       error
}

func (e *WorkerError) Error() string {
	if e.TaskID == "" {
		return fmt.Sprintf("distributed worker %s: %v", e.Operation, e.Err)
	}
	return fmt.Sprintf("distributed worker %s task %s: %v", e.Operation, e.TaskID, e.Err)
}

// Unwrap returns the queue or handler infrastructure cause.
func (e *WorkerError) Unwrap() error { return e.Err }

// Worker is a managed claim/heartbeat/ack-nack execution loop.
type Worker[P any] struct {
	queue   Queue[P]
	handler Handler[P]
	options WorkerOptions
}

// NewWorker validates and constructs a bounded worker.
func NewWorker[P any](queue Queue[P], handler Handler[P], options WorkerOptions) (*Worker[P], error) {
	if queue == nil || handler == nil {
		return nil, fmt.Errorf("%w: queue and handler are required", ErrInvalidRequest)
	}
	if options.BatchSize == 0 {
		options.BatchSize = 1
	}
	if options.MaxConcurrency == 0 {
		options.MaxConcurrency = options.BatchSize
	}
	if options.PollInterval == 0 {
		options.PollInterval = 100 * time.Millisecond
	}
	if !validID(options.WorkerID) || options.BatchSize < 1 || options.MaxConcurrency < 1 ||
		options.LeaseDuration <= 0 || options.HeartbeatInterval <= 0 ||
		options.HeartbeatInterval >= options.LeaseDuration || options.PollInterval <= 0 || options.NackDelay < 0 {
		return nil, fmt.Errorf("%w: invalid worker identity, limits, or durations", ErrInvalidRequest)
	}
	return &Worker[P]{queue: queue, handler: handler, options: options}, nil
}

// Run claims and executes tasks until ctx cancellation or queue infrastructure
// failure. Every spawned task and heartbeat is joined before Run returns.
func (w *Worker[P]) Run(ctx context.Context) error {
	if err := validContext(ctx); err != nil {
		return err
	}
	group, groupContext := errgroup.WithContext(ctx)
	slots := make(chan struct{}, w.options.MaxConcurrency)
	group.Go(func() error { return w.claimLoop(groupContext, group, slots) })
	return group.Wait()
}

func (w *Worker[P]) claimLoop(ctx context.Context, group *errgroup.Group, slots chan struct{}) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		available := cap(slots) - len(slots)
		if available == 0 {
			if !waitPoll(ctx, w.options.PollInterval) {
				return ctx.Err()
			}
			continue
		}
		limit := w.options.BatchSize
		if limit > available {
			limit = available
		}
		tasks, err := w.queue.Claim(ctx, w.options.WorkerID, limit, w.options.LeaseDuration)
		if err != nil {
			return &WorkerError{Operation: "claim", Err: err}
		}
		for _, claimed := range tasks {
			task := claimed
			slots <- struct{}{}
			group.Go(func() error {
				defer func() { <-slots }()
				return w.process(ctx, task)
			})
		}
		if len(tasks) == 0 && !waitPoll(ctx, w.options.PollInterval) {
			return ctx.Err()
		}
	}
}

func (w *Worker[P]) process(workerContext context.Context, task Task[P]) error {
	taskContext, cancel := context.WithCancel(workerContext)
	heartbeatDone := make(chan error, 1)
	go func() {
		err := w.heartbeat(taskContext, task.Lease)
		if err != nil {
			cancel()
		}
		heartbeatDone <- err
	}()
	handlerErr := callHandler(taskContext, w.handler, task)
	cancel()
	heartbeatErr := <-heartbeatDone
	if errors.Is(handlerErr, errLeaseAcknowledged) {
		return nil
	}
	if heartbeatErr != nil {
		return &WorkerError{Operation: "heartbeat", TaskID: task.ID, Err: heartbeatErr}
	}
	if workerContext.Err() != nil {
		return nil
	}
	if handlerErr != nil {
		if err := w.queue.Nack(workerContext, task.Lease, w.options.NackDelay); err != nil {
			return &WorkerError{Operation: "nack", TaskID: task.ID, Err: err}
		}
		return nil
	}
	if err := w.queue.Ack(workerContext, task.Lease); err != nil {
		return &WorkerError{Operation: "ack", TaskID: task.ID, Err: err}
	}
	return nil
}

func (w *Worker[P]) heartbeat(ctx context.Context, lease Lease) error {
	ticker := time.NewTicker(w.options.HeartbeatInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			if err := w.queue.Heartbeat(ctx, lease, w.options.LeaseDuration); err != nil {
				if ctx.Err() != nil {
					return nil
				}
				return err
			}
		}
	}
}

func callHandler[P any](ctx context.Context, handler Handler[P], task Task[P]) (err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("handler panic: %v", recovered)
		}
	}()
	return handler(ctx, task)
}

func waitPoll(ctx context.Context, duration time.Duration) bool {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-timer.C:
		return true
	case <-ctx.Done():
		return false
	}
}
