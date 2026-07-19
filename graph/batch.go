package graph

import (
	"context"
	"fmt"
	"runtime/debug"
	"sync"

	"golang.org/x/sync/errgroup"
)

// BatchItem is one input and its independent per-run configuration.
type BatchItem[I any] struct {
	Input  I
	Config RunConfig
}

// BatchResult preserves one item's output or execution error at the same
// index as its BatchItem.
type BatchResult[O any] struct {
	Output O
	Err    error
}

// BatchCompletion is one completion-order result together with its stable
// input index.
type BatchCompletion[O any] struct {
	Index  int
	Output O
	Err    error
}

// BatchOptions controls concurrency across graph invocations. Node-level
// concurrency remains controlled by each item's RunConfig.MaxConcurrency.
type BatchOptions struct {
	// MaxConcurrency limits active item invocations. Zero uses len(items).
	MaxConcurrency int
}

// Batch invokes independent inputs concurrently and returns results in input
// order. One item failure does not cancel peers.
func (g *CompiledGraph[S, D]) Batch(
	ctx context.Context,
	items []BatchItem[S],
	options BatchOptions,
) ([]BatchResult[S], error) {
	return executeBatch(ctx, items, options, g.Invoke)
}

// Batch invokes the schema-aware graph for every public input and preserves
// result order and per-item adapter errors.
func (g *CompiledSchemaGraph[I, S, D, O]) Batch(
	ctx context.Context,
	items []BatchItem[I],
	options BatchOptions,
) ([]BatchResult[O], error) {
	return executeBatch(ctx, items, options, g.Invoke)
}

// BatchAsCompleted invokes independent inputs concurrently and yields each
// result as it finishes. Consumers must cancel ctx if they stop receiving.
func (g *CompiledGraph[S, D]) BatchAsCompleted(
	ctx context.Context,
	items []BatchItem[S],
	options BatchOptions,
) (<-chan BatchCompletion[S], error) {
	return executeBatchAsCompleted(ctx, items, options, g.Invoke)
}

// BatchAsCompleted is the schema-aware completion-order iterator.
func (g *CompiledSchemaGraph[I, S, D, O]) BatchAsCompleted(
	ctx context.Context,
	items []BatchItem[I],
	options BatchOptions,
) (<-chan BatchCompletion[O], error) {
	return executeBatchAsCompleted(ctx, items, options, g.Invoke)
}

func executeBatch[I, O any](
	ctx context.Context,
	items []BatchItem[I],
	options BatchOptions,
	invoke func(context.Context, I, RunConfig) (O, error),
) ([]BatchResult[O], error) {
	if ctx == nil {
		return nil, fmt.Errorf("%w: context is nil", ErrInvalidRunConfig)
	}
	if options.MaxConcurrency < 0 {
		return nil, fmt.Errorf("%w: batch max concurrency cannot be negative", ErrInvalidRunConfig)
	}
	results := make([]BatchResult[O], len(items))
	if len(items) == 0 {
		return results, nil
	}
	limit := options.MaxConcurrency
	if limit == 0 || limit > len(items) {
		limit = len(items)
	}
	group := errgroup.Group{}
	group.SetLimit(limit)
	for index, item := range items {
		index, item := index, item
		group.Go(func() error {
			defer func() {
				if value := recover(); value != nil {
					results[index].Err = &BatchPanicError{Index: index, Value: value, Stack: debug.Stack()}
				}
			}()
			results[index].Output, results[index].Err = invoke(ctx, item.Input, item.Config)
			return nil
		})
	}
	_ = group.Wait()
	return results, nil
}

func executeBatchAsCompleted[I, O any](
	ctx context.Context,
	items []BatchItem[I],
	options BatchOptions,
	invoke func(context.Context, I, RunConfig) (O, error),
) (<-chan BatchCompletion[O], error) {
	if ctx == nil {
		return nil, fmt.Errorf("%w: context is nil", ErrInvalidRunConfig)
	}
	if options.MaxConcurrency < 0 {
		return nil, fmt.Errorf("%w: batch max concurrency cannot be negative", ErrInvalidRunConfig)
	}
	limit := options.MaxConcurrency
	if limit == 0 || limit > len(items) {
		limit = len(items)
	}
	if limit == 0 {
		closed := make(chan BatchCompletion[O])
		close(closed)
		return closed, nil
	}
	completed := make(chan BatchCompletion[O], limit)
	jobs := make(chan int)
	var workers sync.WaitGroup
	workers.Add(limit)
	for range limit {
		go func() {
			defer workers.Done()
			for index := range jobs {
				item := items[index]
				result := invokeBatchItem(ctx, index, item, invoke)
				select {
				case completed <- result:
				case <-ctx.Done():
					return
				}
			}
		}()
	}
	go func() {
		defer close(completed)
		for index := range items {
			select {
			case jobs <- index:
			case <-ctx.Done():
				close(jobs)
				workers.Wait()
				return
			}
		}
		close(jobs)
		workers.Wait()
	}()
	return completed, nil
}

func invokeBatchItem[I, O any](
	ctx context.Context,
	index int,
	item BatchItem[I],
	invoke func(context.Context, I, RunConfig) (O, error),
) (result BatchCompletion[O]) {
	result.Index = index
	defer func() {
		if value := recover(); value != nil {
			result.Err = &BatchPanicError{Index: index, Value: value, Stack: debug.Stack()}
		}
	}()
	result.Output, result.Err = invoke(ctx, item.Input, item.Config)
	return result
}
