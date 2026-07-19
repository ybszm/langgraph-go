package functional

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"math/rand/v2"
	"time"

	cachepkg "github.com/wahanbo/langgraph-go/cache"
	"github.com/wahanbo/langgraph-go/checkpoint"
	"github.com/wahanbo/langgraph-go/graph"
)

// TaskCachePolicy configures serialized task result caching.
type TaskCachePolicy[I, O any] struct {
	Store     cachepkg.Store
	Namespace string
	Key       func(I) (string, error)
	Codec     checkpoint.Codec[O]
	TTL       time.Duration
}

// TaskPersistencePolicy enables checkpoint pending-write recovery for a task.
type TaskPersistencePolicy[I, O any] struct {
	Codec checkpoint.Codec[O]
	Key   func(I) (string, error)
}

// TaskOptions configures retry and cache behavior for one Task.
type TaskOptions[I, O any] struct {
	// RetryPolicies are checked in order; the first accepting predicate wins.
	RetryPolicies []graph.RetryPolicy
	// Cache optionally persists serialized successful task outputs.
	Cache *TaskCachePolicy[I, O]
	// Persistence optionally records successful outputs on durable attempts.
	Persistence *TaskPersistencePolicy[I, O]
	// Timeout applies independently to each retry attempt.
	Timeout TimeoutPolicy
}

type cacheFlight[O any] struct {
	done  chan struct{}
	value O
	err   error
}

func normalizeTaskOptions[I, O any](options TaskOptions[I, O]) (TaskOptions[I, O], error) {
	if err := validateTimeoutPolicy(options.Timeout); err != nil {
		return options, err
	}
	if len(options.RetryPolicies) > 0 {
		normalized := make([]graph.RetryPolicy, len(options.RetryPolicies))
		for index, policy := range options.RetryPolicies {
			value, err := normalizeRetryPolicy(policy)
			if err != nil {
				return options, fmt.Errorf("retry policy %d: %w", index, err)
			}
			normalized[index] = value
		}
		options.RetryPolicies = normalized
	}
	if options.Cache != nil {
		if options.Cache.Store == nil || options.Cache.Codec == nil || options.Cache.Namespace == "" {
			return options, fmt.Errorf("cache requires store, codec, and namespace")
		}
		if options.Cache.TTL < 0 {
			return options, fmt.Errorf("cache TTL cannot be negative")
		}
		if options.Cache.Key == nil {
			options.Cache.Key = func(input I) (string, error) {
				data, err := json.Marshal(input)
				if err != nil {
					return "", err
				}
				digest := sha256.Sum256(data)
				return hex.EncodeToString(digest[:]), nil
			}
		}
	}
	if options.Persistence != nil {
		if options.Persistence.Codec == nil {
			return options, fmt.Errorf("persistence requires result codec")
		}
		if options.Persistence.Key == nil {
			options.Persistence.Key = func(input I) (string, error) {
				data, err := json.Marshal(input)
				if err != nil {
					return "", err
				}
				digest := sha256.Sum256(data)
				return hex.EncodeToString(digest[:]), nil
			}
		}
	}
	return options, nil
}

func normalizeRetryPolicy(policy graph.RetryPolicy) (graph.RetryPolicy, error) {
	if policy.InitialInterval == 0 {
		policy.InitialInterval = 500 * time.Millisecond
	}
	if policy.BackoffFactor == 0 {
		policy.BackoffFactor = 2
	}
	if policy.MaxInterval == 0 {
		policy.MaxInterval = 128 * time.Second
	}
	if policy.MaxAttempts == 0 {
		policy.MaxAttempts = 3
	}
	if policy.InitialInterval < 0 || policy.MaxInterval < 0 {
		return policy, fmt.Errorf("retry intervals must be positive")
	}
	if policy.BackoffFactor < 1 || math.IsNaN(policy.BackoffFactor) || math.IsInf(policy.BackoffFactor, 0) {
		return policy, fmt.Errorf("backoff factor must be finite and at least 1")
	}
	if policy.MaxAttempts < 1 {
		return policy, fmt.Errorf("max attempts must be at least 1")
	}
	if policy.MaxInterval < policy.InitialInterval {
		return policy, fmt.Errorf("max interval cannot be smaller than initial interval")
	}
	return policy, nil
}

func (t *Task[I, O]) execute(ctx context.Context, input I) (O, error) {
	if t.options.Cache == nil {
		return t.executeWithRetry(ctx, input)
	}
	keyValue, err := t.options.Cache.Key(input)
	if err != nil {
		return zero[O](), fmt.Errorf("functional task cache key: %w", err)
	}
	if keyValue == "" {
		return zero[O](), fmt.Errorf("functional task cache key is empty")
	}
	fullKey := cachepkg.Key{Namespace: t.options.Cache.Namespace + ":" + t.name, Key: keyValue}
	flightKey := fullKey.Namespace + "\x00" + fullKey.Key

	t.cacheMu.Lock()
	if flight := t.inflight[flightKey]; flight != nil {
		t.cacheMu.Unlock()
		select {
		case <-flight.done:
			return flight.value, flight.err
		case <-ctx.Done():
			return zero[O](), ctx.Err()
		}
	}
	flight := &cacheFlight[O]{done: make(chan struct{})}
	t.inflight[flightKey] = flight
	t.cacheMu.Unlock()

	flight.value, flight.err = t.loadOrExecute(ctx, input, fullKey)
	close(flight.done)
	t.cacheMu.Lock()
	delete(t.inflight, flightKey)
	t.cacheMu.Unlock()
	return flight.value, flight.err
}

func (t *Task[I, O]) loadOrExecute(ctx context.Context, input I, key cachepkg.Key) (O, error) {
	values, err := t.options.Cache.Store.Get(ctx, []cachepkg.Key{key})
	if err != nil {
		return zero[O](), fmt.Errorf("functional task cache get: %w", err)
	}
	if data, found := values[key]; found {
		var encoded checkpoint.EncodedValue
		if err := json.Unmarshal(data, &encoded); err != nil {
			return zero[O](), fmt.Errorf("functional task cache envelope: %w", err)
		}
		value, err := t.options.Cache.Codec.Decode(encoded)
		if err != nil {
			return zero[O](), fmt.Errorf("functional task cache decode: %w", err)
		}
		return value, nil
	}
	value, err := t.executeWithRetry(ctx, input)
	if err != nil {
		return value, err
	}
	encoded, err := t.options.Cache.Codec.Encode(value)
	if err != nil {
		return zero[O](), fmt.Errorf("functional task cache encode: %w", err)
	}
	data, err := json.Marshal(encoded)
	if err != nil {
		return zero[O](), fmt.Errorf("functional task cache envelope: %w", err)
	}
	if err := t.options.Cache.Store.Set(ctx, map[cachepkg.Key]cachepkg.Item{
		key: {Data: data, TTL: t.options.Cache.TTL},
	}); err != nil {
		return zero[O](), fmt.Errorf("functional task cache set: %w", err)
	}
	return value, nil
}

func (t *Task[I, O]) executeWithRetry(ctx context.Context, input I) (O, error) {
	for attempt := 1; ; attempt++ {
		attemptCtx, finish := withTimeoutPolicy(ctx, t.options.Timeout)
		value, err := invokeTaskSafely(attemptCtx, input, t.run)
		if timeout := timeoutCause(attemptCtx); timeout != nil {
			err = timeout
		}
		finish()
		if err == nil {
			return value, nil
		}
		policy := matchingRetryPolicy(t.options.RetryPolicies, err)
		if policy == nil || attempt >= policy.MaxAttempts {
			return value, err
		}
		if err := waitRetry(ctx, retryDelay(*policy, attempt)); err != nil {
			return zero[O](), err
		}
	}
}

func matchingRetryPolicy(policies []graph.RetryPolicy, err error) *graph.RetryPolicy {
	for index := range policies {
		predicate := policies[index].RetryOn
		if predicate == nil {
			predicate = func(err error) bool {
				if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
					return false
				}
				var panicErr *TaskPanicError
				return !errors.As(err, &panicErr)
			}
		}
		if predicate(err) {
			return &policies[index]
		}
	}
	return nil
}

func retryDelay(policy graph.RetryPolicy, failedAttempts int) time.Duration {
	factor := math.Pow(policy.BackoffFactor, float64(failedAttempts-1))
	delay := time.Duration(float64(policy.InitialInterval) * factor)
	if delay > policy.MaxInterval || delay < 0 {
		delay = policy.MaxInterval
	}
	if policy.EnableJitter {
		if policy.Jitter != nil {
			delay = policy.Jitter(delay)
		} else {
			delay += time.Duration(rand.Int64N(int64(time.Second) + 1))
		}
	}
	return delay
}

func waitRetry(ctx context.Context, delay time.Duration) error {
	if delay <= 0 {
		return ctx.Err()
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// ClearCache removes every cached result for this task's namespace.
func (t *Task[I, O]) ClearCache(ctx context.Context) error {
	if t == nil || t.options.Cache == nil {
		return nil
	}
	return t.options.Cache.Store.Clear(ctx, []string{t.options.Cache.Namespace + ":" + t.name})
}
