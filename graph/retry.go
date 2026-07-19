package graph

import (
	"context"
	"errors"
	"fmt"
	"math"
	"math/rand/v2"
	"sort"
	"time"
)

// RetryPolicy controls retry attempts for one node. MaxAttempts includes the
// first attempt. A nil RetryOn uses the default predicate, and a nil Jitter
// adds up to one second of random jitter when EnableJitter is true.
type RetryPolicy struct {
	InitialInterval time.Duration
	BackoffFactor   float64
	MaxInterval     time.Duration
	MaxAttempts     int
	EnableJitter    bool
	Jitter          func(time.Duration) time.Duration
	RetryOn         func(error) bool
}

// NodeOption configures one node registration.
type NodeOption func(*nodeOptions) error

type nodeOptions struct {
	retryPolicies    []RetryPolicy
	cachePolicy      *nodeCachePolicy
	timeoutPolicy    *NodeTimeoutPolicy
	errorHandler     any
	channelReads     []string
	channelTriggers  []string
	dynamicInterrupt bool
}

// WithDynamicInterrupts declares that a node may call AwaitResume. The
// compiler can then require durable persistence before any node executes.
func WithDynamicInterrupts() NodeOption {
	return func(options *nodeOptions) error {
		if options.dynamicInterrupt {
			return fmt.Errorf("%w: duplicate dynamic interrupt declaration", ErrInvalidGraph)
		}
		options.dynamicInterrupt = true
		return nil
	}
}

// WithChannelReads declares additional fixed checkpoint channels observed by
// a node. The canonical state channel is always included automatically.
func WithChannelReads(channels ...string) NodeOption {
	return func(options *nodeOptions) error {
		return appendChannelNames(&options.channelReads, "read", channels)
	}
}

// WithChannelTriggers declares fixed checkpoint channels whose fresh versions
// can schedule a node. Trigger channels are also included in its durable read
// set. Existing edge and router routing still determines candidate nodes.
func WithChannelTriggers(channels ...string) NodeOption {
	return func(options *nodeOptions) error {
		if err := appendChannelNames(&options.channelTriggers, "trigger", channels); err != nil {
			return err
		}
		reads := make(map[string]struct{}, len(options.channelReads))
		for _, channel := range options.channelReads {
			reads[channel] = struct{}{}
		}
		for _, channel := range channels {
			if _, exists := reads[channel]; exists {
				continue
			}
			options.channelReads = append(options.channelReads, channel)
		}
		sort.Strings(options.channelReads)
		return nil
	}
}

func appendChannelNames(target *[]string, kind string, channels []string) error {
	if len(channels) == 0 {
		return fmt.Errorf("%w: channel %s list is empty", ErrInvalidGraph, kind)
	}
	seen := make(map[string]struct{}, len(*target)+len(channels))
	for _, channel := range *target {
		seen[channel] = struct{}{}
	}
	for _, channel := range channels {
		if isReservedCheckpointChannel(channel) {
			return fmt.Errorf("%w: %w: invalid %s channel %q", ErrInvalidGraph, ErrCheckpointChannel, kind, channel)
		}
		if _, duplicate := seen[channel]; duplicate {
			return fmt.Errorf("%w: duplicate %s channel %q", ErrInvalidGraph, kind, channel)
		}
		seen[channel] = struct{}{}
		*target = append(*target, channel)
	}
	sort.Strings(*target)
	return nil
}

// WithRetryPolicies assigns ordered retry policies to a node. The first
// policy whose RetryOn predicate accepts the error is used.
func WithRetryPolicies(policies ...RetryPolicy) NodeOption {
	return func(options *nodeOptions) error {
		if len(policies) == 0 {
			return fmt.Errorf("%w: retry policy list is empty", ErrInvalidGraph)
		}
		normalized := make([]RetryPolicy, len(policies))
		for index, policy := range policies {
			value, err := normalizeRetryPolicy(policy)
			if err != nil {
				return fmt.Errorf("%w: retry policy %d: %w", ErrInvalidGraph, index, err)
			}
			normalized[index] = value
		}
		options.retryPolicies = normalized
		return nil
	}
}

func normalizeRetryPolicy(policy RetryPolicy) (RetryPolicy, error) {
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
		return RetryPolicy{}, fmt.Errorf("retry intervals must be positive")
	}
	if policy.BackoffFactor < 1 || math.IsNaN(policy.BackoffFactor) || math.IsInf(policy.BackoffFactor, 0) {
		return RetryPolicy{}, fmt.Errorf("backoff factor must be finite and at least 1")
	}
	if policy.MaxAttempts < 1 {
		return RetryPolicy{}, fmt.Errorf("max attempts must be at least 1")
	}
	if policy.MaxInterval < policy.InitialInterval {
		return RetryPolicy{}, fmt.Errorf("max interval cannot be smaller than initial interval")
	}
	return policy, nil
}

func defaultRetryOn(err error) bool {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) ||
		errors.Is(err, ErrGraphInterrupt) || errors.Is(err, ErrCheckpointerRequired) {
		return false
	}
	var panicErr *NodePanicError
	return !errors.As(err, &panicErr)
}

func matchingRetryPolicy(policies []RetryPolicy, err error) *RetryPolicy {
	for index := range policies {
		predicate := policies[index].RetryOn
		if predicate == nil {
			predicate = defaultRetryOn
		}
		if predicate(err) {
			return &policies[index]
		}
	}
	return nil
}

func retryDelay(policy RetryPolicy, failedAttempts int) time.Duration {
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
