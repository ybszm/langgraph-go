package graph

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

// NodeTimeoutKind identifies the expired component of a node attempt policy.
type NodeTimeoutKind string

const (
	NodeTimeoutRun  NodeTimeoutKind = "run"
	NodeTimeoutIdle NodeTimeoutKind = "idle"
)

// ErrNodeTimeout classifies cooperative node-attempt timeouts.
var ErrNodeTimeout = errors.New("node timeout")

// NodeTimeoutError reports which node-attempt deadline expired.
type NodeTimeoutError struct {
	Kind     NodeTimeoutKind
	Duration time.Duration
}

func (e *NodeTimeoutError) Error() string {
	return fmt.Sprintf("%v: %s timeout after %s", ErrNodeTimeout, e.Kind, e.Duration)
}

func (e *NodeTimeoutError) Unwrap() error { return ErrNodeTimeout }

// NodeTimeoutPolicy configures per-attempt wall-clock and idle deadlines.
// Nodes must cooperate with context cancellation; Go cannot safely terminate
// a goroutine that ignores its context.
type NodeTimeoutPolicy struct {
	RunTimeout  time.Duration
	IdleTimeout time.Duration
}

// WithNodeTimeout assigns one timeout policy independently to every retry
// attempt for a node.
func WithNodeTimeout(policy NodeTimeoutPolicy) NodeOption {
	return func(options *nodeOptions) error {
		if policy.RunTimeout < 0 || policy.IdleTimeout < 0 {
			return fmt.Errorf("%w: node timeouts cannot be negative", ErrInvalidGraph)
		}
		options.timeoutPolicy = &policy
		return nil
	}
}

type nodeTimeoutRuntime struct {
	heartbeat chan struct{}
}

func withNodeTimeoutPolicy(
	parent context.Context,
	policy NodeTimeoutPolicy,
) (context.Context, func(), func()) {
	if policy.RunTimeout == 0 && policy.IdleTimeout == 0 {
		return parent, func() {}, func() {}
	}
	ctx, cancel := context.WithCancelCause(parent)
	runtime := &nodeTimeoutRuntime{heartbeat: make(chan struct{}, 1)}
	heartbeat := func() {
		select {
		case runtime.heartbeat <- struct{}{}:
		default:
		}
	}
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		var runTimer, idleTimer *time.Timer
		var runC, idleC <-chan time.Time
		if policy.RunTimeout > 0 {
			runTimer = time.NewTimer(policy.RunTimeout)
			runC = runTimer.C
			defer runTimer.Stop()
		}
		if policy.IdleTimeout > 0 {
			idleTimer = time.NewTimer(policy.IdleTimeout)
			idleC = idleTimer.C
			defer idleTimer.Stop()
		}
		for {
			select {
			case <-runC:
				cancel(&NodeTimeoutError{Kind: NodeTimeoutRun, Duration: policy.RunTimeout})
				return
			case <-idleC:
				cancel(&NodeTimeoutError{Kind: NodeTimeoutIdle, Duration: policy.IdleTimeout})
				return
			case <-runtime.heartbeat:
				if idleTimer != nil {
					if !idleTimer.Stop() {
						select {
						case <-idleTimer.C:
						default:
						}
					}
					idleTimer.Reset(policy.IdleTimeout)
					idleC = idleTimer.C
				}
			case <-parent.Done():
				cancel(context.Cause(parent))
				return
			case <-stop:
				return
			}
		}
	}()
	var once sync.Once
	finish := func() {
		once.Do(func() {
			close(stop)
			<-done
			cancel(nil)
		})
	}
	return ctx, heartbeat, finish
}

func nodeTimeoutCause(ctx context.Context) *NodeTimeoutError {
	var timeout *NodeTimeoutError
	if errors.As(context.Cause(ctx), &timeout) {
		return timeout
	}
	return nil
}

func resolveNodeTimeoutPolicies[S, D any](
	explicit map[NodeID]NodeTimeoutPolicy,
	nodes map[NodeID]Node[S, D],
	defaultPolicy *NodeTimeoutPolicy,
) map[NodeID]NodeTimeoutPolicy {
	result := make(map[NodeID]NodeTimeoutPolicy, len(nodes))
	for node := range nodes {
		policy, exists := explicit[node]
		if exists {
			result[node] = policy
		} else if defaultPolicy != nil {
			result[node] = *defaultPolicy
		}
	}
	return result
}
