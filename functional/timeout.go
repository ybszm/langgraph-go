package functional

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

// TimeoutKind identifies which Functional timeout expired.
type TimeoutKind string

const (
	// TimeoutRun is the non-refreshable wall-clock deadline.
	TimeoutRun TimeoutKind = "run"
	// TimeoutIdle is refreshed by Heartbeat progress signals.
	TimeoutIdle TimeoutKind = "idle"
)

// ErrTimeout classifies Functional run and idle timeouts.
var ErrTimeout = errors.New("functional timeout")

// TimeoutError reports the expired policy component.
type TimeoutError struct {
	Kind     TimeoutKind
	Duration time.Duration
}

func (e *TimeoutError) Error() string {
	return fmt.Sprintf("%v: %s timeout after %s", ErrTimeout, e.Kind, e.Duration)
}

func (e *TimeoutError) Unwrap() error { return ErrTimeout }

// TimeoutPolicy configures cooperative wall-clock and idle deadlines.
type TimeoutPolicy struct {
	RunTimeout  time.Duration
	IdleTimeout time.Duration
}

func validateTimeoutPolicy(policy TimeoutPolicy) error {
	if policy.RunTimeout < 0 || policy.IdleTimeout < 0 {
		return fmt.Errorf("timeouts cannot be negative")
	}
	return nil
}

type heartbeatContextKey struct{}

type timeoutRuntime struct {
	heartbeat chan struct{}
}

// Heartbeat reports task/entrypoint progress and refreshes an idle timeout.
// It is a no-op when the current execution has no idle timeout.
func Heartbeat(ctx context.Context) {
	runtime, _ := ctx.Value(heartbeatContextKey{}).(*timeoutRuntime)
	if runtime == nil {
		return
	}
	select {
	case runtime.heartbeat <- struct{}{}:
	default:
	}
}

func withTimeoutPolicy(parent context.Context, policy TimeoutPolicy) (context.Context, func()) {
	if policy.RunTimeout == 0 && policy.IdleTimeout == 0 {
		return parent, func() {}
	}
	ctx, cancel := context.WithCancelCause(parent)
	runtime := &timeoutRuntime{heartbeat: make(chan struct{}, 1)}
	ctx = context.WithValue(ctx, heartbeatContextKey{}, runtime)
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
				cancel(&TimeoutError{Kind: TimeoutRun, Duration: policy.RunTimeout})
				return
			case <-idleC:
				cancel(&TimeoutError{Kind: TimeoutIdle, Duration: policy.IdleTimeout})
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
	return ctx, finish
}

func timeoutCause(ctx context.Context) error {
	var timeout *TimeoutError
	cause := context.Cause(ctx)
	if errors.As(cause, &timeout) {
		return timeout
	}
	return nil
}
