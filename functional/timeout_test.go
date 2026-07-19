package functional_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/ybszm/langgraph-go/functional"
)

func TestTaskRunAndIdleTimeouts(t *testing.T) {
	runTimed, err := functional.NewTask("run-timeout", func(ctx context.Context, _ struct{}) (struct{}, error) {
		<-ctx.Done()
		return struct{}{}, ctx.Err()
	}, functional.TaskOptions[struct{}, struct{}]{Timeout: functional.TimeoutPolicy{RunTimeout: 20 * time.Millisecond}})
	if err != nil {
		t.Fatal(err)
	}
	entry, _ := functional.NewEntrypoint("entry", func(ctx context.Context, _ struct{}) (struct{}, error) {
		return runTimed.Call(ctx, struct{}{}).Await(ctx)
	}, functional.EntrypointOptions{})
	_, err = entry.Invoke(context.Background(), struct{}{})
	var timeoutErr *functional.TimeoutError
	if !errors.Is(err, functional.ErrTimeout) || !errors.As(err, &timeoutErr) || timeoutErr.Kind != functional.TimeoutRun {
		t.Fatalf("error=%v", err)
	}

	idleTimed, _ := functional.NewTask("idle-timeout", func(ctx context.Context, _ struct{}) (struct{}, error) {
		<-ctx.Done()
		return struct{}{}, ctx.Err()
	}, functional.TaskOptions[struct{}, struct{}]{Timeout: functional.TimeoutPolicy{IdleTimeout: 20 * time.Millisecond}})
	idleEntry, _ := functional.NewEntrypoint("idle-entry", func(ctx context.Context, _ struct{}) (struct{}, error) {
		return idleTimed.Call(ctx, struct{}{}).Await(ctx)
	}, functional.EntrypointOptions{})
	_, err = idleEntry.Invoke(context.Background(), struct{}{})
	if !errors.As(err, &timeoutErr) || timeoutErr.Kind != functional.TimeoutIdle {
		t.Fatalf("error=%v", err)
	}
}

func TestTaskHeartbeatRefreshesIdleTimeout(t *testing.T) {
	task, err := functional.NewTask("heartbeat", func(ctx context.Context, _ struct{}) (struct{}, error) {
		deadline := time.NewTimer(120 * time.Millisecond)
		defer deadline.Stop()
		ticker := time.NewTicker(5 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				functional.Heartbeat(ctx)
			case <-deadline.C:
				return struct{}{}, nil
			case <-ctx.Done():
				return struct{}{}, ctx.Err()
			}
		}
	}, functional.TaskOptions[struct{}, struct{}]{Timeout: functional.TimeoutPolicy{IdleTimeout: 50 * time.Millisecond}})
	if err != nil {
		t.Fatal(err)
	}
	entry, _ := functional.NewEntrypoint("entry", func(ctx context.Context, _ struct{}) (struct{}, error) {
		return task.Call(ctx, struct{}{}).Await(ctx)
	}, functional.EntrypointOptions{})
	if _, err := entry.Invoke(context.Background(), struct{}{}); err != nil {
		t.Fatal(err)
	}
}

func TestEntrypointTimeoutAndValidation(t *testing.T) {
	entry, err := functional.NewEntrypoint("timeout-entry", func(ctx context.Context, _ struct{}) (struct{}, error) {
		<-ctx.Done()
		return struct{}{}, ctx.Err()
	}, functional.EntrypointOptions{Timeout: functional.TimeoutPolicy{RunTimeout: 20 * time.Millisecond}})
	if err != nil {
		t.Fatal(err)
	}
	_, err = entry.Invoke(context.Background(), struct{}{})
	var timeoutErr *functional.TimeoutError
	if !errors.As(err, &timeoutErr) || timeoutErr.Kind != functional.TimeoutRun {
		t.Fatalf("error=%v", err)
	}
	if _, err := functional.NewTask("bad", func(context.Context, int) (int, error) { return 0, nil }, functional.TaskOptions[int, int]{Timeout: functional.TimeoutPolicy{IdleTimeout: -1}}); err == nil {
		t.Fatal("expected invalid timeout policy")
	}
}
