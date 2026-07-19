package graph_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/ybszm/langgraph-go/graph"
)

func TestNodeRunAndIdleTimeouts(t *testing.T) {
	for name, policy := range map[string]graph.NodeTimeoutPolicy{
		"run":  {RunTimeout: 10 * time.Millisecond},
		"idle": {IdleTimeout: 10 * time.Millisecond},
	} {
		t.Run(name, func(t *testing.T) {
			builder := graph.NewStateGraph(customReducer)
			_ = builder.AddNode("blocked", func(ctx context.Context, _ customState, _ graph.Runtime) (graph.Command[customDelta], error) {
				<-ctx.Done()
				return graph.NoCommand[customDelta](), ctx.Err()
			}, graph.WithNodeTimeout(policy))
			_ = builder.AddEdge(graph.START, "blocked")
			_ = builder.AddEdge("blocked", graph.END)
			compiled, err := builder.Compile()
			if err != nil {
				t.Fatal(err)
			}
			_, err = compiled.Invoke(context.Background(), customState{}, graph.RunConfig{})
			var timeout *graph.NodeTimeoutError
			if !errors.Is(err, graph.ErrNodeTimeout) || !errors.As(err, &timeout) {
				t.Fatalf("error=%v", err)
			}
			want := graph.NodeTimeoutRun
			if name == "idle" {
				want = graph.NodeTimeoutIdle
			}
			if timeout.Kind != want || timeout.Duration != 10*time.Millisecond {
				t.Fatalf("timeout=%+v", timeout)
			}
		})
	}
}

func TestNodeHeartbeatRefreshesIdleTimeout(t *testing.T) {
	builder := graph.NewStateGraph(customReducer)
	_ = builder.AddNode("progress", func(ctx context.Context, _ customState, runtime graph.Runtime) (graph.Command[customDelta], error) {
		for range 4 {
			select {
			case <-time.After(5 * time.Millisecond):
				runtime.Heartbeat()
			case <-ctx.Done():
				return graph.NoCommand[customDelta](), ctx.Err()
			}
		}
		return graph.Update(customDelta{Add: 1}), nil
	}, graph.WithNodeTimeout(graph.NodeTimeoutPolicy{IdleTimeout: 15 * time.Millisecond}))
	_ = builder.AddEdge(graph.START, "progress")
	_ = builder.AddEdge("progress", graph.END)
	compiled, err := builder.Compile()
	if err != nil {
		t.Fatal(err)
	}
	result, err := compiled.Invoke(context.Background(), customState{}, graph.RunConfig{})
	if err != nil || result.Count != 1 {
		t.Fatalf("result=%+v error=%v", result, err)
	}
}

func TestProviderStreamWritesRefreshIdleTimeoutWithoutConsumer(t *testing.T) {
	builder := graph.NewStateGraph(customReducer)
	_ = builder.AddNode("provider", func(ctx context.Context, _ customState, runtime graph.Runtime) (graph.Command[customDelta], error) {
		for index := range 4 {
			select {
			case <-time.After(5 * time.Millisecond):
				if err := runtime.WriteMessage(map[string]any{"role": "assistant", "content": index}); err != nil {
					return graph.NoCommand[customDelta](), err
				}
			case <-ctx.Done():
				return graph.NoCommand[customDelta](), ctx.Err()
			}
		}
		return graph.Update(customDelta{Add: 1}), nil
	}, graph.WithNodeTimeout(graph.NodeTimeoutPolicy{IdleTimeout: 12 * time.Millisecond}))
	_ = builder.AddEdge(graph.START, "provider")
	_ = builder.AddEdge("provider", graph.END)
	compiled, err := builder.Compile()
	if err != nil {
		t.Fatal(err)
	}
	result, err := compiled.Invoke(context.Background(), customState{}, graph.RunConfig{})
	if err != nil || result.Count != 1 {
		t.Fatalf("result=%+v error=%v", result, err)
	}
}

func TestNodeTimeoutParticipatesInRetryAndResetsPerAttempt(t *testing.T) {
	builder := graph.NewStateGraph(customReducer)
	_ = builder.AddNode("retry", func(ctx context.Context, _ customState, runtime graph.Runtime) (graph.Command[customDelta], error) {
		if runtime.Attempt == 1 {
			<-ctx.Done()
			return graph.NoCommand[customDelta](), ctx.Err()
		}
		return graph.Update(customDelta{Add: 1}), nil
	}, graph.WithNodeTimeout(graph.NodeTimeoutPolicy{RunTimeout: 10 * time.Millisecond}), graph.WithRetryPolicies(graph.RetryPolicy{
		MaxAttempts: 2, InitialInterval: time.Nanosecond, MaxInterval: time.Second,
		EnableJitter: true, Jitter: func(time.Duration) time.Duration { return 0 },
		RetryOn: func(err error) bool { return errors.Is(err, graph.ErrNodeTimeout) },
	}))
	_ = builder.AddEdge(graph.START, "retry")
	_ = builder.AddEdge("retry", graph.END)
	compiled, err := builder.Compile()
	if err != nil {
		t.Fatal(err)
	}
	result, err := compiled.Invoke(context.Background(), customState{}, graph.RunConfig{})
	if err != nil || result.Count != 1 {
		t.Fatalf("result=%+v error=%v", result, err)
	}
}

func TestNodeTimeoutRejectsNegativeDurations(t *testing.T) {
	builder := graph.NewStateGraph(customReducer)
	err := builder.AddNode("bad", func(context.Context, customState, graph.Runtime) (graph.Command[customDelta], error) {
		return graph.NoCommand[customDelta](), nil
	}, graph.WithNodeTimeout(graph.NodeTimeoutPolicy{IdleTimeout: -1}))
	if !errors.Is(err, graph.ErrInvalidGraph) {
		t.Fatalf("error=%v", err)
	}
}
