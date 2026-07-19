package remote_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ybszm/langgraph-go/graph"
	"github.com/ybszm/langgraph-go/remote"
)

func TestRemoteTraceSinkRecordsSuccessAndAuthorizationFailure(t *testing.T) {
	var mu sync.Mutex
	var traces []remote.HTTPTrace
	var clock atomic.Int64
	sinkError := errors.New("export unavailable")
	var sinkErrors atomic.Int32
	handler, err := remote.NewServer(fakeInvoker{run: func(context.Context, invokeInput, graph.RunConfig) (invokeOutput, error) {
		return invokeOutput{Value: 8}, nil
	}}, remote.ServerOptions{
		Clock:            func() time.Time { return time.Unix(clock.Add(1), 0).UTC() },
		TraceIDGenerator: func() string { return "generated-trace" },
		Authorize: func(_ context.Context, request *http.Request) error {
			if request.Header.Get("X-Deny") == "true" {
				return errors.New("denied")
			}
			return nil
		},
		TraceSink: remote.TraceSinkFunc(func(_ context.Context, trace remote.HTTPTrace) error {
			mu.Lock()
			traces = append(traces, trace)
			mu.Unlock()
			return sinkError
		}),
		TraceErrorHandler: func(err error) {
			if errors.Is(err, sinkError) {
				sinkErrors.Add(1)
			}
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(handler)
	defer server.Close()
	client, _ := remote.NewClient[invokeInput, invokeOutput](server.URL, server.Client())
	if _, err := client.Invoke(context.Background(), invokeInput{Value: 4}, remote.RunConfig{}); err != nil {
		t.Fatal(err)
	}
	request, _ := http.NewRequest(http.MethodPost, server.URL+remote.InvokePath, nil)
	request.Header.Set(remote.TraceHeader, "caller-trace")
	request.Header.Set("X-Deny", "true")
	response, err := server.Client().Do(request)
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusUnauthorized {
		t.Fatalf("authorization status=%d", response.StatusCode)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(traces) != 2 || traces[0].TraceID != "generated-trace" || traces[0].Status != http.StatusOK ||
		traces[0].Path != remote.InvokePath || traces[1].TraceID != "caller-trace" || traces[1].Status != http.StatusUnauthorized ||
		traces[0].FinishedAt.Before(traces[0].StartedAt) || sinkErrors.Load() != 2 {
		t.Fatalf("traces=%+v sinkErrors=%d", traces, sinkErrors.Load())
	}
}
