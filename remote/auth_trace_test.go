package remote_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/ybszm/langgraph-go/graph"
	"github.com/ybszm/langgraph-go/remote"
)

func TestRemoteAuthorizationAndClientHeaders(t *testing.T) {
	var calls atomic.Int32
	backend := fakeInvoker{run: func(context.Context, invokeInput, graph.RunConfig) (invokeOutput, error) {
		calls.Add(1)
		return invokeOutput{Value: 1}, nil
	}}
	handler, _ := remote.NewServer[invokeInput, invokeOutput](backend, remote.ServerOptions{
		Authorize: func(_ context.Context, request *http.Request) error {
			if request.Header.Get("Authorization") != "Bearer secret" {
				return errors.New("invalid token")
			}
			return nil
		},
	})
	server := httptest.NewServer(handler)
	defer server.Close()
	unauthorized, _ := remote.NewClient[invokeInput, invokeOutput](server.URL, server.Client())
	_, err := unauthorized.Invoke(context.Background(), invokeInput{}, remote.RunConfig{})
	var remoteErr *remote.Error
	if !errors.As(err, &remoteErr) || remoteErr.Code != remote.CodeUnauthorized || calls.Load() != 0 {
		t.Fatalf("err=%v calls=%d", err, calls.Load())
	}
	authorized, err := remote.NewClientWithOptions[invokeInput, invokeOutput](server.URL, server.Client(), remote.ClientOptions{
		Headers: http.Header{"Authorization": []string{"Bearer secret"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := authorized.Invoke(context.Background(), invokeInput{}, remote.RunConfig{}); err != nil || calls.Load() != 1 {
		t.Fatalf("err=%v calls=%d", err, calls.Load())
	}
}

func TestRemoteTraceCorrelationReachesContextAndConfig(t *testing.T) {
	backend := fakeInvoker{run: func(ctx context.Context, _ invokeInput, config graph.RunConfig) (invokeOutput, error) {
		if got := remote.TraceIDFromContext(ctx); got != "trace-fixed" {
			t.Errorf("context trace=%q", got)
		}
		if got := config.Metadata[remote.TraceMetadataKey]; got != "trace-fixed" {
			t.Errorf("metadata trace=%v", got)
		}
		return invokeOutput{}, nil
	}}
	handler, _ := remote.NewServer[invokeInput, invokeOutput](backend, remote.ServerOptions{
		TraceIDGenerator: func() string { return "trace-fixed" },
	})
	server := httptest.NewServer(handler)
	defer server.Close()
	request, _ := http.NewRequest(http.MethodPost, server.URL+remote.InvokePath, strings.NewReader(`{"input":{}}`))
	request.Header.Set(remote.ProtocolHeader, remote.ProtocolVersion)
	response, err := server.Client().Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if got := response.Header.Get(remote.TraceHeader); got != "trace-fixed" {
		t.Fatalf("trace header=%q", got)
	}
}
