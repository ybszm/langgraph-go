package remote_test

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ybszm/langgraph-go/graph"
	"github.com/ybszm/langgraph-go/remote"
)

func TestRemoteBatchPreservesOrderConfigErrorsAndConcurrency(t *testing.T) {
	var active atomic.Int32
	var maximum atomic.Int32
	var invalidConfig atomic.Bool
	backend := fakeInvoker{run: func(_ context.Context, input invokeInput, config graph.RunConfig) (invokeOutput, error) {
		current := active.Add(1)
		defer active.Add(-1)
		for {
			observed := maximum.Load()
			if current <= observed || maximum.CompareAndSwap(observed, current) {
				break
			}
		}
		if config.ThreadID != "thread-"+string(rune('0'+input.Value)) || config.Metadata["item"] != float64(input.Value) {
			invalidConfig.Store(true)
		}
		time.Sleep(2 * time.Millisecond)
		switch input.Value {
		case 1:
			return invokeOutput{}, errors.New("item failed")
		case 2:
			panic("item panic")
		default:
			return invokeOutput{Value: input.Value * 10}, nil
		}
	}}
	handler, err := remote.NewServer[invokeInput, invokeOutput](backend, remote.ServerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(handler)
	defer server.Close()
	client, _ := remote.NewClient[invokeInput, invokeOutput](server.URL, server.Client())
	items := make([]graph.BatchItem[invokeInput], 4)
	for index := range items {
		items[index] = graph.BatchItem[invokeInput]{
			Input:  invokeInput{Value: index},
			Config: graph.RunConfig{ThreadID: "thread-" + string(rune('0'+index)), Metadata: map[string]any{"item": index}},
		}
	}
	results, err := client.Batch(context.Background(), items, graph.BatchOptions{MaxConcurrency: 2})
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 4 || results[0].Output.Value != 0 || results[3].Output.Value != 30 {
		t.Fatalf("results=%+v", results)
	}
	for _, index := range []int{1, 2} {
		var remoteErr *remote.Error
		if !errors.As(results[index].Err, &remoteErr) || remoteErr.Code != remote.CodeExecution {
			t.Fatalf("result %d err=%v", index, results[index].Err)
		}
	}
	if invalidConfig.Load() || maximum.Load() != 2 {
		t.Fatalf("invalid config=%v maximum=%d", invalidConfig.Load(), maximum.Load())
	}
}

func TestRemoteBatchStrictRequestAndResponseCardinality(t *testing.T) {
	handler, _ := remote.NewServer[invokeInput, invokeOutput](fakeInvoker{run: func(context.Context, invokeInput, graph.RunConfig) (invokeOutput, error) {
		return invokeOutput{}, nil
	}}, remote.ServerOptions{})
	server := httptest.NewServer(handler)
	defer server.Close()
	request, _ := http.NewRequest(http.MethodPost, server.URL+remote.BatchPath, bytes.NewBufferString(`{"items":[],"unknown":true}`))
	request.Header.Set(remote.ProtocolHeader, remote.ProtocolVersion)
	request.Header.Set("Content-Type", "application/json")
	response, err := server.Client().Do(request)
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusBadRequest {
		t.Fatalf("strict request status=%d", response.StatusCode)
	}

	broken := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		writer.Header().Set(remote.ProtocolHeader, remote.ProtocolVersion)
		writer.Header().Set("Content-Type", "application/json")
		_, _ = writer.Write([]byte(`{"results":[]}`))
	}))
	defer broken.Close()
	client, _ := remote.NewClient[invokeInput, invokeOutput](broken.URL, broken.Client())
	_, err = client.Batch(context.Background(), []graph.BatchItem[invokeInput]{{Input: invokeInput{Value: 1}}}, graph.BatchOptions{})
	var remoteErr *remote.Error
	if !errors.As(err, &remoteErr) || remoteErr.Code != remote.CodeProtocol {
		t.Fatalf("cardinality err=%v", err)
	}
}
