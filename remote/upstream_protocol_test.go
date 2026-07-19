package remote_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/wahanbo/langgraph-go/graph"
	"github.com/wahanbo/langgraph-go/remote"
)

type upstreamFixture struct {
	Upstream string `json:"upstream"`
	Commit   string `json:"commit"`
	Requests []struct {
		Method, Path, Query string
		Body                json.RawMessage
	} `json:"requests"`
}

func loadUpstreamFixture(t *testing.T) upstreamFixture {
	t.Helper()
	encoded, err := os.ReadFile(filepath.Join("testdata", "upstream_protocol_1_2_9.json"))
	if err != nil {
		t.Fatal(err)
	}
	var fixture upstreamFixture
	if err := json.Unmarshal(encoded, &fixture); err != nil {
		t.Fatal(err)
	}
	return fixture
}

func TestPinnedPythonSDKProtocolFixtureIsCurrent(t *testing.T) {
	python, err := exec.LookPath("python")
	if err != nil {
		t.Skip("python unavailable")
	}
	checkout := filepath.Clean(filepath.Join("..", "..", ".upstream-langgraph-1.2.9", "libs", "sdk-py"))
	if _, err := os.Stat(checkout); err != nil {
		t.Skip("pinned upstream checkout unavailable")
	}
	command := exec.Command(python, filepath.Join("testdata", "generate_upstream_protocol.py"))
	command.Env = append(os.Environ(), "PYTHONPATH="+checkout)
	output, err := command.Output()
	if err != nil {
		t.Fatal(err)
	}
	var generated, recorded any
	if err := json.Unmarshal(output, &generated); err != nil {
		t.Fatal(err)
	}
	encoded, err := os.ReadFile(filepath.Join("testdata", "upstream_protocol_1_2_9.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(encoded, &recorded); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(generated, recorded) {
		t.Fatalf("pinned SDK request fixture changed\ngenerated=%s\nrecorded=%s", output, encoded)
	}
}

func TestPinnedPythonSDKAgainstGoServer(t *testing.T) {
	python, err := exec.LookPath("python")
	if err != nil {
		t.Skip("python unavailable")
	}
	checkout := filepath.Clean(filepath.Join("..", "..", ".upstream-langgraph-1.2.9", "libs", "sdk-py"))
	if _, err := os.Stat(checkout); err != nil {
		t.Skip("pinned upstream checkout unavailable")
	}
	backend := &fakeStateGraph{}
	adapter := stateInvokeAdapter{state: backend, invoke: fakeInvoker{run: func(_ context.Context, input invokeInput, _ graph.RunConfig) (invokeOutput, error) {
		return invokeOutput{Value: input.Value * 3}, nil
	}}}
	handler, err := remote.NewStateServer[invokeInput, invokeOutput, remoteState, remoteDelta](adapter, backend, remote.ServerOptions{LangGraphProtocol: true, AssistantID: "graph-1", IDGenerator: func() string { return "python-sdk-run" }})
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(handler)
	defer server.Close()
	defer handler.Close()
	command := exec.Command(python, filepath.Join("testdata", "verify_upstream_sdk.py"), server.URL)
	command.Env = append(os.Environ(), "PYTHONPATH="+checkout)
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("python SDK: %v\n%s", err, output)
	}
}

func TestLangGraphProtocolGoldenRequestsAndRawResponses(t *testing.T) {
	fixture := loadUpstreamFixture(t)
	if fixture.Commit != "95af6a00718588e7b7ce17310e8006d267896a77" {
		t.Fatalf("commit=%s", fixture.Commit)
	}
	backend := &fakeStateGraph{}
	backendInvoke := fakeInvoker{run: func(_ context.Context, input invokeInput, _ graph.RunConfig) (invokeOutput, error) {
		return invokeOutput{Value: input.Value * 3}, nil
	}}
	type combined struct {
		*fakeStateGraph
		invoke fakeInvoker
	}
	_ = combined{}
	adapter := stateInvokeAdapter{state: backend, invoke: backendInvoke}
	ids := []string{"run-1"}
	next := 0
	handler, err := remote.NewStateServer[invokeInput, invokeOutput, remoteState, remoteDelta](adapter, backend, remote.ServerOptions{LangGraphProtocol: true, AssistantID: "graph-1", IDGenerator: func() string { value := ids[next]; next++; return value }})
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(handler)
	defer server.Close()
	defer handler.Close()
	client := server.Client()
	for index, item := range fixture.Requests {
		target := server.URL + item.Path
		if item.Query != "" {
			target += "?" + item.Query
		}
		var body *bytes.Reader
		if len(item.Body) > 0 && string(item.Body) != "null" {
			body = bytes.NewReader(item.Body)
		} else {
			body = bytes.NewReader(nil)
		}
		request, err := http.NewRequest(item.Method, target, body)
		if err != nil {
			t.Fatal(err)
		}
		if body.Len() > 0 {
			request.Header.Set("Content-Type", "application/json")
		}
		response, err := client.Do(request)
		if err != nil {
			t.Fatalf("request %d: %v", index, err)
		}
		var decoded any
		if response.StatusCode != http.StatusNoContent {
			if err := json.NewDecoder(response.Body).Decode(&decoded); err != nil {
				response.Body.Close()
				t.Fatalf("request %d status=%d decode: %v", index, response.StatusCode, err)
			}
		}
		response.Body.Close()
		if response.StatusCode >= 300 {
			t.Fatalf("request %d %s %s status=%d body=%v", index, item.Method, item.Path, response.StatusCode, decoded)
		}
		switch index {
		case 0:
			resource := decoded.(map[string]any)
			if resource["thread_id"] != "thread-1" || resource["metadata"].(map[string]any)["tenant"] != "acme" {
				t.Fatalf("thread=%v", decoded)
			}
			if _, wrapped := resource["thread"]; wrapped {
				t.Fatal("upstream response must be raw")
			}
		case 2:
			resource := decoded.(map[string]any)
			if resource["run_id"] != "run-1" || resource["assistant_id"] != "graph-1" {
				t.Fatalf("run=%v", decoded)
			}
		case 6:
			resource := decoded.(map[string]any)
			if resource["value"] != float64(9) {
				t.Fatalf("join=%v", decoded)
			}
		case 7:
			resource := decoded.(map[string]any)
			checkpoint := resource["checkpoint"].(map[string]any)
			if checkpoint["checkpoint_id"] != "cp-2" || resource["values"].(map[string]any)["Count"] != float64(3) {
				t.Fatalf("state=%v", decoded)
			}
		case 8:
			resource := decoded.(map[string]any)
			if _, ok := resource["checkpoint"]; !ok {
				t.Fatalf("update=%v", decoded)
			}
		case 9:
			if _, ok := decoded.([]any); !ok {
				t.Fatalf("history=%T", decoded)
			}
		}
	}
}

type stateInvokeAdapter struct {
	state  *fakeStateGraph
	invoke fakeInvoker
}

func (a stateInvokeAdapter) Invoke(ctx context.Context, input invokeInput, config graph.RunConfig) (invokeOutput, error) {
	return a.invoke.Invoke(ctx, input, config)
}
