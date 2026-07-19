package remote_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"

	"github.com/ybszm/langgraph-go/graph"
	"github.com/ybszm/langgraph-go/remote"
)

type commandState struct {
	Value string `json:"value"`
}

type commandDelta struct {
	Add int `json:"add"`
}

type fakeCommandInvoker struct {
	command graph.Command[commandDelta]
	config  graph.RunConfig
}

func (f *fakeCommandInvoker) InvokeCommand(
	_ context.Context,
	command graph.Command[commandDelta],
	config graph.RunConfig,
) (invokeOutput, error) {
	f.command, f.config = command, config
	return invokeOutput{Value: command.Update.Add}, nil
}

func TestRemoteCommandRoundTripsResumeUpdateGotoAndTypedSends(t *testing.T) {
	commander := &fakeCommandInvoker{}
	handler, err := remote.NewCommandServer[invokeInput, invokeOutput, commandState, commandDelta](
		fakeInvoker{}, commander, remote.ServerOptions{},
	)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(handler)
	defer server.Close()
	client, err := remote.NewCommandClient[commandState, commandDelta, invokeOutput](server.URL, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	resume, err := graph.ResumeByID(map[string]any{"interrupt-1": "approved"})
	if err != nil {
		t.Fatal(err)
	}
	command := graph.WithResume(graph.Command[commandDelta]{
		Update: commandDelta{Add: 7}, HasUpdate: true,
		Goto:   []graph.NodeID{"finish"},
		Sends:  []graph.TaskSend{graph.SendTo("worker", commandState{Value: "task-local"})},
		Target: graph.CommandParent,
	}, resume)
	output, err := client.InvokeCommand(context.Background(), command, remote.RunConfig{
		ThreadID: "thread", CheckpointID: "checkpoint", Metadata: map[string]any{"request": "command"},
	})
	if err != nil || output.Value != 7 {
		t.Fatalf("output=%+v err=%v", output, err)
	}
	if !commander.command.HasUpdate || commander.command.Update.Add != 7 ||
		!reflect.DeepEqual(commander.command.Goto, []graph.NodeID{"finish"}) ||
		len(commander.command.Sends) != 1 || commander.command.Sends[0].Node != "worker" ||
		!reflect.DeepEqual(commander.command.Sends[0].State, commandState{Value: "task-local"}) ||
		commander.command.Target != graph.CommandParent || commander.command.Resume == nil {
		t.Fatalf("command=%+v", commander.command)
	}
	encodedResume, err := json.Marshal(commander.command.Resume)
	if err != nil || string(encodedResume) != `{"by_id":{"interrupt-1":"approved"}}` {
		t.Fatalf("resume=%s err=%v", encodedResume, err)
	}
	if commander.config.ThreadID != "thread" || commander.config.CheckpointID != "checkpoint" ||
		commander.config.Metadata["request"] != "command" {
		t.Fatalf("config=%+v", commander.config)
	}
}

func TestRemoteCommandRejectsInvalidSendTypeAndWireFields(t *testing.T) {
	commander := &fakeCommandInvoker{}
	handler, _ := remote.NewCommandServer[invokeInput, invokeOutput, commandState, commandDelta](
		fakeInvoker{}, commander, remote.ServerOptions{},
	)
	server := httptest.NewServer(handler)
	defer server.Close()
	client, _ := remote.NewCommandClient[commandState, commandDelta, invokeOutput](server.URL, server.Client())
	resume, _ := graph.Resume("ok")
	_, err := client.InvokeCommand(context.Background(), graph.WithResume(graph.Command[commandDelta]{
		Sends: []graph.TaskSend{graph.SendTo("worker", "wrong")},
	}, resume), remote.RunConfig{})
	if err == nil {
		t.Fatal("wrong Send state type was accepted")
	}

	body := bytes.NewBufferString(`{"command":{"resume":{"value":"ok"},"unknown":true}}`)
	request, _ := http.NewRequest(http.MethodPost, server.URL+remote.CommandPath, body)
	request.Header.Set(remote.ProtocolHeader, remote.ProtocolVersion)
	request.Header.Set("Content-Type", "application/json")
	response, err := server.Client().Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusBadRequest {
		t.Fatalf("status=%d", response.StatusCode)
	}
}

func TestRemoteCommandEndpointRequiresCapability(t *testing.T) {
	handler, _ := remote.NewServer(fakeInvoker{}, remote.ServerOptions{})
	server := httptest.NewServer(handler)
	defer server.Close()
	client, _ := remote.NewCommandClient[commandState, commandDelta, invokeOutput](server.URL, server.Client())
	resume, _ := graph.Resume("ok")
	_, err := client.InvokeCommand(context.Background(), graph.ResumeAsCommand[commandDelta](resume), remote.RunConfig{})
	var remoteErr *remote.Error
	if err == nil || !errors.As(err, &remoteErr) || remoteErr.Code != remote.CodeProtocol {
		t.Fatalf("err=%v", err)
	}
}
