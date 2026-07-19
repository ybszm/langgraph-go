package prebuilt_test

import (
	"errors"
	"reflect"
	"testing"

	"github.com/ybszm/langgraph-go/prebuilt"
)

func TestAddMessagesReplacesByIDAndAppendsMissingID(t *testing.T) {
	ids := []string{"generated-1"}
	result, err := prebuilt.AddMessages(
		[]prebuilt.Message{
			prebuilt.UserMessage{ID: "user-1", Content: "old"},
			prebuilt.AssistantMessage{ID: "assistant-1", Content: "old answer"},
		},
		[]prebuilt.Message{
			prebuilt.UserMessage{ID: "user-1", Content: "new"},
			prebuilt.SystemMessage{Content: "system"},
		},
		func() (string, error) {
			id := ids[0]
			ids = ids[1:]
			return id, nil
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	want := []prebuilt.Message{
		prebuilt.UserMessage{ID: "user-1", Content: "new"},
		prebuilt.AssistantMessage{ID: "assistant-1", Content: "old answer"},
		prebuilt.SystemMessage{ID: "generated-1", Content: "system"},
	}
	if !reflect.DeepEqual(result, want) {
		t.Fatalf("messages=%#v", result)
	}
}

func TestAddMessagesRemoveAndRemoveAll(t *testing.T) {
	left := []prebuilt.Message{
		prebuilt.UserMessage{ID: "one", Content: "one"},
		prebuilt.AssistantMessage{ID: "two", Content: "two"},
	}
	result, err := prebuilt.AddMessages(left, []prebuilt.Message{prebuilt.RemoveMessage{ID: "one"}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(result, []prebuilt.Message{prebuilt.AssistantMessage{ID: "two", Content: "two"}}) {
		t.Fatalf("messages=%#v", result)
	}
	result, err = prebuilt.AddMessages(left, []prebuilt.Message{
		prebuilt.UserMessage{ID: "ignored", Content: "ignored"},
		prebuilt.RemoveMessage{ID: prebuilt.RemoveAllMessages},
		prebuilt.AssistantMessage{ID: "fresh", Content: "fresh"},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(result, []prebuilt.Message{prebuilt.AssistantMessage{ID: "fresh", Content: "fresh"}}) {
		t.Fatalf("messages=%#v", result)
	}
}

func TestAddMessagesRejectsUnknownRemoveAndGeneratorFailure(t *testing.T) {
	_, err := prebuilt.AddMessages(nil, []prebuilt.Message{prebuilt.RemoveMessage{ID: "missing"}}, nil)
	if !errors.Is(err, prebuilt.ErrMessageNotFound) {
		t.Fatalf("error=%v", err)
	}
	generated := errors.New("entropy unavailable")
	_, err = prebuilt.AddMessages(nil, []prebuilt.Message{prebuilt.UserMessage{Content: "hello"}}, func() (string, error) {
		return "", generated
	})
	if !errors.Is(err, generated) {
		t.Fatalf("error=%v", err)
	}
}

func TestAddMessagesDoesNotMutateInputs(t *testing.T) {
	call := prebuilt.ToolCall{ID: "call", Name: "tool", Arguments: []byte(`{"value":1}`)}
	leftMessage := prebuilt.AssistantMessage{ID: "assistant", ToolCalls: []prebuilt.ToolCall{call}}
	left := []prebuilt.Message{leftMessage}
	result, err := prebuilt.AddMessages(left, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	resultAssistant := result[0].(prebuilt.AssistantMessage)
	resultAssistant.ToolCalls[0].Arguments[0] = '['
	if leftMessage.ToolCalls[0].Arguments[0] != '{' {
		t.Fatal("input message arguments were aliased")
	}
}
