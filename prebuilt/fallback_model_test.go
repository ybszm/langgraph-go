package prebuilt_test

import (
	"context"
	"errors"
	"testing"

	"github.com/ybszm/langgraph-go/graph"
	"github.com/ybszm/langgraph-go/prebuilt"
)

func TestFallbackChatModelUsesNextModel(t *testing.T) {
	model := prebuilt.FallbackChatModel[int]{Models: []prebuilt.ChatModel[int]{
		prebuilt.ChatModelFunc[int]{Run: func(context.Context, int, graph.Runtime) (prebuilt.AssistantMessage, error) {
			return prebuilt.AssistantMessage{}, errors.New("unavailable")
		}},
		prebuilt.ChatModelFunc[int]{Run: func(context.Context, int, graph.Runtime) (prebuilt.AssistantMessage, error) {
			return prebuilt.AssistantMessage{Content: "fallback"}, nil
		}},
	}}
	message, err := model.Invoke(context.Background(), 0, graph.Runtime{})
	if err != nil || message.Content != "fallback" {
		t.Fatalf("message=%+v err=%v", message, err)
	}
}

func TestFallbackChatModelCanStopOnPermanentError(t *testing.T) {
	called := false
	permanent := errors.New("permanent")
	model := prebuilt.FallbackChatModel[int]{
		Models: []prebuilt.ChatModel[int]{
			prebuilt.ChatModelFunc[int]{Run: func(context.Context, int, graph.Runtime) (prebuilt.AssistantMessage, error) {
				return prebuilt.AssistantMessage{}, permanent
			}},
			prebuilt.ChatModelFunc[int]{Run: func(context.Context, int, graph.Runtime) (prebuilt.AssistantMessage, error) {
				called = true
				return prebuilt.AssistantMessage{}, nil
			}},
		},
		ShouldFallback: func(error) bool { return false },
	}
	_, err := model.Invoke(context.Background(), 0, graph.Runtime{})
	if !errors.Is(err, permanent) || called {
		t.Fatalf("called=%v err=%v", called, err)
	}
}
