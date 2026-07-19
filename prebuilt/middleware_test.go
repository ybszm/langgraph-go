package prebuilt_test

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/ybszm/langgraph-go/graph"
	"github.com/ybszm/langgraph-go/prebuilt"
)

func TestModelMiddlewareOrderTransformAndShortCircuit(t *testing.T) {
	var order []string
	base := prebuilt.ChatModelFunc[int]{Run: func(_ context.Context, state int, _ graph.Runtime) (prebuilt.AssistantMessage, error) {
		order = append(order, "base")
		return prebuilt.AssistantMessage{Content: string(rune('0' + state))}, nil
	}}
	outer := prebuilt.ModelMiddlewareFunc[int](func(ctx context.Context, state int, runtime graph.Runtime, next prebuilt.ModelHandler[int]) (prebuilt.AssistantMessage, error) {
		order = append(order, "outer-before")
		result, err := next(ctx, state+1, runtime)
		order = append(order, "outer-after")
		result.Content += "!"
		return result, err
	})
	inner := prebuilt.ModelMiddlewareFunc[int](func(ctx context.Context, state int, runtime graph.Runtime, next prebuilt.ModelHandler[int]) (prebuilt.AssistantMessage, error) {
		order = append(order, "inner-before")
		result, err := next(ctx, state+1, runtime)
		order = append(order, "inner-after")
		return result, err
	})
	model, err := prebuilt.WrapChatModel[int](base, outer, inner)
	if err != nil {
		t.Fatal(err)
	}
	message, err := model.Invoke(context.Background(), 1, graph.Runtime{})
	want := []string{"outer-before", "inner-before", "base", "inner-after", "outer-after"}
	if err != nil || message.Content != "3!" || !reflect.DeepEqual(order, want) {
		t.Fatalf("message=%+v order=%v err=%v", message, order, err)
	}
	short, _ := prebuilt.WrapChatModel[int](base, prebuilt.ModelMiddlewareFunc[int](func(context.Context, int, graph.Runtime, prebuilt.ModelHandler[int]) (prebuilt.AssistantMessage, error) {
		return prebuilt.AssistantMessage{Content: "cached"}, nil
	}))
	message, err = short.Invoke(context.Background(), 9, graph.Runtime{})
	if err != nil || message.Content != "cached" || len(order) != len(want) {
		t.Fatalf("short message=%+v order=%v err=%v", message, order, err)
	}
}

func TestToolMiddlewarePreservesTrustedRuntimeAndErrorChain(t *testing.T) {
	sentinel := errors.New("blocked")
	base := prebuilt.ToolFunc[int, string]{ToolName: "lookup", Run: func(_ context.Context, call prebuilt.ToolCall, runtime prebuilt.ToolRuntime[int]) (prebuilt.ToolResult[string], error) {
		if runtime.State != 7 || runtime.Call.ID != call.ID || runtime.Context != "trusted" {
			t.Fatalf("runtime=%+v call=%+v", runtime, call)
		}
		return prebuilt.TextResult[string]("ok"), nil
	}}
	tool, err := prebuilt.WrapTool[int, string](base,
		prebuilt.ToolMiddlewareFunc[int, string](func(ctx context.Context, call prebuilt.ToolCall, runtime prebuilt.ToolRuntime[int], next prebuilt.ToolHandler[int, string]) (prebuilt.ToolResult[string], error) {
			if call.Name == "blocked" {
				return prebuilt.ToolResult[string]{}, sentinel
			}
			return next(ctx, call, runtime)
		}),
	)
	if err != nil || tool.Name() != "lookup" {
		t.Fatalf("tool=%v err=%v", tool, err)
	}
	call := prebuilt.ToolCall{ID: "call-1", Name: "lookup"}
	result, err := tool.Invoke(context.Background(), call, prebuilt.ToolRuntime[int]{State: 7, Call: call, Context: "trusted"})
	if err != nil || result.Message == nil || result.Message.Content != "ok" {
		t.Fatalf("result=%+v err=%v", result, err)
	}
	_, err = tool.Invoke(context.Background(), prebuilt.ToolCall{ID: "call-2", Name: "blocked"}, prebuilt.ToolRuntime[int]{})
	if !errors.Is(err, sentinel) {
		t.Fatalf("blocked err=%v", err)
	}
}

func TestMiddlewareValidation(t *testing.T) {
	if _, err := prebuilt.WrapChatModel[int](nil); err == nil {
		t.Fatal("nil model accepted")
	}
	base := prebuilt.ChatModelFunc[int]{Run: func(context.Context, int, graph.Runtime) (prebuilt.AssistantMessage, error) {
		return prebuilt.AssistantMessage{}, nil
	}}
	if _, err := prebuilt.WrapChatModel[int](base, nil); err == nil {
		t.Fatal("nil model middleware accepted")
	}
	if _, err := prebuilt.WrapTool[int, string](nil); err == nil {
		t.Fatal("nil tool accepted")
	}
}
