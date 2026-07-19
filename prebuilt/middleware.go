package prebuilt

import (
	"context"
	"fmt"

	"github.com/ybszm/langgraph-go/graph"
)

// ModelHandler is one ChatModel invocation in a middleware chain.
type ModelHandler[S any] func(context.Context, S, graph.Runtime) (AssistantMessage, error)

// ModelMiddleware intercepts model calls. The first configured middleware is
// outermost and may call next, transform its result, or short-circuit.
type ModelMiddleware[S any] interface {
	InvokeModel(context.Context, S, graph.Runtime, ModelHandler[S]) (AssistantMessage, error)
}

// ModelMiddlewareFunc adapts a function to ModelMiddleware.
type ModelMiddlewareFunc[S any] func(context.Context, S, graph.Runtime, ModelHandler[S]) (AssistantMessage, error)

func (middleware ModelMiddlewareFunc[S]) InvokeModel(
	ctx context.Context,
	state S,
	runtime graph.Runtime,
	next ModelHandler[S],
) (AssistantMessage, error) {
	return middleware(ctx, state, runtime, next)
}

type wrappedChatModel[S any] struct{ handler ModelHandler[S] }

func (model wrappedChatModel[S]) Invoke(ctx context.Context, state S, runtime graph.Runtime) (AssistantMessage, error) {
	return model.handler(ctx, state, runtime)
}

// WrapChatModel composes model middleware without changing the base model or
// introducing provider SDK types into the ReAct graph.
func WrapChatModel[S any](base ChatModel[S], middleware ...ModelMiddleware[S]) (ChatModel[S], error) {
	if isNilChatModel(base) {
		return nil, fmt.Errorf("wrap chat model: base model is nil")
	}
	handler := ModelHandler[S](base.Invoke)
	for index := len(middleware) - 1; index >= 0; index-- {
		current := middleware[index]
		if current == nil {
			return nil, fmt.Errorf("wrap chat model: middleware %d is nil", index)
		}
		next := handler
		handler = func(ctx context.Context, state S, runtime graph.Runtime) (AssistantMessage, error) {
			return current.InvokeModel(ctx, state, runtime, next)
		}
	}
	return wrappedChatModel[S]{handler: handler}, nil
}

// ToolHandler is one Tool invocation in a middleware chain.
type ToolHandler[S, D any] func(context.Context, ToolCall, ToolRuntime[S]) (ToolResult[D], error)

// ToolMiddleware intercepts tool calls while retaining trusted ToolRuntime.
type ToolMiddleware[S, D any] interface {
	InvokeTool(context.Context, ToolCall, ToolRuntime[S], ToolHandler[S, D]) (ToolResult[D], error)
}

// ToolMiddlewareFunc adapts a function to ToolMiddleware.
type ToolMiddlewareFunc[S, D any] func(context.Context, ToolCall, ToolRuntime[S], ToolHandler[S, D]) (ToolResult[D], error)

func (middleware ToolMiddlewareFunc[S, D]) InvokeTool(
	ctx context.Context,
	call ToolCall,
	runtime ToolRuntime[S],
	next ToolHandler[S, D],
) (ToolResult[D], error) {
	return middleware(ctx, call, runtime, next)
}

type wrappedTool[S, D any] struct {
	name    string
	handler ToolHandler[S, D]
}

func (tool wrappedTool[S, D]) Name() string { return tool.name }
func (tool wrappedTool[S, D]) Invoke(ctx context.Context, call ToolCall, runtime ToolRuntime[S]) (ToolResult[D], error) {
	return tool.handler(ctx, call, runtime)
}

// WrapTool composes tool middleware and preserves the base tool's stable name.
func WrapTool[S, D any](base Tool[S, D], middleware ...ToolMiddleware[S, D]) (Tool[S, D], error) {
	if base == nil || base.Name() == "" {
		return nil, fmt.Errorf("wrap tool: base tool is nil or unnamed")
	}
	handler := ToolHandler[S, D](base.Invoke)
	for index := len(middleware) - 1; index >= 0; index-- {
		current := middleware[index]
		if current == nil {
			return nil, fmt.Errorf("wrap tool: middleware %d is nil", index)
		}
		next := handler
		handler = func(ctx context.Context, call ToolCall, runtime ToolRuntime[S]) (ToolResult[D], error) {
			return current.InvokeTool(ctx, call, runtime, next)
		}
	}
	return wrappedTool[S, D]{name: base.Name(), handler: handler}, nil
}
