package prebuilt

import (
	"context"
	"fmt"
	"reflect"

	"github.com/wahanbo/langgraph-go/graph"
)

const (
	// AgentNodeID is the stable model node name used by ReAct agents.
	AgentNodeID graph.NodeID = "agent"
	// ToolsNodeID is the stable ToolNode name used by ReAct agents.
	ToolsNodeID graph.NodeID = "tools"
	// PreModelHookNodeID is the stable pre-model hook node name.
	PreModelHookNodeID graph.NodeID = "pre_model_hook"
	// PostModelHookNodeID is the stable post-model hook node name.
	PostModelHookNodeID graph.NodeID = "post_model_hook"
	// GenerateStructuredResponseNodeID is the stable final structured-output node.
	GenerateStructuredResponseNodeID graph.NodeID = "generate_structured_response"
	// StepsExhaustedMessage matches the upstream graceful ReAct termination
	// response when a model requests tools without enough remaining steps.
	StepsExhaustedMessage = "Sorry, need more steps to process this request."
)

// ChatModel is the provider-neutral model boundary used by CreateReactAgent.
// SDK-specific adapters can bind tools and translate their message types while
// the graph continues to use an application-defined typed state.
type ChatModel[S any] interface {
	Invoke(context.Context, S, graph.Runtime) (AssistantMessage, error)
}

// ChatModelFunc adapts a function to ChatModel.
type ChatModelFunc[S any] struct {
	Run func(context.Context, S, graph.Runtime) (AssistantMessage, error)
}

// StructuredModel is a provider-neutral structured-output model boundary.
// An SDK adapter owns schema binding and returns the decoded response value.
type StructuredModel[S any] interface {
	InvokeStructured(context.Context, S, graph.Runtime) (any, error)
}

// StructuredModelFunc adapts a function to StructuredModel.
type StructuredModelFunc[S any] struct {
	Run func(context.Context, S, graph.Runtime) (any, error)
}

// InvokeStructured implements StructuredModel.
func (f StructuredModelFunc[S]) InvokeStructured(
	ctx context.Context,
	state S,
	runtime graph.Runtime,
) (any, error) {
	if f.Run == nil {
		return nil, fmt.Errorf("structured model function is nil")
	}
	return f.Run(ctx, state, runtime)
}

// Invoke implements ChatModel.
func (f ChatModelFunc[S]) Invoke(
	ctx context.Context,
	state S,
	runtime graph.Runtime,
) (AssistantMessage, error) {
	if f.Run == nil {
		return AssistantMessage{}, fmt.Errorf("chat model function is nil")
	}
	return f.Run(ctx, state, runtime)
}

// ReactAgentAdapter binds the provider-neutral agent loop to an application's
// typed state and serializable delta.
type ReactAgentAdapter[S, D any] struct {
	// PreModelHook runs before every model turn, including turns after tools.
	PreModelHook graph.Node[S, D]
	// PostModelHook runs after every model response and before routing.
	PostModelHook graph.Node[S, D]
	// PreModelHookSpec registers a hook with an independent typed input schema.
	// It is mutually exclusive with PreModelHook.
	PreModelHookSpec HookSpec[S, D]
	// PostModelHookSpec is the typed-schema counterpart to PostModelHook.
	PostModelHookSpec HookSpec[S, D]
	// ChatHistory exposes provider-neutral history for validation immediately
	// before each model call. A nil accessor disables this optional check.
	ChatHistory func(context.Context, S) ([]AssistantMessage, []ToolMessage, error)
	// StructuredModel is invoked after a normal agent termination.
	StructuredModel StructuredModel[S]
	// StructuredResponse converts the decoded structured value into state.
	StructuredResponse func(context.Context, S, any) (D, error)
	// ModelMessage converts one model response into a state delta.
	ModelMessage func(context.Context, S, AssistantMessage) (D, error)
	// ToolNode extracts calls and converts normal tool results into a delta.
	ToolNode ToolNodeAdapter[S, D]
	// ToolCallState creates one task-local state for a v2 Send tool call.
	ToolCallState func(context.Context, S, ToolCall) (S, error)
	// ToolMessages returns tool results visible in current state. It is needed
	// only when ReturnDirect is configured.
	ToolMessages func(context.Context, S) ([]ToolMessage, error)
	// RemainingSteps returns the agent step budget and whether the state tracks
	// one. A nil function disables agent-specific graceful exhaustion handling.
	RemainingSteps func(context.Context, S) (remaining int, available bool, err error)
}

// ReactAgentVersion selects batched (v1) or per-call Send (v2) tool routing.
type ReactAgentVersion string

const (
	// ReactAgentV1 executes all calls inside one parallel ToolNode task.
	ReactAgentV1 ReactAgentVersion = "v1"
	// ReactAgentV2 distributes each call as an independent graph Send task.
	ReactAgentV2 ReactAgentVersion = "v2"
)

// ReactAgentConfig controls the prebuilt agent loop.
type ReactAgentConfig struct {
	ToolNode ToolNodeConfig
	// Version defaults to v1 for compatibility with the initial Go API.
	Version ReactAgentVersion
	// ReturnDirect lists tools whose result terminates the agent without a
	// subsequent model call.
	ReturnDirect []string
}

// CreateReactAgent builds the provider-neutral model -> tools -> model loop.
// Tool calls are executed with ToolNode's deterministic parallel semantics.
// Compile options expose persistence, Store, static interrupts, and caching
// without duplicating those runtime contracts in the prebuilt package.
func CreateReactAgent[S, D any](
	model ChatModel[S],
	tools []Tool[S, D],
	reducer graph.Reducer[S, D],
	adapter ReactAgentAdapter[S, D],
	config ReactAgentConfig,
	options ...graph.CompileOption[S, D],
) (*graph.CompiledGraph[S, D], error) {
	if isNilChatModel(model) {
		return nil, fmt.Errorf("react agent requires a model")
	}
	if reducer == nil {
		return nil, fmt.Errorf("react agent requires a reducer")
	}
	if adapter.ModelMessage == nil {
		return nil, fmt.Errorf("react agent requires a ModelMessage adapter")
	}
	version := config.Version
	if version == "" {
		version = ReactAgentV1
	}
	if version != ReactAgentV1 && version != ReactAgentV2 {
		return nil, fmt.Errorf("react agent version must be %q or %q, got %q", ReactAgentV1, ReactAgentV2, version)
	}
	structuredEnabled := !isNilStructuredModel(adapter.StructuredModel)
	if structuredEnabled != (adapter.StructuredResponse != nil) {
		return nil, fmt.Errorf("react agent requires StructuredModel and StructuredResponse together")
	}
	returnDirect, err := validateReturnDirect(tools, config.ReturnDirect)
	if err != nil {
		return nil, err
	}
	if len(tools) > 0 && (adapter.ToolNode.Calls == nil || adapter.ToolNode.Messages == nil) {
		return nil, fmt.Errorf("react agent requires ToolNode Calls and Messages adapters")
	}
	if len(tools) > 0 && version == ReactAgentV2 && adapter.ToolCallState == nil {
		return nil, fmt.Errorf("react agent v2 requires a ToolCallState adapter")
	}
	if len(returnDirect) > 0 && adapter.ToolMessages == nil {
		return nil, fmt.Errorf("react agent ReturnDirect requires a ToolMessages adapter")
	}
	if adapter.PreModelHook != nil && adapter.PreModelHookSpec != nil {
		return nil, fmt.Errorf("react agent PreModelHook and PreModelHookSpec are mutually exclusive")
	}
	if adapter.PostModelHook != nil && adapter.PostModelHookSpec != nil {
		return nil, fmt.Errorf("react agent PostModelHook and PostModelHookSpec are mutually exclusive")
	}
	if binder, ok := model.(ToolBindingChatModel[S]); ok && len(tools) > 0 {
		definitions, err := ToolDefinitions(tools)
		if err != nil {
			return nil, err
		}
		model, err = binder.BindTools(definitions)
		if err != nil {
			return nil, fmt.Errorf("bind model tools: %w", err)
		}
		if isNilChatModel(model) {
			return nil, fmt.Errorf("bind model tools returned nil model")
		}
	}
	builder := graph.NewStateGraph(reducer)
	modelNode := func(ctx context.Context, state S, runtime graph.Runtime) (graph.Command[D], error) {
		if adapter.ChatHistory != nil {
			assistants, toolMessages, err := adapter.ChatHistory(ctx, state)
			if err != nil {
				return graph.Command[D]{}, fmt.Errorf("read chat history: %w", err)
			}
			if err := ValidateChatHistory(assistants, toolMessages); err != nil {
				return graph.Command[D]{}, err
			}
		}
		response, err := model.Invoke(ctx, state, runtime)
		if err != nil {
			return graph.Command[D]{}, err
		}
		if adapter.RemainingSteps != nil && len(response.ToolCalls) > 0 {
			remaining, available, err := adapter.RemainingSteps(ctx, state)
			if err != nil {
				return graph.Command[D]{}, fmt.Errorf("read remaining steps: %w", err)
			}
			if available && reactStepsInsufficient(remaining, response.ToolCalls, returnDirect) {
				response = AssistantMessage{ID: response.ID, Content: StepsExhaustedMessage}
			}
		}
		delta, err := adapter.ModelMessage(ctx, state, response)
		if err != nil {
			return graph.Command[D]{}, fmt.Errorf("adapt model message: %w", err)
		}
		return graph.Update(delta), nil
	}
	if err := builder.AddNode(AgentNodeID, modelNode); err != nil {
		return nil, err
	}
	finalTarget := graph.END
	if structuredEnabled {
		structuredNode := func(ctx context.Context, state S, runtime graph.Runtime) (graph.Command[D], error) {
			response, err := adapter.StructuredModel.InvokeStructured(ctx, state, runtime)
			if err != nil {
				return graph.Command[D]{}, err
			}
			delta, err := adapter.StructuredResponse(ctx, state, response)
			if err != nil {
				return graph.Command[D]{}, fmt.Errorf("adapt structured response: %w", err)
			}
			return graph.Update(delta), nil
		}
		if err := builder.AddNode(GenerateStructuredResponseNodeID, structuredNode); err != nil {
			return nil, err
		}
		if err := builder.AddEdge(GenerateStructuredResponseNodeID, graph.END); err != nil {
			return nil, err
		}
		finalTarget = GenerateStructuredResponseNodeID
	}
	entrypoint := AgentNodeID
	if adapter.PreModelHook != nil || adapter.PreModelHookSpec != nil {
		var err error
		if adapter.PreModelHookSpec != nil {
			err = adapter.PreModelHookSpec.AddHook(builder, PreModelHookNodeID)
		} else {
			err = builder.AddNode(PreModelHookNodeID, adapter.PreModelHook)
		}
		if err != nil {
			return nil, err
		}
		if err := builder.AddEdge(PreModelHookNodeID, AgentNodeID); err != nil {
			return nil, err
		}
		entrypoint = PreModelHookNodeID
	}
	modelRouteSource := AgentNodeID
	if adapter.PostModelHook != nil || adapter.PostModelHookSpec != nil {
		var err error
		if adapter.PostModelHookSpec != nil {
			err = adapter.PostModelHookSpec.AddHook(builder, PostModelHookNodeID)
		} else {
			err = builder.AddNode(PostModelHookNodeID, adapter.PostModelHook)
		}
		if err != nil {
			return nil, err
		}
		if err := builder.AddEdge(AgentNodeID, PostModelHookNodeID); err != nil {
			return nil, err
		}
		modelRouteSource = PostModelHookNodeID
	}
	if err := builder.AddEdge(graph.START, entrypoint); err != nil {
		return nil, err
	}
	if len(tools) == 0 {
		if err := builder.AddEdge(modelRouteSource, finalTarget); err != nil {
			return nil, err
		}
		return builder.Compile(options...)
	}

	toolNode, err := NewToolNode(tools, adapter.ToolNode, config.ToolNode)
	if err != nil {
		return nil, err
	}
	if err := builder.AddNode(ToolsNodeID, toolNode.Node()); err != nil {
		return nil, err
	}
	if version == ReactAgentV1 {
		if err := builder.AddConditionalEdges(modelRouteSource, func(ctx context.Context, state S) ([]graph.NodeID, error) {
			calls, err := adapter.ToolNode.Calls(ctx, state)
			if err != nil {
				return nil, fmt.Errorf("route model tool calls: %w", err)
			}
			if len(calls) == 0 {
				return []graph.NodeID{finalTarget}, nil
			}
			return []graph.NodeID{ToolsNodeID}, nil
		}, ToolsNodeID, finalTarget); err != nil {
			return nil, err
		}
	} else {
		if err := builder.AddConditionalEdges(modelRouteSource, func(ctx context.Context, state S) ([]graph.NodeID, error) {
			calls, err := adapter.ToolNode.Calls(ctx, state)
			if err != nil {
				return nil, fmt.Errorf("route model tool calls: %w", err)
			}
			if len(calls) == 0 {
				return []graph.NodeID{finalTarget}, nil
			}
			return nil, nil
		}, finalTarget); err != nil {
			return nil, err
		}
		if err := builder.AddSendEdges(modelRouteSource, func(ctx context.Context, state S) ([]graph.Send[S], error) {
			calls, err := adapter.ToolNode.Calls(ctx, state)
			if err != nil {
				return nil, fmt.Errorf("distribute model tool calls: %w", err)
			}
			sends := make([]graph.Send[S], len(calls))
			for index, call := range calls {
				taskState, err := adapter.ToolCallState(ctx, state, call)
				if err != nil {
					return nil, fmt.Errorf("adapt tool call %q: %w", call.ID, err)
				}
				sends[index] = graph.Send[S]{Node: ToolsNodeID, State: taskState}
			}
			return sends, nil
		}, ToolsNodeID); err != nil {
			return nil, err
		}
	}
	if len(returnDirect) == 0 {
		if err := builder.AddEdge(ToolsNodeID, entrypoint); err != nil {
			return nil, err
		}
	} else {
		if err := builder.AddConditionalEdges(ToolsNodeID, func(ctx context.Context, state S) ([]graph.NodeID, error) {
			messages, err := adapter.ToolMessages(ctx, state)
			if err != nil {
				return nil, fmt.Errorf("route tool responses: %w", err)
			}
			for index := len(messages) - 1; index >= 0; index-- {
				if _, direct := returnDirect[messages[index].Name]; direct {
					return []graph.NodeID{graph.END}, nil
				}
			}
			return []graph.NodeID{entrypoint}, nil
		}, entrypoint, graph.END); err != nil {
			return nil, err
		}
	}
	return builder.Compile(options...)
}

func reactStepsInsufficient(
	remaining int,
	calls []ToolCall,
	returnDirect map[string]struct{},
) bool {
	allReturnDirect := len(calls) > 0
	for _, call := range calls {
		if _, direct := returnDirect[call.Name]; !direct {
			allReturnDirect = false
			break
		}
	}
	if allReturnDirect {
		return remaining < 1
	}
	return remaining < 2
}

func validateReturnDirect[S, D any](tools []Tool[S, D], names []string) (map[string]struct{}, error) {
	available := make(map[string]struct{}, len(tools))
	for _, tool := range tools {
		if tool != nil {
			available[tool.Name()] = struct{}{}
		}
	}
	result := make(map[string]struct{}, len(names))
	for _, name := range names {
		if name == "" {
			return nil, fmt.Errorf("react agent ReturnDirect contains an empty tool name")
		}
		if _, duplicate := result[name]; duplicate {
			return nil, fmt.Errorf("react agent ReturnDirect contains duplicate tool %q", name)
		}
		if _, exists := available[name]; !exists {
			return nil, fmt.Errorf("react agent ReturnDirect references unknown tool %q", name)
		}
		result[name] = struct{}{}
	}
	return result, nil
}

func isNilChatModel[S any](model ChatModel[S]) bool {
	if model == nil {
		return true
	}
	value := reflect.ValueOf(model)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}

func isNilStructuredModel[S any](model StructuredModel[S]) bool {
	if model == nil {
		return true
	}
	value := reflect.ValueOf(model)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}
