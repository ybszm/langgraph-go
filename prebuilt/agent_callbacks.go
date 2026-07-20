package prebuilt

import (
	"context"

	"github.com/ybszm/langgraph-go/graph"
)

// AgentCallback is a marker for observers that implement one or more of
// AgentModelCallback, AgentToolCallback, or AgentDelegationCallback.
type AgentCallback interface{}

type AgentModelStartEvent struct {
	Agent   string
	State   AgentState
	Runtime graph.Runtime
}

type AgentModelEndEvent struct {
	Agent   string
	State   AgentState
	Message AssistantMessage
	Runtime graph.Runtime
}

type AgentModelErrorEvent struct {
	Agent   string
	State   AgentState
	Err     error
	Runtime graph.Runtime
}

type AgentToolStartEvent struct {
	Agent   string
	Call    ToolCall
	State   AgentState
	Runtime graph.Runtime
}

type AgentToolEndEvent struct {
	Agent   string
	Call    ToolCall
	Result  ToolResult[AgentDelta]
	Runtime graph.Runtime
}

type AgentToolErrorEvent struct {
	Agent   string
	Call    ToolCall
	Err     error
	Runtime graph.Runtime
}

type AgentDelegationStartEvent struct {
	Supervisor string
	SubAgent   string
	Task       string
	Call       ToolCall
	Runtime    graph.Runtime
}

type AgentDelegationEndEvent struct {
	Supervisor string
	SubAgent   string
	Task       string
	Output     AgentState
	Call       ToolCall
	Runtime    graph.Runtime
}

type AgentDelegationErrorEvent struct {
	Supervisor string
	SubAgent   string
	Task       string
	Err        error
	Call       ToolCall
	Runtime    graph.Runtime
}

// AgentModelCallback observes individual model calls.
type AgentModelCallback interface {
	OnAgentModelStart(context.Context, AgentModelStartEvent)
	OnAgentModelEnd(context.Context, AgentModelEndEvent)
	OnAgentModelError(context.Context, AgentModelErrorEvent)
}

// AgentToolCallback observes individual tool calls, including calls executed
// concurrently in one ToolNode super-step.
type AgentToolCallback interface {
	OnAgentToolStart(context.Context, AgentToolStartEvent)
	OnAgentToolEnd(context.Context, AgentToolEndEvent)
	OnAgentToolError(context.Context, AgentToolErrorEvent)
}

// AgentDelegationCallback observes supervisor-to-worker boundaries.
type AgentDelegationCallback interface {
	OnAgentDelegationStart(context.Context, AgentDelegationStartEvent)
	OnAgentDelegationEnd(context.Context, AgentDelegationEndEvent)
	OnAgentDelegationError(context.Context, AgentDelegationErrorEvent)
}

// AgentCallbackFuncs adapts optional functions to all agent callback types.
type AgentCallbackFuncs struct {
	ModelStart      func(context.Context, AgentModelStartEvent)
	ModelEnd        func(context.Context, AgentModelEndEvent)
	ModelError      func(context.Context, AgentModelErrorEvent)
	ToolStart       func(context.Context, AgentToolStartEvent)
	ToolEnd         func(context.Context, AgentToolEndEvent)
	ToolError       func(context.Context, AgentToolErrorEvent)
	DelegationStart func(context.Context, AgentDelegationStartEvent)
	DelegationEnd   func(context.Context, AgentDelegationEndEvent)
	DelegationError func(context.Context, AgentDelegationErrorEvent)
}

func (callback AgentCallbackFuncs) OnAgentModelStart(ctx context.Context, event AgentModelStartEvent) {
	if callback.ModelStart != nil {
		callback.ModelStart(ctx, event)
	}
}
func (callback AgentCallbackFuncs) OnAgentModelEnd(ctx context.Context, event AgentModelEndEvent) {
	if callback.ModelEnd != nil {
		callback.ModelEnd(ctx, event)
	}
}
func (callback AgentCallbackFuncs) OnAgentModelError(ctx context.Context, event AgentModelErrorEvent) {
	if callback.ModelError != nil {
		callback.ModelError(ctx, event)
	}
}
func (callback AgentCallbackFuncs) OnAgentToolStart(ctx context.Context, event AgentToolStartEvent) {
	if callback.ToolStart != nil {
		callback.ToolStart(ctx, event)
	}
}
func (callback AgentCallbackFuncs) OnAgentToolEnd(ctx context.Context, event AgentToolEndEvent) {
	if callback.ToolEnd != nil {
		callback.ToolEnd(ctx, event)
	}
}
func (callback AgentCallbackFuncs) OnAgentToolError(ctx context.Context, event AgentToolErrorEvent) {
	if callback.ToolError != nil {
		callback.ToolError(ctx, event)
	}
}
func (callback AgentCallbackFuncs) OnAgentDelegationStart(ctx context.Context, event AgentDelegationStartEvent) {
	if callback.DelegationStart != nil {
		callback.DelegationStart(ctx, event)
	}
}
func (callback AgentCallbackFuncs) OnAgentDelegationEnd(ctx context.Context, event AgentDelegationEndEvent) {
	if callback.DelegationEnd != nil {
		callback.DelegationEnd(ctx, event)
	}
}
func (callback AgentCallbackFuncs) OnAgentDelegationError(ctx context.Context, event AgentDelegationErrorEvent) {
	if callback.DelegationError != nil {
		callback.DelegationError(ctx, event)
	}
}

type callbackChatModel struct {
	name      string
	model     ChatModel[AgentState]
	callbacks []AgentCallback
}

func (model callbackChatModel) Invoke(ctx context.Context, state AgentState, runtime graph.Runtime) (AssistantMessage, error) {
	for _, callback := range model.callbacks {
		if observer, ok := callback.(AgentModelCallback); callback != nil && ok {
			observer.OnAgentModelStart(ctx, AgentModelStartEvent{Agent: model.name, State: cloneAgentState(state), Runtime: runtime})
		}
	}
	message, err := model.model.Invoke(ctx, state, runtime)
	if err != nil {
		for _, callback := range model.callbacks {
			if observer, ok := callback.(AgentModelCallback); callback != nil && ok {
				observer.OnAgentModelError(ctx, AgentModelErrorEvent{Agent: model.name, State: cloneAgentState(state), Err: err, Runtime: runtime})
			}
		}
		return AssistantMessage{}, err
	}
	message = cloneAssistantMessage(message)
	for _, callback := range model.callbacks {
		if observer, ok := callback.(AgentModelCallback); callback != nil && ok {
			observer.OnAgentModelEnd(ctx, AgentModelEndEvent{Agent: model.name, State: cloneAgentState(state), Message: cloneAssistantMessage(message), Runtime: runtime})
		}
	}
	return message, nil
}

func (model callbackChatModel) BindTools(definitions []ToolDefinition) (ChatModel[AgentState], error) {
	binder, ok := model.model.(ToolBindingChatModel[AgentState])
	if !ok {
		return model, nil
	}
	bound, err := binder.BindTools(definitions)
	if err != nil {
		return nil, err
	}
	model.model = bound
	return model, nil
}

type callbackTool struct {
	name      string
	tool      Tool[AgentState, AgentDelta]
	callbacks []AgentCallback
}

func (tool callbackTool) Name() string { return tool.tool.Name() }
func (tool callbackTool) Invoke(ctx context.Context, call ToolCall, runtime ToolRuntime[AgentState]) (ToolResult[AgentDelta], error) {
	for _, callback := range tool.callbacks {
		if observer, ok := callback.(AgentToolCallback); callback != nil && ok {
			observer.OnAgentToolStart(ctx, AgentToolStartEvent{Agent: tool.name, Call: cloneToolCall(call), State: cloneAgentState(runtime.State), Runtime: runtime.Graph})
		}
	}
	result, err := tool.tool.Invoke(ctx, call, runtime)
	if err != nil {
		for _, callback := range tool.callbacks {
			if observer, ok := callback.(AgentToolCallback); callback != nil && ok {
				observer.OnAgentToolError(ctx, AgentToolErrorEvent{Agent: tool.name, Call: cloneToolCall(call), Err: err, Runtime: runtime.Graph})
			}
		}
		return ToolResult[AgentDelta]{}, err
	}
	for _, callback := range tool.callbacks {
		if observer, ok := callback.(AgentToolCallback); callback != nil && ok {
			observer.OnAgentToolEnd(ctx, AgentToolEndEvent{Agent: tool.name, Call: cloneToolCall(call), Result: cloneAgentToolResult(result), Runtime: runtime.Graph})
		}
	}
	return result, nil
}

func observeAgentTools(name string, tools []Tool[AgentState, AgentDelta], callbacks []AgentCallback) ([]Tool[AgentState, AgentDelta], error) {
	if len(callbacks) == 0 {
		return append([]Tool[AgentState, AgentDelta](nil), tools...), nil
	}
	definitions, err := ToolDefinitions(tools)
	if err != nil {
		return nil, err
	}
	result := make([]Tool[AgentState, AgentDelta], len(tools))
	for index, tool := range tools {
		result[index], err = WithToolDefinition[AgentState, AgentDelta](callbackTool{name: name, tool: tool, callbacks: callbacks}, definitions[index])
		if err != nil {
			return nil, err
		}
	}
	return result, nil
}

func cloneAgentState(state AgentState) AgentState {
	return AgentState{Messages: cloneAgentMessages(state.Messages), Todos: cloneTodos(state.Todos), ActiveAgent: state.ActiveAgent}
}

func cloneAssistantMessage(message AssistantMessage) AssistantMessage {
	message.ToolCalls = cloneToolCalls(message.ToolCalls)
	message.ContentBlocks = CloneContentBlocks(message.ContentBlocks)
	return message
}

func cloneToolCall(call ToolCall) ToolCall {
	call.Arguments = append([]byte(nil), call.Arguments...)
	return call
}

func cloneAgentToolResult(result ToolResult[AgentDelta]) ToolResult[AgentDelta] {
	cloned := ToolResult[AgentDelta]{}
	if result.Message != nil {
		message := *result.Message
		message.ContentBlocks = CloneContentBlocks(message.ContentBlocks)
		cloned.Message = &message
	}
	if result.Command != nil {
		command := *result.Command
		command.Goto = append([]graph.NodeID(nil), command.Goto...)
		command.Sends = append([]graph.TaskSend(nil), command.Sends...)
		if command.HasUpdate {
			command.Update.Messages = cloneAgentMessages(command.Update.Messages)
			command.Update.Todos = cloneTodos(command.Update.Todos)
		}
		cloned.Command = &command
	}
	return cloned
}
