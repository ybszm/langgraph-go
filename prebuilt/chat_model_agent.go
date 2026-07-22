package prebuilt

import (
	"context"
	"errors"
	"strings"

	"github.com/ybszm/langgraph-go/graph"
)

const defaultChatModelAgentName = "chat-model-agent"

// AgentRunnable is the common execution boundary used by coordinators and
// subagents. Both ChatModelAgent and CompiledGraph[AgentState, AgentDelta]
// satisfy it.
type AgentRunnable interface {
	Invoke(context.Context, AgentState, graph.RunConfig) (AgentState, error)
}

// ChatModelAgentConfig is the batteries-included configuration for a default
// checkpoint-safe message agent.
type ChatModelAgentConfig struct {
	Name         string
	SystemPrompt string
	Model        ChatModel[AgentState]
	Tools        []Tool[AgentState, AgentDelta]
	Callbacks    []AgentCallback
	React        ReactAgentConfig
	Compile      []graph.CompileOption[AgentState, AgentDelta]
}

// ChatModelAgent wraps the default ReAct graph with convenient text input,
// system-prompt injection, and access to the underlying compiled graph.
type ChatModelAgent struct {
	name         string
	systemPrompt string
	graph        *graph.CompiledGraph[AgentState, AgentDelta]
}

// NewChatModelAgent creates a provider-neutral chat agent without requiring
// callers to define state, reducers, or adapters.
func NewChatModelAgent(config ChatModelAgentConfig) (*ChatModelAgent, error) {
	if isNilChatModel(config.Model) {
		return nil, errors.New("chat model agent requires a model")
	}
	name := strings.TrimSpace(config.Name)
	if name == "" {
		name = defaultChatModelAgentName
	}
	callbacks := append([]AgentCallback(nil), config.Callbacks...)
	model := config.Model
	tools := append([]Tool[AgentState, AgentDelta](nil), config.Tools...)
	if len(callbacks) > 0 {
		model = callbackChatModel{name: name, model: model, callbacks: callbacks}
		var err error
		tools, err = observeAgentTools(name, tools, callbacks)
		if err != nil {
			return nil, err
		}
	}
	compiled, err := NewAgent(model, tools, config.React, config.Compile...)
	if err != nil {
		return nil, err
	}
	return &ChatModelAgent{name: name, systemPrompt: strings.TrimSpace(config.SystemPrompt), graph: compiled}, nil
}

// Name returns the stable display name used for run metadata.
func (agent *ChatModelAgent) Name() string {
	if agent == nil {
		return ""
	}
	return agent.name
}

// Graph exposes the compiled graph for streaming, resume, inspection, and
// other advanced runtime operations.
func (agent *ChatModelAgent) Graph() *graph.CompiledGraph[AgentState, AgentDelta] {
	if agent == nil {
		return nil
	}
	return agent.graph
}

// Run starts an agent from one user request.
func (agent *ChatModelAgent) Run(ctx context.Context, input string, config graph.RunConfig) (AgentState, error) {
	return agent.Invoke(ctx, NewAgentState(input), config)
}

// Stream runs one text request and emits the selected typed graph modes.
func (agent *ChatModelAgent) Stream(ctx context.Context, input string, config graph.RunConfig, options graph.StreamOptions) <-chan graph.StreamEvent[AgentState, AgentDelta] {
	return agent.StreamState(ctx, NewAgentState(input), config, options)
}

// StreamState streams an explicit agent state after prompt injection.
func (agent *ChatModelAgent) StreamState(ctx context.Context, state AgentState, config graph.RunConfig, options graph.StreamOptions) <-chan graph.StreamEvent[AgentState, AgentDelta] {
	if agent == nil || agent.graph == nil {
		return agentStreamError(errors.New("chat model agent is nil"))
	}
	state = agent.withSystemPrompt(state)
	if config.RunName == "" {
		config.RunName = agent.name
	}
	return agent.graph.StreamWithOptions(ctx, state, config, options)
}

// StreamEvents runs one text request using the graph run-event protocol.
func (agent *ChatModelAgent) StreamEvents(ctx context.Context, input string, config graph.RunConfig, version graph.StreamEventsVersion) <-chan graph.RunStreamEvent {
	if agent == nil || agent.graph == nil {
		output := make(chan graph.RunStreamEvent, 1)
		output <- graph.RunStreamEvent{Err: errors.New("chat model agent is nil")}
		close(output)
		return output
	}
	state := agent.withSystemPrompt(NewAgentState(input))
	if config.RunName == "" {
		config.RunName = agent.name
	}
	return agent.graph.StreamEvents(ctx, state, config, version)
}

// Invoke runs an explicit state after injecting the configured system prompt.
func (agent *ChatModelAgent) Invoke(ctx context.Context, state AgentState, config graph.RunConfig) (AgentState, error) {
	if agent == nil || agent.graph == nil {
		return AgentState{}, errors.New("chat model agent is nil")
	}
	state = agent.withSystemPrompt(state)
	if config.RunName == "" {
		config.RunName = agent.name
	}
	return agent.graph.Invoke(ctx, state, config)
}

func agentStreamError(err error) <-chan graph.StreamEvent[AgentState, AgentDelta] {
	events := make(chan graph.StreamEvent[AgentState, AgentDelta], 1)
	events <- graph.StreamEvent[AgentState, AgentDelta]{Mode: graph.StreamError, Step: -1, Err: err}
	close(events)
	return events
}

func (agent *ChatModelAgent) withSystemPrompt(state AgentState) AgentState {
	result := AgentState{Messages: cloneAgentMessages(state.Messages), Todos: cloneTodos(state.Todos), ActiveAgent: state.ActiveAgent}
	if agent.systemPrompt == "" {
		return result
	}
	for _, message := range result.Messages {
		if message.Role == AgentRoleSystem && message.Content == agent.systemPrompt {
			return result
		}
	}
	result.Messages = append([]AgentMessage{{Role: AgentRoleSystem, Content: agent.systemPrompt}}, result.Messages...)
	return result
}
