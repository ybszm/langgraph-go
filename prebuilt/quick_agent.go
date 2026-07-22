package prebuilt

import (
	"context"
	"errors"
	"strings"

	"github.com/ybszm/langgraph-go/graph"
)

// QuickAgentConfig is the smallest batteries-included agent surface.
// Prefer ChatModelAgentConfig when you need hooks, structured output, or
// advanced React routing options.
type QuickAgentConfig struct {
	// Model is required.
	Model ChatModel[AgentState]
	// Tools is optional. Nil or empty builds a pure chat agent.
	Tools []Tool[AgentState, AgentDelta]
	// SystemPrompt is injected once at the head of the conversation.
	SystemPrompt string
	// Name defaults to "quick-agent".
	Name string
	// Compile options such as WithPersistence, WithStore, and interrupts.
	Compile []graph.CompileOption[AgentState, AgentDelta]
}

// QuickAgent is a thin facade over ChatModelAgent for the common “model + tools
// + optional system prompt” path without defining state or reducers.
type QuickAgent struct {
	inner *ChatModelAgent
}

// NewQuickAgent constructs a checkpoint-safe ReAct agent with default
// AgentState/AgentDelta. It is the recommended entry point for tutorials and
// small services.
func NewQuickAgent(config QuickAgentConfig) (*QuickAgent, error) {
	if isNilChatModel(config.Model) {
		return nil, errors.New("quick agent requires a model")
	}
	name := strings.TrimSpace(config.Name)
	if name == "" {
		name = "quick-agent"
	}
	agent, err := NewChatModelAgent(ChatModelAgentConfig{
		Name:         name,
		SystemPrompt: config.SystemPrompt,
		Model:        config.Model,
		Tools:        append([]Tool[AgentState, AgentDelta](nil), config.Tools...),
		Compile:      append([]graph.CompileOption[AgentState, AgentDelta](nil), config.Compile...),
	})
	if err != nil {
		return nil, err
	}
	return &QuickAgent{inner: agent}, nil
}

// Name returns the stable agent name.
func (agent *QuickAgent) Name() string {
	if agent == nil || agent.inner == nil {
		return ""
	}
	return agent.inner.Name()
}

// Graph exposes the compiled graph for resume, inspection, and streaming.
func (agent *QuickAgent) Graph() *graph.CompiledGraph[AgentState, AgentDelta] {
	if agent == nil || agent.inner == nil {
		return nil
	}
	return agent.inner.Graph()
}

// Run answers one user text turn.
func (agent *QuickAgent) Run(ctx context.Context, input string, config graph.RunConfig) (AgentState, error) {
	if agent == nil || agent.inner == nil {
		return AgentState{}, errors.New("quick agent is nil")
	}
	return agent.inner.Run(ctx, input, config)
}

// ChatModelAgent returns the underlying batteries-included agent.
func (agent *QuickAgent) ChatModelAgent() *ChatModelAgent {
	if agent == nil {
		return nil
	}
	return agent.inner
}
