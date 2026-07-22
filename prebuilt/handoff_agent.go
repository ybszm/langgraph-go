package prebuilt

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/ybszm/langgraph-go/graph"
)

// HandoffAgentSpec is one directly user-facing persona in a handoff team.
type HandoffAgentSpec struct {
	Name         string
	Description  string
	SystemPrompt string
	Model        ChatModel[AgentState]
	Tools        []Tool[AgentState, AgentDelta]
}

// HandoffAgentConfig configures stateful agent-to-agent transfers. CommonTools
// are visible to every persona; persona Tools must have globally unique names.
type HandoffAgentConfig struct {
	Name         string
	SystemPrompt string
	InitialAgent string
	Agents       []HandoffAgentSpec
	CommonTools  []Tool[AgentState, AgentDelta]
	Callbacks    []AgentCallback
	React        ReactAgentConfig
	Compile      []graph.CompileOption[AgentState, AgentDelta]
}

// HandoffAgent switches the active model, prompt, and tool schemas through
// transfer tools while keeping one shared, checkpoint-safe conversation state.
type HandoffAgent struct {
	*ChatModelAgent
	initial string
}

// NewHandoffAgent creates a multi-turn handoff team. Every persona receives a
// transfer_to_<name> tool for the other configured personas.
func NewHandoffAgent(config HandoffAgentConfig) (*HandoffAgent, error) {
	if len(config.Agents) < 2 {
		return nil, errors.New("handoff agent requires at least two personas")
	}
	if strings.TrimSpace(config.Name) == "" {
		config.Name = "handoff-agent"
	}
	specs := make(map[string]HandoffAgentSpec, len(config.Agents))
	order := make([]string, 0, len(config.Agents))
	toolOwner := make(map[string]string)
	allTools := append([]Tool[AgentState, AgentDelta](nil), config.CommonTools...)
	for _, tool := range config.CommonTools {
		if tool == nil || tool.Name() == "" {
			return nil, errors.New("handoff common tools contain nil or unnamed tool")
		}
		toolOwner[tool.Name()] = "common"
	}
	for index, spec := range config.Agents {
		spec.Name = strings.TrimSpace(spec.Name)
		spec.Description = strings.TrimSpace(spec.Description)
		if !agentNamePattern.MatchString(spec.Name) {
			return nil, fmt.Errorf("handoff persona %d has invalid name %q", index, spec.Name)
		}
		if spec.Description == "" || isNilChatModel(spec.Model) {
			return nil, fmt.Errorf("handoff persona %q requires description and model", spec.Name)
		}
		if _, duplicate := specs[spec.Name]; duplicate {
			return nil, fmt.Errorf("duplicate handoff persona %q", spec.Name)
		}
		for _, tool := range spec.Tools {
			if tool == nil || tool.Name() == "" {
				return nil, fmt.Errorf("handoff persona %q has nil or unnamed tool", spec.Name)
			}
			if owner, duplicate := toolOwner[tool.Name()]; duplicate {
				return nil, fmt.Errorf("handoff tool %q is shared by %q and %q; move shared tools to CommonTools", tool.Name(), owner, spec.Name)
			}
			toolOwner[tool.Name()] = spec.Name
			allTools = append(allTools, tool)
		}
		specs[spec.Name] = spec
		order = append(order, spec.Name)
	}
	initial := strings.TrimSpace(config.InitialAgent)
	if initial == "" {
		initial = order[0]
	}
	if _, ok := specs[initial]; !ok {
		return nil, fmt.Errorf("initial handoff persona %q is not configured", initial)
	}

	transferNames := make(map[string]string, len(order))
	for _, target := range order {
		name := "transfer_to_" + strings.ReplaceAll(target, "-", "_")
		if prior, duplicate := transferNames[name]; duplicate {
			return nil, fmt.Errorf("handoff personas %q and %q produce duplicate transfer tool %q", prior, target, name)
		}
		if owner, duplicate := toolOwner[name]; duplicate {
			return nil, fmt.Errorf("transfer tool %q conflicts with tool owned by %q", name, owner)
		}
		transferNames[name] = target
		toolOwner[name] = "handoff"
		tool, err := newTransferTool(name, target)
		if err != nil {
			return nil, err
		}
		allTools = append(allTools, tool)
	}

	model := &handoffModel{initial: initial, specs: specs, order: order, transferNames: transferNames, commonTools: toolNames(config.CommonTools)}
	agent, err := NewChatModelAgent(ChatModelAgentConfig{
		Name: config.Name, SystemPrompt: config.SystemPrompt, Model: model, Tools: allTools,
		Callbacks: config.Callbacks, React: config.React, Compile: config.Compile,
	})
	if err != nil {
		return nil, fmt.Errorf("create handoff agent: %w", err)
	}
	return &HandoffAgent{ChatModelAgent: agent, initial: initial}, nil
}

func (agent *HandoffAgent) Run(ctx context.Context, input string, config graph.RunConfig) (AgentState, error) {
	return agent.Invoke(ctx, NewAgentState(input), config)
}

func (agent *HandoffAgent) Invoke(ctx context.Context, state AgentState, config graph.RunConfig) (AgentState, error) {
	if agent == nil || agent.ChatModelAgent == nil {
		return AgentState{}, errors.New("handoff agent is nil")
	}
	if state.ActiveAgent == "" {
		state.ActiveAgent = agent.initial
	}
	return agent.ChatModelAgent.Invoke(ctx, state, config)
}

func (agent *HandoffAgent) Stream(ctx context.Context, input string, config graph.RunConfig, options graph.StreamOptions) <-chan graph.StreamEvent[AgentState, AgentDelta] {
	return agent.StreamState(ctx, NewAgentState(input), config, options)
}

func (agent *HandoffAgent) StreamState(ctx context.Context, state AgentState, config graph.RunConfig, options graph.StreamOptions) <-chan graph.StreamEvent[AgentState, AgentDelta] {
	if agent == nil || agent.ChatModelAgent == nil {
		return agentStreamError(errors.New("handoff agent is nil"))
	}
	if state.ActiveAgent == "" {
		state.ActiveAgent = agent.initial
	}
	return agent.ChatModelAgent.StreamState(ctx, state, config, options)
}

type handoffModel struct {
	initial       string
	specs         map[string]HandoffAgentSpec
	order         []string
	transferNames map[string]string
	commonTools   []string
}

func (model *handoffModel) Invoke(ctx context.Context, state AgentState, runtime graph.Runtime) (AssistantMessage, error) {
	name := state.ActiveAgent
	if name == "" {
		name = model.initial
	}
	spec, ok := model.specs[name]
	if !ok {
		return AssistantMessage{}, fmt.Errorf("active handoff persona %q is not configured", name)
	}
	if prompt := strings.TrimSpace(spec.SystemPrompt); prompt != "" {
		state = cloneAgentState(state)
		state.Messages = append([]AgentMessage{{Role: AgentRoleSystem, Content: prompt}}, state.Messages...)
	}
	return spec.Model.Invoke(ctx, state, runtime)
}

func (model *handoffModel) BindTools(definitions []ToolDefinition) (ChatModel[AgentState], error) {
	cloned := &handoffModel{initial: model.initial, specs: make(map[string]HandoffAgentSpec, len(model.specs)), order: append([]string(nil), model.order...), transferNames: make(map[string]string, len(model.transferNames)), commonTools: append([]string(nil), model.commonTools...)}
	for key, value := range model.transferNames {
		cloned.transferNames[key] = value
	}
	byName := make(map[string]ToolDefinition, len(definitions))
	for _, definition := range definitions {
		byName[definition.Name] = definition
	}
	for _, name := range model.order {
		spec := model.specs[name]
		allowed := append([]string(nil), model.commonTools...)
		allowed = append(allowed, toolNames(spec.Tools)...)
		for transfer := range model.transferNames {
			allowed = append(allowed, transfer)
		}
		selected := make([]ToolDefinition, 0, len(allowed))
		for _, toolName := range allowed {
			if definition, ok := byName[toolName]; ok {
				selected = append(selected, definition)
			}
		}
		if binder, ok := spec.Model.(ToolBindingChatModel[AgentState]); ok {
			bound, err := binder.BindTools(selected)
			if err != nil {
				return nil, fmt.Errorf("bind handoff persona %q tools: %w", name, err)
			}
			spec.Model = bound
		}
		cloned.specs[name] = spec
	}
	return cloned, nil
}

func newTransferTool(name, target string) (Tool[AgentState, AgentDelta], error) {
	base := ToolFunc[AgentState, AgentDelta]{ToolName: name, Run: func(_ context.Context, call ToolCall, _ ToolRuntime[AgentState]) (ToolResult[AgentDelta], error) {
		message := agentToolMessage(ToolMessage{ToolCallID: call.ID, Name: name, Content: "Transferred to " + target, Status: ToolStatusSuccess})
		return CommandResult(graph.Update(AgentDelta{Messages: []AgentMessage{message}, ActiveAgent: target, SetActiveAgent: true})), nil
	}}
	return WithToolDefinition[AgentState, AgentDelta](base, ToolDefinition{
		Name: name, Description: "Transfer the conversation to the " + target + " agent.", InputSchema: json.RawMessage(`{"type":"object","additionalProperties":false}`),
	})
}

func toolNames(tools []Tool[AgentState, AgentDelta]) []string {
	result := make([]string, 0, len(tools))
	for _, tool := range tools {
		if tool != nil {
			result = append(result, tool.Name())
		}
	}
	return result
}
