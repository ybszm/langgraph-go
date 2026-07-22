package prebuilt

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"regexp"
	"sort"
	"strings"

	"github.com/ybszm/langgraph-go/graph"
)

const (
	// DelegateToolName is the model-visible tool used to invoke a subagent.
	DelegateToolName = "task"
	// TodoToolName is the model-visible planning tool installed by DeepAgent.
	TodoToolName = "write_todos"
	// GeneralPurposeAgentName is the default subagent installed by DeepAgent.
	GeneralPurposeAgentName = "general-purpose"
	defaultDeepAgentName    = "deep-agent"
	defaultCoordinatorName  = "multi-agent-coordinator"
)

var agentNamePattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_-]*$`)

// SubAgent describes one context-isolated worker available to a supervisor.
// Description should tell the supervisor when to choose this worker.
type SubAgent struct {
	Name        string
	Description string
	Agent       AgentRunnable
	// RunConfig optionally maps the parent runtime and model tool call into a
	// child configuration. Use it to assign durable child thread IDs or custom
	// checkpoint namespaces. Context and RunName receive safe defaults.
	RunConfig func(graph.Runtime, ToolCall) graph.RunConfig
}

// MultiAgentCoordinatorConfig configures a supervisor that delegates through
// one task tool. Multiple task calls in one model response execute concurrently
// through ToolNode while results remain in model call order.
type MultiAgentCoordinatorConfig struct {
	Name         string
	SystemPrompt string
	Model        ChatModel[AgentState]
	Agents       []SubAgent
	Tools        []Tool[AgentState, AgentDelta]
	Callbacks    []AgentCallback
	React        ReactAgentConfig
	Compile      []graph.CompileOption[AgentState, AgentDelta]
}

// MultiAgentCoordinator is a model-directed supervisor with isolated workers.
type MultiAgentCoordinator struct {
	*ChatModelAgent
	agents []SubAgent
}

// RouterAgentConfig configures one classify/fan-out/synthesize routing pass.
type RouterAgentConfig struct {
	Name         string
	SystemPrompt string
	Model        ChatModel[AgentState]
	Routes       []SubAgent
	Tools        []Tool[AgentState, AgentDelta]
	Callbacks    []AgentCallback
	React        ReactAgentConfig
	Compile      []graph.CompileOption[AgentState, AgentDelta]
}

// RouterAgent is a constrained supervisor for selective parallel routing.
type RouterAgent struct {
	*ChatModelAgent
	routes []SubAgent
}

// NewRouterAgent creates a model-directed router. The generated prompt asks
// the model to select zero or more routes once, issue selected task calls in
// parallel, and synthesize their results without multi-hop delegation.
func NewRouterAgent(config RouterAgentConfig) (*RouterAgent, error) {
	if strings.TrimSpace(config.Name) == "" {
		config.Name = "router-agent"
	}
	routes, err := validateSubAgents(config.Routes)
	if err != nil {
		return nil, err
	}
	if len(routes) == 0 {
		return nil, errors.New("router agent requires at least one route")
	}
	delegate, err := newDelegateTool(config.Name, routes, config.Callbacks)
	if err != nil {
		return nil, err
	}
	tools := append([]Tool[AgentState, AgentDelta](nil), config.Tools...)
	tools = append(tools, delegate)
	prompt := joinPrompts(config.SystemPrompt, routerPrompt(routes))
	agent, err := NewChatModelAgent(ChatModelAgentConfig{
		Name: config.Name, SystemPrompt: prompt, Model: config.Model, Tools: tools,
		Callbacks: config.Callbacks, React: config.React, Compile: config.Compile,
	})
	if err != nil {
		return nil, fmt.Errorf("create router agent: %w", err)
	}
	return &RouterAgent{ChatModelAgent: agent, routes: routes}, nil
}

// Routes returns detached route metadata.
func (agent *RouterAgent) Routes() []SubAgent {
	if agent == nil {
		return nil
	}
	return append([]SubAgent(nil), agent.routes...)
}

// NewMultiAgentCoordinator creates a supervisor whose task tool selects a
// named worker and passes only the delegated task into the worker's context.
func NewMultiAgentCoordinator(config MultiAgentCoordinatorConfig) (*MultiAgentCoordinator, error) {
	if strings.TrimSpace(config.Name) == "" {
		config.Name = defaultCoordinatorName
	}
	agents, err := validateSubAgents(config.Agents)
	if err != nil {
		return nil, err
	}
	if len(agents) == 0 {
		return nil, errors.New("multi-agent coordinator requires at least one subagent")
	}
	delegate, err := newDelegateTool(config.Name, agents, config.Callbacks)
	if err != nil {
		return nil, err
	}
	tools := append([]Tool[AgentState, AgentDelta]{}, config.Tools...)
	tools = append(tools, delegate)
	prompt := joinPrompts(config.SystemPrompt, coordinatorPrompt(agents))
	agent, err := NewChatModelAgent(ChatModelAgentConfig{
		Name: config.Name, SystemPrompt: prompt, Model: config.Model,
		Tools: tools, Callbacks: config.Callbacks, React: config.React, Compile: config.Compile,
	})
	if err != nil {
		return nil, fmt.Errorf("create multi-agent supervisor: %w", err)
	}
	return &MultiAgentCoordinator{ChatModelAgent: agent, agents: agents}, nil
}

// Agents returns detached worker metadata. Runnable values are shared because
// compiled agents are immutable after construction.
func (coordinator *MultiAgentCoordinator) Agents() []SubAgent {
	if coordinator == nil {
		return nil
	}
	return append([]SubAgent(nil), coordinator.agents...)
}

// TodoStatus is the lifecycle state of one DeepAgent planning item.
type TodoStatus string

const (
	TodoPending    TodoStatus = "pending"
	TodoInProgress TodoStatus = "in_progress"
	TodoCompleted  TodoStatus = "completed"
)

// Todo is one model-authored planning item.
type Todo struct {
	Content string     `json:"content"`
	Status  TodoStatus `json:"status"`
}

// DeepAgentConfig adds planning and optional automatic general-purpose
// delegation to the default chat-model agent.
type DeepAgentConfig struct {
	Name                  string
	SystemPrompt          string
	Model                 ChatModel[AgentState]
	Tools                 []Tool[AgentState, AgentDelta]
	SubAgents             []SubAgent
	Callbacks             []AgentCallback
	DisableGeneralPurpose bool
	React                 ReactAgentConfig
	Compile               []graph.CompileOption[AgentState, AgentDelta]
}

// DeepAgent is a batteries-included planning and delegation harness.
type DeepAgent struct {
	*ChatModelAgent
	agents []SubAgent
}

// NewDeepAgent creates an agent with write_todos and task tools. Unless
// disabled or replaced, a context-isolated general-purpose worker using the
// same model and application tools is installed automatically.
func NewDeepAgent(config DeepAgentConfig) (*DeepAgent, error) {
	if strings.TrimSpace(config.Name) == "" {
		config.Name = defaultDeepAgentName
	}
	agents := append([]SubAgent(nil), config.SubAgents...)
	validated, err := validateSubAgents(agents)
	if err != nil {
		return nil, err
	}
	if !config.DisableGeneralPurpose && !hasSubAgent(validated, GeneralPurposeAgentName) {
		worker, err := NewChatModelAgent(ChatModelAgentConfig{
			Name: GeneralPurposeAgentName, SystemPrompt: joinPrompts(config.SystemPrompt, generalPurposePrompt),
			Model: config.Model, Tools: config.Tools, Callbacks: config.Callbacks, React: config.React,
		})
		if err != nil {
			return nil, fmt.Errorf("create general-purpose subagent: %w", err)
		}
		validated = append(validated, SubAgent{
			Name: GeneralPurposeAgentName, Description: "Handles complex general tasks in an isolated context.", Agent: worker,
		})
	}
	validated, err = validateSubAgents(validated)
	if err != nil {
		return nil, err
	}

	todo, err := NewTodoListTool()
	if err != nil {
		return nil, err
	}
	tools := append([]Tool[AgentState, AgentDelta]{}, config.Tools...)
	tools = append(tools, todo)
	if len(validated) > 0 {
		delegate, delegateErr := newDelegateTool(config.Name, validated, config.Callbacks)
		if delegateErr != nil {
			return nil, delegateErr
		}
		tools = append(tools, delegate)
	}
	prompt := joinPrompts(config.SystemPrompt, deepAgentPrompt(validated))
	agent, err := NewChatModelAgent(ChatModelAgentConfig{
		Name: config.Name, SystemPrompt: prompt, Model: config.Model,
		Tools: tools, Callbacks: config.Callbacks, React: config.React, Compile: config.Compile,
	})
	if err != nil {
		return nil, fmt.Errorf("create deep agent: %w", err)
	}
	return &DeepAgent{ChatModelAgent: agent, agents: validated}, nil
}

// Agents returns the workers available to this DeepAgent.
func (agent *DeepAgent) Agents() []SubAgent {
	if agent == nil {
		return nil
	}
	return append([]SubAgent(nil), agent.agents...)
}

// NewTodoListTool creates DeepAgent's checkpoint-safe planning tool. NewAgent
// copies successful plans into AgentState.Todos and also keeps the tool message
// in conversation history.
func NewTodoListTool() (Tool[AgentState, AgentDelta], error) {
	base := ToolFunc[AgentState, AgentDelta]{ToolName: TodoToolName, Run: func(_ context.Context, call ToolCall, _ ToolRuntime[AgentState]) (ToolResult[AgentDelta], error) {
		var input struct {
			Todos []Todo `json:"todos"`
		}
		if err := json.Unmarshal(call.Arguments, &input); err != nil {
			return ToolResult[AgentDelta]{}, fmt.Errorf("decode todo plan: %w", err)
		}
		if len(input.Todos) == 0 {
			return ToolResult[AgentDelta]{}, errors.New("todo plan requires at least one item")
		}
		for index, todo := range input.Todos {
			if strings.TrimSpace(todo.Content) == "" {
				return ToolResult[AgentDelta]{}, fmt.Errorf("todo %d content is empty", index)
			}
			switch todo.Status {
			case TodoPending, TodoInProgress, TodoCompleted:
			default:
				return ToolResult[AgentDelta]{}, fmt.Errorf("todo %d has invalid status %q", index, todo.Status)
			}
		}
		encoded, err := json.Marshal(input.Todos)
		if err != nil {
			return ToolResult[AgentDelta]{}, err
		}
		return TextResult[AgentDelta](string(encoded)), nil
	}}
	return WithToolDefinition[AgentState, AgentDelta](base, ToolDefinition{
		Name:        TodoToolName,
		Description: "Create or replace the concise plan for a multi-step task. Update statuses as work progresses.",
		InputSchema: json.RawMessage(`{"type":"object","properties":{"todos":{"type":"array","minItems":1,"items":{"type":"object","properties":{"content":{"type":"string"},"status":{"type":"string","enum":["pending","in_progress","completed"]}},"required":["content","status"],"additionalProperties":false}}},"required":["todos"],"additionalProperties":false}`),
	})
}

func newDelegateTool(supervisor string, agents []SubAgent, callbacks []AgentCallback) (Tool[AgentState, AgentDelta], error) {
	byName := make(map[string]SubAgent, len(agents))
	names := make([]string, 0, len(agents))
	for _, agent := range agents {
		byName[agent.Name] = agent
		names = append(names, agent.Name)
	}
	schema, err := json.Marshal(map[string]any{
		"type": "object",
		"properties": map[string]any{
			"agent": map[string]any{"type": "string", "enum": names, "description": "Worker selected for this task."},
			"task":  map[string]any{"type": "string", "description": "Self-contained task and expected output."},
		},
		"required":             []string{"agent", "task"},
		"additionalProperties": false,
	})
	if err != nil {
		return nil, err
	}
	base := ToolFunc[AgentState, AgentDelta]{ToolName: DelegateToolName, Run: func(ctx context.Context, call ToolCall, runtime ToolRuntime[AgentState]) (ToolResult[AgentDelta], error) {
		var request struct {
			Agent string `json:"agent"`
			Task  string `json:"task"`
		}
		if err := json.Unmarshal(call.Arguments, &request); err != nil {
			return ToolResult[AgentDelta]{}, fmt.Errorf("decode delegated task: %w", err)
		}
		worker, ok := byName[request.Agent]
		if !ok {
			return ToolResult[AgentDelta]{}, fmt.Errorf("unknown subagent %q", request.Agent)
		}
		request.Task = strings.TrimSpace(request.Task)
		if request.Task == "" {
			return ToolResult[AgentDelta]{}, errors.New("delegated task is empty")
		}
		notifyDelegationStart(ctx, callbacks, AgentDelegationStartEvent{
			Supervisor: supervisor, SubAgent: request.Agent, Task: request.Task, Call: cloneToolCall(call), Runtime: runtime.Graph,
		})
		childConfig := subAgentRunConfig(runtime.Graph, request.Agent)
		if worker.RunConfig != nil {
			childConfig = worker.RunConfig(runtime.Graph, call)
			if childConfig.Context == nil {
				childConfig.Context = runtime.Graph.Context
			}
			if childConfig.RunName == "" {
				childConfig.RunName = request.Agent
			}
		}
		state, err := invokeSubAgent(ctx, worker.Agent, NewAgentState(request.Task), childConfig, runtime.Graph, request.Agent)
		if err != nil {
			notifyDelegationError(ctx, callbacks, AgentDelegationErrorEvent{
				Supervisor: supervisor, SubAgent: request.Agent, Task: request.Task, Call: cloneToolCall(call), Err: err, Runtime: runtime.Graph,
			})
			return ToolResult[AgentDelta]{}, fmt.Errorf("subagent %q: %w", request.Agent, err)
		}
		notifyDelegationEnd(ctx, callbacks, AgentDelegationEndEvent{
			Supervisor: supervisor, SubAgent: request.Agent, Task: request.Task, Call: cloneToolCall(call), Output: cloneAgentState(state), Runtime: runtime.Graph,
		})
		response := state.FinalResponse()
		if response == "" {
			return ToolResult[AgentDelta]{}, fmt.Errorf("subagent %q returned no assistant response", request.Agent)
		}
		return TextResult[AgentDelta](response), nil
	}}
	return WithToolDefinition[AgentState, AgentDelta](base, ToolDefinition{
		Name:        DelegateToolName,
		Description: "Delegate a self-contained task to one specialized worker. Emit multiple task calls together when work can run in parallel.",
		InputSchema: schema,
	})
}

// SubAgentStreamEvent wraps custom data forwarded from a delegated agent.
type SubAgentStreamEvent struct {
	Agent     string
	Namespace []string
	Data      any
}

func invokeSubAgent(ctx context.Context, agent AgentRunnable, input AgentState, config graph.RunConfig, parent graph.Runtime, name string) (AgentState, error) {
	streamer, ok := agent.(interface {
		StreamState(context.Context, AgentState, graph.RunConfig, graph.StreamOptions) <-chan graph.StreamEvent[AgentState, AgentDelta]
	})
	if !ok {
		return agent.Invoke(ctx, input, config)
	}
	var final AgentState
	done := false
	stream := streamer.StreamState(ctx, input, config, graph.StreamOptions{
		Modes:     []graph.StreamMode{graph.StreamMessages, graph.StreamCustom, graph.StreamDone, graph.StreamError, graph.StreamInterrupt},
		Subgraphs: true,
	})
	for event := range stream {
		switch event.Mode {
		case graph.StreamMessages:
			if event.Message == nil {
				continue
			}
			metadata := cloneStreamMetadata(event.Message.Metadata)
			metadata["langgraph_subagent"] = name
			if event.Message.ContentBlock != nil {
				if err := parent.WriteContentBlock(*event.Message.ContentBlock, metadata); err != nil {
					return AgentState{}, err
				}
			} else if err := parent.WriteMessage(event.Message.Message, metadata); err != nil {
				return AgentState{}, err
			}
		case graph.StreamCustom:
			if err := parent.WriteCustom(SubAgentStreamEvent{Agent: name, Namespace: append([]string(nil), event.Namespace...), Data: event.Custom}); err != nil {
				return AgentState{}, err
			}
		case graph.StreamDone:
			final, done = event.State, true
		case graph.StreamError:
			return event.State, event.Err
		case graph.StreamInterrupt:
			return event.State, &graph.GraphInterruptError{Interrupts: append([]graph.Interrupt(nil), event.Interrupts...)}
		}
	}
	if !done {
		return AgentState{}, errors.New("subagent stream ended without a terminal event")
	}
	return final, nil
}

func cloneStreamMetadata(source map[string]any) map[string]any {
	result := make(map[string]any, len(source)+1)
	for key, value := range source {
		result[key] = value
	}
	return result
}

func notifyDelegationStart(ctx context.Context, callbacks []AgentCallback, event AgentDelegationStartEvent) {
	for _, callback := range callbacks {
		if observer, ok := callback.(AgentDelegationCallback); callback != nil && ok {
			observer.OnAgentDelegationStart(ctx, event)
		}
	}
}
func notifyDelegationEnd(ctx context.Context, callbacks []AgentCallback, event AgentDelegationEndEvent) {
	for _, callback := range callbacks {
		if observer, ok := callback.(AgentDelegationCallback); callback != nil && ok {
			observer.OnAgentDelegationEnd(ctx, event)
		}
	}
}
func notifyDelegationError(ctx context.Context, callbacks []AgentCallback, event AgentDelegationErrorEvent) {
	for _, callback := range callbacks {
		if observer, ok := callback.(AgentDelegationCallback); callback != nil && ok {
			observer.OnAgentDelegationError(ctx, event)
		}
	}
}

func validateSubAgents(source []SubAgent) ([]SubAgent, error) {
	result := append([]SubAgent(nil), source...)
	seen := make(map[string]struct{}, len(result))
	for index := range result {
		result[index].Name = strings.TrimSpace(result[index].Name)
		result[index].Description = strings.TrimSpace(result[index].Description)
		if !agentNamePattern.MatchString(result[index].Name) {
			return nil, fmt.Errorf("subagent %d has invalid name %q", index, result[index].Name)
		}
		if result[index].Description == "" {
			return nil, fmt.Errorf("subagent %q requires a description", result[index].Name)
		}
		if isNilAgentRunnable(result[index].Agent) {
			return nil, fmt.Errorf("subagent %q requires an agent", result[index].Name)
		}
		if _, duplicate := seen[result[index].Name]; duplicate {
			return nil, fmt.Errorf("duplicate subagent %q", result[index].Name)
		}
		seen[result[index].Name] = struct{}{}
	}
	return result, nil
}

func hasSubAgent(agents []SubAgent, name string) bool {
	for _, agent := range agents {
		if agent.Name == name {
			return true
		}
	}
	return false
}

func subAgentRunConfig(runtime graph.Runtime, agentName string) graph.RunConfig {
	return graph.RunConfig{Context: runtime.Context, RunName: agentName}
}

func isNilAgentRunnable(agent AgentRunnable) bool {
	if agent == nil {
		return true
	}
	value := reflect.ValueOf(agent)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}

func coordinatorPrompt(agents []SubAgent) string {
	return "You are a supervisor. Break the request into self-contained tasks, delegate each task to the best worker with the task tool, run independent tasks in parallel, and synthesize the worker results into one final answer.\n\nAvailable workers:\n" + formatSubAgents(agents)
}

func routerPrompt(routes []SubAgent) string {
	return "You are a router. Classify the request, select only relevant routes, and emit all selected task calls together in the first routing step. After route results return, synthesize one final answer without delegating again. If no route is relevant, answer directly.\n\nAvailable routes:\n" + formatSubAgents(routes)
}

func deepAgentPrompt(agents []SubAgent) string {
	prompt := "You are a deep agent for complex, multi-step work. Use write_todos for tasks that benefit from planning, keep the plan current, use tools to gather evidence, and return a concise final result."
	if len(agents) > 0 {
		prompt += " Delegate context-heavy or specialized work with the task tool; send independent task calls together, then synthesize their results.\n\nAvailable workers:\n" + formatSubAgents(agents)
	}
	return prompt
}

func formatSubAgents(agents []SubAgent) string {
	lines := make([]string, len(agents))
	for index, agent := range agents {
		lines[index] = "- " + agent.Name + ": " + agent.Description
	}
	sort.Strings(lines)
	return strings.Join(lines, "\n")
}

func joinPrompts(parts ...string) string {
	nonempty := make([]string, 0, len(parts))
	for _, part := range parts {
		if trimmed := strings.TrimSpace(part); trimmed != "" {
			nonempty = append(nonempty, trimmed)
		}
	}
	return strings.Join(nonempty, "\n\n")
}

const generalPurposePrompt = "Work autonomously on the delegated task. Use available tools as needed and return only the result needed by the parent agent, without raw intermediate output."
