package prebuilt

import (
	"context"
	"errors"

	"github.com/ybszm/langgraph-go/graph"
)

// AgentHarness is the common high-level contract implemented by
// ChatModelAgent and every harness that embeds it, including DeepAgent,
// RouterAgent, MultiAgentCoordinator, and HandoffAgent.
type AgentHarness interface {
	AgentRunnable
	Name() string
	Graph() *graph.CompiledGraph[AgentState, AgentDelta]
	StreamState(context.Context, AgentState, graph.RunConfig, graph.StreamOptions) <-chan graph.StreamEvent[AgentState, AgentDelta]
}

// AgentEventKind identifies the high-level payload emitted by AgentRunner.
type AgentEventKind string

const (
	// AgentEventMessage carries a provider-neutral message or content-block
	// chunk emitted by a model, tool, or delegated child agent.
	AgentEventMessage AgentEventKind = "message"
	// AgentEventCustom carries application data written through Runtime.
	AgentEventCustom AgentEventKind = "custom"
	// AgentEventInterrupt reports durable human input requested by the agent.
	AgentEventInterrupt AgentEventKind = "interrupt"
	// AgentEventDone carries the final checkpoint-safe AgentState.
	AgentEventDone AgentEventKind = "done"
	// AgentEventError is the terminal failure event.
	AgentEventError AgentEventKind = "error"
	// AgentEventGraph preserves explicitly requested low-level graph modes such
	// as values, updates, and debug without leaking them into the default API.
	AgentEventGraph AgentEventKind = "graph"
)

// AgentEvent is the batteries-included streaming envelope returned by Query
// and Resume. Kind selects the populated payload.
type AgentEvent struct {
	Kind       AgentEventKind
	Step       int
	State      AgentState
	Message    *graph.MessageStreamEvent
	Custom     any
	Interrupts []graph.Interrupt
	Err        error
	Namespace  []string
	GraphMode  graph.StreamMode
	// Subgraph preserves a heterogeneous child event when the caller opts into
	// nested graph visibility.
	Subgraph *graph.SubgraphStreamEvent
}

// AgentRunnerConfig configures a reusable high-level execution facade.
// RunConfig is copied for each call; callers may safely reuse the runner from
// concurrent goroutines when the configured model, tools, and callbacks are
// themselves concurrency-safe.
type AgentRunnerConfig struct {
	Agent         AgentHarness
	RunConfig     graph.RunConfig
	StreamOptions graph.StreamOptions
}

// AgentRunner provides Eino-style Run, Query, and Resume entry points without
// requiring callers to select graph stream modes or access CompiledGraph.
type AgentRunner struct {
	agent         AgentHarness
	runConfig     graph.RunConfig
	streamOptions graph.StreamOptions
}

// NewAgentRunner validates and creates a high-level runner.
func NewAgentRunner(config AgentRunnerConfig) (*AgentRunner, error) {
	if isNilAgentHarness(config.Agent) {
		return nil, errors.New("agent runner requires an agent")
	}
	if config.Agent.Graph() == nil {
		return nil, errors.New("agent runner requires an agent with a compiled graph")
	}
	options := cloneAgentStreamOptions(config.StreamOptions)
	if len(options.Modes) == 0 {
		options.Modes = []graph.StreamMode{graph.StreamMessages, graph.StreamCustom, graph.StreamDone}
	}
	runConfig := cloneAgentRunConfig(config.RunConfig)
	if runConfig.RunName == "" {
		runConfig.RunName = config.Agent.Name()
	}
	return &AgentRunner{
		agent:         config.Agent,
		runConfig:     runConfig,
		streamOptions: options,
	}, nil
}

// Agent returns the immutable harness executed by this runner.
func (runner *AgentRunner) Agent() AgentHarness {
	if runner == nil {
		return nil
	}
	return runner.agent
}

// Run executes one text request and returns the final state.
func (runner *AgentRunner) Run(ctx context.Context, input string) (AgentState, error) {
	return runner.RunState(ctx, NewAgentState(input))
}

// RunState executes an explicit state with the runner's copied RunConfig.
func (runner *AgentRunner) RunState(ctx context.Context, state AgentState) (AgentState, error) {
	if runner == nil || isNilAgentHarness(runner.agent) {
		return AgentState{}, errors.New("agent runner is nil")
	}
	return runner.agent.Invoke(ctx, state, cloneAgentRunConfig(runner.runConfig))
}

// Query streams one text request as high-level AgentEvents.
// Consumers that stop early must cancel ctx so graph and child-agent writers
// can exit without blocking.
func (runner *AgentRunner) Query(ctx context.Context, input string) <-chan AgentEvent {
	return runner.QueryState(ctx, NewAgentState(input))
}

// QueryState streams an explicit checkpoint-safe state.
func (runner *AgentRunner) QueryState(ctx context.Context, state AgentState) <-chan AgentEvent {
	if runner == nil || isNilAgentHarness(runner.agent) {
		return terminalAgentEvent(errors.New("agent runner is nil"))
	}
	stream := runner.agent.StreamState(
		ctx,
		state,
		cloneAgentRunConfig(runner.runConfig),
		cloneAgentStreamOptions(runner.streamOptions),
	)
	return mapAgentEvents(ctx, stream)
}

// Resume continues the latest interrupted checkpoint selected by RunConfig.
func (runner *AgentRunner) Resume(ctx context.Context, command graph.ResumeCommand) <-chan AgentEvent {
	if runner == nil || isNilAgentHarness(runner.agent) || runner.agent.Graph() == nil {
		return terminalAgentEvent(errors.New("agent runner is nil"))
	}
	stream := runner.agent.Graph().ResumeStreamWithOptions(
		ctx,
		cloneAgentRunConfig(runner.runConfig),
		command,
		cloneAgentStreamOptions(runner.streamOptions),
	)
	return mapAgentEvents(ctx, stream)
}

func mapAgentEvents(ctx context.Context, source <-chan graph.StreamEvent[AgentState, AgentDelta]) <-chan AgentEvent {
	output := make(chan AgentEvent, 1)
	go func() {
		defer close(output)
		for event := range source {
			mapped := AgentEvent{
				Step:       event.Step,
				State:      cloneAgentState(event.State),
				Namespace:  append([]string(nil), event.Namespace...),
				GraphMode:  event.Mode,
				Subgraph:   event.Subgraph,
				Interrupts: append([]graph.Interrupt(nil), event.Interrupts...),
			}
			switch event.Mode {
			case graph.StreamMessages:
				mapped.Kind, mapped.Message = AgentEventMessage, event.Message
			case graph.StreamCustom:
				mapped.Kind, mapped.Custom = AgentEventCustom, event.Custom
			case graph.StreamInterrupt:
				mapped.Kind = AgentEventInterrupt
			case graph.StreamDone:
				mapped.Kind = AgentEventDone
			case graph.StreamError:
				mapped.Kind, mapped.Err = AgentEventError, event.Err
			default:
				mapped.Kind = AgentEventGraph
			}
			select {
			case output <- mapped:
			case <-ctx.Done():
				return
			}
		}
	}()
	return output
}

func terminalAgentEvent(err error) <-chan AgentEvent {
	output := make(chan AgentEvent, 1)
	output <- AgentEvent{Kind: AgentEventError, Step: -1, Err: err}
	close(output)
	return output
}

func cloneAgentRunConfig(source graph.RunConfig) graph.RunConfig {
	result := source
	result.Tags = append([]string(nil), source.Tags...)
	if source.Metadata != nil {
		result.Metadata = make(map[string]any, len(source.Metadata))
		for key, value := range source.Metadata {
			result.Metadata[key] = value
		}
	}
	result.Callbacks = append([]graph.GraphCallback(nil), source.Callbacks...)
	return result
}

func cloneAgentStreamOptions(source graph.StreamOptions) graph.StreamOptions {
	result := source
	result.Modes = append([]graph.StreamMode(nil), source.Modes...)
	return result
}

func isNilAgentHarness(agent AgentHarness) bool {
	return isNilAgentRunnable(agent)
}
