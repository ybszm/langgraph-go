package prebuilt

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/wahanbo/langgraph-go/graph"
)

// AgentToolInput maps a parent tool call into child graph state and run config.
type AgentToolInput[ParentState, ChildState any] func(context.Context, ToolCall, ToolRuntime[ParentState]) (ChildState, graph.RunConfig, error)

// AgentToolOutput converts completed child state into a parent tool result.
type AgentToolOutput[ParentDelta, ChildState any] func(context.Context, ChildState) (ToolResult[ParentDelta], error)

// AgentToolConfig wraps any compiled graph as a tool for another agent.
type AgentToolConfig[ParentState, ParentDelta, ChildState, ChildDelta any] struct {
	Name        string
	Description string
	InputSchema json.RawMessage
	Agent       *graph.CompiledGraph[ChildState, ChildDelta]
	Input       AgentToolInput[ParentState, ChildState]
	Output      AgentToolOutput[ParentDelta, ChildState]
}

// AgentAsTool creates a schema-bearing tool that invokes a child graph. The
// mapper owns thread IDs and namespace isolation when the child is persistent.
func AgentAsTool[ParentState, ParentDelta, ChildState, ChildDelta any](config AgentToolConfig[ParentState, ParentDelta, ChildState, ChildDelta]) (Tool[ParentState, ParentDelta], error) {
	if config.Name == "" || config.Agent == nil || config.Input == nil || config.Output == nil {
		return nil, errors.New("agent tool requires name, compiled agent, input mapper, and output mapper")
	}
	if !json.Valid(config.InputSchema) {
		return nil, errors.New("agent tool input schema is invalid")
	}
	base := ToolFunc[ParentState, ParentDelta]{ToolName: config.Name, Run: func(ctx context.Context, call ToolCall, runtime ToolRuntime[ParentState]) (ToolResult[ParentDelta], error) {
		input, runConfig, err := config.Input(ctx, call, runtime)
		if err != nil {
			return ToolResult[ParentDelta]{}, fmt.Errorf("map agent tool %q input: %w", config.Name, err)
		}
		if runConfig.Context == nil {
			runConfig.Context = runtime.Graph.Context
		}
		output, err := config.Agent.Invoke(ctx, input, runConfig)
		if err != nil {
			return ToolResult[ParentDelta]{}, fmt.Errorf("invoke agent tool %q: %w", config.Name, err)
		}
		result, err := config.Output(ctx, output)
		if err != nil {
			return ToolResult[ParentDelta]{}, fmt.Errorf("map agent tool %q output: %w", config.Name, err)
		}
		return result, nil
	}}
	return WithToolDefinition[ParentState, ParentDelta](base, ToolDefinition{Name: config.Name, Description: config.Description, InputSchema: config.InputSchema})
}
