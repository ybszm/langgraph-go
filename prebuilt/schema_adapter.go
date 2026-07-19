package prebuilt

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/wahanbo/langgraph-go/graph"
)

// ToolDefinition is a provider-neutral function/tool schema. InputSchema must
// be a detached JSON Schema object.
type ToolDefinition struct {
	Name        string          `json:"name"`
	Description string          `json:"description,omitempty"`
	InputSchema json.RawMessage `json:"input_schema"`
}

// ToolDefinitionProvider supplies an SDK-bindable schema for a Tool.
type ToolDefinitionProvider interface {
	ToolDefinition() ToolDefinition
}

type definedTool[S, D any] struct {
	Tool[S, D]
	definition ToolDefinition
}

func (tool definedTool[S, D]) ToolDefinition() ToolDefinition {
	return cloneToolDefinition(tool.definition)
}

// WithToolDefinition validates and attaches a JSON Schema to a Tool.
func WithToolDefinition[S, D any](tool Tool[S, D], definition ToolDefinition) (Tool[S, D], error) {
	if tool == nil || tool.Name() == "" {
		return nil, fmt.Errorf("tool definition requires a named tool")
	}
	if definition.Name == "" {
		definition.Name = tool.Name()
	}
	if definition.Name != tool.Name() {
		return nil, fmt.Errorf("tool definition name %q does not match tool %q", definition.Name, tool.Name())
	}
	if err := validateToolSchema(definition.InputSchema); err != nil {
		return nil, err
	}
	return definedTool[S, D]{Tool: tool, definition: cloneToolDefinition(definition)}, nil
}

// ToolDefinitions returns stable detached definitions. Tools without an
// explicit schema receive an open object schema for backward compatibility.
func ToolDefinitions[S, D any](tools []Tool[S, D]) ([]ToolDefinition, error) {
	result := make([]ToolDefinition, len(tools))
	for index, tool := range tools {
		if tool == nil || tool.Name() == "" {
			return nil, fmt.Errorf("tool %d is nil or unnamed", index)
		}
		definition := ToolDefinition{Name: tool.Name(), InputSchema: json.RawMessage(`{"type":"object","additionalProperties":true}`)}
		if provider, ok := tool.(ToolDefinitionProvider); ok {
			definition = provider.ToolDefinition()
		}
		if definition.Name != tool.Name() {
			return nil, fmt.Errorf("tool %q schema name is %q", tool.Name(), definition.Name)
		}
		if err := validateToolSchema(definition.InputSchema); err != nil {
			return nil, fmt.Errorf("tool %q: %w", tool.Name(), err)
		}
		result[index] = cloneToolDefinition(definition)
	}
	return result, nil
}

// ToolBindingChatModel is implemented by SDK model adapters that bind tool
// definitions at agent construction time.
type ToolBindingChatModel[S any] interface {
	ChatModel[S]
	BindTools([]ToolDefinition) (ChatModel[S], error)
}

func validateToolSchema(raw json.RawMessage) error {
	if len(raw) == 0 || !json.Valid(raw) {
		return fmt.Errorf("tool input schema is not valid JSON")
	}
	var object map[string]any
	if err := json.Unmarshal(raw, &object); err != nil || object == nil {
		return fmt.Errorf("tool input schema must be a JSON object")
	}
	if schemaType, exists := object["type"]; exists && schemaType != "object" {
		return fmt.Errorf("tool input schema type is %v, want object", schemaType)
	}
	return nil
}

func cloneToolDefinition(source ToolDefinition) ToolDefinition {
	source.InputSchema = append(json.RawMessage(nil), source.InputSchema...)
	return source
}

// HookSpec registers a hook node and may declare a node-specific input schema.
type HookSpec[S, D any] interface {
	AddHook(*graph.StateGraph[S, D], graph.NodeID) error
}

// TypedHook projects graph state into a hook-specific input type. Its schema
// is visible through compiled graph inspection.
type TypedHook[S, D, I any] struct {
	Project graph.NodeInputMapper[S, I]
	Run     graph.TypedNode[I, D]
	Options []graph.NodeOption
}

// AddHook implements HookSpec.
func (hook TypedHook[S, D, I]) AddHook(builder *graph.StateGraph[S, D], id graph.NodeID) error {
	return graph.AddTypedNode(builder, id, hook.Project, hook.Run, hook.Options...)
}

// HookFunc creates a typed hook from ordinary projection and node functions.
func HookFunc[S, D, I any](
	project func(context.Context, S) (I, error),
	run func(context.Context, I, graph.Runtime) (graph.Command[D], error),
	options ...graph.NodeOption,
) HookSpec[S, D] {
	return TypedHook[S, D, I]{Project: project, Run: run, Options: append([]graph.NodeOption(nil), options...)}
}
