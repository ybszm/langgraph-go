package prebuilt_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/ybszm/langgraph-go/graph"
	"github.com/ybszm/langgraph-go/prebuilt"
)

type bindingModel struct {
	definitions *[]prebuilt.ToolDefinition
}

func (model bindingModel) Invoke(context.Context, reactState, graph.Runtime) (prebuilt.AssistantMessage, error) {
	return prebuilt.AssistantMessage{}, errors.New("unbound model invoked")
}

func (model bindingModel) BindTools(definitions []prebuilt.ToolDefinition) (prebuilt.ChatModel[reactState], error) {
	*model.definitions = definitions
	return prebuilt.ChatModelFunc[reactState]{Run: func(context.Context, reactState, graph.Runtime) (prebuilt.AssistantMessage, error) {
		return prebuilt.AssistantMessage{ID: "bound", Content: "done"}, nil
	}}, nil
}

type hookInput struct {
	AssistantCount int `json:"assistant_count"`
}

func TestCreateReactAgentBindsDetachedToolSchemasAndTypedHookInput(t *testing.T) {
	schema := json.RawMessage(`{"type":"object","properties":{"city":{"type":"string"}},"required":["city"]}`)
	baseTool := prebuilt.ToolFunc[reactState, reactDelta]{ToolName: "weather", Run: func(context.Context, prebuilt.ToolCall, prebuilt.ToolRuntime[reactState]) (prebuilt.ToolResult[reactDelta], error) {
		return prebuilt.TextResult[reactDelta]("sunny"), nil
	}}
	tool, err := prebuilt.WithToolDefinition[reactState, reactDelta](baseTool, prebuilt.ToolDefinition{
		Description: "Look up weather", InputSchema: schema,
	})
	if err != nil {
		t.Fatal(err)
	}
	var definitions []prebuilt.ToolDefinition
	adapter := reactAdapter()
	adapter.PreModelHookSpec = prebuilt.HookFunc[reactState, reactDelta, hookInput](
		func(_ context.Context, state reactState) (hookInput, error) {
			return hookInput{AssistantCount: len(state.Assistant)}, nil
		},
		func(_ context.Context, input hookInput, _ graph.Runtime) (graph.Command[reactDelta], error) {
			return graph.Update(reactDelta{Event: string(rune('0' + input.AssistantCount))}), nil
		},
	)
	agent, err := prebuilt.CreateReactAgent[reactState, reactDelta](
		bindingModel{definitions: &definitions}, []prebuilt.Tool[reactState, reactDelta]{tool},
		reactReducer, adapter, prebuilt.ReactAgentConfig{},
	)
	if err != nil {
		t.Fatal(err)
	}
	schema[0] = '['
	if len(definitions) != 1 || definitions[0].Name != "weather" || definitions[0].Description != "Look up weather" ||
		!json.Valid(definitions[0].InputSchema) {
		t.Fatalf("definitions=%+v", definitions)
	}
	view := agent.Inspect()
	var hookSchema string
	for _, node := range view.Nodes {
		if node.ID == prebuilt.PreModelHookNodeID {
			hookSchema = node.InputSchema
		}
	}
	if hookSchema != "prebuilt_test.hookInput" {
		t.Fatalf("hook input schema=%q nodes=%+v", hookSchema, view.Nodes)
	}
	result, err := agent.Invoke(context.Background(), reactState{}, graph.RunConfig{})
	if err != nil || len(result.Assistant) != 1 || result.Assistant[0].Content != "done" || len(result.Events) != 1 {
		t.Fatalf("result=%+v err=%v", result, err)
	}
}

func TestToolSchemaAndHookValidation(t *testing.T) {
	base := prebuilt.ToolFunc[reactState, reactDelta]{ToolName: "tool", Run: func(context.Context, prebuilt.ToolCall, prebuilt.ToolRuntime[reactState]) (prebuilt.ToolResult[reactDelta], error) {
		return prebuilt.TextResult[reactDelta]("ok"), nil
	}}
	if _, err := prebuilt.WithToolDefinition[reactState, reactDelta](base, prebuilt.ToolDefinition{Name: "other", InputSchema: json.RawMessage(`{}`)}); err == nil {
		t.Fatal("mismatched tool schema name accepted")
	}
	if _, err := prebuilt.WithToolDefinition[reactState, reactDelta](base, prebuilt.ToolDefinition{InputSchema: json.RawMessage(`{"type":"array"}`)}); err == nil {
		t.Fatal("non-object tool schema accepted")
	}
	adapter := reactAdapter()
	adapter.PreModelHook = func(context.Context, reactState, graph.Runtime) (graph.Command[reactDelta], error) {
		return graph.NoCommand[reactDelta](), nil
	}
	adapter.PreModelHookSpec = prebuilt.HookFunc[reactState, reactDelta, hookInput](nil, nil)
	model := prebuilt.ChatModelFunc[reactState]{Run: func(context.Context, reactState, graph.Runtime) (prebuilt.AssistantMessage, error) {
		return prebuilt.AssistantMessage{}, nil
	}}
	if _, err := prebuilt.CreateReactAgent(model, nil, reactReducer, adapter, prebuilt.ReactAgentConfig{}); err == nil {
		t.Fatal("conflicting hook declarations accepted")
	}
}
