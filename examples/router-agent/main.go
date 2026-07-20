package main

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/ybszm/langgraph-go/graph"
	"github.com/ybszm/langgraph-go/prebuilt"
)

type routeModel struct{ result string }

func (model routeModel) Invoke(context.Context, prebuilt.AgentState, graph.Runtime) (prebuilt.AssistantMessage, error) {
	return prebuilt.AssistantMessage{Content: model.result}, nil
}

type routerModel struct{}

func (routerModel) Invoke(_ context.Context, state prebuilt.AgentState, _ graph.Runtime) (prebuilt.AssistantMessage, error) {
	results := []string{}
	for _, message := range state.Messages {
		if message.Role == prebuilt.AgentRoleTool {
			results = append(results, message.Content)
		}
	}
	if len(results) > 0 {
		return prebuilt.AssistantMessage{Content: "combined: " + strings.Join(results, " + ")}, nil
	}
	return prebuilt.AssistantMessage{ToolCalls: []prebuilt.ToolCall{
		{ID: "docs", Name: prebuilt.DelegateToolName, Arguments: json.RawMessage(`{"agent":"docs","task":"search docs"}`)},
		{ID: "code", Name: prebuilt.DelegateToolName, Arguments: json.RawMessage(`{"agent":"code","task":"search code"}`)},
	}}, nil
}

func child(name, result string) *prebuilt.ChatModelAgent {
	agent, _ := prebuilt.NewChatModelAgent(prebuilt.ChatModelAgentConfig{Name: name, Model: routeModel{result: result}})
	return agent
}
func main() {
	router, err := prebuilt.NewRouterAgent(prebuilt.RouterAgentConfig{Model: routerModel{}, Routes: []prebuilt.SubAgent{
		{Name: "docs", Description: "Searches documentation.", Agent: child("docs", "documentation result")},
		{Name: "code", Description: "Searches source code.", Agent: child("code", "code result")},
	}})
	if err != nil {
		panic(err)
	}
	result, err := router.Run(context.Background(), "How does streaming work?", graph.RunConfig{})
	if err != nil {
		panic(err)
	}
	fmt.Println(result.FinalResponse())
}
