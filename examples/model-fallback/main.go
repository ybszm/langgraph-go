package main

import (
	"context"
	"errors"
	"fmt"

	"github.com/ybszm/langgraph-go/graph"
	"github.com/ybszm/langgraph-go/prebuilt"
)

func main() {
	model := prebuilt.FallbackChatModel[prebuilt.AgentState]{Models: []prebuilt.ChatModel[prebuilt.AgentState]{
		prebuilt.ChatModelFunc[prebuilt.AgentState]{Run: func(context.Context, prebuilt.AgentState, graph.Runtime) (prebuilt.AssistantMessage, error) {
			return prebuilt.AssistantMessage{}, errors.New("primary unavailable")
		}},
		prebuilt.ChatModelFunc[prebuilt.AgentState]{Run: func(context.Context, prebuilt.AgentState, graph.Runtime) (prebuilt.AssistantMessage, error) {
			return prebuilt.AssistantMessage{Content: "fallback response"}, nil
		}},
	}}
	agent, _ := prebuilt.NewChatModelAgent(prebuilt.ChatModelAgentConfig{Model: model})
	result, err := agent.Run(context.Background(), "hello", graph.RunConfig{})
	if err != nil {
		panic(err)
	}
	fmt.Println(result.FinalResponse())
}
