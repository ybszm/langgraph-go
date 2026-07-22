package main

import (
	"context"
	"fmt"

	"github.com/ybszm/langgraph-go/graph"
	"github.com/ybszm/langgraph-go/prebuilt"
)

type model struct{}

func (model) Invoke(_ context.Context, _ prebuilt.AgentState, runtime graph.Runtime) (prebuilt.AssistantMessage, error) {
	_ = runtime.WriteCustom(map[string]any{"stage": "thinking"})
	_ = runtime.WriteMessage(prebuilt.AssistantMessage{ID: "chunk", Content: "partial"}, map[string]any{"provider": "local"})
	return prebuilt.AssistantMessage{ID: "final", Content: "complete"}, nil
}

func main() {
	agent, _ := prebuilt.NewChatModelAgent(prebuilt.ChatModelAgentConfig{Name: "streamer", Model: model{}})
	events := agent.Stream(context.Background(), "go", graph.RunConfig{}, graph.StreamOptions{Modes: []graph.StreamMode{graph.StreamMessages, graph.StreamCustom, graph.StreamDone}})
	for event := range events {
		switch event.Mode {
		case graph.StreamMessages:
			fmt.Printf("message: %+v\n", event.Message.Message)
		case graph.StreamCustom:
			fmt.Printf("custom: %+v\n", event.Custom)
		case graph.StreamDone:
			fmt.Println("done:", event.State.FinalResponse())
		}
	}
}
