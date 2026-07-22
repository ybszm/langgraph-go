package main

import (
	"context"
	"fmt"

	"github.com/ybszm/langgraph-go/graph"
	"github.com/ybszm/langgraph-go/prebuilt"
)

type model struct{}

func (model) Invoke(_ context.Context, _ prebuilt.AgentState, runtime graph.Runtime) (prebuilt.AssistantMessage, error) {
	if err := runtime.WriteMessage(prebuilt.AssistantMessage{ID: "chunk", Content: "hello"}); err != nil {
		return prebuilt.AssistantMessage{}, err
	}
	return prebuilt.AssistantMessage{ID: "final", Content: "hello from AgentRunner"}, nil
}

func main() {
	agent, err := prebuilt.NewChatModelAgent(prebuilt.ChatModelAgentConfig{
		Name:         "assistant",
		SystemPrompt: "Answer concisely.",
		Model:        model{},
	})
	if err != nil {
		panic(err)
	}
	runner, err := prebuilt.NewAgentRunner(prebuilt.AgentRunnerConfig{Agent: agent})
	if err != nil {
		panic(err)
	}
	for event := range runner.Query(context.Background(), "Say hello") {
		switch event.Kind {
		case prebuilt.AgentEventMessage:
			if message, ok := event.Message.Message.(prebuilt.AssistantMessage); ok {
				fmt.Println("chunk:", message.Content)
			}
		case prebuilt.AgentEventDone:
			fmt.Println("final:", event.State.FinalResponse())
		case prebuilt.AgentEventError:
			panic(event.Err)
		}
	}
}
