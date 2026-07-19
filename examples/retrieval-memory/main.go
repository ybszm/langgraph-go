package main

import (
	"context"
	"fmt"
	"strings"

	"github.com/ybszm/langgraph-go/graph"
	"github.com/ybszm/langgraph-go/memory"
	"github.com/ybszm/langgraph-go/prebuilt"
	"github.com/ybszm/langgraph-go/retrieval"
)

type localModel struct{}

func (localModel) Invoke(_ context.Context, state prebuilt.AgentState, _ graph.Runtime) (prebuilt.AssistantMessage, error) {
	for _, message := range state.Messages {
		if message.Role == prebuilt.AgentRoleSystem && strings.Contains(message.Content, "retrieved context") {
			return prebuilt.AssistantMessage{Content: "Grounded answer from injected context: checkpoints persist graph state."}, nil
		}
	}
	return prebuilt.AssistantMessage{Content: "No relevant context found."}, nil
}

func main() {
	index, err := retrieval.NewBM25([]retrieval.Document{
		{ID: "checkpoint-guide", Content: "Checkpoints persist graph state and enable resume after interruption.", Metadata: map[string]any{"source": "guide"}},
		{ID: "streaming-guide", Content: "Message streams expose model chunks as they arrive.", Metadata: map[string]any{"source": "guide"}},
	}, retrieval.BM25Options{})
	if err != nil {
		panic(err)
	}

	adapter := memory.AgentStateAdapter()
	rag, err := memory.NewRetrieval(adapter, index, func(_ context.Context, state prebuilt.AgentState) (retrieval.Query, error) {
		return retrieval.Query{Text: state.Messages[len(state.Messages)-1].Content, Limit: 2}, nil
	})
	if err != nil {
		panic(err)
	}
	window, err := memory.NewWindow(adapter, 2)
	if err != nil {
		panic(err)
	}
	model, err := prebuilt.WrapChatModel[prebuilt.AgentState](localModel{}, rag, window)
	if err != nil {
		panic(err)
	}

	state := prebuilt.AgentState{Messages: []prebuilt.AgentMessage{
		{Role: prebuilt.AgentRoleSystem, Content: "Answer only from retrieved context."},
		{Role: prebuilt.AgentRoleUser, Content: "An old unrelated question"},
		{Role: prebuilt.AgentRoleAssistant, Content: "An old unrelated answer"},
		{Role: prebuilt.AgentRoleUser, Content: "How can graph state survive an interruption?"},
	}}
	answer, err := model.Invoke(context.Background(), state, graph.Runtime{})
	if err != nil {
		panic(err)
	}
	fmt.Println(answer.Content)
}
