// Package memory provides provider-neutral context projection middleware.
//
// Middleware in this package changes only the state passed to the next model
// invocation. It does not mutate durable graph or checkpoint history.
package memory

import (
	"context"
	"errors"
	"fmt"

	"github.com/wahanbo/langgraph-go/prebuilt"
)

// MessageAdapter reads provider-neutral messages from an application state and
// returns a copy of that state with a replacement model context.
type MessageAdapter[S any] struct {
	Messages     func(context.Context, S) ([]prebuilt.Message, error)
	WithMessages func(S, []prebuilt.Message) (S, error)
}

func (adapter MessageAdapter[S]) validate() error {
	if adapter.Messages == nil || adapter.WithMessages == nil {
		return errors.New("memory message adapter requires Messages and WithMessages")
	}
	return nil
}

// AgentStateAdapter adapts prebuilt.AgentState without sharing mutable message
// slices with the original state.
func AgentStateAdapter() MessageAdapter[prebuilt.AgentState] {
	return MessageAdapter[prebuilt.AgentState]{
		Messages: prebuilt.AgentModelMessages,
		WithMessages: func(state prebuilt.AgentState, messages []prebuilt.Message) (prebuilt.AgentState, error) {
			converted := make([]prebuilt.AgentMessage, len(messages))
			for index, message := range messages {
				switch value := message.(type) {
				case prebuilt.SystemMessage:
					converted[index] = prebuilt.AgentMessage{Role: prebuilt.AgentRoleSystem, ID: value.ID, Content: value.Content, ContentBlocks: prebuilt.CloneContentBlocks(value.ContentBlocks)}
				case prebuilt.UserMessage:
					converted[index] = prebuilt.AgentMessage{Role: prebuilt.AgentRoleUser, ID: value.ID, Content: value.Content, ContentBlocks: prebuilt.CloneContentBlocks(value.ContentBlocks)}
				case prebuilt.AssistantMessage:
					converted[index] = prebuilt.AgentMessage{Role: prebuilt.AgentRoleAssistant, ID: value.ID, Content: value.Content, ContentBlocks: prebuilt.CloneContentBlocks(value.ContentBlocks), ToolCalls: cloneToolCalls(value.ToolCalls)}
				case prebuilt.ToolMessage:
					converted[index] = prebuilt.AgentMessage{Role: prebuilt.AgentRoleTool, ID: value.ID, Content: value.Content, ContentBlocks: prebuilt.CloneContentBlocks(value.ContentBlocks), ToolCallID: value.ToolCallID, Name: value.Name, Status: value.Status, Artifact: value.Artifact}
				default:
					return state, fmt.Errorf("memory adapter: unsupported message %T at index %d", message, index)
				}
			}
			state.Messages = converted
			return state, nil
		},
	}
}

func cloneMessages(source []prebuilt.Message) []prebuilt.Message {
	result := make([]prebuilt.Message, len(source))
	for index, message := range source {
		if message != nil {
			result[index] = message.CloneMessage()
		}
	}
	return result
}

func cloneToolCalls(source []prebuilt.ToolCall) []prebuilt.ToolCall {
	result := make([]prebuilt.ToolCall, len(source))
	for index, call := range source {
		result[index] = call
		result[index].Arguments = append([]byte(nil), call.Arguments...)
	}
	return result
}
