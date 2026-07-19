package prebuilt

import (
	"errors"
	"fmt"
)

// ErrInvalidChatHistory indicates assistant tool calls without corresponding
// tool results. Most chat model providers reject such a history.
var ErrInvalidChatHistory = errors.New("invalid chat history")

// ValidateChatHistory requires every ToolCall in assistant history to have a
// ToolMessage with the same call ID. Tool result order is not significant.
func ValidateChatHistory(assistants []AssistantMessage, tools []ToolMessage) error {
	results := make(map[string]struct{}, len(tools))
	for _, message := range tools {
		results[message.ToolCallID] = struct{}{}
	}
	missing := make([]ToolCall, 0)
	for _, message := range assistants {
		for _, call := range message.ToolCalls {
			if _, exists := results[call.ID]; !exists {
				missing = append(missing, call)
			}
		}
	}
	if len(missing) == 0 {
		return nil
	}
	preview := missing
	if len(preview) > 3 {
		preview = preview[:3]
	}
	return fmt.Errorf(
		"%w: assistant tool calls have no corresponding ToolMessage: %+v",
		ErrInvalidChatHistory,
		preview,
	)
}
