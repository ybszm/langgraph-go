// Package prebuilt provides model/tool agent building blocks on top of graph.
package prebuilt

import (
	"encoding/json"

	"github.com/ybszm/langgraph-go/graph"
)

// Message is the minimal provider-neutral history contract used by the
// message reducer. SDK adapters may implement it for their own message types.
type Message = graph.Message

// UserMessage is a provider-neutral human/user message.
type UserMessage struct {
	ID            string         `json:"id,omitempty"`
	Content       string         `json:"content,omitempty"`
	ContentBlocks []ContentBlock `json:"content_blocks,omitempty"`
}

// MessageID returns the reducer identity.
func (m UserMessage) MessageID() string { return m.ID }

// WithMessageID returns a copy carrying id.
func (m UserMessage) WithMessageID(id string) Message {
	m.ID = id
	return m.CloneMessage()
}

// CloneMessage returns an independent message value.
func (m UserMessage) CloneMessage() Message {
	m.ContentBlocks = CloneContentBlocks(m.ContentBlocks)
	return m
}

// SystemMessage is a provider-neutral system instruction message.
type SystemMessage struct {
	ID            string         `json:"id,omitempty"`
	Content       string         `json:"content,omitempty"`
	ContentBlocks []ContentBlock `json:"content_blocks,omitempty"`
}

// MessageID returns the reducer identity.
func (m SystemMessage) MessageID() string { return m.ID }

// WithMessageID returns a copy carrying id.
func (m SystemMessage) WithMessageID(id string) Message {
	m.ID = id
	return m.CloneMessage()
}

// CloneMessage returns an independent message value.
func (m SystemMessage) CloneMessage() Message {
	m.ContentBlocks = CloneContentBlocks(m.ContentBlocks)
	return m
}

// ToolCall is the normalized function call requested by an assistant model.
type ToolCall struct {
	ID        string          `json:"id"`
	Name      string          `json:"name"`
	Arguments json.RawMessage `json:"arguments"`
}

// AssistantMessage is a minimal provider-neutral assistant message.
type AssistantMessage struct {
	ID            string         `json:"id,omitempty"`
	Content       string         `json:"content,omitempty"`
	ContentBlocks []ContentBlock `json:"content_blocks,omitempty"`
	ToolCalls     []ToolCall     `json:"tool_calls,omitempty"`
}

// MessageID returns the reducer identity.
func (m AssistantMessage) MessageID() string { return m.ID }

// WithMessageID returns a deep copy carrying id.
func (m AssistantMessage) WithMessageID(id string) Message {
	m.ID = id
	return m.CloneMessage()
}

// CloneMessage returns a copy with isolated tool calls and arguments.
func (m AssistantMessage) CloneMessage() Message {
	m.ToolCalls = cloneToolCalls(m.ToolCalls)
	m.ContentBlocks = CloneContentBlocks(m.ContentBlocks)
	return m
}

// ToolMessageStatus distinguishes successful and handled-error tool results.
type ToolMessageStatus string

const (
	ToolStatusSuccess ToolMessageStatus = "success"
	ToolStatusError   ToolMessageStatus = "error"
)

// ToolMessage terminates one ToolCall in provider-neutral message history.
type ToolMessage struct {
	ID            string            `json:"id,omitempty"`
	ToolCallID    string            `json:"tool_call_id"`
	Name          string            `json:"name"`
	Content       string            `json:"content"`
	ContentBlocks []ContentBlock    `json:"content_blocks,omitempty"`
	Artifact      any               `json:"artifact,omitempty"`
	Status        ToolMessageStatus `json:"status"`
}

// MessageID returns the reducer identity.
func (m ToolMessage) MessageID() string { return m.ID }

// WithMessageID returns a copy carrying id.
func (m ToolMessage) WithMessageID(id string) Message {
	m.ID = id
	return m.CloneMessage()
}

// CloneMessage returns an independent message value.
func (m ToolMessage) CloneMessage() Message {
	m.ContentBlocks = CloneContentBlocks(m.ContentBlocks)
	return m
}

// RemoveMessage deletes the existing message with the same ID.
type RemoveMessage struct {
	ID string `json:"id"`
}

// MessageID returns the target identity.
func (m RemoveMessage) MessageID() string { return m.ID }

// WithMessageID returns a removal targeting id.
func (m RemoveMessage) WithMessageID(id string) Message { m.ID = id; return m }

// CloneMessage returns an independent removal value.
func (m RemoveMessage) CloneMessage() Message { return m }

// LastAssistantToolCalls returns a defensive copy of the final assistant
// message's calls. It is convenient inside a ToolNode Adapter.Calls function.
func LastAssistantToolCalls(messages []AssistantMessage) []ToolCall {
	if len(messages) == 0 {
		return nil
	}
	return cloneToolCalls(messages[len(messages)-1].ToolCalls)
}

func cloneToolCalls(source []ToolCall) []ToolCall {
	if source == nil {
		return nil
	}
	result := make([]ToolCall, len(source))
	for index, call := range source {
		result[index] = call
		result[index].Arguments = append(json.RawMessage(nil), call.Arguments...)
	}
	return result
}
