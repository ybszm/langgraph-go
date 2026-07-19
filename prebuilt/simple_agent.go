package prebuilt

import (
	"context"
	"fmt"

	"github.com/ybszm/langgraph-go/graph"
)

// AgentRole is the serializable role of one message in the simplified agent
// state. It avoids interface-typed history so JSON checkpoint codecs can round
// trip a default agent without application-specific adapters.
type AgentRole string

const (
	AgentRoleSystem    AgentRole = "system"
	AgentRoleUser      AgentRole = "user"
	AgentRoleAssistant AgentRole = "assistant"
	AgentRoleTool      AgentRole = "tool"
)

// AgentMessage is the checkpoint-safe union used by AgentState.
type AgentMessage struct {
	Role          AgentRole         `json:"role"`
	ID            string            `json:"id,omitempty"`
	Content       string            `json:"content,omitempty"`
	ContentBlocks []ContentBlock    `json:"content_blocks,omitempty"`
	ToolCalls     []ToolCall        `json:"tool_calls,omitempty"`
	ToolCallID    string            `json:"tool_call_id,omitempty"`
	Name          string            `json:"name,omitempty"`
	Status        ToolMessageStatus `json:"status,omitempty"`
	Artifact      any               `json:"artifact,omitempty"`
}

// AgentState is the default chronological message state used by NewAgent.
type AgentState struct {
	Messages []AgentMessage `json:"messages"`
}

// AgentDelta appends chronological messages to AgentState.
type AgentDelta struct {
	Messages []AgentMessage `json:"messages"`
}

// NewAgentState creates one state containing a user message.
func NewAgentState(input string) AgentState {
	return AgentState{Messages: []AgentMessage{{Role: AgentRoleUser, Content: input}}}
}

// ProviderMessages converts checkpoint-safe history to provider-neutral model
// messages. Returned values do not share mutable slices with state.
func (state AgentState) ProviderMessages() ([]Message, error) {
	result := make([]Message, len(state.Messages))
	for index, message := range state.Messages {
		switch message.Role {
		case AgentRoleSystem:
			result[index] = SystemMessage{ID: message.ID, Content: message.Content, ContentBlocks: CloneContentBlocks(message.ContentBlocks)}
		case AgentRoleUser:
			result[index] = UserMessage{ID: message.ID, Content: message.Content, ContentBlocks: CloneContentBlocks(message.ContentBlocks)}
		case AgentRoleAssistant:
			result[index] = AssistantMessage{ID: message.ID, Content: message.Content, ContentBlocks: CloneContentBlocks(message.ContentBlocks), ToolCalls: cloneToolCalls(message.ToolCalls)}
		case AgentRoleTool:
			result[index] = ToolMessage{ID: message.ID, ToolCallID: message.ToolCallID, Name: message.Name, Content: message.Content, ContentBlocks: CloneContentBlocks(message.ContentBlocks), Artifact: message.Artifact, Status: message.Status}
		default:
			return nil, fmt.Errorf("agent message %d has unknown role %q", index, message.Role)
		}
	}
	return result, nil
}

// AgentModelMessages adapts AgentState for provider model Config.Messages.
func AgentModelMessages(_ context.Context, state AgentState) ([]Message, error) {
	return state.ProviderMessages()
}

// NewAgent builds a ReAct agent with the default serializable message state,
// reducer, history validation, and tool adapters. Advanced applications can
// continue to use CreateReactAgent with their own state and delta types.
func NewAgent(
	model ChatModel[AgentState],
	tools []Tool[AgentState, AgentDelta],
	config ReactAgentConfig,
	options ...graph.CompileOption[AgentState, AgentDelta],
) (*graph.CompiledGraph[AgentState, AgentDelta], error) {
	adapter := ReactAgentAdapter[AgentState, AgentDelta]{
		ModelMessage: func(_ context.Context, _ AgentState, message AssistantMessage) (AgentDelta, error) {
			return AgentDelta{Messages: []AgentMessage{agentAssistantMessage(message)}}, nil
		},
		ToolNode: ToolNodeAdapter[AgentState, AgentDelta]{
			Calls: func(_ context.Context, state AgentState) ([]ToolCall, error) {
				for index := len(state.Messages) - 1; index >= 0; index-- {
					if state.Messages[index].Role == AgentRoleAssistant {
						return cloneToolCalls(state.Messages[index].ToolCalls), nil
					}
				}
				return nil, nil
			},
			Messages: func(_ context.Context, _ AgentState, messages []ToolMessage) (AgentDelta, error) {
				delta := AgentDelta{Messages: make([]AgentMessage, len(messages))}
				for index, message := range messages {
					delta.Messages[index] = agentToolMessage(message)
				}
				return delta, nil
			},
		},
		ToolMessages: func(_ context.Context, state AgentState) ([]ToolMessage, error) {
			result := make([]ToolMessage, 0)
			for _, message := range state.Messages {
				if message.Role == AgentRoleTool {
					result = append(result, ToolMessage{ID: message.ID, ToolCallID: message.ToolCallID, Name: message.Name, Content: message.Content, ContentBlocks: CloneContentBlocks(message.ContentBlocks), Artifact: message.Artifact, Status: message.Status})
				}
			}
			return result, nil
		},
		ChatHistory: func(_ context.Context, state AgentState) ([]AssistantMessage, []ToolMessage, error) {
			assistants := make([]AssistantMessage, 0)
			tools := make([]ToolMessage, 0)
			for _, message := range state.Messages {
				switch message.Role {
				case AgentRoleAssistant:
					assistants = append(assistants, AssistantMessage{ID: message.ID, Content: message.Content, ContentBlocks: CloneContentBlocks(message.ContentBlocks), ToolCalls: cloneToolCalls(message.ToolCalls)})
				case AgentRoleTool:
					tools = append(tools, ToolMessage{ID: message.ID, ToolCallID: message.ToolCallID, Name: message.Name, Content: message.Content, ContentBlocks: CloneContentBlocks(message.ContentBlocks), Artifact: message.Artifact, Status: message.Status})
				}
			}
			return assistants, tools, nil
		},
	}
	return CreateReactAgent(model, tools, reduceAgentState, adapter, config, options...)
}

func reduceAgentState(_ context.Context, state AgentState, updates []AgentDelta) (AgentState, error) {
	result := AgentState{Messages: cloneAgentMessages(state.Messages)}
	for _, update := range updates {
		result.Messages = append(result.Messages, cloneAgentMessages(update.Messages)...)
	}
	return result, nil
}

func agentAssistantMessage(message AssistantMessage) AgentMessage {
	return AgentMessage{Role: AgentRoleAssistant, ID: message.ID, Content: message.Content, ContentBlocks: CloneContentBlocks(message.ContentBlocks), ToolCalls: cloneToolCalls(message.ToolCalls)}
}

func agentToolMessage(message ToolMessage) AgentMessage {
	return AgentMessage{Role: AgentRoleTool, ID: message.ID, ToolCallID: message.ToolCallID, Name: message.Name, Content: message.Content, ContentBlocks: CloneContentBlocks(message.ContentBlocks), Artifact: message.Artifact, Status: message.Status}
}

func cloneAgentMessages(source []AgentMessage) []AgentMessage {
	result := make([]AgentMessage, len(source))
	for index, message := range source {
		result[index] = message
		result[index].ContentBlocks = CloneContentBlocks(message.ContentBlocks)
		result[index].ToolCalls = cloneToolCalls(message.ToolCalls)
	}
	return result
}
