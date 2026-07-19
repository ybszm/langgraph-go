package prebuilt

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"

	"github.com/wahanbo/langgraph-go/graph"
)

// ToolCallChunk is one positional fragment of a streamed tool call.
type ToolCallChunk struct {
	Index     int
	ID        string
	Name      string
	Arguments string
}

// AssistantMessageChunk is a provider-neutral streamed assistant fragment.
// Metadata remains open for finish reasons, token usage, and provider fields.
type AssistantMessageChunk struct {
	ID               string
	Content          string
	ContentBlocks    []ContentBlock
	ToolCallChunks   []ToolCallChunk
	ResponseMetadata map[string]any
}

func (chunk AssistantMessageChunk) MessageID() string { return chunk.ID }
func (chunk AssistantMessageChunk) WithMessageID(id string) graph.Message {
	chunk.ID = id
	return chunk.CloneMessage()
}
func (chunk AssistantMessageChunk) CloneMessage() graph.Message {
	chunk.ContentBlocks = CloneContentBlocks(chunk.ContentBlocks)
	chunk.ToolCallChunks = append([]ToolCallChunk(nil), chunk.ToolCallChunks...)
	chunk.ResponseMetadata = cloneChunkMetadata(chunk.ResponseMetadata)
	return chunk
}

// MessageChunkAdapter converts one concrete provider SDK chunk.
type MessageChunkAdapter[C any] interface {
	AdaptMessageChunk(context.Context, C) (AssistantMessageChunk, error)
}

// MessageChunkAdapterFunc adapts a function to MessageChunkAdapter.
type MessageChunkAdapterFunc[C any] func(context.Context, C) (AssistantMessageChunk, error)

func (adapter MessageChunkAdapterFunc[C]) AdaptMessageChunk(ctx context.Context, chunk C) (AssistantMessageChunk, error) {
	return adapter(ctx, chunk)
}

// NativeStreamingChatModel is a ChatModel that can expose provider chunks to
// a direct caller while also publishing them through graph message streams.
// A nil emit callback is valid and still produces graph stream events when the
// supplied Runtime has a message writer.
type NativeStreamingChatModel[S any] interface {
	ChatModel[S]
	Stream(context.Context, S, graph.Runtime, func(AssistantMessageChunk) error) (AssistantMessage, error)
}

// StreamingChatModel adapts an SDK streaming callback into ChatModel. Every
// chunk is published through Runtime.WriteMessage and merged into one final
// AssistantMessage for graph state.
type StreamingChatModel[S, C any] struct {
	Stream   func(context.Context, S, graph.Runtime, func(C) error) error
	Adapter  MessageChunkAdapter[C]
	Metadata map[string]any
	// Emit observes normalized chunks after a stable message ID is assigned.
	// Returning an error cancels assembly and the model invocation.
	Emit func(AssistantMessageChunk) error
}

// Invoke implements ChatModel.
func (model StreamingChatModel[S, C]) Invoke(
	ctx context.Context,
	state S,
	runtime graph.Runtime,
) (AssistantMessage, error) {
	if model.Stream == nil || model.Adapter == nil {
		return AssistantMessage{}, fmt.Errorf("streaming chat model requires Stream and Adapter")
	}
	chunks := make([]AssistantMessageChunk, 0)
	messageID := ""
	err := model.Stream(ctx, state, runtime, func(providerChunk C) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		chunk, err := model.Adapter.AdaptMessageChunk(ctx, providerChunk)
		if err != nil {
			return fmt.Errorf("adapt provider message chunk: %w", err)
		}
		if chunk.ID != "" {
			if messageID != "" && chunk.ID != messageID {
				return fmt.Errorf("provider message chunk ID changed %q -> %q", messageID, chunk.ID)
			}
			messageID = chunk.ID
		} else {
			if messageID == "" {
				messageID, err = randomChunkMessageID()
				if err != nil {
					return err
				}
			}
			chunk.ID = messageID
		}
		cloned := chunk.CloneMessage().(AssistantMessageChunk)
		if model.Emit != nil {
			if err := model.Emit(cloned); err != nil {
				return err
			}
		}
		chunks = append(chunks, cloned)
		return runtime.WriteMessage(cloned, cloneChunkMetadata(model.Metadata))
	})
	if err != nil {
		return AssistantMessage{}, err
	}
	return MergeAssistantMessageChunks(chunks)
}

// MergeAssistantMessageChunks deterministically assembles content and
// positional tool-call argument fragments, validating final JSON arguments.
func MergeAssistantMessageChunks(chunks []AssistantMessageChunk) (AssistantMessage, error) {
	result := AssistantMessage{}
	type callAccumulator struct {
		id, name, arguments string
	}
	calls := make(map[int]callAccumulator)
	for chunkIndex, chunk := range chunks {
		if chunk.ID != "" {
			if result.ID != "" && chunk.ID != result.ID {
				return AssistantMessage{}, fmt.Errorf("assistant chunk %d ID %q conflicts with %q", chunkIndex, chunk.ID, result.ID)
			}
			result.ID = chunk.ID
		}
		result.Content += chunk.Content
		result.ContentBlocks = append(result.ContentBlocks, CloneContentBlocks(chunk.ContentBlocks)...)
		for _, fragment := range chunk.ToolCallChunks {
			if fragment.Index < 0 {
				return AssistantMessage{}, fmt.Errorf("assistant chunk %d has negative tool-call index", chunkIndex)
			}
			call := calls[fragment.Index]
			if fragment.ID != "" {
				if call.id != "" && call.id != fragment.ID {
					return AssistantMessage{}, fmt.Errorf("tool-call %d ID changed %q -> %q", fragment.Index, call.id, fragment.ID)
				}
				call.id = fragment.ID
			}
			if fragment.Name != "" {
				if call.name != "" && call.name != fragment.Name {
					return AssistantMessage{}, fmt.Errorf("tool-call %d name changed %q -> %q", fragment.Index, call.name, fragment.Name)
				}
				call.name = fragment.Name
			}
			call.arguments += fragment.Arguments
			calls[fragment.Index] = call
		}
	}
	indexes := make([]int, 0, len(calls))
	for index := range calls {
		indexes = append(indexes, index)
	}
	sort.Ints(indexes)
	for _, index := range indexes {
		call := calls[index]
		if call.id == "" || call.name == "" {
			return AssistantMessage{}, fmt.Errorf("tool-call %d is missing ID or name", index)
		}
		if call.arguments == "" {
			call.arguments = "{}"
		}
		arguments := json.RawMessage(call.arguments)
		if !json.Valid(arguments) {
			return AssistantMessage{}, fmt.Errorf("tool-call %d arguments are not valid JSON", index)
		}
		result.ToolCalls = append(result.ToolCalls, ToolCall{ID: call.id, Name: call.name, Arguments: append(json.RawMessage(nil), arguments...)})
	}
	return result, nil
}

func randomChunkMessageID() (string, error) {
	value := make([]byte, 16)
	if _, err := rand.Read(value); err != nil {
		return "", err
	}
	return "msg-" + hex.EncodeToString(value), nil
}

func cloneChunkMetadata(source map[string]any) map[string]any {
	if source == nil {
		return nil
	}
	result := make(map[string]any, len(source))
	for key, value := range source {
		switch typed := value.(type) {
		case map[string]any:
			result[key] = cloneChunkMetadata(typed)
		case []byte:
			result[key] = append([]byte(nil), typed...)
		case []any:
			result[key] = append([]any(nil), typed...)
		default:
			result[key] = value
		}
	}
	return result
}

var _ graph.Message = AssistantMessageChunk{}
