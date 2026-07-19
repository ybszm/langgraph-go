package graph

import (
	"context"
	"crypto/rand"
	"fmt"

	"github.com/ybszm/langgraph-go/checkpoint"
)

var rawMessageRoles = map[string]struct{}{
	"user": {}, "human": {}, "assistant": {}, "ai": {}, "tool": {},
	"system": {}, "function": {},
}

var rawMessageTypes = map[string]struct{}{
	"human": {}, "ai": {}, "tool": {}, "system": {}, "function": {}, "remove": {},
}

func recognizedRawMessage(message any) (map[string]any, bool) {
	value, ok := message.(map[string]any)
	if !ok {
		return nil, false
	}
	if role, ok := value["role"].(string); ok {
		if _, known := rawMessageRoles[role]; known {
			return value, true
		}
	}
	if messageType, ok := value["type"].(string); ok {
		if _, known := rawMessageTypes[messageType]; known {
			return value, true
		}
	}
	return nil, false
}

func streamMessageID(message any) string {
	if identified, ok := message.(MessageIdentifier); ok {
		return identified.MessageID()
	}
	if raw, ok := recognizedRawMessage(message); ok {
		id, _ := raw["id"].(string)
		return id
	}
	return ""
}

func randomMessageID() (string, error) {
	value := make([]byte, 16)
	if _, err := rand.Read(value); err != nil {
		return "", err
	}
	value[6] = (value[6] & 0x0f) | 0x40
	value[8] = (value[8] & 0x3f) | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", value[0:4], value[4:6], value[6:8], value[8:10], value[10:16]), nil
}

func normalizeStreamMessage(message any, generator MessageIDGenerator) (any, error) {
	typed, ok := message.(Message)
	if ok {
		cloned := typed.CloneMessage()
		if cloned == nil {
			return nil, fmt.Errorf("clone stream message: nil message")
		}
		if cloned.MessageID() != "" {
			return cloned, nil
		}
		id, err := generator()
		if err != nil {
			return nil, fmt.Errorf("generate stream message ID: %w", err)
		}
		if id == "" {
			return nil, fmt.Errorf("generate stream message ID: empty ID")
		}
		identified := cloned.WithMessageID(id)
		if identified == nil || identified.MessageID() == "" {
			return nil, fmt.Errorf("assign stream message ID: empty ID")
		}
		return identified, nil
	}
	raw, ok := recognizedRawMessage(message)
	if !ok {
		return message, nil
	}
	cloned := cloneMessageMetadata(raw)
	if value, exists := cloned["id"]; exists && value != nil {
		id, valid := value.(string)
		if !valid {
			return nil, fmt.Errorf("stream message ID has type %T, want string", value)
		}
		if id != "" {
			return cloned, nil
		}
	}
	id, err := generator()
	if err != nil {
		return nil, fmt.Errorf("generate stream message ID: %w", err)
	}
	if id == "" {
		return nil, fmt.Errorf("generate stream message ID: empty ID")
	}
	cloned["id"] = id
	return cloned, nil
}

// NormalizeMessageValue clones and assigns IDs to provider-neutral Message
// values and recognized raw role/type maps. It also accepts []Message, []any,
// and []map[string]any while preserving the input container shape. Unrelated
// values are returned unchanged. It is intended for DeltaNormalizer adapters
// that run before durable pending writes are encoded.
func NormalizeMessageValue(value any, generator MessageIDGenerator) (any, error) {
	if generator == nil {
		generator = randomMessageID
	}
	switch typed := value.(type) {
	case []Message:
		if typed == nil {
			return []Message(nil), nil
		}
		result := make([]Message, len(typed))
		for index, message := range typed {
			if message == nil {
				return nil, fmt.Errorf("normalize message %d: nil message", index)
			}
			normalized, err := normalizeStreamMessage(message, generator)
			if err != nil {
				return nil, fmt.Errorf("normalize message %d: %w", index, err)
			}
			result[index] = normalized.(Message)
		}
		return result, nil
	case []map[string]any:
		if typed == nil {
			return []map[string]any(nil), nil
		}
		result := make([]map[string]any, len(typed))
		for index, message := range typed {
			normalized, err := normalizeStreamMessage(message, generator)
			if err != nil {
				return nil, fmt.Errorf("normalize message %d: %w", index, err)
			}
			result[index] = normalized.(map[string]any)
		}
		return result, nil
	case []any:
		if typed == nil {
			return []any(nil), nil
		}
		result := make([]any, len(typed))
		for index, message := range typed {
			normalized, err := normalizeStreamMessage(message, generator)
			if err != nil {
				return nil, fmt.Errorf("normalize message %d: %w", index, err)
			}
			result[index] = normalized
		}
		return result, nil
	default:
		return normalizeStreamMessage(value, generator)
	}
}

func cloneMessageMetadata(source map[string]any) map[string]any {
	if source == nil {
		return nil
	}
	result := make(map[string]any, len(source))
	for key, value := range source {
		result[key] = cloneMessageMetadataValue(value)
	}
	return result
}

func cloneMessageMetadataValue(value any) any {
	switch typed := value.(type) {
	case map[string]any:
		return cloneMessageMetadata(typed)
	case []string:
		return append([]string(nil), typed...)
	case []any:
		result := make([]any, len(typed))
		for index, item := range typed {
			result[index] = cloneMessageMetadataValue(item)
		}
		return result
	default:
		return value
	}
}

func cloneMessageStreamEvent(source *MessageStreamEvent) *MessageStreamEvent {
	if source == nil {
		return nil
	}
	return &MessageStreamEvent{
		Message: source.Message, ContentBlock: cloneContentBlockStreamEvent(source.ContentBlock),
		Metadata: cloneMessageMetadata(source.Metadata),
		dedupe:   source.dedupe, remember: source.remember,
	}
}

func cloneContentBlockStreamEvent(source *ContentBlockStreamEvent) *ContentBlockStreamEvent {
	if source == nil {
		return nil
	}
	result := *source
	result.ContentBlock = cloneMessageMetadata(source.ContentBlock)
	return &result
}

func (g *CompiledGraph[S, D]) rememberInputMessages(
	ctx context.Context,
	emit eventEmitter[S, D],
	step int,
	task scheduledTask,
	state S,
	runConfig RunConfig,
) error {
	if emit == nil || g.inputMessageExtractor == nil {
		return nil
	}
	if _, enabled := runConfig.streamModes[StreamMessages]; !enabled {
		return nil
	}
	messages, err := g.inputMessageExtractor(ctx, state)
	if err != nil {
		return &NodeExecutionError{
			Step: step, Node: task.node, TaskID: task.taskID,
			Err: fmt.Errorf("extract input messages: %w", err),
		}
	}
	for _, message := range messages {
		if err := emit(StreamEvent[S, D]{
			Mode: StreamMessages, Step: step,
			Message: &MessageStreamEvent{Message: message, remember: true},
		}); err != nil {
			return err
		}
	}
	return nil
}

func messageStreamMetadata(
	step int,
	node NodeID,
	taskID string,
	config checkpoint.Config,
	runConfig RunConfig,
	extra map[string]any,
	cached, recovered bool,
) map[string]any {
	metadata := cloneMessageMetadata(runConfig.Metadata)
	if metadata == nil {
		metadata = make(map[string]any)
	}
	for key, value := range extra {
		metadata[key] = cloneMessageMetadataValue(value)
	}
	// Scheduler-owned keys cannot be spoofed by integration metadata.
	metadata["langgraph_step"] = step
	metadata["langgraph_node"] = string(node)
	metadata["langgraph_task_id"] = taskID
	metadata["langgraph_path"] = []string{"__pregel_pull", string(node)}
	namespace := config.Namespace
	if namespace == "" {
		namespace = runConfig.CheckpointNamespace
	}
	metadata["langgraph_checkpoint_ns"] = namespace
	metadata["checkpoint_ns"] = namespace
	threadID := config.ThreadID
	if threadID == "" {
		threadID = runConfig.ThreadID
	}
	if threadID != "" {
		metadata["thread_id"] = threadID
	}
	checkpointID := config.CheckpointID
	if checkpointID == "" {
		checkpointID = runConfig.CheckpointID
	}
	if checkpointID != "" {
		metadata["checkpoint_id"] = checkpointID
	}
	if runConfig.RunID != "" {
		metadata["run_id"] = runConfig.RunID
	}
	if cached {
		metadata["langgraph_cached"] = true
	}
	if recovered {
		metadata["langgraph_recovered"] = true
	}
	return metadata
}

func (g *CompiledGraph[S, D]) emitCommandMessages(
	ctx context.Context,
	emit eventEmitter[S, D],
	step int,
	task scheduledTask,
	checkpointConfig checkpoint.Config,
	runConfig RunConfig,
	command Command[D],
	cached, recovered bool,
) error {
	if emit == nil || g.messageExtractor == nil || !command.HasUpdate {
		return nil
	}
	if _, enabled := runConfig.streamModes[StreamMessages]; !enabled {
		return nil
	}
	emissions, err := g.messageExtractor(ctx, command.Update)
	if err != nil {
		return err
	}
	for _, emission := range emissions {
		if err := emit(StreamEvent[S, D]{
			Mode: StreamMessages,
			Step: step,
			Message: &MessageStreamEvent{
				Message: emission.Message,
				dedupe:  true,
				Metadata: messageStreamMetadata(
					step, task.node, task.taskID, checkpointConfig, runConfig,
					emission.Metadata, cached, recovered,
				),
			},
		}); err != nil {
			return err
		}
	}
	return nil
}
