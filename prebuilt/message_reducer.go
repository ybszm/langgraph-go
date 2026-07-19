package prebuilt

import (
	"crypto/rand"
	"errors"
	"fmt"
)

// RemoveAllMessages is the sentinel RemoveMessage ID that clears history.
const RemoveAllMessages = "__remove_all__"

// ErrMessageNotFound indicates removal of an ID absent from history.
var ErrMessageNotFound = errors.New("message not found")

// MessageIDGenerator creates one non-empty message ID.
type MessageIDGenerator func() (string, error)

// AddMessages merges right into left, replacing messages in place by ID.
// Missing IDs are generated, RemoveMessage deletes by ID, and the final
// RemoveAllMessages marker discards all messages through that marker.
func AddMessages(
	left []Message,
	right []Message,
	generator MessageIDGenerator,
) ([]Message, error) {
	if generator == nil {
		generator = randomMessageID
	}
	normalizedLeft, err := NormalizeMessages(left, generator)
	if err != nil {
		return nil, fmt.Errorf("normalize left messages: %w", err)
	}
	normalizedRight, err := NormalizeMessages(right, generator)
	if err != nil {
		return nil, fmt.Errorf("normalize right messages: %w", err)
	}

	removeAllIndex := -1
	for index, message := range normalizedRight {
		if removal, ok := message.(RemoveMessage); ok && removal.ID == RemoveAllMessages {
			removeAllIndex = index
		}
	}
	if removeAllIndex >= 0 {
		return cloneMessages(normalizedRight[removeAllIndex+1:]), nil
	}

	merged := cloneMessages(normalizedLeft)
	byID := make(map[string]int, len(merged))
	for index, message := range merged {
		byID[message.MessageID()] = index
	}
	removed := make(map[string]struct{})
	for _, message := range normalizedRight {
		id := message.MessageID()
		index, exists := byID[id]
		_, removal := message.(RemoveMessage)
		if exists {
			if removal {
				removed[id] = struct{}{}
				continue
			}
			delete(removed, id)
			merged[index] = message.CloneMessage()
			continue
		}
		if removal {
			return nil, fmt.Errorf("%w: cannot delete message ID %q", ErrMessageNotFound, id)
		}
		byID[id] = len(merged)
		merged = append(merged, message.CloneMessage())
	}
	result := make([]Message, 0, len(merged)-len(removed))
	for _, message := range merged {
		if _, deleted := removed[message.MessageID()]; !deleted {
			result = append(result, message.CloneMessage())
		}
	}
	return result, nil
}

// NormalizeMessages clones messages and assigns non-empty identities where
// missing. It is suitable for graph.DeltaNormalizer adapters so durable task
// writes contain the same IDs later observed during replay.
func NormalizeMessages(messages []Message, generator MessageIDGenerator) ([]Message, error) {
	if generator == nil {
		generator = randomMessageID
	}
	result := make([]Message, len(messages))
	for index, message := range messages {
		if message == nil {
			return nil, fmt.Errorf("message %d is nil", index)
		}
		cloned := message.CloneMessage()
		if cloned.MessageID() == "" {
			id, err := generator()
			if err != nil {
				return nil, fmt.Errorf("generate message %d ID: %w", index, err)
			}
			if id == "" {
				return nil, fmt.Errorf("generate message %d ID: empty ID", index)
			}
			cloned = cloned.WithMessageID(id)
		}
		result[index] = cloned
	}
	return result, nil
}

func cloneMessages(messages []Message) []Message {
	result := make([]Message, len(messages))
	for index, message := range messages {
		result[index] = message.CloneMessage()
	}
	return result
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
