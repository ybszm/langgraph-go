package functional

import (
	"encoding/json"
	"fmt"

	"github.com/ybszm/langgraph-go/checkpoint"
)

const functionalTaskResultType = "functional_task_result"

type durableTaskRuntime struct {
	saver     checkpoint.Saver
	config    checkpoint.Config
	recovered map[string]checkpoint.PendingWrite
}

type persistedTaskResult struct {
	Name     string                  `json:"name"`
	InputKey string                  `json:"input_key"`
	Output   checkpoint.EncodedValue `json:"output"`
}

func recoverTaskResult[I, O any](
	manager *taskManager,
	task *Task[I, O],
	taskID string,
	input I,
) (O, bool, error) {
	if manager.durable == nil || task.options.Persistence == nil {
		return zero[O](), false, nil
	}
	write, exists := manager.durable.recovered[taskID]
	if !exists {
		return zero[O](), false, nil
	}
	if write.Channel != checkpoint.TaskResultChannel || write.Value.Type != functionalTaskResultType || write.Value.Version != 1 {
		return zero[O](), false, fmt.Errorf("invalid recovered functional task envelope for %q", taskID)
	}
	var stored persistedTaskResult
	if err := json.Unmarshal(write.Value.Data, &stored); err != nil {
		return zero[O](), false, fmt.Errorf("decode recovered functional task %q: %w", taskID, err)
	}
	inputKey, err := task.options.Persistence.Key(input)
	if err != nil {
		return zero[O](), false, fmt.Errorf("functional task persistence key: %w", err)
	}
	if stored.Name != task.name || stored.InputKey != inputKey {
		return zero[O](), false, fmt.Errorf("recovered functional task %q definition/input mismatch", taskID)
	}
	value, err := task.options.Persistence.Codec.Decode(stored.Output)
	if err != nil {
		return zero[O](), false, fmt.Errorf("decode recovered functional task %q output: %w", taskID, err)
	}
	return value, true, nil
}

func persistTaskResult[I, O any](
	manager *taskManager,
	task *Task[I, O],
	taskID string,
	input I,
	output O,
) error {
	if manager.durable == nil || task.options.Persistence == nil {
		return nil
	}
	inputKey, err := task.options.Persistence.Key(input)
	if err != nil {
		return fmt.Errorf("functional task persistence key: %w", err)
	}
	if inputKey == "" {
		return fmt.Errorf("functional task persistence key is empty")
	}
	encoded, err := task.options.Persistence.Codec.Encode(output)
	if err != nil {
		return fmt.Errorf("encode functional task %q output: %w", taskID, err)
	}
	data, err := json.Marshal(persistedTaskResult{Name: task.name, InputKey: inputKey, Output: encoded})
	if err != nil {
		return fmt.Errorf("encode functional task %q envelope: %w", taskID, err)
	}
	return manager.durable.saver.PutWrites(manager.ctx, manager.durable.config, []checkpoint.PendingWrite{{
		TaskID: taskID, TaskPath: task.name, Index: 0, Channel: checkpoint.TaskResultChannel,
		Value: checkpoint.EncodedValue{Type: functionalTaskResultType, Version: 1, Data: data},
	}})
}
