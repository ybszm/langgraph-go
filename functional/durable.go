package functional

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sync"

	"github.com/ybszm/langgraph-go/checkpoint"
)

const functionalPreviousChannel = "__functional_previous__"

// Final separates the value returned to the caller from the value saved as
// previous state for the next invocation.
type Final[O, S any] struct {
	Value O
	Save  S
}

// PreviousStateMigration upgrades one persisted previous-state value from an
// older workflow revision. The callback owns decoding the old envelope and
// returns a value encodable by the current Codec.
type PreviousStateMigration[S any] func(
	ctx context.Context,
	fromRevision string,
	value checkpoint.EncodedValue,
) (S, error)

// DurableMigrationPolicy declares the code revision expected by a durable
// Functional workflow. Completed checkpoints may be upgraded explicitly;
// running attempts never migrate because task/interrupt call positions are
// part of the replay contract.
type DurableMigrationPolicy[S any] struct {
	Revision        string
	MigratePrevious PreviousStateMigration[S]
}

// DurableEntrypointConfig supplies persistence and scheduling dependencies.
type DurableEntrypointConfig[S any] struct {
	Saver              checkpoint.Saver
	Codec              checkpoint.Codec[S]
	Namespace          string
	Clock              checkpoint.Clock
	IDGenerator        checkpoint.IDGenerator
	MaxConcurrency     int
	EnableTaskRecovery bool
	Timeout            TimeoutPolicy
	Migration          *DurableMigrationPolicy[S]
}

// DurableRunConfig selects one persistent workflow thread.
type DurableRunConfig struct {
	ThreadID     string
	CheckpointID string
}

// DurableEntrypoint is a typed functional workflow with previous-value state.
type DurableEntrypoint[I, O, S any] struct {
	name   string
	run    func(context.Context, I, *S) (Final[O, S], error)
	config DurableEntrypointConfig[S]
	locks  sync.Map
}

// NewDurableEntrypoint validates and constructs a durable workflow.
func NewDurableEntrypoint[I, O, S any](
	name string,
	run func(context.Context, I, *S) (Final[O, S], error),
	config DurableEntrypointConfig[S],
) (*DurableEntrypoint[I, O, S], error) {
	if name == "" {
		return nil, fmt.Errorf("%w: durable entrypoint name is empty", ErrInvalidDefinition)
	}
	if run == nil {
		return nil, fmt.Errorf("%w: durable entrypoint %q function is nil", ErrInvalidDefinition, name)
	}
	if config.Saver == nil || config.Codec == nil {
		return nil, fmt.Errorf("%w: durable entrypoint requires saver and codec", ErrInvalidDefinition)
	}
	if config.MaxConcurrency < 0 {
		return nil, fmt.Errorf("%w: durable entrypoint max concurrency cannot be negative", ErrInvalidDefinition)
	}
	if err := validateTimeoutPolicy(config.Timeout); err != nil {
		return nil, fmt.Errorf("%w: durable entrypoint timeout: %v", ErrInvalidDefinition, err)
	}
	if config.Migration != nil && config.Migration.Revision == "" {
		return nil, fmt.Errorf("%w: durable migration revision is empty", ErrInvalidDefinition)
	}
	if config.Clock == nil {
		config.Clock = checkpoint.SystemClock{}
	}
	if config.IDGenerator == nil {
		generator, err := checkpoint.NewMonotonicIDGenerator()
		if err != nil {
			return nil, fmt.Errorf("create durable entrypoint ID generator: %w", err)
		}
		config.IDGenerator = generator
	}
	return &DurableEntrypoint[I, O, S]{name: name, run: run, config: config}, nil
}

// Name returns the durable entrypoint's stable logical name.
func (e *DurableEntrypoint[I, O, S]) Name() string {
	if e == nil {
		return ""
	}
	return e.name
}

// Invoke runs one serialized thread invocation and atomically appends the
// returned Final.Save value to its checkpoint lineage on success.
func (e *DurableEntrypoint[I, O, S]) Invoke(
	ctx context.Context,
	input I,
	runConfig DurableRunConfig,
) (output O, err error) {
	return e.invoke(ctx, input, runConfig, nil, nil, nil)
}

func (e *DurableEntrypoint[I, O, S]) invoke(
	ctx context.Context,
	input I,
	runConfig DurableRunConfig,
	emit func(any) error,
	emitDebug func(DebugEvent) error,
	emitMessage func(any, map[string]any) error,
) (output O, err error) {
	if e == nil || e.run == nil {
		return output, fmt.Errorf("%w: nil durable entrypoint", ErrInvalidDefinition)
	}
	config := checkpoint.Config{ThreadID: runConfig.ThreadID, Namespace: e.config.Namespace, CheckpointID: runConfig.CheckpointID}
	if err := config.Validate(); err != nil {
		return output, err
	}
	if err := ctx.Err(); err != nil {
		return output, err
	}
	if emitDebug != nil {
		if err := emitDebug(DebugEvent{Kind: DebugEntrypointStart, Entrypoint: e.name}); err != nil {
			return output, err
		}
		defer func() {
			if emitErr := emitDebug(DebugEvent{Kind: DebugEntrypointResult, Entrypoint: e.name, Err: err}); err == nil && emitErr != nil {
				err = emitErr
			}
		}()
	}
	unlock := e.lockThread(config.ThreadID, config.Namespace)
	defer unlock()

	tuple, found, err := e.config.Saver.GetTuple(ctx, config)
	if err != nil {
		return output, fmt.Errorf("functional checkpoint get: %w", err)
	}
	if runConfig.CheckpointID != "" && !found {
		return output, fmt.Errorf("%w: functional checkpoint %q", checkpoint.ErrNotFound, runConfig.CheckpointID)
	}
	var previous *S
	var previousEncoded *checkpoint.EncodedValue
	parent := config
	step := 0
	source := checkpoint.SourceInput
	if found {
		if storedName, ok := tuple.Metadata["entrypoint"].(string); ok && storedName != "" && storedName != e.name {
			return output, fmt.Errorf("%w: checkpoint belongs to entrypoint %q, current %q", ErrIncompatibleRevision, storedName, e.name)
		}
		if err := e.validateRunningRevision(tuple); err != nil {
			return output, err
		}
		encoded, exists := tuple.Checkpoint.Values[functionalPreviousChannel]
		if !exists && (!e.config.EnableTaskRecovery || tuple.Metadata["status"] != "running") {
			return output, fmt.Errorf("functional checkpoint %q has no previous value", tuple.Config.CheckpointID)
		}
		if exists {
			value, migrated, err := e.decodePrevious(ctx, tuple, encoded, true)
			if err != nil {
				return output, fmt.Errorf("functional checkpoint decode previous: %w", err)
			}
			previous = &value
			if migrated {
				current, encodeErr := e.config.Codec.Encode(value)
				if encodeErr != nil {
					return output, fmt.Errorf("functional checkpoint encode migrated previous: %w", encodeErr)
				}
				previousEncoded = &current
			} else {
				cloned := checkpoint.CloneEncodedValue(encoded)
				previousEncoded = &cloned
			}
		}
		parent = tuple.Config
		step = tuple.Checkpoint.Step + 1
		source = checkpoint.SourceLoop
	}

	var taskDurable *durableTaskRuntime
	if e.config.EnableTaskRecovery {
		inputHash, err := functionalInputHash(input)
		if err != nil {
			return output, fmt.Errorf("functional recovery input: %w", err)
		}
		attemptConfig := checkpoint.Config{}
		recovered := make(map[string]checkpoint.PendingWrite)
		if found && tuple.Metadata["status"] == "running" && tuple.Metadata["input_hash"] == inputHash {
			attemptConfig = tuple.Config
			for _, write := range tuple.PendingWrites {
				recovered[write.TaskID] = write
			}
			step = tuple.Checkpoint.Step + 1
		} else {
			values := make(map[string]checkpoint.EncodedValue)
			if previousEncoded != nil {
				values[functionalPreviousChannel] = checkpoint.CloneEncodedValue(*previousEncoded)
			}
			attemptConfig, err = e.putCheckpoint(ctx, parent, step, values, e.revisionMetadata(checkpoint.Metadata{
				"source": string(source), "step": step, "entrypoint": e.name,
				"status": "running", "input_hash": inputHash,
			}))
			if err != nil {
				return output, err
			}
			step++
		}
		parent = attemptConfig
		source = checkpoint.SourceLoop
		taskDurable = &durableTaskRuntime{saver: e.config.Saver, config: attemptConfig, recovered: recovered}
	}
	executionCtx, finishTimeout := withTimeoutPolicy(ctx, e.config.Timeout)
	defer finishTimeout()
	manager := newTaskManager(executionCtx, e.config.MaxConcurrency)
	manager.durable = taskDurable
	manager.emit = emit
	manager.emitDebug = emitDebug
	manager.emitMessage = emitMessage
	manager.entrypoint = e.name
	entryCtx := context.WithValue(manager.ctx, runtimeContextKey{}, manager)
	entryCtx = context.WithValue(entryCtx, interruptScopeKey{}, &interruptScope{id: "entrypoint"})
	result, entryErr := invokeEntrypointSafely(entryCtx, input, func(callCtx context.Context, callInput I) (Final[O, S], error) {
		return e.run(callCtx, callInput, previous)
	})
	entryInterrupted := errors.Is(entryErr, ErrInterrupted)
	if entryErr != nil && !entryInterrupted {
		manager.cancel()
	}
	manager.wait()
	manager.cancel()
	if timeout := timeoutCause(executionCtx); timeout != nil {
		entryErr = timeout
	}
	taskErr := manager.err()
	if entryErr != nil {
		if entryInterrupted && taskErr != nil {
			entryErr = taskErr
		}
		return output, &EntrypointError{Name: e.name, Err: entryErr}
	}
	if taskErr != nil {
		return output, &EntrypointError{Name: e.name, Err: taskErr}
	}

	encoded, err := e.config.Codec.Encode(result.Save)
	if err != nil {
		return output, fmt.Errorf("functional checkpoint encode previous: %w", err)
	}
	_, err = e.putCheckpoint(ctx, parent, step, map[string]checkpoint.EncodedValue{
		functionalPreviousChannel: encoded,
	}, e.revisionMetadata(checkpoint.Metadata{"source": string(source), "step": step, "entrypoint": e.name, "status": "complete"}))
	if err != nil {
		return output, err
	}
	return result.Value, nil
}

func (e *DurableEntrypoint[I, O, S]) validateRunningRevision(tuple checkpoint.Tuple) error {
	if tuple.Metadata["status"] != "running" {
		return nil
	}
	currentRevision := ""
	if e.config.Migration != nil {
		currentRevision = e.config.Migration.Revision
	}
	storedRevision, _ := tuple.Metadata["functional_revision"].(string)
	if storedRevision != currentRevision {
		return fmt.Errorf(
			"%w: running attempt uses revision %q, current %q",
			ErrIncompatibleRevision, storedRevision, currentRevision,
		)
	}
	return nil
}

func (e *DurableEntrypoint[I, O, S]) revisionMetadata(metadata checkpoint.Metadata) checkpoint.Metadata {
	if e.config.Migration != nil {
		metadata["functional_revision"] = e.config.Migration.Revision
	}
	return metadata
}

func (e *DurableEntrypoint[I, O, S]) decodePrevious(
	ctx context.Context,
	tuple checkpoint.Tuple,
	encoded checkpoint.EncodedValue,
	rejectRunningMigration bool,
) (S, bool, error) {
	currentRevision := ""
	var migrate PreviousStateMigration[S]
	if e.config.Migration != nil {
		currentRevision = e.config.Migration.Revision
		migrate = e.config.Migration.MigratePrevious
	}
	storedRevision, _ := tuple.Metadata["functional_revision"].(string)
	if storedRevision == currentRevision {
		value, err := e.config.Codec.Decode(encoded)
		return value, false, err
	}
	if rejectRunningMigration && tuple.Metadata["status"] == "running" {
		return zero[S](), false, fmt.Errorf(
			"%w: running attempt uses revision %q, current %q",
			ErrIncompatibleRevision, storedRevision, currentRevision,
		)
	}
	if migrate == nil {
		return zero[S](), false, fmt.Errorf(
			"%w: persisted revision %q, current %q has no previous-state migration",
			ErrIncompatibleRevision, storedRevision, currentRevision,
		)
	}
	value, err := migrate(ctx, storedRevision, checkpoint.CloneEncodedValue(encoded))
	if err != nil {
		return zero[S](), false, fmt.Errorf(
			"%w: migrate previous state from %q to %q: %v",
			ErrIncompatibleRevision, storedRevision, currentRevision, err,
		)
	}
	return value, true, nil
}

func (e *DurableEntrypoint[I, O, S]) putCheckpoint(
	ctx context.Context,
	parent checkpoint.Config,
	step int,
	values map[string]checkpoint.EncodedValue,
	metadata checkpoint.Metadata,
) (checkpoint.Config, error) {
	timestamp := e.config.Clock.Now().UTC()
	id, err := e.config.IDGenerator.NewID(timestamp)
	if err != nil {
		return checkpoint.Config{}, fmt.Errorf("functional checkpoint generate ID: %w", err)
	}
	versions := make(map[string]string, len(values))
	updated := make([]string, 0, len(values))
	for channel := range values {
		versions[channel] = id
		updated = append(updated, channel)
	}
	value := checkpoint.Checkpoint{
		Version: checkpoint.CurrentVersion, ID: id, Timestamp: timestamp, Step: step,
		Values: values, ChannelVersions: versions,
		VersionsSeen: map[string]map[string]string{}, UpdatedChannels: updated,
	}
	stored, err := e.config.Saver.Put(ctx, parent, value, metadata, versions)
	if err != nil {
		return checkpoint.Config{}, fmt.Errorf("functional checkpoint put: %w", err)
	}
	return stored, nil
}

func functionalInputHash[I any](input I) (string, error) {
	data, err := json.Marshal(input)
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(data)
	return hex.EncodeToString(digest[:]), nil
}

func (e *DurableEntrypoint[I, O, S]) lockThread(threadID, namespace string) func() {
	key := threadID + "\x00" + namespace
	value, _ := e.locks.LoadOrStore(key, &sync.Mutex{})
	lock := value.(*sync.Mutex)
	lock.Lock()
	return lock.Unlock
}
