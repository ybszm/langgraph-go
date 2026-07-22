package graph

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"time"

	"github.com/ybszm/langgraph-go/checkpoint"
	lgstore "github.com/ybszm/langgraph-go/store"
)

const taskResultType = "langgraph.go/task-result"

// PersistenceConfig supplies all dependencies required for durable graph
// execution. Clock and IDGenerator receive safe defaults when nil.
type PersistenceConfig[S, D any] struct {
	Saver      checkpoint.Saver
	StateCodec checkpoint.Codec[S]
	DeltaCodec checkpoint.Codec[D]
	// ErrorCodec serializes retry-exhausted source failures before an error
	// handler starts. Nil uses a safe type-and-message codec.
	ErrorCodec  checkpoint.Codec[error]
	Clock       checkpoint.Clock
	IDGenerator checkpoint.IDGenerator
}

// CompileOption configures a compiled graph.
type CompileOption[S, D any] func(*compileConfig[S, D]) error

type compileConfig[S, D any] struct {
	persistence          *persistenceRuntime[S, D]
	cache                *cacheRuntime[S, D]
	store                lgstore.Store
	interruptBefore      map[NodeID]struct{}
	interruptAfter       map[NodeID]struct{}
	contextSchema        *runtimeContextSchema
	allowUnreachable     bool
	allowUnreachableEND  bool
}

// WithAllowUnreachableNodes skips the “every node must be reachable from START”
// compile check. Useful when migrating Python graphs that keep helper nodes
// offline, or when wiring destinations only through runtime Command/Send.
// END reachability is still required unless WithAllowUnreachableEND is also set.
func WithAllowUnreachableNodes[S, D any]() CompileOption[S, D] {
	return func(target *compileConfig[S, D]) error {
		target.allowUnreachable = true
		return nil
	}
}

// WithAllowUnreachableEND permits compiling graphs that never reach END
// (long-running loops that only interrupt or await external resume).
func WithAllowUnreachableEND[S, D any]() CompileOption[S, D] {
	return func(target *compileConfig[S, D]) error {
		target.allowUnreachableEND = true
		return nil
	}
}

type runtimeContextSchema struct {
	typeOf   reflect.Type
	validate func(any) error
}

// WithContextSchema declares the exact typed Runtime.Context contract for a
// compiled graph. It is validated once before run initialization and checked
// recursively across embedded subgraphs at compile time.
func WithContextSchema[S, D, C any]() CompileOption[S, D] {
	expected := reflect.TypeOf((*C)(nil)).Elem()
	return func(target *compileConfig[S, D]) error {
		if target.contextSchema != nil {
			return fmt.Errorf("%w: context schema configured more than once", ErrInvalidGraph)
		}
		target.contextSchema = &runtimeContextSchema{
			typeOf: expected,
			validate: func(value any) error {
				if value == nil {
					return fmt.Errorf("%w: expected %s, got <nil>", ErrRuntimeContextType, expected)
				}
				actual := reflect.TypeOf(value)
				if !actual.AssignableTo(expected) {
					return fmt.Errorf("%w: expected %s, got %s", ErrRuntimeContextType, expected, actual)
				}
				return nil
			},
		}
		return nil
	}
}

// WithInterruptAfter pauses after selected node results are reduced and the
// next-task checkpoint is durable.
func WithInterruptAfter[S, D any](nodes ...NodeID) CompileOption[S, D] {
	return func(target *compileConfig[S, D]) error {
		if len(nodes) == 0 {
			return fmt.Errorf("%w: interrupt-after node list is empty", ErrInvalidGraph)
		}
		if target.interruptAfter == nil {
			target.interruptAfter = make(map[NodeID]struct{}, len(nodes))
		}
		for _, node := range nodes {
			if node == "" || node == START || node == END {
				return fmt.Errorf("%w: invalid interrupt-after node %q", ErrInvalidGraph, node)
			}
			target.interruptAfter[node] = struct{}{}
		}
		return nil
	}
}

// WithInterruptBefore pauses a persistent graph before any super-step that
// contains one of nodes. Continue resumes the same durable task set.
func WithInterruptBefore[S, D any](nodes ...NodeID) CompileOption[S, D] {
	return func(target *compileConfig[S, D]) error {
		if len(nodes) == 0 {
			return fmt.Errorf("%w: interrupt-before node list is empty", ErrInvalidGraph)
		}
		if target.interruptBefore == nil {
			target.interruptBefore = make(map[NodeID]struct{}, len(nodes))
		}
		for _, node := range nodes {
			if node == "" || node == START || node == END {
				return fmt.Errorf("%w: invalid interrupt-before node %q", ErrInvalidGraph, node)
			}
			target.interruptBefore[node] = struct{}{}
		}
		return nil
	}
}

// WithStore injects long-term, cross-thread memory into every node Runtime.
func WithStore[S, D any](value lgstore.Store) CompileOption[S, D] {
	return func(target *compileConfig[S, D]) error {
		if value == nil {
			return fmt.Errorf("%w: store is nil", ErrInvalidGraph)
		}
		if target.store != nil {
			return fmt.Errorf("%w: store configured more than once", ErrInvalidGraph)
		}
		target.store = value
		return nil
	}
}

type persistenceRuntime[S, D any] struct {
	saver       checkpoint.Saver
	stateCodec  checkpoint.Codec[S]
	deltaCodec  checkpoint.Codec[D]
	errorCodec  checkpoint.Codec[error]
	clock       checkpoint.Clock
	idGenerator checkpoint.IDGenerator
}

// WithPersistence enables checkpoint-backed execution.
func WithPersistence[S, D any](
	config PersistenceConfig[S, D],
) CompileOption[S, D] {
	return func(target *compileConfig[S, D]) error {
		if target.persistence != nil {
			return fmt.Errorf("%w: persistence configured more than once", ErrInvalidGraph)
		}
		if config.Saver == nil {
			return fmt.Errorf("%w: checkpoint saver is nil", ErrInvalidGraph)
		}
		if config.StateCodec == nil {
			return fmt.Errorf("%w: state codec is nil", ErrInvalidGraph)
		}
		if config.DeltaCodec == nil {
			return fmt.Errorf("%w: delta codec is nil", ErrInvalidGraph)
		}
		if config.Clock == nil {
			config.Clock = checkpoint.SystemClock{}
		}
		if config.IDGenerator == nil {
			generator, err := checkpoint.NewMonotonicIDGenerator()
			if err != nil {
				return err
			}
			config.IDGenerator = generator
		}
		if config.ErrorCodec == nil {
			config.ErrorCodec = recoveredNodeErrorCodec{}
		}
		target.persistence = &persistenceRuntime[S, D]{
			saver:       config.Saver,
			stateCodec:  config.StateCodec,
			deltaCodec:  config.DeltaCodec,
			errorCodec:  config.ErrorCodec,
			clock:       config.Clock,
			idGenerator: config.IDGenerator,
		}
		return nil
	}
}

const (
	nodeFailureType            = "langgraph.go/node-failure"
	recoveredErrorType         = "langgraph.go/recovered-error"
	nodeFailureVersion         = 1
	recoveredErrorCodecVersion = 1
)

type recoveredNodeErrorCodec struct{}

type persistedErrorText struct {
	Type    string `json:"type"`
	Message string `json:"message"`
}

func (recoveredNodeErrorCodec) Encode(err error) (checkpoint.EncodedValue, error) {
	if err == nil {
		return checkpoint.EncodedValue{}, fmt.Errorf("cannot encode a nil node error")
	}
	typeName := ""
	if typ := reflect.TypeOf(err); typ != nil {
		typeName = typ.String()
	}
	data, marshalErr := json.Marshal(persistedErrorText{Type: typeName, Message: err.Error()})
	if marshalErr != nil {
		return checkpoint.EncodedValue{}, marshalErr
	}
	return checkpoint.EncodedValue{Type: recoveredErrorType, Version: recoveredErrorCodecVersion, Data: data}, nil
}

func (recoveredNodeErrorCodec) Decode(value checkpoint.EncodedValue) (error, error) {
	if value.Type != recoveredErrorType || value.Version != recoveredErrorCodecVersion {
		return nil, fmt.Errorf("%w: recovered node error has %s v%d", checkpoint.ErrCodecMismatch, value.Type, value.Version)
	}
	var persisted persistedErrorText
	if err := json.Unmarshal(value.Data, &persisted); err != nil {
		return nil, err
	}
	if persisted.Message == "" {
		return nil, fmt.Errorf("%w: recovered node error message is empty", checkpoint.ErrInvalidCheckpoint)
	}
	return &RecoveredNodeError{Type: persisted.Type, Message: persisted.Message}, nil
}

type persistedNodeFailure struct {
	Source string                  `json:"source"`
	Error  checkpoint.EncodedValue `json:"error"`
}

type recoveredTaskFailure struct {
	source NodeID
	err    error
}

func (p *persistenceRuntime[S, D]) encodeNodeFailure(source NodeID, failure error) (checkpoint.EncodedValue, error) {
	encodedError, err := p.errorCodec.Encode(failure)
	if err != nil {
		return checkpoint.EncodedValue{}, err
	}
	data, err := json.Marshal(persistedNodeFailure{Source: string(source), Error: encodedError})
	if err != nil {
		return checkpoint.EncodedValue{}, err
	}
	return checkpoint.EncodedValue{Type: nodeFailureType, Version: nodeFailureVersion, Data: data}, nil
}

func (p *persistenceRuntime[S, D]) decodeNodeFailure(value checkpoint.EncodedValue) (recoveredTaskFailure, error) {
	if value.Type != nodeFailureType || value.Version != nodeFailureVersion {
		return recoveredTaskFailure{}, fmt.Errorf("%w: node failure has %s v%d", checkpoint.ErrCodecMismatch, value.Type, value.Version)
	}
	var persisted persistedNodeFailure
	if err := json.Unmarshal(value.Data, &persisted); err != nil {
		return recoveredTaskFailure{}, err
	}
	if persisted.Source == "" {
		return recoveredTaskFailure{}, fmt.Errorf("%w: node failure source is empty", checkpoint.ErrInvalidCheckpoint)
	}
	failure, err := p.errorCodec.Decode(persisted.Error)
	if err != nil {
		return recoveredTaskFailure{}, err
	}
	if failure == nil {
		return recoveredTaskFailure{}, fmt.Errorf("%w: decoded node failure is nil", checkpoint.ErrInvalidCheckpoint)
	}
	return recoveredTaskFailure{source: NodeID(persisted.Source), err: failure}, nil
}

type persistedTaskResult struct {
	Node      string                   `json:"node"`
	HandledBy string                   `json:"handled_by,omitempty"`
	HasUpdate bool                     `json:"has_update"`
	Update    *checkpoint.EncodedValue `json:"update"`
	Goto      []string                 `json:"goto"`
	Sends     []persistedTaskSend      `json:"sends,omitempty"`
}

type persistedTaskSend struct {
	Node  string                  `json:"node"`
	State checkpoint.EncodedValue `json:"state"`
}

func (p *persistenceRuntime[S, D]) encodeTaskResult(
	result taskResult[D],
) (checkpoint.EncodedValue, error) {
	persisted := persistedTaskResult{
		Node:      string(result.node),
		HandledBy: string(result.handledBy),
		HasUpdate: result.cmd.HasUpdate,
	}
	if result.cmd.HasUpdate {
		encoded, err := p.deltaCodec.Encode(result.cmd.Update)
		if err != nil {
			return checkpoint.EncodedValue{}, err
		}
		persisted.Update = &encoded
	}
	if result.cmd.Goto != nil {
		persisted.Goto = make([]string, len(result.cmd.Goto))
		for index, destination := range result.cmd.Goto {
			persisted.Goto[index] = string(destination)
		}
	}
	if len(result.cmd.Sends) > 0 {
		if p.stateCodec == nil {
			return checkpoint.EncodedValue{}, fmt.Errorf("task result with Sends requires a state codec")
		}
		persisted.Sends = make([]persistedTaskSend, len(result.cmd.Sends))
		for index, send := range result.cmd.Sends {
			state, ok := send.State.(S)
			if !ok {
				return checkpoint.EncodedValue{}, fmt.Errorf("Send target %q state has type %T", send.Node, send.State)
			}
			encoded, err := p.stateCodec.Encode(state)
			if err != nil {
				return checkpoint.EncodedValue{}, err
			}
			persisted.Sends[index] = persistedTaskSend{Node: string(send.Node), State: encoded}
		}
	}
	data, err := json.Marshal(persisted)
	if err != nil {
		return checkpoint.EncodedValue{}, fmt.Errorf("encode task result: %w", err)
	}
	return checkpoint.EncodedValue{
		Type:    taskResultType,
		Version: 1,
		Data:    data,
	}, nil
}

func (p *persistenceRuntime[S, D]) decodeTaskResult(
	taskID string,
	value checkpoint.EncodedValue,
) (taskResult[D], error) {
	if value.Type != taskResultType || value.Version != 1 {
		return taskResult[D]{}, fmt.Errorf(
			"%w: pending task result has %s v%d",
			checkpoint.ErrCodecMismatch,
			value.Type,
			value.Version,
		)
	}
	var persisted persistedTaskResult
	if err := json.Unmarshal(value.Data, &persisted); err != nil {
		return taskResult[D]{}, fmt.Errorf("decode task result: %w", err)
	}
	result := taskResult[D]{
		node:      NodeID(persisted.Node),
		handledBy: NodeID(persisted.HandledBy),
		taskID:    taskID,
		cmd: Command[D]{
			HasUpdate: persisted.HasUpdate,
		},
	}
	if persisted.HasUpdate {
		if persisted.Update == nil {
			return taskResult[D]{}, fmt.Errorf("pending task result is missing its update")
		}
		update, err := p.deltaCodec.Decode(*persisted.Update)
		if err != nil {
			return taskResult[D]{}, err
		}
		result.cmd.Update = update
	}
	if persisted.Goto != nil {
		result.cmd.Goto = make([]NodeID, len(persisted.Goto))
		for index, destination := range persisted.Goto {
			result.cmd.Goto[index] = NodeID(destination)
		}
	}
	if len(persisted.Sends) > 0 {
		if p.stateCodec == nil {
			return taskResult[D]{}, fmt.Errorf("pending task Sends require a state codec")
		}
		result.cmd.Sends = make([]TaskSend, len(persisted.Sends))
		for index, send := range persisted.Sends {
			state, err := p.stateCodec.Decode(send.State)
			if err != nil {
				return taskResult[D]{}, err
			}
			result.cmd.Sends[index] = TaskSend{Node: NodeID(send.Node), State: state}
		}
	}
	return result, nil
}

func (p *persistenceRuntime[S, D]) nextID() (string, time.Time, error) {
	now := p.clock.Now().UTC()
	if now.IsZero() {
		return "", now, fmt.Errorf("checkpoint clock returned zero time")
	}
	id, err := p.idGenerator.NewID(now)
	if err != nil {
		return "", now, err
	}
	if id == "" {
		return "", now, fmt.Errorf("checkpoint ID generator returned an empty ID")
	}
	return id, now, nil
}

func persistenceError(operation string, config checkpoint.Config, err error) error {
	return &PersistenceError{
		Operation:    operation,
		ThreadID:     config.ThreadID,
		Namespace:    config.Namespace,
		CheckpointID: config.CheckpointID,
		Err:          err,
	}
}

func checkpointMetadata(
	source checkpoint.Source,
	step int,
	runID string,
	extra ...checkpoint.Metadata,
) checkpoint.Metadata {
	metadata := checkpoint.Metadata{
		"source": string(source),
		"step":   step,
	}
	if runID != "" {
		metadata["run_id"] = runID
	}
	for _, values := range extra {
		for key, value := range values {
			metadata[key] = value
		}
	}
	return metadata
}

func saverGetTuple(
	ctx context.Context,
	saver checkpoint.Saver,
	config checkpoint.Config,
) (checkpoint.Tuple, bool, error) {
	tuple, found, err := saver.GetTuple(ctx, config)
	if err != nil {
		return checkpoint.Tuple{}, false, persistenceError("get", config, err)
	}
	return tuple, found, nil
}
