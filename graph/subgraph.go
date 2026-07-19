package graph

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"

	"github.com/wahanbo/langgraph-go/checkpoint"
)

const subgraphControlType = "langgraph.go/subgraph-control"

// SubgraphAdapter maps between independently typed parent and child states.
// StateCodec and DeltaCodec are required when a persistent parent must lend
// its saver to the child (including dynamic interrupts).
type SubgraphAdapter[PS, PD, CS, CD any] struct {
	Input  func(context.Context, PS) (CS, error)
	Output func(context.Context, PS, CS) (Command[PD], error)
	// Parent maps a child Command targeted at the closest parent into the
	// parent's delta and task-input types. When nil, a checked identity mapping
	// is attempted, which is convenient when parent and child share types.
	Parent     func(context.Context, PS, Command[CD]) (Command[PD], error)
	StateCodec checkpoint.Codec[CS]
	DeltaCodec checkpoint.Codec[CD]
	// Stateful reuses one logical child namespace across parent invocations.
	Stateful bool
	// InstanceKey optionally partitions a stateful subgraph into independent
	// logical instances. It is evaluated against each task-local parent state;
	// equal keys retain the same child history across invocations, while
	// different keys never share child state. An empty key is rejected.
	InstanceKey func(context.Context, PS, Runtime) (string, error)
	// MergeInput combines retained child state with each new invocation input.
	// It is required when Stateful is true.
	MergeInput InputMerger[CS]
}

type subgraphSpec[PS, PD any] interface {
	invoke(
		context.Context,
		PS,
		Runtime,
		*persistenceRuntime[PS, PD],
		RunConfig,
		eventEmitter[PS, PD],
	) (Command[PD], error)
	snapshot(context.Context, *persistenceRuntime[PS, PD], string, string, string) (*NestedStateSnapshot, error)
	forkInterrupt(
		context.Context,
		*persistenceRuntime[PS, PD],
		checkpoint.Config,
		checkpoint.Task,
		persistedSubgraphControl,
	) (persistedSubgraphControl, error)
	validateInterruptFork(
		context.Context,
		*persistenceRuntime[PS, PD],
		string,
		persistedSubgraphControl,
	) error
	inspectRecursive() GraphDescription
	validateCompile(bool, reflect.Type) error
	requiresPersistence() bool
}

func (s *typedSubgraphSpec[PS, PD, CS, CD]) inspectRecursive() GraphDescription {
	return s.child.InspectRecursive()
}

func (s *typedSubgraphSpec[PS, PD, CS, CD]) requiresPersistence() bool {
	return s.adapter.Stateful || s.child.requiresPersistence()
}

func (s *typedSubgraphSpec[PS, PD, CS, CD]) validateCompile(parentPersistent bool, parentContext reflect.Type) error {
	if s.adapter.InstanceKey != nil && !s.adapter.Stateful {
		return fmt.Errorf("subgraph InstanceKey requires Stateful")
	}
	if s.adapter.Stateful && s.adapter.MergeInput == nil {
		return fmt.Errorf("stateful subgraph requires MergeInput")
	}
	if s.requiresPersistence() && !parentPersistent {
		return fmt.Errorf("subgraph requires parent persistence")
	}
	effectiveContext := parentContext
	if s.child.contextSchema != nil {
		childContext := s.child.contextSchema.typeOf
		if parentContext == nil {
			return fmt.Errorf("child context schema %s requires a parent context schema", childContext)
		}
		if !parentContext.AssignableTo(childContext) {
			return fmt.Errorf("parent context schema %s is not assignable to child schema %s", parentContext, childContext)
		}
		effectiveContext = childContext
	}
	if parentPersistent {
		stateCodec := s.adapter.StateCodec
		deltaCodec := s.adapter.DeltaCodec
		if stateCodec == nil && s.child.persistence != nil {
			stateCodec = s.child.persistence.stateCodec
		}
		if deltaCodec == nil && s.child.persistence != nil {
			deltaCodec = s.child.persistence.deltaCodec
		}
		if stateCodec == nil || deltaCodec == nil {
			return fmt.Errorf("inherited persistence requires child state and delta codecs")
		}
	}
	return s.child.validateSubgraphCompile(parentPersistent, effectiveContext)
}

func (g *CompiledGraph[S, D]) requiresPersistence() bool {
	if len(g.interruptBefore) > 0 || len(g.interruptAfter) > 0 || len(g.dynamicInterruptNodes) > 0 {
		return true
	}
	for _, child := range g.subgraphs {
		if child.requiresPersistence() {
			return true
		}
	}
	return false
}

func (g *CompiledGraph[S, D]) validateSubgraphCompile(parentPersistent bool, parentContext reflect.Type) error {
	ids := make([]NodeID, 0, len(g.subgraphs))
	for id := range g.subgraphs {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	for _, id := range ids {
		if err := g.subgraphs[id].validateCompile(parentPersistent, parentContext); err != nil {
			return &SubgraphValidationError{Node: id, Err: err}
		}
	}
	return nil
}

type typedSubgraphSpec[PS, PD, CS, CD any] struct {
	child   *CompiledGraph[CS, CD]
	adapter SubgraphAdapter[PS, PD, CS, CD]
}

// AddSubgraph registers a compiled graph as a node while preserving the
// child's own state and delta types. The parent owns routing after Output.
func AddSubgraph[PS, PD, CS, CD any](
	parent *StateGraph[PS, PD],
	id NodeID,
	child *CompiledGraph[CS, CD],
	adapter SubgraphAdapter[PS, PD, CS, CD],
	options ...NodeOption,
) error {
	if parent == nil || child == nil {
		return fmt.Errorf("%w: parent or child subgraph is nil", ErrInvalidGraph)
	}
	if adapter.Input == nil || adapter.Output == nil {
		return fmt.Errorf("%w: subgraph %q requires input and output adapters", ErrInvalidGraph, id)
	}
	placeholder := func(context.Context, PS, Runtime) (Command[PD], error) {
		return Command[PD]{}, fmt.Errorf("subgraph %q was not bound by compiler", id)
	}
	if err := parent.AddNode(id, placeholder, options...); err != nil {
		return err
	}
	parent.subgraphs[id] = &typedSubgraphSpec[PS, PD, CS, CD]{child: child, adapter: adapter}
	return nil
}

func cloneSubgraphs[S, D any](source map[NodeID]subgraphSpec[S, D]) map[NodeID]subgraphSpec[S, D] {
	result := make(map[NodeID]subgraphSpec[S, D], len(source))
	for id, spec := range source {
		result[id] = spec
	}
	return result
}

// InheritedSubgraph returns one registered typed child bound to the parent's
// saver, codecs, clock, ID generator, error codec, and run locks. Repeating
// this operation walks an arbitrary-depth heterogeneous subgraph path while
// retaining compile-time state/delta types at every boundary.
func InheritedSubgraph[PS, PD, CS, CD any](
	parent *CompiledGraph[PS, PD],
	node NodeID,
) (*CompiledGraph[CS, CD], error) {
	if parent == nil || parent.persistence == nil {
		return nil, ErrCheckpointerRequired
	}
	specValue, exists := parent.subgraphs[node]
	if !exists {
		return nil, fmt.Errorf("%w: %w: %q", ErrInvalidGraph, ErrUnknownNode, node)
	}
	spec, ok := specValue.(*typedSubgraphSpec[PS, PD, CS, CD])
	if !ok {
		return nil, fmt.Errorf("%w: subgraph %q has incompatible state or delta schema", ErrSubgraphValidation, node)
	}
	child, err := spec.inheritedGraph(parent.persistence)
	if err != nil {
		return nil, fmt.Errorf("%w: subgraph %q: %w", ErrSubgraphValidation, node, err)
	}
	return child, nil
}

// BulkUpdateSubgraphState applies typed artificial supersteps directly to one
// registered child checkpoint namespace while reusing the parent's saver and
// the child adapter codecs. The returned snapshot selects the new exact child
// checkpoint; the parent checkpoint remains immutable.
func BulkUpdateSubgraphState[PS, PD, CS, CD any](
	ctx context.Context,
	parent *CompiledGraph[PS, PD],
	node NodeID,
	coordinate checkpoint.Config,
	supersteps [][]StateUpdate[CD],
) (StateSnapshot[CS, CD], error) {
	if parent == nil || parent.persistence == nil {
		return StateSnapshot[CS, CD]{}, ErrCheckpointerRequired
	}
	if err := coordinate.Validate(); err != nil {
		return StateSnapshot[CS, CD]{}, fmt.Errorf("%w: child coordinate: %w", ErrInvalidStateUpdate, err)
	}
	child, err := InheritedSubgraph[PS, PD, CS, CD](parent, node)
	if err != nil {
		return StateSnapshot[CS, CD]{}, fmt.Errorf("%w: subgraph %q: %w", ErrInvalidStateUpdate, node, err)
	}
	config := RunConfig{
		ThreadID: coordinate.ThreadID, CheckpointNamespace: coordinate.Namespace,
		CheckpointID: coordinate.CheckpointID,
	}
	stored, err := child.BulkUpdateState(ctx, config, supersteps)
	if err != nil {
		return StateSnapshot[CS, CD]{}, err
	}
	return child.GetState(ctx, RunConfig{
		ThreadID: stored.ThreadID, CheckpointNamespace: stored.Namespace, CheckpointID: stored.CheckpointID,
	})
}

func (s *typedSubgraphSpec[PS, PD, CS, CD]) inheritedGraph(
	parentPersistence *persistenceRuntime[PS, PD],
) (*CompiledGraph[CS, CD], error) {
	stateCodec := s.adapter.StateCodec
	deltaCodec := s.adapter.DeltaCodec
	if stateCodec == nil && s.child.persistence != nil {
		stateCodec = s.child.persistence.stateCodec
	}
	if deltaCodec == nil && s.child.persistence != nil {
		deltaCodec = s.child.persistence.deltaCodec
	}
	if stateCodec == nil || deltaCodec == nil {
		return nil, fmt.Errorf("inherited persistence requires child state and delta codecs")
	}
	child := *s.child
	child.runLocks = s.child.runLocks
	child.persistence = &persistenceRuntime[CS, CD]{
		saver: parentPersistence.saver, stateCodec: stateCodec, deltaCodec: deltaCodec,
		errorCodec: parentPersistence.errorCodec,
		clock:      parentPersistence.clock, idGenerator: parentPersistence.idGenerator,
	}
	if s.adapter.Stateful {
		child.inputMerger = s.adapter.MergeInput
	}
	return &child, nil
}

func (s *typedSubgraphSpec[PS, PD, CS, CD]) invoke(
	ctx context.Context,
	parentState PS,
	runtime Runtime,
	parentPersistence *persistenceRuntime[PS, PD],
	runConfig RunConfig,
	emit eventEmitter[PS, PD],
) (Command[PD], error) {
	input, err := s.adapter.Input(ctx, parentState)
	if err != nil {
		return Command[PD]{}, fmt.Errorf("adapt subgraph input: %w", err)
	}

	child := *s.child
	child.runLocks = s.child.runLocks
	// The parent run owns dependency injection. This mirrors LangGraph's
	// propagation of Runtime.store through nested Pregel graphs.
	child.store = runtime.Store
	childConfig := RunConfig{
		RecursionLimit:  runConfig.RecursionLimit,
		MaxConcurrency:  runConfig.MaxConcurrency,
		ThreadID:        runtime.ThreadID,
		RunID:           nodeCallbackRunID(runConfig.RunID, runtime.TaskID, runtime.Attempt) + "/subgraph",
		ParentRunID:     nodeCallbackRunID(runConfig.RunID, runtime.TaskID, runtime.Attempt),
		RunName:         string(runtime.Node),
		Tags:            cloneTags(runConfig.Tags),
		StreamSubgraphs: runConfig.StreamSubgraphs,
		Context:         runConfig.Context,
		Metadata:        cloneMessageMetadata(runConfig.Metadata),
		Callbacks:       append([]GraphCallback(nil), runConfig.Callbacks...),
		streamModes:     runConfig.streamModes,
		resuming:        runConfig.resuming,
	}
	if parentPersistence != nil {
		stateCodec := s.adapter.StateCodec
		deltaCodec := s.adapter.DeltaCodec
		if stateCodec == nil && child.persistence != nil {
			stateCodec = child.persistence.stateCodec
		}
		if deltaCodec == nil && child.persistence != nil {
			deltaCodec = child.persistence.deltaCodec
		}
		if stateCodec == nil || deltaCodec == nil {
			return Command[PD]{}, fmt.Errorf("subgraph %q requires child state and delta codecs for inherited persistence", runtime.Node)
		}
		child.persistence = &persistenceRuntime[CS, CD]{
			saver: parentPersistence.saver, stateCodec: stateCodec, deltaCodec: deltaCodec,
			errorCodec: parentPersistence.errorCodec,
			clock:      parentPersistence.clock, idGenerator: parentPersistence.idGenerator,
		}
		childConfig.CheckpointNamespace = subgraphNamespace(runtime.CheckpointNamespace, runtime.Node, runtime.CheckpointID, runtime.TaskID)
		childConfig.invocationParentCheckpoint = runtime.CheckpointID
		if s.adapter.Stateful {
			if s.adapter.MergeInput == nil {
				return Command[PD]{}, fmt.Errorf("stateful subgraph %q requires MergeInput", runtime.Node)
			}
			child.inputMerger = s.adapter.MergeInput
			instanceKey := ""
			if s.adapter.InstanceKey != nil {
				instanceKey, err = s.adapter.InstanceKey(ctx, parentState, runtime)
				if err != nil {
					return Command[PD]{}, fmt.Errorf("resolve stateful subgraph %q instance key: %w", runtime.Node, err)
				}
				if instanceKey == "" {
					return Command[PD]{}, fmt.Errorf("stateful subgraph %q instance key is empty", runtime.Node)
				}
			}
			childConfig.CheckpointNamespace = statefulSubgraphNamespace(runtime.CheckpointNamespace, runtime.Node, instanceKey)
			latestRequest := checkpoint.Config{ThreadID: runtime.ThreadID, Namespace: childConfig.CheckpointNamespace}
			latest, found, getErr := saverGetTuple(ctx, parentPersistence.saver, latestRequest)
			if getErr != nil {
				return Command[PD]{}, getErr
			}
			currentInvocation := false
			if found {
				marker, _ := latest.Metadata["invocation_parent_checkpoint"].(string)
				currentInvocation = marker == runtime.CheckpointID
			}
			if !currentInvocation {
				upperBound := runtime.CheckpointID
				if runtime.replayUpperBound != "" {
					upperBound = runtime.replayUpperBound
				}
				listOptions := checkpoint.ListOptions{
					Config: &latestRequest, Limit: 1,
					Before: &checkpoint.Config{ThreadID: runtime.ThreadID, Namespace: childConfig.CheckpointNamespace, CheckpointID: upperBound},
				}
				previous, listErr := parentPersistence.saver.List(ctx, listOptions)
				if listErr != nil {
					return Command[PD]{}, persistenceError("list-stateful-subgraph", latestRequest, listErr)
				}
				childConfig.NewRun = true
				if len(previous) > 0 {
					childConfig.CheckpointID = previous[0].Config.CheckpointID
				}
			}
		}
		if runConfig.resuming && runtime.subgraphCheckpointID != "" {
			childConfig.CheckpointNamespace = runtime.subgraphNamespace
			childConfig.CheckpointID = runtime.subgraphCheckpointID
		}
	} else {
		// An embedded child cannot safely use an unrelated saver because the
		// parent has no durable coordinate to resume it from.
		child.persistence = nil
	}

	streamNamespace := childConfig.CheckpointNamespace
	if streamNamespace == "" {
		streamNamespace = subgraphNamespace(runtime.CheckpointNamespace, runtime.Node, runtime.CheckpointID, runtime.TaskID)
	}
	var childEmit eventEmitter[CS, CD]
	if runConfig.StreamSubgraphs && emit != nil {
		namespace := splitNamespace(streamNamespace)
		childEmit = func(event StreamEvent[CS, CD]) error {
			return emit(StreamEvent[PS, PD]{
				Mode: event.Mode, Step: event.Step,
				Namespace: namespace,
				Subgraph: &SubgraphStreamEvent{
					Mode: event.Mode, Step: event.Step, State: event.State,
					Updates: event.Updates, Interrupts: cloneInterrupts(event.Interrupts), Err: event.Err,
					Custom:  event.Custom,
					Debug:   event.Debug,
					Message: cloneMessageStreamEvent(event.Message),
				},
			})
		}
	}

	final, err := child.run(ctx, input, childConfig, childEmit)
	if err != nil {
		var parentErr *ParentCommandError
		if errors.As(err, &parentErr) {
			childCommand, ok := parentErr.Command.(Command[CD])
			if !ok {
				return Command[PD]{}, fmt.Errorf("parent command from subgraph %q has type %T", runtime.Node, parentErr.Command)
			}
			var parentCommand Command[PD]
			if s.adapter.Parent != nil {
				parentCommand, err = s.adapter.Parent(ctx, parentState, childCommand)
			} else {
				parentCommand, err = identityParentCommand[PS, PD](childCommand)
			}
			if err != nil {
				return Command[PD]{}, fmt.Errorf("adapt parent command from subgraph %q: %w", runtime.Node, err)
			}
			if parentCommand.Target != CommandCurrent {
				return Command[PD]{}, fmt.Errorf("adapt parent command from subgraph %q returned non-current target", runtime.Node)
			}
			return parentCommand, nil
		}
		var interruptErr *GraphInterruptError
		if errors.As(err, &interruptErr) {
			checkpointID := ""
			if child.persistence != nil {
				latest, found, getErr := saverGetTuple(ctx, child.persistence.saver, checkpoint.Config{
					ThreadID: runtime.ThreadID, Namespace: childConfig.CheckpointNamespace,
				})
				if getErr != nil {
					return Command[PD]{}, getErr
				}
				if found {
					checkpointID = latest.Config.CheckpointID
				}
			}
			return Command[PD]{}, &subgraphInterruptSignal{
				namespace:    childConfig.CheckpointNamespace,
				checkpointID: checkpointID,
				interrupts:   cloneInterrupts(interruptErr.Interrupts),
			}
		}
		return Command[PD]{}, err
	}
	command, err := s.adapter.Output(ctx, parentState, final)
	if err != nil {
		return Command[PD]{}, fmt.Errorf("adapt subgraph output: %w", err)
	}
	return command, nil
}

func identityParentCommand[PS, PD any, CD any](child Command[CD]) (Command[PD], error) {
	parent := Command[PD]{
		Goto:   cloneNodeIDs(child.Goto),
		Target: CommandCurrent,
	}
	if child.HasUpdate {
		update, ok := any(child.Update).(PD)
		if !ok {
			return Command[PD]{}, fmt.Errorf("child delta %T is not assignable to parent delta", child.Update)
		}
		parent.Update = update
		parent.HasUpdate = true
	}
	if len(child.Sends) > 0 {
		parent.Sends = make([]TaskSend, len(child.Sends))
		for index, send := range child.Sends {
			state, ok := send.State.(PS)
			if !ok {
				return Command[PD]{}, fmt.Errorf("Send target %q child state %T is not assignable to parent state", send.Node, send.State)
			}
			parent.Sends[index] = TaskSend{Node: send.Node, State: state}
		}
	}
	return parent, nil
}

func subgraphNamespace(parent string, node NodeID, checkpointID, taskID string) string {
	component := fmt.Sprintf("%s:%s:%s", node, checkpointID, taskID)
	if parent == "" {
		return component
	}
	return parent + "|" + component
}

func statefulSubgraphNamespace(parent string, node NodeID, instanceKey string) string {
	component := string(node)
	if instanceKey != "" {
		component += ":instance:" + base64.RawURLEncoding.EncodeToString([]byte(instanceKey))
	}
	if parent == "" {
		return component
	}
	return parent + "|" + component
}

func splitNamespace(namespace string) []string {
	if namespace == "" {
		return nil
	}
	return strings.Split(namespace, "|")
}

type persistedSubgraphControl struct {
	Namespace    string      `json:"namespace"`
	CheckpointID string      `json:"checkpoint_id,omitempty"`
	Interrupts   []Interrupt `json:"interrupts"`
}

type subgraphInterruptSignal struct {
	namespace    string
	checkpointID string
	interrupts   []Interrupt
}

func (e *subgraphInterruptSignal) Error() string {
	return fmt.Sprintf("subgraph %q interrupted", e.namespace)
}

func encodeSubgraphControl(control persistedSubgraphControl) (checkpoint.EncodedValue, error) {
	data, err := json.Marshal(control)
	if err != nil {
		return checkpoint.EncodedValue{}, err
	}
	return checkpoint.EncodedValue{Type: subgraphControlType, Version: 1, Data: data}, nil
}

func decodeSubgraphControl(value checkpoint.EncodedValue) (persistedSubgraphControl, error) {
	if value.Type != subgraphControlType || value.Version != 1 {
		return persistedSubgraphControl{}, fmt.Errorf("%w: subgraph control has %s v%d", checkpoint.ErrCodecMismatch, value.Type, value.Version)
	}
	var control persistedSubgraphControl
	if err := json.Unmarshal(value.Data, &control); err != nil {
		return persistedSubgraphControl{}, err
	}
	if control.Namespace == "" {
		return persistedSubgraphControl{}, fmt.Errorf("%w: subgraph namespace is empty", checkpoint.ErrInvalidCheckpoint)
	}
	return control, nil
}

func cloneInterrupts(source []Interrupt) []Interrupt {
	result := make([]Interrupt, len(source))
	for index, item := range source {
		result[index] = Interrupt{ID: item.ID, Value: append(json.RawMessage(nil), item.Value...), Namespace: item.Namespace}
	}
	return result
}

func (s *typedSubgraphSpec[PS, PD, CS, CD]) snapshot(
	ctx context.Context,
	parentPersistence *persistenceRuntime[PS, PD],
	threadID, namespace, checkpointID string,
) (*NestedStateSnapshot, error) {
	if parentPersistence == nil {
		return nil, nil
	}
	child := *s.child
	stateCodec := s.adapter.StateCodec
	deltaCodec := s.adapter.DeltaCodec
	if stateCodec == nil && child.persistence != nil {
		stateCodec = child.persistence.stateCodec
	}
	if deltaCodec == nil && child.persistence != nil {
		deltaCodec = child.persistence.deltaCodec
	}
	if stateCodec == nil || deltaCodec == nil {
		return nil, nil
	}
	child.persistence = &persistenceRuntime[CS, CD]{
		saver: parentPersistence.saver, stateCodec: stateCodec, deltaCodec: deltaCodec,
		clock: parentPersistence.clock, idGenerator: parentPersistence.idGenerator,
	}
	snapshot, err := child.GetState(ctx, RunConfig{
		ThreadID: threadID, CheckpointNamespace: namespace, CheckpointID: checkpointID,
	}, WithSubgraphs())
	if err != nil {
		return nil, err
	}
	return nestedSnapshot(snapshot), nil
}

func (s *typedSubgraphSpec[PS, PD, CS, CD]) forkInterrupt(
	ctx context.Context,
	parentPersistence *persistenceRuntime[PS, PD],
	parentFork checkpoint.Config,
	parentTask checkpoint.Task,
	control persistedSubgraphControl,
) (persistedSubgraphControl, error) {
	if parentPersistence == nil {
		return persistedSubgraphControl{}, ErrCheckpointerRequired
	}
	child := *s.child
	stateCodec := s.adapter.StateCodec
	deltaCodec := s.adapter.DeltaCodec
	if stateCodec == nil && child.persistence != nil {
		stateCodec = child.persistence.stateCodec
	}
	if deltaCodec == nil && child.persistence != nil {
		deltaCodec = child.persistence.deltaCodec
	}
	if stateCodec == nil || deltaCodec == nil {
		return persistedSubgraphControl{}, fmt.Errorf("subgraph %q requires codecs for historical resume", parentTask.Name)
	}
	child.persistence = &persistenceRuntime[CS, CD]{
		saver: parentPersistence.saver, stateCodec: stateCodec, deltaCodec: deltaCodec,
		clock: parentPersistence.clock, idGenerator: parentPersistence.idGenerator,
	}
	source, err := child.interruptedTuple(ctx, parentFork.ThreadID, control)
	if err != nil {
		return persistedSubgraphControl{}, err
	}
	targetNamespace := subgraphNamespace(
		parentFork.Namespace, NodeID(parentTask.Name), parentFork.CheckpointID, parentTask.ID,
	)
	if s.adapter.Stateful {
		// The persisted control is the source of truth for stateful instance
		// identity. Recomputing it here would lose a task-local InstanceKey.
		targetNamespace = control.Namespace
	}
	forked, err := child.cloneInterruptedTuple(ctx, source, targetNamespace, parentFork.CheckpointID)
	if err != nil {
		return persistedSubgraphControl{}, err
	}
	return persistedSubgraphControl{
		Namespace:    targetNamespace,
		CheckpointID: forked.Config.CheckpointID,
		Interrupts:   cloneInterrupts(control.Interrupts),
	}, nil
}

func (s *typedSubgraphSpec[PS, PD, CS, CD]) validateInterruptFork(
	ctx context.Context,
	parentPersistence *persistenceRuntime[PS, PD],
	threadID string,
	control persistedSubgraphControl,
) error {
	if parentPersistence == nil {
		return ErrCheckpointerRequired
	}
	child := *s.child
	stateCodec := s.adapter.StateCodec
	deltaCodec := s.adapter.DeltaCodec
	if stateCodec == nil && child.persistence != nil {
		stateCodec = child.persistence.stateCodec
	}
	if deltaCodec == nil && child.persistence != nil {
		deltaCodec = child.persistence.deltaCodec
	}
	if stateCodec == nil || deltaCodec == nil {
		return fmt.Errorf("subgraph requires codecs for historical resume")
	}
	child.persistence = &persistenceRuntime[CS, CD]{
		saver: parentPersistence.saver, stateCodec: stateCodec, deltaCodec: deltaCodec,
		clock: parentPersistence.clock, idGenerator: parentPersistence.idGenerator,
	}
	tuple, err := child.interruptedTuple(ctx, threadID, control)
	if err != nil {
		return err
	}
	return child.validateInterruptedTuple(ctx, tuple)
}

func (g *CompiledGraph[S, D]) validateInterruptedTuple(ctx context.Context, tuple checkpoint.Tuple) error {
	tasks := make(map[string]checkpoint.Task, len(tuple.Checkpoint.Next))
	for _, task := range tuple.Checkpoint.Next {
		tasks[task.ID] = task
	}
	found := false
	for _, write := range tuple.PendingWrites {
		switch write.Channel {
		case checkpoint.InterruptChannel:
			if _, exists := tasks[write.TaskID]; !exists {
				continue
			}
			if _, err := g.persistence.decodeTaskControl(write.Value); err != nil {
				return persistenceError("validate-historical-subgraph-interrupt", tuple.Config, err)
			}
			found = true
		case checkpoint.SubgraphChannel:
			control, err := decodeSubgraphControl(write.Value)
			if err != nil {
				return persistenceError("validate-historical-deep-subgraph", tuple.Config, err)
			}
			task, exists := tasks[write.TaskID]
			if !exists {
				return persistenceError("validate-historical-subgraph-task", tuple.Config, checkpoint.ErrInvalidCheckpoint)
			}
			spec, exists := g.subgraphs[NodeID(task.Name)]
			if !exists {
				return persistenceError("validate-historical-subgraph-spec", tuple.Config, checkpoint.ErrInvalidCheckpoint)
			}
			if err := spec.validateInterruptFork(ctx, g.persistence, tuple.Config.ThreadID, control); err != nil {
				return err
			}
			found = true
		}
	}
	if !found {
		return persistenceError("validate-historical-subgraph", tuple.Config, checkpoint.ErrNotFound)
	}
	return nil
}

func (g *CompiledGraph[S, D]) interruptedTuple(
	ctx context.Context,
	threadID string,
	control persistedSubgraphControl,
) (checkpoint.Tuple, error) {
	requested := checkpoint.Config{
		ThreadID: threadID, Namespace: control.Namespace, CheckpointID: control.CheckpointID,
	}
	if control.CheckpointID != "" {
		tuple, found, err := saverGetTuple(ctx, g.persistence.saver, requested)
		if err != nil {
			return checkpoint.Tuple{}, err
		}
		if !found {
			return checkpoint.Tuple{}, persistenceError("get-historical-subgraph", requested, checkpoint.ErrNotFound)
		}
		matches, matchErr := g.tupleContainsInterrupts(tuple, control.Interrupts)
		if matchErr != nil {
			return checkpoint.Tuple{}, matchErr
		}
		if !matches {
			return checkpoint.Tuple{}, persistenceError("match-historical-subgraph", requested, checkpoint.ErrInvalidCheckpoint)
		}
		return tuple, nil
	}
	// Version-1 subgraph controls created before checkpoint_id was added are
	// still readable. Find the newest checkpoint containing the summarized
	// interrupt IDs rather than accidentally selecting a later completed head.
	history, err := g.persistence.saver.List(ctx, checkpoint.ListOptions{
		Config: &checkpoint.Config{ThreadID: threadID, Namespace: control.Namespace},
	})
	if err != nil {
		return checkpoint.Tuple{}, persistenceError("list-historical-subgraph", requested, err)
	}
	for _, tuple := range history {
		matches, matchErr := g.tupleContainsInterrupts(tuple, control.Interrupts)
		if matchErr != nil {
			return checkpoint.Tuple{}, matchErr
		}
		if matches {
			return tuple, nil
		}
	}
	return checkpoint.Tuple{}, persistenceError("find-historical-subgraph", requested, checkpoint.ErrNotFound)
}

func (g *CompiledGraph[S, D]) tupleContainsInterrupts(
	tuple checkpoint.Tuple,
	want []Interrupt,
) (bool, error) {
	if len(want) == 0 {
		return false, nil
	}
	found := make(map[string]struct{}, len(want))
	tasks := checkpointScheduledTaskIDs(tuple.Checkpoint)
	for _, write := range tuple.PendingWrites {
		switch write.Channel {
		case checkpoint.InterruptChannel:
			if _, exists := tasks[write.TaskID]; !exists {
				continue
			}
			control, err := g.persistence.decodeTaskControl(write.Value)
			if err != nil {
				return false, persistenceError("decode-historical-subgraph-interrupt", tuple.Config, err)
			}
			for _, item := range control.Interrupts {
				found[item.ID] = struct{}{}
			}
		case checkpoint.SubgraphChannel:
			control, err := decodeSubgraphControl(write.Value)
			if err != nil {
				return false, persistenceError("decode-historical-nested-subgraph", tuple.Config, err)
			}
			for _, item := range control.Interrupts {
				found[item.ID] = struct{}{}
			}
		}
	}
	for _, item := range want {
		if _, exists := found[item.ID]; !exists {
			return false, nil
		}
	}
	return true, nil
}

func (g *CompiledGraph[S, D]) cloneInterruptedTuple(
	ctx context.Context,
	source checkpoint.Tuple,
	targetNamespace string,
	invocationParentCheckpoint string,
) (checkpoint.Tuple, error) {
	id, timestamp, err := g.persistence.nextID()
	if err != nil {
		return checkpoint.Tuple{}, persistenceError("generate-subgraph-fork-id", source.Config, err)
	}
	value := checkpoint.CloneCheckpoint(source.Checkpoint)
	value.ID = id
	value.Timestamp = timestamp
	value.ChannelVersions = make(map[string]string, len(value.Values))
	value.UpdatedChannels = make([]string, 0, len(value.Values))
	newVersions := make(map[string]string, len(value.Values))
	for channel := range value.Values {
		version := id
		value.ChannelVersions[channel] = version
		newVersions[channel] = version
		value.UpdatedChannels = append(value.UpdatedChannels, channel)
	}
	value.VersionsSeen = map[string]map[string]string{}
	parent := checkpoint.Config{ThreadID: source.Config.ThreadID, Namespace: targetNamespace}
	if targetNamespace == source.Config.Namespace {
		parent.CheckpointID = source.Config.CheckpointID
	}
	stored, err := g.persistence.saver.Put(
		ctx,
		parent,
		value,
		checkpointMetadata(checkpoint.SourceFork, value.Step, "", checkpoint.Metadata{
			"nested_resume_fork":           true,
			"forked_from_namespace":        source.Config.Namespace,
			"forked_from_checkpoint_id":    source.Config.CheckpointID,
			"invocation_parent_checkpoint": invocationParentCheckpoint,
		}),
		newVersions,
	)
	if err != nil {
		return checkpoint.Tuple{}, persistenceError("put-subgraph-resume-fork", parent, err)
	}

	tasks := make(map[string]checkpoint.Task, len(value.Next))
	for _, task := range value.Next {
		tasks[task.ID] = task
	}
	interruptedTasks := make(map[string]struct{})
	for _, write := range source.PendingWrites {
		if write.Channel == checkpoint.InterruptChannel || write.Channel == checkpoint.SubgraphChannel {
			interruptedTasks[write.TaskID] = struct{}{}
		}
	}
	writes := make([]checkpoint.PendingWrite, 0, len(source.PendingWrites))
	for _, write := range source.PendingWrites {
		if write.Channel == checkpoint.TaskResultChannel {
			if _, interrupted := interruptedTasks[write.TaskID]; interrupted {
				// The original branch can append a successful result to the same
				// checkpoint after it is resumed. Reusing that result would bypass
				// the newly supplied historical resume value. Results belonging to
				// unrelated parallel tasks remain safe to recover.
				continue
			}
		}
		cloned := write
		cloned.Value = checkpoint.CloneEncodedValue(write.Value)
		switch write.Channel {
		case checkpoint.InterruptChannel:
			if _, exists := tasks[write.TaskID]; !exists {
				// Auxiliary task protocols own their envelopes. Preserve them
				// byte-for-byte when cloning the checkpoint branch.
				break
			}
			control, decodeErr := g.persistence.decodeTaskControl(write.Value)
			if decodeErr != nil {
				return checkpoint.Tuple{}, persistenceError("decode-subgraph-resume-fork", source.Config, decodeErr)
			}
			control.Resumes = nil
			cloned.Value, err = g.persistence.encodeTaskControl(control)
			if err != nil {
				return checkpoint.Tuple{}, persistenceError("encode-subgraph-resume-fork", stored, err)
			}
		case checkpoint.SubgraphChannel:
			control, decodeErr := decodeSubgraphControl(write.Value)
			if decodeErr != nil {
				return checkpoint.Tuple{}, persistenceError("decode-deep-subgraph-resume-fork", source.Config, decodeErr)
			}
			task, exists := tasks[write.TaskID]
			if !exists {
				return checkpoint.Tuple{}, persistenceError("map-deep-subgraph-resume-fork", source.Config, checkpoint.ErrInvalidCheckpoint)
			}
			spec, exists := g.subgraphs[NodeID(task.Name)]
			if !exists {
				return checkpoint.Tuple{}, persistenceError("find-deep-subgraph-resume-fork", source.Config, checkpoint.ErrInvalidCheckpoint)
			}
			forkedControl, forkErr := spec.forkInterrupt(ctx, g.persistence, stored, task, control)
			if forkErr != nil {
				return checkpoint.Tuple{}, forkErr
			}
			cloned.Value, err = encodeSubgraphControl(forkedControl)
			if err != nil {
				return checkpoint.Tuple{}, persistenceError("encode-deep-subgraph-resume-fork", stored, err)
			}
		}
		writes = append(writes, cloned)
	}
	if err := g.persistence.saver.PutWrites(ctx, stored, writes); err != nil {
		return checkpoint.Tuple{}, persistenceError("put-subgraph-resume-fork-writes", stored, err)
	}
	forked, found, err := saverGetTuple(ctx, g.persistence.saver, stored)
	if err != nil {
		return checkpoint.Tuple{}, err
	}
	if !found {
		return checkpoint.Tuple{}, persistenceError("get-subgraph-resume-fork", stored, checkpoint.ErrNotFound)
	}
	return forked, nil
}
