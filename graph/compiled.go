package graph

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"runtime/debug"
	"sort"
	"strings"
	"sync"
	"time"

	cachepkg "github.com/ybszm/langgraph-go/cache"
	"github.com/ybszm/langgraph-go/checkpoint"
	"github.com/ybszm/langgraph-go/managed"
	lgstore "github.com/ybszm/langgraph-go/store"
	"golang.org/x/sync/errgroup"
)

// CompiledGraph is an immutable graph execution plan. It is safe for
// concurrent invocation as long as the registered Node, Router, Reducer, and
// StateCloner functions are themselves safe for concurrent use. Durable runs
// sharing a thread and namespace are serialized by the compiled graph.
type CompiledGraph[S, D any] struct {
	reducer                 Reducer[S, D]
	cloner                  StateCloner[S]
	managedValues           managed.Projector[S]
	checkpointChannels      CheckpointChannelProjector[S]
	contextSchema           *runtimeContextSchema
	inputMerger             InputMerger[S]
	deltaNormalizer         DeltaNormalizer[D]
	messageExtractor        MessageExtractor[D]
	inputMessageExtractor   InputMessageExtractor[S]
	nodes                   map[NodeID]Node[S, D]
	edges                   map[NodeID][]NodeID
	branches                map[NodeID][]conditionalBranch[S]
	commandDestinations     map[NodeID][]NodeID
	retryPolicies           map[NodeID][]RetryPolicy
	errorHandlers           map[NodeID]resolvedErrorHandler[S, D]
	cachePolicies           map[NodeID]*nodeCachePolicy
	timeoutPolicies         map[NodeID]NodeTimeoutPolicy
	persistence             *persistenceRuntime[S, D]
	cache                   *cacheRuntime[S, D]
	store                   lgstore.Store
	interruptBefore         map[NodeID]struct{}
	interruptAfter          map[NodeID]struct{}
	waitingEdges            []waitingEdge
	sendBranches            map[NodeID]sendBranch[S]
	subgraphs               map[NodeID]subgraphSpec[S, D]
	nodeSchemas             map[NodeID]nodeSchemaInfo
	checkpointChannelSchema map[string]CheckpointChannelBinding[S]
	nodeChannelReads        map[NodeID][]string
	nodeChannelTriggers     map[NodeID][]string
	dynamicInterruptNodes   map[NodeID]struct{}
	deferredNodes           map[NodeID]struct{}
	runLocks                *runLockSet
}

type scheduledTask struct {
	node     NodeID
	taskID   string
	triggers []NodeID
	input    any
	hasInput bool
}

type routedDestination struct {
	node    NodeID
	trigger NodeID
}

type nodeSchemaInfo struct {
	input      string
	output     string
	inputType  reflect.Type
	outputType reflect.Type
}

func cloneNodeSchemas(source map[NodeID]nodeSchemaInfo) map[NodeID]nodeSchemaInfo {
	result := make(map[NodeID]nodeSchemaInfo, len(source))
	for id, schema := range source {
		result[id] = schema
	}
	return result
}

func cloneNodeChannelReads(source map[NodeID][]string) map[NodeID][]string {
	result := make(map[NodeID][]string, len(source))
	for node, reads := range source {
		result[node] = append([]string(nil), reads...)
	}
	return result
}

type taskResult[D any] struct {
	node      NodeID
	taskID    string
	cmd       Command[D]
	cached    bool
	handledBy NodeID
}

func (result taskResult[D]) outputNode() NodeID {
	if result.handledBy != "" {
		return result.handledBy
	}
	return result.node
}

func (result taskResult[D]) outputTaskID() string {
	if result.handledBy != "" {
		return errorHandlerTaskID(result.taskID, result.handledBy)
	}
	return result.taskID
}

type runInitialization[S any, D any] struct {
	state                S
	tasks                []scheduledTask
	nextStep             int
	committedStep        int
	checkpointConfig     checkpoint.Config
	recovered            map[string]taskResult[D]
	failures             map[string]recoveredTaskFailure
	controls             map[string]persistedTaskControl
	subgraphControls     map[string]persistedSubgraphControl
	waiting              map[string]map[NodeID]struct{}
	replayUpperBound     string
	staticBeforeConsumed bool
}

type eventEmitter[S, D any] func(StreamEvent[S, D]) error

type runLockSet struct {
	locks sync.Map
}

func newRunLockSet() *runLockSet {
	return &runLockSet{}
}

func (s *runLockSet) lock(key string) func() {
	value, _ := s.locks.LoadOrStore(key, &sync.Mutex{})
	lock := value.(*sync.Mutex)
	lock.Lock()
	return lock.Unlock
}

// Invoke executes the graph until every branch reaches END or an error occurs.
// When persistence is configured, an existing thread resumes its latest (or
// explicitly selected) checkpoint and input is used only for a new thread.
func (g *CompiledGraph[S, D]) Invoke(
	ctx context.Context,
	input S,
	config RunConfig,
) (S, error) {
	return g.run(ctx, input, config, nil)
}

// Nodes returns all user node IDs in lexical order.
func (g *CompiledGraph[S, D]) Nodes() []NodeID {
	ids := make([]NodeID, 0, len(g.nodes))
	for id := range g.nodes {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids
}

// Edges returns a stable copy of all declared static edges.
func (g *CompiledGraph[S, D]) Edges() []Edge {
	fromIDs := make([]NodeID, 0, len(g.edges))
	for from := range g.edges {
		fromIDs = append(fromIDs, from)
	}
	sort.Slice(fromIDs, func(i, j int) bool { return fromIDs[i] < fromIDs[j] })

	edges := make([]Edge, 0)
	for _, from := range fromIDs {
		for _, to := range g.edges[from] {
			edges = append(edges, Edge{From: from, To: to})
		}
	}
	return edges
}

// CommandDestinations returns the declared command-routing targets for source.
func (g *CompiledGraph[S, D]) CommandDestinations(source NodeID) []NodeID {
	return cloneNodeIDs(g.commandDestinations[source])
}

func (g *CompiledGraph[S, D]) run(
	ctx context.Context,
	input S,
	config RunConfig,
	emit eventEmitter[S, D],
) (S, error) {
	return g.runInternal(ctx, input, config, emit, true)
}

func (g *CompiledGraph[S, D]) runInternal(
	ctx context.Context,
	input S,
	config RunConfig,
	emit eventEmitter[S, D],
	acquireLock bool,
) (output S, runErr error) {
	if ctx == nil {
		var zero S
		return zero, fmt.Errorf("%w: context is nil", ErrInvalidRunConfig)
	}
	if config.RecursionLimit < 0 {
		return input, fmt.Errorf(
			"%w: recursion limit cannot be negative",
			ErrInvalidRunConfig,
		)
	}
	if config.MaxConcurrency < 0 {
		return input, fmt.Errorf(
			"%w: max concurrency cannot be negative",
			ErrInvalidRunConfig,
		)
	}
	switch config.Durability {
	case DurabilityUnspecified, DurabilitySync:
		// supported (sync is the persistent runtime default)
	case DurabilityAsync, DurabilityExit:
		return input, fmt.Errorf("%w: %q (only %q is implemented; see docs/DURABILITY.md)",
			ErrUnsupportedDurability, config.Durability, DurabilitySync)
	default:
		return input, fmt.Errorf("%w: unknown durability %q", ErrInvalidRunConfig, config.Durability)
	}
	if g.persistence == nil && (config.CheckpointID != "" || config.CheckpointNamespace != "") {
		return input, fmt.Errorf(
			"%w: checkpoint selection requires a configured checkpointer",
			ErrInvalidRunConfig,
		)
	}
	if g.contextSchema != nil {
		if err := g.contextSchema.validate(config.Context); err != nil {
			return input, err
		}
	}

	recursionLimit := config.RecursionLimit
	if recursionLimit == 0 {
		recursionLimit = DefaultRecursionLimit
	}

	if g.persistence != nil {
		if config.ThreadID == "" {
			return input, fmt.Errorf(
				"%w: thread ID is required for persistent execution",
				ErrInvalidRunConfig,
			)
		}
		if acquireLock {
			unlock := g.runLocks.lock(config.ThreadID + "\x00" + config.CheckpointNamespace)
			defer unlock()
		}
	}

	config.Callbacks = append([]GraphCallback(nil), config.Callbacks...)
	notifyGraphRunStart(ctx, config, input)
	defer func() {
		if runErr == nil || errors.Is(runErr, ErrGraphInterrupt) {
			notifyGraphRunEnd(ctx, config, output)
			return
		}
		notifyGraphRunError(ctx, config, runErr)
	}()
	initialized, err := g.initializeRun(ctx, input, config)
	if err != nil {
		return input, err
	}
	if initialized.replayUpperBound != "" {
		config.replayUpperBound = initialized.replayUpperBound
	}
	if config.resuming {
		notifyGraphResume(ctx, config, initialized.checkpointConfig)
	}
	state := initialized.state
	tasks := initialized.tasks
	step := initialized.nextStep
	committedStep := initialized.committedStep
	currentCheckpoint := initialized.checkpointConfig
	recovered := initialized.recovered
	failures := initialized.failures
	controls := initialized.controls
	subgraphControls := initialized.subgraphControls
	waiting := initialized.waiting
	staticBeforeConsumed := initialized.staticBeforeConsumed

	if err := emitEvent(emit, StreamEvent[S, D]{
		Mode:  StreamValues,
		Step:  committedStep,
		State: state,
	}); err != nil {
		return state, err
	}

	for len(tasks) > 0 {
		if step >= recursionLimit {
			return state, &RecursionError{Limit: recursionLimit, Step: step}
		}
		if err := ctx.Err(); err != nil {
			return state, err
		}
		if g.matchesInterruptBefore(tasks) && !staticBeforeConsumed {
			if err := g.putStaticInterrupt(ctx, currentCheckpoint, tasks, "before", false); err != nil {
				return state, err
			}
			notifyGraphInterrupt(ctx, config, LifecycleInterruptBefore, currentCheckpoint, nil)
			return state, &GraphInterruptError{}
		}

		results, err := g.executeStep(
			ctx,
			step,
			tasks,
			state,
			recursionLimit,
			config.MaxConcurrency,
			currentCheckpoint,
			recovered,
			failures,
			controls,
			subgraphControls,
			config,
			emit,
		)
		if err != nil {
			if debugErr := g.emitPendingCheckpointDebug(ctx, emit, step, state, currentCheckpoint, tasks, config); debugErr != nil {
				return state, debugErr
			}
			var interruptErr *GraphInterruptError
			if errors.As(err, &interruptErr) {
				notifyGraphInterrupt(ctx, config, LifecyclePending, currentCheckpoint, interruptErr.Interrupts)
			}
			return state, err
		}

		deltas := make([]D, 0, len(results))
		updates := make([]NodeUpdate[D], 0, len(results))
		for _, result := range results {
			if !result.cmd.HasUpdate {
				continue
			}
			deltas = append(deltas, result.cmd.Update)
			updates = append(updates, NodeUpdate[D]{
				Node:   result.outputNode(),
				TaskID: result.outputTaskID(),
				Delta:  result.cmd.Update,
				Cached: result.cached,
			})
		}

		if len(deltas) > 0 {
			state, err = g.reducer(ctx, state, deltas)
			if err != nil {
				return state, fmt.Errorf("reduce step %d: %w", step, err)
			}
		}

		nextTasks, err := g.resolveNextTasks(
			ctx, step, results, state, waiting, recursionLimit, config, currentCheckpoint,
		)
		if err != nil {
			return state, err
		}

		parentCheckpoint := currentCheckpoint
		if g.persistence != nil {
			currentCheckpoint, nextTasks, err = g.commitCheckpoint(
				ctx,
				currentCheckpoint,
				state,
				nextTasks,
				step,
				config.RunID,
				waiting,
				results,
				config.invocationParentCheckpoint,
			)
			if err != nil {
				return state, err
			}
		}
		committedStep = step
		debugNext := make([]NodeID, len(nextTasks))
		debugTasks := make([]DebugTaskSnapshot, len(nextTasks))
		for index, task := range nextTasks {
			debugNext[index] = task.node
			debugTasks[index] = DebugTaskSnapshot{TaskID: task.taskID, Node: task.node}
		}
		if err := emitEvent(emit, StreamEvent[S, D]{
			Mode: StreamDebug, Step: step,
			Debug: &DebugEvent{
				Kind: DebugCheckpoint, Step: step,
				Checkpoint: currentCheckpoint, ParentCheckpoint: parentCheckpoint,
				Values: state, Next: debugNext, Tasks: debugTasks,
				Metadata: cloneMessageMetadata(checkpointMetadata(
					checkpoint.SourceLoop, step, config.RunID,
					invocationMetadata(config.invocationParentCheckpoint),
				)),
			},
		}); err != nil {
			return state, err
		}
		if len(updates) > 0 {
			if err := emitEvent(emit, StreamEvent[S, D]{
				Mode:    StreamUpdates,
				Step:    step,
				State:   state,
				Updates: updates,
			}); err != nil {
				return state, err
			}
		}
		if err := emitEvent(emit, StreamEvent[S, D]{
			Mode:  StreamValues,
			Step:  step,
			State: state,
		}); err != nil {
			return state, err
		}
		if g.matchesInterruptAfter(results) {
			if err := g.putStaticInterrupt(ctx, currentCheckpoint, nextTasks, "after", false); err != nil {
				return state, err
			}
			notifyGraphInterrupt(ctx, config, LifecycleInterruptAfter, currentCheckpoint, nil)
			return state, &GraphInterruptError{}
		}

		tasks = nextTasks
		recovered = nil
		failures = nil
		controls = nil
		subgraphControls = nil
		staticBeforeConsumed = false
		step++
	}

	if err := emitEvent(emit, StreamEvent[S, D]{
		Mode:  StreamDone,
		Step:  committedStep,
		State: state,
	}); err != nil {
		return state, err
	}
	return state, nil
}

func (g *CompiledGraph[S, D]) emitPendingCheckpointDebug(
	ctx context.Context,
	emit eventEmitter[S, D],
	step int,
	state S,
	coordinate checkpoint.Config,
	tasks []scheduledTask,
	config RunConfig,
) error {
	if g.persistence == nil || emit == nil || coordinate.CheckpointID == "" {
		return nil
	}
	if _, enabled := config.streamModes[StreamDebug]; !enabled {
		return nil
	}
	tuple, found, err := saverGetTuple(ctx, g.persistence.saver, coordinate)
	if err != nil {
		return persistenceError("debug-pending-get", coordinate, err)
	}
	if !found {
		return persistenceError("debug-pending-get", coordinate, checkpoint.ErrNotFound)
	}
	snapshot, err := g.stateSnapshot(ctx, tuple, false)
	if err != nil {
		return err
	}
	byID := make(map[string]StateTask[S, D], len(snapshot.Tasks))
	for _, task := range snapshot.Tasks {
		byID[task.ID] = task
	}
	failures := make(map[string]error)
	for _, write := range tuple.PendingWrites {
		if write.Channel != checkpoint.NodeErrorChannel {
			continue
		}
		failure, decodeErr := g.persistence.decodeNodeFailure(write.Value)
		if decodeErr != nil {
			return persistenceError("debug-pending-error", coordinate, decodeErr)
		}
		failures[write.TaskID] = failure.err
	}
	debugTasks := make([]DebugTaskSnapshot, len(tasks))
	next := make([]NodeID, 0, len(tasks))
	for index, task := range tasks {
		projected := byID[task.taskID]
		debugTasks[index] = DebugTaskSnapshot{
			TaskID: task.taskID, Node: task.node, Err: failures[task.taskID],
			Interrupts: cloneInterrupts(projected.Interrupts),
		}
		if projected.Result != nil {
			command := *projected.Result
			command.Goto = cloneNodeIDs(command.Goto)
			command.Sends = cloneTaskSends(command.Sends)
			debugTasks[index].Result = command
		}
		if !projected.Completed {
			next = append(next, task.node)
		}
	}
	parent := checkpoint.Config{}
	if tuple.ParentConfig != nil {
		parent = *tuple.ParentConfig
	}
	return emitEvent(emit, StreamEvent[S, D]{
		Mode: StreamDebug, Step: step,
		Debug: &DebugEvent{
			Kind: DebugCheckpoint, Step: step,
			Checkpoint: coordinate, ParentCheckpoint: parent,
			Values: state, Next: next, Tasks: debugTasks,
			Metadata: cloneMessageMetadata(tuple.Metadata),
		},
	})
}

func (g *CompiledGraph[S, D]) initializeRun(
	ctx context.Context,
	input S,
	config RunConfig,
) (runInitialization[S, D], error) {
	if g.persistence == nil {
		tasks, err := g.scheduleDefaultTasks(ctx, -1, 0, START, input)
		if err != nil {
			return runInitialization[S, D]{}, err
		}
		return runInitialization[S, D]{
			state:         input,
			tasks:         tasks,
			nextStep:      0,
			committedStep: -1,
			waiting:       make(map[string]map[NodeID]struct{}),
		}, nil
	}

	requested := checkpoint.Config{
		ThreadID:     config.ThreadID,
		Namespace:    config.CheckpointNamespace,
		CheckpointID: config.CheckpointID,
	}
	tuple, found, err := saverGetTuple(ctx, g.persistence.saver, requested)
	if err != nil {
		return runInitialization[S, D]{}, err
	}
	if found {
		restored, err := g.restoreRun(tuple)
		if err != nil {
			return runInitialization[S, D]{}, err
		}
		if config.NewRun && len(restored.tasks) == 0 {
			state := input
			if g.inputMerger != nil {
				state, err = g.inputMerger(ctx, restored.state, input)
				if err != nil {
					return runInitialization[S, D]{}, fmt.Errorf("merge new-run input: %w", err)
				}
			}
			inputStep := tuple.Checkpoint.Step + 1
			tasks, routeErr := g.scheduleDefaultTasks(ctx, inputStep, inputStep+1, START, state)
			if routeErr != nil {
				return runInitialization[S, D]{}, routeErr
			}
			stored, putErr := g.putCheckpoint(
				ctx, tuple.Config, state, tasks, inputStep, checkpoint.SourceInput,
				config.RunID, restored.waiting, nil, invocationMetadata(config.invocationParentCheckpoint),
			)
			if putErr != nil {
				return runInitialization[S, D]{}, putErr
			}
			initialized := runInitialization[S, D]{
				state: state, tasks: tasks, nextStep: inputStep + 1, committedStep: inputStep,
				checkpointConfig: stored, waiting: restored.waiting,
			}
			if config.CheckpointID != "" {
				initialized.replayUpperBound = tuple.Config.CheckpointID
			}
			return initialized, nil
		}
		if config.CheckpointID == "" {
			return restored, nil
		}

		latestRequest := requested
		latestRequest.CheckpointID = ""
		latest, latestFound, err := saverGetTuple(ctx, g.persistence.saver, latestRequest)
		if err != nil {
			return runInitialization[S, D]{}, err
		}
		if !latestFound || latest.Config.CheckpointID == tuple.Config.CheckpointID {
			restored.replayUpperBound = tuple.Config.CheckpointID
			return restored, nil
		}
		// Resume traversal may intentionally select the exact child checkpoint
		// recorded by a parent subgraph control even when another child branch is
		// newer. Its freshly written resume controls must remain attached; treating
		// this as ordinary time travel would clear them and re-fire the interrupt.
		if config.resuming {
			restored.replayUpperBound = tuple.Config.CheckpointID
			return restored, nil
		}

		// An exact historical checkpoint is a replay boundary. Results written
		// by the original branch must not suppress re-execution on the new
		// branch. update/fork checkpoints are already explicit branch points;
		// all other sources first receive a visible fork checkpoint.
		restored.recovered = nil
		restored.controls = nil
		restored.staticBeforeConsumed = false
		source, _ := tuple.Metadata["source"].(string)
		if source == string(checkpoint.SourceUpdate) || source == string(checkpoint.SourceFork) {
			restored.replayUpperBound = tuple.Config.CheckpointID
			return restored, nil
		}
		forkStep := tuple.Checkpoint.Step + 1
		forkTasks := rescheduleTasks(forkStep+1, restored.tasks)
		forkConfig, err := g.putCheckpoint(
			ctx,
			tuple.Config,
			restored.state,
			forkTasks,
			forkStep,
			checkpoint.SourceFork,
			config.RunID,
			restored.waiting,
			nil,
			invocationMetadata(config.invocationParentCheckpoint),
		)
		if err != nil {
			return runInitialization[S, D]{}, err
		}
		restored.tasks = forkTasks
		restored.nextStep = forkStep + 1
		restored.committedStep = forkStep
		restored.checkpointConfig = forkConfig
		restored.replayUpperBound = tuple.Config.CheckpointID
		return restored, nil
	}
	if config.CheckpointID != "" {
		return runInitialization[S, D]{}, persistenceError(
			"get",
			requested,
			checkpoint.ErrNotFound,
		)
	}

	tasks, err := g.scheduleDefaultTasks(ctx, -1, 0, START, input)
	if err != nil {
		return runInitialization[S, D]{}, err
	}
	stored, err := g.putCheckpoint(
		ctx,
		requested,
		input,
		tasks,
		-1,
		checkpoint.SourceInput,
		config.RunID,
		make(map[string]map[NodeID]struct{}),
		nil,
		invocationMetadata(config.invocationParentCheckpoint),
	)
	if err != nil {
		return runInitialization[S, D]{}, err
	}
	return runInitialization[S, D]{
		state:            input,
		tasks:            tasks,
		nextStep:         0,
		committedStep:    -1,
		checkpointConfig: stored,
		waiting:          make(map[string]map[NodeID]struct{}),
	}, nil
}

func (g *CompiledGraph[S, D]) restoreRun(
	tuple checkpoint.Tuple,
) (runInitialization[S, D], error) {
	encodedState, exists := tuple.Checkpoint.Values[checkpoint.StateChannel]
	if !exists {
		return runInitialization[S, D]{}, persistenceError(
			"decode-state",
			tuple.Config,
			fmt.Errorf("%w: state channel is missing", checkpoint.ErrInvalidCheckpoint),
		)
	}
	state, err := g.persistence.stateCodec.Decode(encodedState)
	if err != nil {
		return runInitialization[S, D]{}, persistenceError("decode-state", tuple.Config, err)
	}
	tasks := make([]scheduledTask, len(tuple.Checkpoint.Next))
	for index, task := range tuple.Checkpoint.Next {
		nodeID := NodeID(task.Name)
		if _, exists := g.nodes[nodeID]; !exists {
			return runInitialization[S, D]{}, persistenceError(
				"restore-tasks",
				tuple.Config,
				fmt.Errorf("%w: %q", ErrUnknownNode, nodeID),
			)
		}
		triggers := make([]NodeID, len(task.Triggers))
		for triggerIndex, trigger := range task.Triggers {
			triggers[triggerIndex] = NodeID(trigger)
		}
		tasks[index] = scheduledTask{node: nodeID, taskID: task.ID, triggers: triggers}
		if task.Input != nil {
			input, err := g.persistence.stateCodec.Decode(*task.Input)
			if err != nil {
				return runInitialization[S, D]{}, persistenceError("decode-task-input", tuple.Config, err)
			}
			tasks[index].input, tasks[index].hasInput = input, true
		}
	}

	recovered := make(map[string]taskResult[D])
	failures := make(map[string]recoveredTaskFailure)
	controls := make(map[string]persistedTaskControl)
	subgraphControls := make(map[string]persistedSubgraphControl)
	staticBeforeConsumed := false
	scheduledTaskIDs := make(map[string]struct{}, len(tasks))
	for _, task := range tasks {
		scheduledTaskIDs[task.taskID] = struct{}{}
	}
	for _, write := range tuple.PendingWrites {
		switch write.Channel {
		case checkpoint.TaskResultChannel:
			if _, scheduled := scheduledTaskIDs[write.TaskID]; !scheduled {
				continue
			}
			if write.Index != 0 {
				continue
			}
			result, err := g.persistence.decodeTaskResult(write.TaskID, write.Value)
			if err != nil {
				return runInitialization[S, D]{}, persistenceError(
					"decode-pending-write",
					tuple.Config,
					err,
				)
			}
			recovered[write.TaskID] = result
		case checkpoint.NodeErrorChannel:
			if _, scheduled := scheduledTaskIDs[write.TaskID]; !scheduled {
				continue
			}
			failure, err := g.persistence.decodeNodeFailure(write.Value)
			if err != nil {
				return runInitialization[S, D]{}, persistenceError(
					"decode-node-error", tuple.Config, err,
				)
			}
			failures[write.TaskID] = failure
		case checkpoint.InterruptChannel:
			if _, scheduled := scheduledTaskIDs[write.TaskID]; !scheduled {
				continue
			}
			control, err := g.persistence.decodeTaskControl(write.Value)
			if err != nil {
				return runInitialization[S, D]{}, persistenceError(
					"decode-interrupt",
					tuple.Config,
					err,
				)
			}
			controls[write.TaskID] = control
		case checkpoint.SubgraphChannel:
			if _, scheduled := scheduledTaskIDs[write.TaskID]; !scheduled {
				continue
			}
			control, err := decodeSubgraphControl(write.Value)
			if err != nil {
				return runInitialization[S, D]{}, persistenceError("decode-subgraph", tuple.Config, err)
			}
			subgraphControls[write.TaskID] = control
		case checkpoint.StaticInterruptChannel:
			gate, gateErr := decodeStaticInterrupt(write.Value)
			if gateErr != nil {
				return runInitialization[S, D]{}, persistenceError("decode-static-interrupt", tuple.Config, gateErr)
			}
			if gate.Kind == "before" {
				staticBeforeConsumed = gate.Consumed
			}
		}
	}
	return runInitialization[S, D]{
		state:                state,
		tasks:                tasks,
		nextStep:             tuple.Checkpoint.Step + 1,
		committedStep:        tuple.Checkpoint.Step,
		checkpointConfig:     tuple.Config,
		recovered:            recovered,
		failures:             failures,
		controls:             controls,
		subgraphControls:     subgraphControls,
		waiting:              waitingFromCheckpoint(tuple.Checkpoint.Waiting),
		staticBeforeConsumed: staticBeforeConsumed,
	}, nil
}

func (g *CompiledGraph[S, D]) executeStep(
	ctx context.Context,
	step int,
	tasks []scheduledTask,
	state S,
	recursionStop int,
	maxConcurrency int,
	checkpointConfig checkpoint.Config,
	recovered map[string]taskResult[D],
	failures map[string]recoveredTaskFailure,
	controls map[string]persistedTaskControl,
	subgraphControls map[string]persistedSubgraphControl,
	runConfig RunConfig,
	emit eventEmitter[S, D],
) ([]taskResult[D], error) {
	results := make([]taskResult[D], len(tasks))
	interrupted := make([]*interruptSignal, len(tasks))
	subgraphInterrupted := make([]*subgraphInterruptSignal, len(tasks))
	group, groupCtx := errgroup.WithContext(ctx)
	limit := maxConcurrency
	if limit == 0 || limit > len(tasks) {
		limit = len(tasks)
	}
	group.SetLimit(limit)
	taskStates := make([]S, len(tasks))
	for index, task := range tasks {
		taskState := state
		if task.hasInput {
			input, ok := task.input.(S)
			if !ok {
				return nil, fmt.Errorf("task %q input has type %T", task.taskID, task.input)
			}
			taskState = input
		}
		taskStates[index] = taskState
		if g.managedValues != nil {
			var projectionErr error
			taskState, projectionErr = g.projectManagedState(ctx, taskState, managed.Scope{
				Step: step, Stop: recursionStop, Node: string(task.node), TaskID: task.taskID,
				ThreadID:            runConfig.ThreadID,
				CheckpointNamespace: checkpointConfig.Namespace,
				CheckpointID:        checkpointConfig.CheckpointID,
			})
			if projectionErr != nil {
				return nil, &NodeExecutionError{
					Step: step, Node: task.node, TaskID: task.taskID,
					Err: projectionErr,
				}
			}
			taskStates[index] = taskState
		}
		if err := g.rememberInputMessages(ctx, emit, step, task, taskState, runConfig); err != nil {
			return nil, err
		}
	}
	prefetched, err := g.getCachedBatch(ctx, tasks, taskStates, recovered)
	if err != nil {
		return nil, fmt.Errorf("cache batch get: %w", err)
	}
	type cacheWrite struct {
		key     cachepkg.Key
		item    cachepkg.Item
		enabled bool
	}
	cacheWrites := make([]cacheWrite, len(tasks))

	for index, task := range tasks {
		index := index
		task := task
		taskState := taskStates[index]
		if err := emitEvent(emit, StreamEvent[S, D]{
			Mode: StreamDebug, Step: step,
			Debug: &DebugEvent{
				Kind: DebugTask, Step: step, Node: task.node, TaskID: task.taskID,
				Triggers: cloneNodeIDs(task.triggers), Input: taskState,
				Metadata: cloneMessageMetadata(runConfig.Metadata),
			},
		}); err != nil {
			return nil, err
		}
		if cached, exists := recovered[task.taskID]; exists {
			if cached.node != task.node {
				return nil, persistenceError(
					"restore-task",
					checkpointConfig,
					fmt.Errorf(
						"%w: task %q expected node %q, pending write contains %q",
						checkpoint.ErrInvalidCheckpoint,
						task.taskID,
						task.node,
						cached.node,
					),
				)
			}
			results[index] = cached
			outputTask := task
			outputTask.node = cached.outputNode()
			outputTask.taskID = cached.outputTaskID()
			if err := g.emitCommandMessages(
				ctx, emit, step, outputTask, checkpointConfig, runConfig, cached.cmd, cached.cached, true,
			); err != nil {
				return nil, err
			}
			if err := emitEvent(emit, StreamEvent[S, D]{
				Mode: StreamDebug, Step: step,
				Debug: &DebugEvent{
					Kind: DebugTaskResult, Step: step, Node: outputTask.node, TaskID: outputTask.taskID,
					Result: debugCommandResult(cached.cmd), Recovered: true,
				},
			}); err != nil {
				return nil, err
			}
			continue
		}

		group.Go(func() error {
			cached, found := prefetched[index].result, prefetched[index].found
			if found {
				results[index] = cached
				if err := g.emitCommandMessages(
					groupCtx, emit, step, task, checkpointConfig, runConfig, cached.cmd, true, false,
				); err != nil {
					return err
				}
				if err := emitEvent(emit, StreamEvent[S, D]{
					Mode: StreamDebug, Step: step,
					Debug: &DebugEvent{
						Kind: DebugTaskResult, Step: step, Node: task.node, TaskID: task.taskID,
						Result: debugCommandResult(cached.cmd), Cached: true,
					},
				}); err != nil {
					return err
				}
				return nil
			}
			var command Command[D]
			var handledBy NodeID
			var err error
			taskRunConfig := runConfig
			if subgraph, exists := subgraphControls[task.taskID]; exists {
				taskRunConfig.subgraphNamespace = subgraph.Namespace
				taskRunConfig.subgraphCheckpointID = subgraph.CheckpointID
			}
			if failure, recoveredFailure := failures[task.taskID]; recoveredFailure {
				if failure.source != task.node {
					return persistenceError("restore-node-error", checkpointConfig, fmt.Errorf(
						"%w: task %q expected node %q, failure marker contains %q",
						checkpoint.ErrInvalidCheckpoint, task.taskID, task.node, failure.source,
					))
				}
				command, handledBy, err = g.invokeErrorHandler(
					groupCtx, step, task.node, task.taskID, taskState, controls[task.taskID],
					taskRunConfig, checkpointConfig, emit, failure.err, true,
				)
			} else {
				command, handledBy, err = g.invokeNode(
					groupCtx,
					step,
					task.node,
					task.taskID,
					taskState,
					controls[task.taskID],
					taskRunConfig,
					checkpointConfig,
					emit,
				)
			}
			if err != nil {
				resultNode, resultTaskID := task.node, task.taskID
				if handledBy != "" {
					resultNode, resultTaskID = handledBy, errorHandlerTaskID(task.taskID, handledBy)
				}
				var subgraphSignal *subgraphInterruptSignal
				if errors.As(err, &subgraphSignal) {
					if emitErr := emitEvent(emit, StreamEvent[S, D]{
						Mode: StreamDebug, Step: step,
						Debug: &DebugEvent{Kind: DebugTaskResult, Step: step, Node: resultNode, TaskID: resultTaskID, Interrupts: cloneInterrupts(subgraphSignal.interrupts)},
					}); emitErr != nil {
						return emitErr
					}
					encoded, encodeErr := encodeSubgraphControl(persistedSubgraphControl{
						Namespace:    subgraphSignal.namespace,
						CheckpointID: subgraphSignal.checkpointID,
						Interrupts:   subgraphSignal.interrupts,
					})
					if encodeErr != nil {
						return persistenceError("encode-subgraph", checkpointConfig, encodeErr)
					}
					if writeErr := g.persistence.saver.PutWrites(ctx, checkpointConfig, []checkpoint.PendingWrite{{
						TaskID: task.taskID, TaskPath: string(task.node), Index: -2,
						Channel: checkpoint.SubgraphChannel, Value: encoded,
					}}); writeErr != nil {
						return persistenceError("put-subgraph", checkpointConfig, writeErr)
					}
					subgraphInterrupted[index] = subgraphSignal
					return nil
				}
				var signal *interruptSignal
				if errors.As(err, &signal) {
					if emitErr := emitEvent(emit, StreamEvent[S, D]{
						Mode: StreamDebug, Step: step,
						Debug: &DebugEvent{Kind: DebugTaskResult, Step: step, Node: resultNode, TaskID: resultTaskID, Interrupts: []Interrupt{{ID: signal.interrupt.ID, Value: append(json.RawMessage(nil), signal.interrupt.Value...)}}},
					}); emitErr != nil {
						return emitErr
					}
					encoded, encodeErr := g.persistence.encodeTaskControl(signal.control)
					if encodeErr != nil {
						return persistenceError("encode-interrupt", checkpointConfig, encodeErr)
					}
					writeErr := g.persistence.saver.PutWrites(ctx, checkpointConfig, []checkpoint.PendingWrite{{
						TaskID: task.taskID, TaskPath: string(task.node), Index: -1,
						Channel: checkpoint.InterruptChannel, Value: encoded,
					}})
					if writeErr != nil {
						return persistenceError("put-interrupt", checkpointConfig, writeErr)
					}
					interrupted[index] = signal
					return nil
				}
				if emitErr := emitEvent(emit, StreamEvent[S, D]{
					Mode: StreamDebug, Step: step,
					Debug: &DebugEvent{Kind: DebugTaskResult, Step: step, Node: resultNode, TaskID: resultTaskID, Err: err},
				}); emitErr != nil {
					return emitErr
				}
				return err
			}
			result := taskResult[D]{
				node: task.node, taskID: task.taskID, cmd: command, handledBy: handledBy,
			}
			outputTask := task
			if handledBy != "" {
				outputTask.node = handledBy
				outputTask.taskID = errorHandlerTaskID(task.taskID, handledBy)
			}
			if err := g.emitCommandMessages(
				groupCtx, emit, step, outputTask, checkpointConfig, runConfig, command, false, false,
			); err != nil {
				return err
			}
			if handledBy == "" {
				key, item, enabled, err := g.prepareCached(taskState, result)
				if err != nil {
					return fmt.Errorf("cache prepare for node %q: %w", task.node, err)
				}
				cacheWrites[index] = cacheWrite{key: key, item: item, enabled: enabled}
			}
			if g.persistence != nil {
				encoded, err := g.persistence.encodeTaskResult(result)
				if err != nil {
					return persistenceError("encode-pending-write", checkpointConfig, err)
				}
				err = g.persistence.saver.PutWrites(ctx, checkpointConfig, []checkpoint.PendingWrite{{
					TaskID:   task.taskID,
					TaskPath: string(task.node),
					Index:    0,
					Channel:  checkpoint.TaskResultChannel,
					Value:    encoded,
				}})
				if err != nil {
					return persistenceError("put-writes", checkpointConfig, err)
				}
			}
			results[index] = result
			if err := emitEvent(emit, StreamEvent[S, D]{
				Mode: StreamDebug, Step: step,
				Debug: &DebugEvent{
					Kind: DebugTaskResult, Step: step, Node: outputTask.node, TaskID: outputTask.taskID,
					Result: debugCommandResult(command),
				},
			}); err != nil {
				return err
			}
			return nil
		})
	}

	waitErr := group.Wait()
	if g.cache != nil {
		items := make(map[cachepkg.Key]cachepkg.Item)
		for _, write := range cacheWrites {
			if write.enabled {
				items[write.key] = write.item
			}
		}
		if len(items) > 0 {
			if err := g.cache.store.Set(ctx, items); err != nil && waitErr == nil {
				return nil, fmt.Errorf("cache batch set: %w", err)
			}
		}
	}
	if waitErr != nil {
		return nil, waitErr
	}
	interrupts := make([]Interrupt, 0)
	for _, signal := range interrupted {
		if signal == nil {
			continue
		}
		interrupts = append(interrupts, Interrupt{
			ID:    signal.interrupt.ID,
			Value: append(json.RawMessage(nil), signal.interrupt.Value...),
		})
	}
	for _, signal := range subgraphInterrupted {
		if signal == nil {
			continue
		}
		interrupts = append(interrupts, cloneInterrupts(signal.interrupts)...)
	}
	if len(interrupts) > 0 {
		return nil, &GraphInterruptError{Interrupts: interrupts}
	}
	return results, nil
}

func (g *CompiledGraph[S, D]) projectManagedState(
	ctx context.Context,
	state S,
	scope managed.Scope,
) (S, error) {
	if g.managedValues == nil {
		return state, nil
	}
	projectionInput := state
	if g.cloner != nil {
		var err error
		projectionInput, err = g.cloner(state)
		if err != nil {
			return state, fmt.Errorf("%w: clone canonical state: %w", ErrManagedValue, err)
		}
	}
	projected, err := g.managedValues(ctx, projectionInput, scope)
	if err != nil {
		return state, fmt.Errorf("%w: %w", ErrManagedValue, err)
	}
	return projected, nil
}

func debugCommandResult[D any](command Command[D]) any {
	if !command.HasUpdate {
		return nil
	}
	return command.Update
}

func (g *CompiledGraph[S, D]) invokeNode(
	ctx context.Context,
	step int,
	nodeID NodeID,
	taskID string,
	state S,
	control persistedTaskControl,
	runConfig RunConfig,
	checkpointConfig checkpoint.Config,
	emit eventEmitter[S, D],
) (Command[D], NodeID, error) {
	body := g.nodes[nodeID]
	if subgraph, exists := g.subgraphs[nodeID]; exists {
		body = func(ctx context.Context, state S, runtime Runtime) (Command[D], error) {
			return subgraph.invoke(ctx, state, runtime, g.persistence, runConfig, emit)
		}
	}
	command, err := g.invokeWithRetry(
		ctx, step, nodeID, taskID, state, control, runConfig, checkpointConfig, emit,
		g.retryPolicies[nodeID], g.timeoutPolicies[nodeID], body,
	)
	if err == nil {
		return command, "", nil
	}
	var signal *interruptSignal
	var subgraphSignal *subgraphInterruptSignal
	var parentCommand *ParentCommandError
	if errors.As(err, &signal) || errors.As(err, &subgraphSignal) || errors.As(err, &parentCommand) {
		return command, "", err
	}
	_, exists := g.errorHandlers[nodeID]
	if !exists {
		return command, "", err
	}
	if g.persistence != nil {
		encoded, encodeErr := g.persistence.encodeNodeFailure(nodeID, err)
		if encodeErr != nil {
			return Command[D]{}, "", persistenceError("encode-node-error", checkpointConfig, encodeErr)
		}
		if writeErr := g.persistence.saver.PutWrites(ctx, checkpointConfig, []checkpoint.PendingWrite{{
			TaskID: taskID, TaskPath: string(nodeID), Index: -4,
			Channel: checkpoint.NodeErrorChannel, Value: encoded,
		}}); writeErr != nil {
			return Command[D]{}, "", persistenceError("put-node-error", checkpointConfig, writeErr)
		}
	}
	return g.invokeErrorHandler(
		ctx, step, nodeID, taskID, state, control, runConfig, checkpointConfig, emit, err, false,
	)
}

func (g *CompiledGraph[S, D]) invokeErrorHandler(
	ctx context.Context,
	step int,
	nodeID NodeID,
	taskID string,
	state S,
	control persistedTaskControl,
	runConfig RunConfig,
	checkpointConfig checkpoint.Config,
	emit eventEmitter[S, D],
	sourceErr error,
	recovered bool,
) (Command[D], NodeID, error) {
	handler, exists := g.errorHandlers[nodeID]
	if !exists {
		return Command[D]{}, "", sourceErr
	}
	if emitErr := emitEvent(emit, StreamEvent[S, D]{
		Mode: StreamDebug, Step: step,
		Debug: &DebugEvent{
			Kind: DebugTaskResult, Step: step, Node: nodeID, TaskID: taskID,
			Err: sourceErr, Recovered: recovered,
		},
	}); emitErr != nil {
		return Command[D]{}, handler.node, emitErr
	}
	handlerTaskID := errorHandlerTaskID(taskID, handler.node)
	if emitErr := emitEvent(emit, StreamEvent[S, D]{
		Mode: StreamDebug, Step: step,
		Debug: &DebugEvent{
			Kind: DebugTask, Step: step, Node: handler.node, TaskID: handlerTaskID,
			Triggers: []NodeID{nodeID}, Input: state,
			Metadata: cloneMessageMetadata(runConfig.Metadata),
		},
	}); emitErr != nil {
		return Command[D]{}, handler.node, emitErr
	}
	failure := NodeError{Node: nodeID, Err: sourceErr}
	handlerBody := func(ctx context.Context, state S, runtime Runtime) (Command[D], error) {
		return handler.handler(ctx, state, failure, runtime)
	}
	command, err := g.invokeWithRetry(
		ctx, step, handler.node, handlerTaskID, state, control, runConfig, checkpointConfig, emit,
		g.retryPolicies[handler.node], g.timeoutPolicies[handler.node], handlerBody,
	)
	return command, handler.node, err
}

func errorHandlerTaskID(sourceTaskID string, handler NodeID) string {
	return sourceTaskID + ":error-handler:" + string(handler)
}

func (g *CompiledGraph[S, D]) invokeWithRetry(
	ctx context.Context,
	step int,
	nodeID NodeID,
	taskID string,
	state S,
	control persistedTaskControl,
	runConfig RunConfig,
	checkpointConfig checkpoint.Config,
	emit eventEmitter[S, D],
	policies []RetryPolicy,
	timeoutPolicy NodeTimeoutPolicy,
	body Node[S, D],
) (Command[D], error) {
	firstAttemptTime := time.Now().UTC()
	for attempt := 1; ; attempt++ {
		command, err := g.invokeNodeAttempt(
			ctx,
			step,
			nodeID,
			taskID,
			state,
			control,
			attempt,
			firstAttemptTime,
			runConfig,
			checkpointConfig,
			emit,
			timeoutPolicy,
			body,
		)
		if err == nil {
			return command, nil
		}
		var signal *interruptSignal
		if errors.As(err, &signal) {
			return command, err
		}
		var subgraphSignal *subgraphInterruptSignal
		if errors.As(err, &subgraphSignal) {
			return command, err
		}
		var parentCommand *ParentCommandError
		if errors.As(err, &parentCommand) {
			return command, err
		}
		policy := matchingRetryPolicy(policies, err)
		if policy == nil || attempt >= policy.MaxAttempts {
			return command, err
		}
		if err := waitRetry(ctx, retryDelay(*policy, attempt)); err != nil {
			return command, err
		}
	}
}

func (g *CompiledGraph[S, D]) invokeNodeAttempt(
	ctx context.Context,
	step int,
	nodeID NodeID,
	taskID string,
	state S,
	control persistedTaskControl,
	attempt int,
	firstAttemptTime time.Time,
	runConfig RunConfig,
	checkpointConfig checkpoint.Config,
	emit eventEmitter[S, D],
	timeoutPolicy NodeTimeoutPolicy,
	body Node[S, D],
) (command Command[D], err error) {
	var traceStart NodeRunStartEvent
	traceStarted := false
	traceFinished := false
	defer func() {
		if value := recover(); value != nil {
			err = &NodeExecutionError{
				Step:   step,
				Node:   nodeID,
				TaskID: taskID,
				Err: &NodePanicError{
					Value: value,
					Stack: debug.Stack(),
				},
			}
		}
		if traceStarted && !traceFinished && err != nil {
			notifyNodeRunError(ctx, runConfig, traceStart, err)
		}
	}()
	attemptCtx, heartbeat, finishTimeout := withNodeTimeoutPolicy(ctx, timeoutPolicy)
	timeoutFinished := false
	defer func() {
		if !timeoutFinished {
			finishTimeout()
		}
	}()
	ctx = attemptCtx

	nodeState := state
	if g.cloner != nil {
		nodeState, err = g.cloner(state)
		if err != nil {
			return command, &NodeExecutionError{
				Step:   step,
				Node:   nodeID,
				TaskID: taskID,
				Err:    fmt.Errorf("clone state: %w", err),
			}
		}
	}

	interrupt := g.interruptHandler(checkpointConfig.Namespace, taskID, control)
	runtime := Runtime{
		Step:                 step,
		Node:                 nodeID,
		TaskID:               taskID,
		Attempt:              attempt,
		FirstAttemptTime:     firstAttemptTime,
		ThreadID:             runConfig.ThreadID,
		CheckpointNamespace:  checkpointConfig.Namespace,
		CheckpointID:         checkpointConfig.CheckpointID,
		Store:                g.store,
		Checkpointer:         nil,
		Context:              runConfig.Context,
		interrupt:            interrupt,
		replayUpperBound:     runConfig.replayUpperBound,
		heartbeat:            heartbeat,
		subgraphNamespace:    runConfig.subgraphNamespace,
		subgraphCheckpointID: runConfig.subgraphCheckpointID,
	}
	if g.persistence != nil {
		runtime.Checkpointer = g.persistence.saver
	}
	if emit != nil {
		runtime.writeCustom = func(value any) error {
			return emit(StreamEvent[S, D]{Mode: StreamCustom, Step: step, Custom: value})
		}
		if _, enabled := runConfig.streamModes[StreamMessages]; enabled {
			runtime.writeMessage = func(message any, metadata map[string]any) error {
				return emit(StreamEvent[S, D]{
					Mode: StreamMessages,
					Step: step,
					Message: &MessageStreamEvent{
						Message: message,
						Metadata: messageStreamMetadata(
							step, nodeID, taskID, checkpointConfig, runConfig, metadata, false, false,
						),
					},
				})
			}
			runtime.writeContentBlock = func(content ContentBlockStreamEvent, metadata map[string]any) error {
				return emit(StreamEvent[S, D]{
					Mode: StreamMessages,
					Step: step,
					Message: &MessageStreamEvent{
						ContentBlock: cloneContentBlockStreamEvent(&content),
						Metadata: messageStreamMetadata(
							step, nodeID, taskID, checkpointConfig, runConfig, metadata, false, false,
						),
					},
				})
			}
		}
	}
	traceStart = nodeRunStartEvent(runConfig, checkpointConfig, step, nodeID, taskID, attempt, nodeState)
	notifyNodeRunStart(ctx, runConfig, traceStart)
	traceStarted = true
	command, err = body(ctx, nodeState, runtime)
	finishTimeout()
	timeoutFinished = true
	if timeout := nodeTimeoutCause(ctx); timeout != nil {
		err = timeout
	}
	if err != nil {
		var subgraphSignal *subgraphInterruptSignal
		if errors.As(err, &subgraphSignal) {
			return command, err
		}
		var parentCommand *ParentCommandError
		if errors.As(err, &parentCommand) {
			return command, err
		}
		return command, &NodeExecutionError{
			Step:   step,
			Node:   nodeID,
			TaskID: taskID,
			Err:    err,
		}
	}
	if command.Resume != nil {
		return Command[D]{}, &NodeExecutionError{
			Step: step, Node: nodeID, TaskID: taskID,
			Err: fmt.Errorf("%w: node returned an invocation resume command", ErrInvalidResume),
		}
	}
	if command.HasUpdate && g.deltaNormalizer != nil {
		command.Update, err = g.deltaNormalizer(ctx, command.Update)
		if err != nil {
			return command, &NodeExecutionError{
				Step:   step,
				Node:   nodeID,
				TaskID: taskID,
				Err:    fmt.Errorf("normalize delta: %w", err),
			}
		}
	}
	notifyNodeRunEnd(ctx, runConfig, traceStart, command)
	traceFinished = true
	switch command.Target {
	case CommandCurrent:
		return command, nil
	case CommandParent:
		return Command[D]{}, &ParentCommandError{Command: command}
	default:
		return Command[D]{}, &NodeExecutionError{
			Step: step, Node: nodeID, TaskID: taskID,
			Err: fmt.Errorf("invalid command target %d", command.Target),
		}
	}
}

func (g *CompiledGraph[S, D]) interruptHandler(
	checkpointNamespace string,
	taskID string,
	control persistedTaskControl,
) interruptHandler {
	if g.persistence == nil {
		return nil
	}
	index := 0
	var interruptMu sync.Mutex
	return func(value any, decode func(json.RawMessage) (any, error)) (any, error) {
		interruptMu.Lock()
		defer interruptMu.Unlock()
		coordinate, prompt, scoped := unwrapInterruptValue(value)
		current := index
		if scoped {
			current = coordinate.index
		} else {
			index++
		}
		raw, err := json.Marshal(prompt)
		if err != nil {
			return nil, fmt.Errorf("encode interrupt prompt: %w", err)
		}
		interrupt := persistedInterrupt{
			ID:    durableInterruptID(checkpointNamespace, taskID, current),
			Value: raw,
		}
		position := current
		if scoped {
			position = -1
			for candidate, persisted := range control.Interrupts {
				if persisted.Scope == coordinate.scope && persisted.Index == coordinate.index {
					position = candidate
					break
				}
			}
			interrupt.Scope = coordinate.scope
			interrupt.Index = coordinate.index
			interrupt.ID = durableInterruptID(
				checkpointNamespace, taskID+"\x00"+coordinate.scope, coordinate.index,
			)
		}
		if position >= 0 && position < len(control.Interrupts) {
			interrupt.ID = control.Interrupts[position].ID
			control.Interrupts[position] = interrupt
		} else {
			control.Interrupts = append(control.Interrupts, interrupt)
			position = len(control.Interrupts) - 1
		}
		if position < len(control.Resumes) && control.Resumes[position] != nil {
			result, err := decode(control.Resumes[position])
			if err != nil {
				return nil, fmt.Errorf("decode resume for interrupt %q: %w", interrupt.ID, err)
			}
			return result, nil
		}
		return nil, &interruptSignal{control: control, interrupt: interrupt}
	}
}

func (g *CompiledGraph[S, D]) resolveNextWithStates(
	ctx context.Context,
	step int,
	results []taskResult[D],
	state S,
	routingStates map[string]S,
	waiting map[string]map[NodeID]struct{},
) ([]NodeID, map[NodeID][]NodeID, error) {
	next := make([]NodeID, 0)
	seen := make(map[NodeID]struct{})
	triggers := make(map[NodeID][]NodeID)
	add := func(destination NodeID, sources ...NodeID) {
		if destination == END {
			return
		}
		for _, source := range sources {
			duplicate := false
			for _, existing := range triggers[destination] {
				if existing == source {
					duplicate = true
					break
				}
			}
			if !duplicate {
				triggers[destination] = append(triggers[destination], source)
			}
		}
		if _, duplicate := seen[destination]; !duplicate {
			seen[destination] = struct{}{}
			next = append(next, destination)
		}
	}
	for _, result := range results {
		source := result.outputNode()
		var routes []routedDestination
		var err error
		if result.cmd.Goto != nil {
			destinations, validationErr := g.validateExplicitDestinations(
				step,
				source,
				result.cmd.Goto,
			)
			err = validationErr
			for _, destination := range destinations {
				routes = append(routes, routedDestination{node: destination, trigger: branchToTrigger(destination)})
			}
		} else {
			routingState := state
			if projected, exists := routingStates[result.outputTaskID()]; exists {
				routingState = projected
			}
			routes, err = g.defaultRoutes(ctx, step, source, routingState)
		}
		if err != nil {
			return nil, nil, err
		}
		for _, route := range routes {
			add(route.node, route.trigger)
		}
	}
	for _, edge := range g.waitingEdges {
		barrier := waiting[edge.id]
		if barrier == nil {
			barrier = make(map[NodeID]struct{})
			waiting[edge.id] = barrier
		}
		for _, result := range results {
			resultNode := result.outputNode()
			for _, source := range edge.sources {
				if resultNode == source {
					barrier[source] = struct{}{}
					break
				}
			}
		}
		if len(barrier) != len(edge.sources) {
			continue
		}
		clear(barrier)
		add(edge.target, waitingTrigger(edge))
	}
	return next, triggers, nil
}

func (g *CompiledGraph[S, D]) resolveNextTasks(
	ctx context.Context,
	step int,
	results []taskResult[D],
	state S,
	waiting map[string]map[NodeID]struct{},
	recursionStop int,
	runConfig RunConfig,
	checkpointConfig checkpoint.Config,
) ([]scheduledTask, error) {
	routingStates := make(map[string]S, len(results))
	for _, result := range results {
		node, taskID := result.outputNode(), result.outputTaskID()
		projected, err := g.projectManagedState(ctx, state, managed.Scope{
			Step: step, Stop: recursionStop, Node: string(node), TaskID: taskID,
			ThreadID:            runConfig.ThreadID,
			CheckpointNamespace: checkpointConfig.Namespace,
			CheckpointID:        checkpointConfig.CheckpointID,
		})
		if err != nil {
			return nil, &RouterError{Step: step, Source: node, Err: err}
		}
		routingStates[taskID] = projected
	}
	destinations, triggers, err := g.resolveNextWithStates(ctx, step, results, state, routingStates, waiting)
	if err != nil {
		return nil, err
	}
	tasks := scheduleTasks(step+1, destinations)
	for index := range tasks {
		tasks[index].triggers = cloneNodeIDs(triggers[tasks[index].node])
	}
	for _, result := range results {
		source := result.outputNode()
		for _, send := range result.cmd.Sends {
			if send.Node == START || send.Node == END {
				return nil, &RouterError{Step: step, Source: source, Err: fmt.Errorf("%w: invalid Command Send target %q", ErrUnknownNode, send.Node)}
			}
			if _, exists := g.nodes[send.Node]; !exists {
				return nil, &RouterError{Step: step, Source: source, Err: fmt.Errorf("%w: Command Send target %q", ErrUnknownNode, send.Node)}
			}
			input, ok := send.State.(S)
			if !ok {
				return nil, &RouterError{Step: step, Source: source, Err: fmt.Errorf("Command Send target %q state has type %T", send.Node, send.State)}
			}
			tasks = append(tasks, scheduledTask{
				node: send.Node, input: input, hasInput: true,
				triggers: []NodeID{"__pregel_push"},
				taskID:   fmt.Sprintf("step:%d:task:%d:node:%s", step+1, len(tasks), send.Node),
			})
		}
		branch, exists := g.sendBranches[source]
		if !exists {
			continue
		}
		routingState := state
		if projected, exists := routingStates[result.outputTaskID()]; exists {
			routingState = projected
		}
		sends, err := branch.router(ctx, routingState)
		if err != nil {
			return nil, &RouterError{Step: step, Source: source, Err: err}
		}
		for _, send := range sends {
			if _, allowed := branch.allowed[send.Node]; !allowed {
				return nil, &RouterError{Step: step, Source: source, Err: fmt.Errorf("%w: Send target %q", ErrUnknownNode, send.Node)}
			}
			tasks = append(tasks, scheduledTask{
				node: send.Node, input: send.State, hasInput: true,
				triggers: []NodeID{"__pregel_push"},
				taskID:   fmt.Sprintf("step:%d:task:%d:node:%s", step+1, len(tasks), send.Node),
			})
		}
	}
	return tasks, nil
}

func (g *CompiledGraph[S, D]) commitCheckpoint(
	ctx context.Context,
	parent checkpoint.Config,
	state S,
	next []scheduledTask,
	step int,
	runID string,
	waiting map[string]map[NodeID]struct{},
	results []taskResult[D],
	invocationParentCheckpoint string,
) (checkpoint.Config, []scheduledTask, error) {
	seenNodes := make([]NodeID, 0, len(results))
	for _, result := range results {
		seenNodes = append(seenNodes, result.outputNode())
	}
	return g.putCheckpointWithTasks(
		ctx,
		parent,
		state,
		next,
		step,
		checkpoint.SourceLoop,
		runID,
		waiting,
		seenNodes,
		invocationMetadata(invocationParentCheckpoint),
	)
}

func invocationMetadata(parentCheckpoint string) checkpoint.Metadata {
	if parentCheckpoint == "" {
		return nil
	}
	return checkpoint.Metadata{"invocation_parent_checkpoint": parentCheckpoint}
}

func (g *CompiledGraph[S, D]) putCheckpoint(
	ctx context.Context,
	parent checkpoint.Config,
	state S,
	next []scheduledTask,
	step int,
	source checkpoint.Source,
	runID string,
	waiting map[string]map[NodeID]struct{},
	seenNodes []NodeID,
	extraMetadata ...checkpoint.Metadata,
) (checkpoint.Config, error) {
	stored, _, err := g.putCheckpointWithTasks(
		ctx, parent, state, next, step, source, runID, waiting, seenNodes, extraMetadata...,
	)
	return stored, err
}

func (g *CompiledGraph[S, D]) putCheckpointWithTasks(
	ctx context.Context,
	parent checkpoint.Config,
	state S,
	next []scheduledTask,
	step int,
	source checkpoint.Source,
	runID string,
	waiting map[string]map[NodeID]struct{},
	seenNodes []NodeID,
	extraMetadata ...checkpoint.Metadata,
) (checkpoint.Config, []scheduledTask, error) {
	id, timestamp, err := g.persistence.nextID()
	if err != nil {
		return checkpoint.Config{}, nil, persistenceError("generate-id", parent, err)
	}
	encodedState, err := g.persistence.stateCodec.Encode(state)
	if err != nil {
		return checkpoint.Config{}, nil, persistenceError("encode-state", parent, err)
	}
	values := map[string]checkpoint.EncodedValue{
		checkpoint.StateChannel: checkpoint.CloneEncodedValue(encodedState),
	}
	fixedChannels := make([]string, 0, len(g.checkpointChannelSchema))
	for channel := range g.checkpointChannelSchema {
		fixedChannels = append(fixedChannels, channel)
	}
	sort.Strings(fixedChannels)
	for _, channel := range fixedChannels {
		encoded, projectionErr := g.checkpointChannelSchema[channel].project(ctx, state)
		if projectionErr != nil {
			return checkpoint.Config{}, nil, persistenceError("project-channels", parent, projectionErr)
		}
		if encoded.Type == "" || encoded.Version <= 0 {
			return checkpoint.Config{}, nil, persistenceError(
				"project-channels", parent,
				fmt.Errorf("%w: channel %q has an invalid encoded value", ErrCheckpointChannel, channel),
			)
		}
		values[channel] = checkpoint.CloneEncodedValue(encoded)
	}
	if g.checkpointChannels != nil {
		projected, projectionErr := g.checkpointChannels(ctx, state)
		if projectionErr != nil {
			return checkpoint.Config{}, nil, persistenceError("project-channels", parent, projectionErr)
		}
		for channel, encoded := range projected {
			if isReservedCheckpointChannel(channel) {
				return checkpoint.Config{}, nil, persistenceError(
					"project-channels", parent,
					fmt.Errorf("%w: reserved channel %q", ErrCheckpointChannel, channel),
				)
			}
			if _, fixed := g.checkpointChannelSchema[channel]; fixed {
				return checkpoint.Config{}, nil, persistenceError(
					"project-channels", parent,
					fmt.Errorf("%w: duplicate fixed channel %q", ErrCheckpointChannel, channel),
				)
			}
			if encoded.Type == "" || encoded.Version <= 0 {
				return checkpoint.Config{}, nil, persistenceError(
					"project-channels", parent,
					fmt.Errorf("%w: channel %q has an invalid encoded value", ErrCheckpointChannel, channel),
				)
			}
			values[channel] = checkpoint.CloneEncodedValue(encoded)
		}
	}
	var previous checkpoint.Checkpoint
	if parent.CheckpointID != "" {
		tuple, found, getErr := saverGetTuple(ctx, g.persistence.saver, parent)
		if getErr != nil {
			return checkpoint.Config{}, nil, persistenceError("get-parent-channels", parent, getErr)
		}
		if !found {
			return checkpoint.Config{}, nil, persistenceError("get-parent-channels", parent, checkpoint.ErrNotFound)
		}
		previous = tuple.Checkpoint
	}
	channelNames := make(map[string]struct{}, len(values)+len(previous.ChannelVersions))
	for channel := range values {
		channelNames[channel] = struct{}{}
	}
	for channel := range previous.ChannelVersions {
		channelNames[channel] = struct{}{}
	}
	orderedChannels := make([]string, 0, len(channelNames))
	for channel := range channelNames {
		orderedChannels = append(orderedChannels, channel)
	}
	sort.Strings(orderedChannels)
	channelVersions := make(map[string]string, len(orderedChannels))
	newVersions := make(map[string]string)
	updatedChannels := make([]string, 0, len(orderedChannels))
	for _, channel := range orderedChannels {
		current, currentPresent := values[channel]
		prior, priorPresent := previous.Values[channel]
		priorVersion, tracked := previous.ChannelVersions[channel]
		unchanged := tracked && currentPresent == priorPresent
		if unchanged && currentPresent {
			unchanged = encodedValuesEqual(current, prior)
		}
		if unchanged {
			channelVersions[channel] = priorVersion
			continue
		}
		channelVersions[channel] = id
		newVersions[channel] = id
		updatedChannels = append(updatedChannels, channel)
	}
	versionsSeen := cloneVersionsSeen(previous.VersionsSeen)
	for _, node := range seenNodes {
		observed := make(map[string]string)
		for _, channel := range g.readChannelsForNode(node) {
			if version := previous.ChannelVersions[channel]; version != "" {
				observed[channel] = version
			}
		}
		versionsSeen[string(node)] = observed
	}
	next = g.freshChannelTasks(next, channelVersions, versionsSeen)
	checkpointNext, err := g.checkpointTasks(next)
	if err != nil {
		return checkpoint.Config{}, nil, persistenceError("encode-task-input", parent, err)
	}
	value := checkpoint.Checkpoint{
		Version:         checkpoint.CurrentVersion,
		ID:              id,
		Timestamp:       timestamp,
		Step:            step,
		Values:          values,
		ChannelVersions: channelVersions,
		VersionsSeen:    versionsSeen,
		UpdatedChannels: updatedChannels,
		Next:            checkpointNext,
		Waiting:         waitingCheckpoint(waiting, g.waitingEdges),
	}
	stored, err := g.persistence.saver.Put(
		ctx,
		parent,
		value,
		checkpointMetadata(source, step, runID, extraMetadata...),
		newVersions,
	)
	if err != nil {
		return checkpoint.Config{}, nil, persistenceError("put", parent, err)
	}
	return stored, next, nil
}

func (g *CompiledGraph[S, D]) freshChannelTasks(
	tasks []scheduledTask,
	versions map[string]string,
	seenByNode map[string]map[string]string,
) []scheduledTask {
	result := make([]scheduledTask, 0, len(tasks))
	for _, task := range tasks {
		triggers := g.nodeChannelTriggers[task.node]
		if len(triggers) == 0 {
			result = append(result, task)
			continue
		}
		seen, previouslyRan := seenByNode[string(task.node)]
		if !previouslyRan {
			result = append(result, task)
			continue
		}
		for _, channel := range triggers {
			if versions[channel] != seen[channel] {
				result = append(result, task)
				break
			}
		}
	}
	return result
}

func encodedValuesEqual(left, right checkpoint.EncodedValue) bool {
	return left.Type == right.Type && left.Version == right.Version && bytes.Equal(left.Data, right.Data)
}

func cloneVersionsSeen(source map[string]map[string]string) map[string]map[string]string {
	result := make(map[string]map[string]string, len(source))
	for node, versions := range source {
		copy := make(map[string]string, len(versions))
		for channel, version := range versions {
			copy[channel] = version
		}
		result[node] = copy
	}
	return result
}

func isReservedCheckpointChannel(channel string) bool {
	switch channel {
	case "", checkpoint.StateChannel, checkpoint.TaskResultChannel, checkpoint.NodeErrorChannel,
		checkpoint.InterruptChannel, checkpoint.SubgraphChannel, checkpoint.StaticInterruptChannel:
		return true
	default:
		return false
	}
}

func waitingFromCheckpoint(stored map[string][]string) map[string]map[NodeID]struct{} {
	result := make(map[string]map[NodeID]struct{}, len(stored))
	for id, sources := range stored {
		seen := make(map[NodeID]struct{}, len(sources))
		for _, source := range sources {
			seen[NodeID(source)] = struct{}{}
		}
		result[id] = seen
	}
	return result
}

func waitingCheckpoint(
	waiting map[string]map[NodeID]struct{},
	edges []waitingEdge,
) map[string][]string {
	result := make(map[string][]string)
	for _, edge := range edges {
		seen := waiting[edge.id]
		for _, source := range edge.sources {
			if _, exists := seen[source]; exists {
				result[edge.id] = append(result[edge.id], string(source))
			}
		}
	}
	return result
}

func (g *CompiledGraph[S, D]) defaultRoutes(
	ctx context.Context,
	step int,
	source NodeID,
	state S,
) ([]routedDestination, error) {
	routes := make([]routedDestination, 0, len(g.edges[source]))
	for _, destination := range g.edges[source] {
		routes = append(routes, routedDestination{node: destination, trigger: branchToTrigger(destination)})
	}
	if branches, exists := g.branches[source]; exists {
		for _, branch := range branches {
			conditional, err := routeConditional(ctx, step, source, state, branch)
			if err != nil {
				return nil, err
			}
			for _, destination := range conditional {
				routes = append(routes, routedDestination{
					node:    destination,
					trigger: NodeID(fmt.Sprintf("branch:%s:%s:%s", source, branch.name, destination)),
				})
			}
		}
	}
	return routes, nil
}

func (g *CompiledGraph[S, D]) scheduleDefaultTasks(
	ctx context.Context, routeStep, taskStep int, source NodeID, state S,
) ([]scheduledTask, error) {
	routes, err := g.defaultRoutes(ctx, routeStep, source, state)
	if err != nil {
		return nil, err
	}
	tasks := make([]scheduledTask, 0, len(routes))
	byNode := make(map[NodeID]int, len(routes))
	for _, route := range routes {
		if route.node == END {
			continue
		}
		if index, exists := byNode[route.node]; exists {
			tasks[index].triggers = appendUniqueNodeID(tasks[index].triggers, route.trigger)
			continue
		}
		byNode[route.node] = len(tasks)
		tasks = append(tasks, scheduledTask{
			node: route.node, triggers: []NodeID{route.trigger},
			taskID: fmt.Sprintf("step:%d:task:%d:node:%s", taskStep, len(tasks), route.node),
		})
	}
	return tasks, nil
}

// IsDeferred reports whether a node was registered with WithDeferred.
// Runtime still schedules deferred nodes with peers today; full deferred
// barrier semantics remain Partial (COMPATIBILITY.md).
func (g *CompiledGraph[S, D]) IsDeferred(id NodeID) bool {
	if g == nil {
		return false
	}
	_, ok := g.deferredNodes[id]
	return ok
}

func branchToTrigger(destination NodeID) NodeID {
	return NodeID("branch:to:" + string(destination))
}

func waitingTrigger(edge waitingEdge) NodeID {
	parts := make([]string, len(edge.sources))
	for index, source := range edge.sources {
		parts[index] = string(source)
	}
	return NodeID(fmt.Sprintf("join:%s:%s", strings.Join(parts, "+"), edge.target))
}

func appendUniqueNodeID(values []NodeID, value NodeID) []NodeID {
	for _, existing := range values {
		if existing == value {
			return values
		}
	}
	return append(values, value)
}

func (g *CompiledGraph[S, D]) validateExplicitDestinations(
	step int,
	source NodeID,
	destinations []NodeID,
) ([]NodeID, error) {
	for _, destination := range destinations {
		if destination == END {
			continue
		}
		if destination == START {
			return nil, &RouterError{
				Step:   step,
				Source: source,
				Err:    fmt.Errorf("%w: cannot route to START", ErrInvalidGraph),
			}
		}
		if _, exists := g.nodes[destination]; !exists {
			return nil, &RouterError{
				Step:   step,
				Source: source,
				Err:    fmt.Errorf("%w: %q", ErrUnknownNode, destination),
			}
		}
	}
	return cloneNodeIDs(destinations), nil
}

func scheduleTasks(step int, destinations []NodeID, triggers ...NodeID) []scheduledTask {
	tasks := make([]scheduledTask, len(destinations))
	for index, node := range destinations {
		tasks[index] = scheduledTask{
			node: node, triggers: cloneNodeIDs(triggers),
			taskID: fmt.Sprintf("step:%d:task:%d:node:%s", step, index, node),
		}
	}
	return tasks
}

func rescheduleTasks(step int, source []scheduledTask) []scheduledTask {
	result := make([]scheduledTask, len(source))
	for index, task := range source {
		result[index] = scheduledTask{
			node: task.node, triggers: cloneNodeIDs(task.triggers),
			input: task.input, hasInput: task.hasInput,
			taskID: fmt.Sprintf("step:%d:task:%d:node:%s", step, index, task.node),
		}
	}
	return result
}

func (g *CompiledGraph[S, D]) checkpointTasks(tasks []scheduledTask) ([]checkpoint.Task, error) {
	result := make([]checkpoint.Task, len(tasks))
	for index, task := range tasks {
		triggers := make([]string, len(task.triggers))
		for triggerIndex, trigger := range task.triggers {
			triggers[triggerIndex] = string(trigger)
		}
		result[index] = checkpoint.Task{
			ID: task.taskID, Name: string(task.node), Triggers: triggers,
			ReadChannels: g.readChannelsForNode(task.node),
		}
		if task.hasInput {
			input, ok := task.input.(S)
			if !ok {
				return nil, fmt.Errorf("task %q input has type %T", task.taskID, task.input)
			}
			encoded, err := g.persistence.stateCodec.Encode(input)
			if err != nil {
				return nil, err
			}
			result[index].Input = &encoded
		}
	}
	return result, nil
}

func (g *CompiledGraph[S, D]) readChannelsForNode(node NodeID) []string {
	return channelReads(true, g.nodeChannelReads[node])
}

func emitEvent[S, D any](
	emit eventEmitter[S, D],
	event StreamEvent[S, D],
) error {
	if emit == nil {
		return nil
	}
	return emit(event)
}

func uniqueDestinations(destinations []NodeID) []NodeID {
	result := make([]NodeID, 0, len(destinations))
	seen := make(map[NodeID]struct{}, len(destinations))
	for _, destination := range destinations {
		if _, duplicate := seen[destination]; duplicate {
			continue
		}
		seen[destination] = struct{}{}
		result = append(result, destination)
	}
	return result
}
