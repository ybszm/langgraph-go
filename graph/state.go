package graph

import (
	"context"
	"fmt"
	"time"

	"github.com/wahanbo/langgraph-go/checkpoint"
)

// StateTask describes one task attached to a state snapshot. Result is set
// when the task already has a durable pending write.
type StateTask[S, D any] struct {
	ID         string
	Node       NodeID
	Completed  bool
	Result     *Command[D]
	Interrupts []Interrupt
	Input      *S
	// State is populated for an interrupted child when GetState is called with
	// WithSubgraphs.
	State *NestedStateSnapshot
}

// NestedStateSnapshot is the type-erased representation needed when child
// graph state and delta types differ from their parent.
type NestedStateSnapshot struct {
	Values       any
	Next         []NodeID
	Config       checkpoint.Config
	Metadata     checkpoint.Metadata
	CreatedAt    time.Time
	ParentConfig *checkpoint.Config
	Tasks        []NestedStateTask
	Interrupts   []Interrupt
}

type NestedStateTask struct {
	ID         string
	Node       NodeID
	Completed  bool
	Result     any
	Interrupts []Interrupt
	Input      any
	State      *NestedStateSnapshot
}

type getStateConfig struct{ subgraphs bool }

// GetStateOption controls state snapshot expansion.
type GetStateOption func(*getStateConfig)

// WithSubgraphs expands interrupted child state recursively.
func WithSubgraphs() GetStateOption {
	return func(config *getStateConfig) { config.subgraphs = true }
}

// StateSnapshot is the typed Go equivalent of LangGraph's StateSnapshot.
// Values may include projected pending writes when GetState selects the
// latest checkpoint. Exact historical snapshots never project them.
type StateSnapshot[S, D any] struct {
	Values       S
	Next         []NodeID
	Config       checkpoint.Config
	Metadata     checkpoint.Metadata
	CreatedAt    time.Time
	ParentConfig *checkpoint.Config
	Tasks        []StateTask[S, D]
	Interrupts   []Interrupt
}

// StateHistoryOptions controls GetStateHistory filtering and pagination.
type StateHistoryOptions struct {
	Filter             checkpoint.Metadata
	BeforeCheckpointID string
	Limit              int
}

// StateUpdate applies Delta as if it were emitted by AsNode. AsNode may be
// omitted only for a graph containing exactly one node.
type StateUpdate[D any] struct {
	Delta  D
	AsNode NodeID
	TaskID string
}

// BulkUpdateState applies each inner slice as one artificial super-step and
// persists one checkpoint per super-step. Updates inside a super-step are
// reduced in caller order.
func (g *CompiledGraph[S, D]) BulkUpdateState(
	ctx context.Context,
	config RunConfig,
	supersteps [][]StateUpdate[D],
) (checkpoint.Config, error) {
	requested, err := g.persistenceConfig(ctx, config)
	if err != nil {
		return checkpoint.Config{}, err
	}
	if len(supersteps) == 0 {
		return checkpoint.Config{}, fmt.Errorf("%w: no supersteps provided", ErrInvalidStateUpdate)
	}
	for stepIndex, updates := range supersteps {
		if len(updates) == 0 {
			return checkpoint.Config{}, fmt.Errorf("%w: superstep %d has no updates", ErrInvalidStateUpdate, stepIndex)
		}
		for updateIndex, update := range updates {
			if update.AsNode == "" {
				return checkpoint.Config{}, fmt.Errorf("%w: superstep %d update %d has no AsNode", ErrInvalidStateUpdate, stepIndex, updateIndex)
			}
			if update.AsNode == START || update.AsNode == END {
				return checkpoint.Config{}, fmt.Errorf("%w: reserved AsNode %q", ErrInvalidStateUpdate, update.AsNode)
			}
			if _, exists := g.nodes[update.AsNode]; !exists {
				return checkpoint.Config{}, fmt.Errorf("%w: %w: %q", ErrInvalidStateUpdate, ErrUnknownNode, update.AsNode)
			}
		}
	}

	unlock := g.runLocks.lock(requested.ThreadID + "\x00" + requested.Namespace)
	defer unlock()
	tuple, found, err := saverGetTuple(ctx, g.persistence.saver, requested)
	if err != nil {
		return checkpoint.Config{}, err
	}
	if !found && requested.CheckpointID != "" {
		return checkpoint.Config{}, persistenceError("bulk-update", requested, checkpoint.ErrNotFound)
	}
	var state S
	parent := requested
	step := -1
	waiting := make(map[string]map[NodeID]struct{})
	if found {
		snapshot, snapshotErr := g.stateSnapshot(ctx, tuple, false)
		if snapshotErr != nil {
			return checkpoint.Config{}, snapshotErr
		}
		state = snapshot.Values
		parent = tuple.Config
		step = tuple.Checkpoint.Step
		waiting = waitingFromCheckpoint(tuple.Checkpoint.Waiting)
	}

	for superstepIndex, updates := range supersteps {
		deltas := make([]D, len(updates))
		results := make([]taskResult[D], len(updates))
		for updateIndex, update := range updates {
			deltas[updateIndex] = update.Delta
			taskID := update.TaskID
			if taskID == "" {
				taskID = fmt.Sprintf("bulk:%d:update:%d:node:%s", step+1, updateIndex, update.AsNode)
			}
			results[updateIndex] = taskResult[D]{
				node: update.AsNode, taskID: taskID,
				cmd: Command[D]{Update: update.Delta, HasUpdate: true},
			}
		}
		state, err = g.reducer(ctx, state, deltas)
		if err != nil {
			return checkpoint.Config{}, fmt.Errorf("%w: reduce bulk superstep %d: %w", ErrInvalidStateUpdate, superstepIndex, err)
		}
		step++
		next, routeErr := g.resolveNextTasks(
			ctx, step, results, state, waiting, resolvedRecursionLimit(config), config, parent,
		)
		if routeErr != nil {
			return checkpoint.Config{}, fmt.Errorf("%w: route bulk superstep %d: %w", ErrInvalidStateUpdate, superstepIndex, routeErr)
		}
		seenNodes := make([]NodeID, 0, len(results))
		for _, result := range results {
			seenNodes = append(seenNodes, result.outputNode())
		}
		parent, err = g.putCheckpoint(
			ctx, parent, state, next, step, checkpoint.SourceUpdate, config.RunID, waiting,
			seenNodes,
			checkpoint.Metadata{"bulk": true, "bulk_index": superstepIndex},
		)
		if err != nil {
			return checkpoint.Config{}, err
		}
	}
	return parent, nil
}

// GetState returns the latest or exact durable state. A thread without a
// checkpoint returns an empty snapshot containing the requested config.
func (g *CompiledGraph[S, D]) GetState(
	ctx context.Context,
	config RunConfig,
	options ...GetStateOption,
) (StateSnapshot[S, D], error) {
	resolved := getStateConfig{}
	for _, option := range options {
		if option != nil {
			option(&resolved)
		}
	}
	requested, err := g.persistenceConfig(ctx, config)
	if err != nil {
		return StateSnapshot[S, D]{}, err
	}
	tuple, found, err := saverGetTuple(ctx, g.persistence.saver, requested)
	if err != nil {
		return StateSnapshot[S, D]{}, err
	}
	if !found {
		return StateSnapshot[S, D]{Config: requested}, nil
	}
	return g.stateSnapshotExpanded(ctx, tuple, config.CheckpointID == "", resolved.subgraphs)
}

// GetStateHistory returns durable snapshots in descending checkpoint-ID
// order. Pending writes are reported on tasks but are not projected into
// historical Values.
func (g *CompiledGraph[S, D]) GetStateHistory(
	ctx context.Context,
	config RunConfig,
	options StateHistoryOptions,
) ([]StateSnapshot[S, D], error) {
	requested, err := g.persistenceConfig(ctx, config)
	if err != nil {
		return nil, err
	}
	listOptions := checkpoint.ListOptions{
		Config: &requested,
		Filter: options.Filter,
		Limit:  options.Limit,
	}
	if options.BeforeCheckpointID != "" {
		listOptions.Before = &checkpoint.Config{
			ThreadID:     requested.ThreadID,
			Namespace:    requested.Namespace,
			CheckpointID: options.BeforeCheckpointID,
		}
	}
	tuples, err := g.persistence.saver.List(ctx, listOptions)
	if err != nil {
		return nil, persistenceError("list", requested, err)
	}
	result := make([]StateSnapshot[S, D], 0, len(tuples))
	for _, tuple := range tuples {
		snapshot, err := g.stateSnapshot(ctx, tuple, false)
		if err != nil {
			return nil, err
		}
		result = append(result, snapshot)
	}
	return result, nil
}

// UpdateState creates a new immutable checkpoint after reducing one typed
// delta as if it came from AsNode. The new checkpoint becomes the thread head;
// updating an exact historical checkpoint therefore creates a new branch.
func (g *CompiledGraph[S, D]) UpdateState(
	ctx context.Context,
	config RunConfig,
	update StateUpdate[D],
) (checkpoint.Config, error) {
	requested, err := g.persistenceConfig(ctx, config)
	if err != nil {
		return checkpoint.Config{}, err
	}
	unlock := g.runLocks.lock(requested.ThreadID + "\x00" + requested.Namespace)
	defer unlock()

	tuple, found, err := saverGetTuple(ctx, g.persistence.saver, requested)
	if err != nil {
		return checkpoint.Config{}, err
	}
	if !found {
		return checkpoint.Config{}, persistenceError("update", requested, checkpoint.ErrNotFound)
	}
	asNode := update.AsNode
	if asNode == "" && len(g.nodes) == 1 {
		for node := range g.nodes {
			asNode = node
		}
	}
	if asNode == "" {
		return checkpoint.Config{}, fmt.Errorf(
			"%w: AsNode is required when the graph has multiple nodes",
			ErrInvalidStateUpdate,
		)
	}
	if _, exists := g.nodes[asNode]; !exists {
		return checkpoint.Config{}, fmt.Errorf(
			"%w: %w: %q",
			ErrInvalidStateUpdate,
			ErrUnknownNode,
			asNode,
		)
	}

	// Manual updates are based on the committed checkpoint value. Pending task
	// writes remain observable in task history but are not silently folded into
	// the update, matching exact-checkpoint update semantics.
	snapshot, err := g.stateSnapshot(ctx, tuple, false)
	if err != nil {
		return checkpoint.Config{}, err
	}
	state, err := g.reducer(ctx, snapshot.Values, []D{update.Delta})
	if err != nil {
		return checkpoint.Config{}, fmt.Errorf("%w: reduce: %w", ErrInvalidStateUpdate, err)
	}
	newStep := tuple.Checkpoint.Step + 1
	waiting := waitingFromCheckpoint(tuple.Checkpoint.Waiting)
	next, err := g.resolveNextTasks(
		ctx,
		newStep,
		[]taskResult[D]{{node: asNode, taskID: "manual-update"}},
		state,
		waiting,
		resolvedRecursionLimit(config),
		config,
		tuple.Config,
	)
	if err != nil {
		return checkpoint.Config{}, fmt.Errorf("%w: route: %w", ErrInvalidStateUpdate, err)
	}
	return g.putCheckpoint(
		ctx,
		tuple.Config,
		state,
		next,
		newStep,
		checkpoint.SourceUpdate,
		config.RunID,
		waiting,
		[]NodeID{asNode},
		checkpoint.Metadata{"as_node": string(asNode)},
	)
}

func (g *CompiledGraph[S, D]) persistenceConfig(
	ctx context.Context,
	config RunConfig,
) (checkpoint.Config, error) {
	if ctx == nil {
		return checkpoint.Config{}, fmt.Errorf("%w: context is nil", ErrInvalidRunConfig)
	}
	if err := ctx.Err(); err != nil {
		return checkpoint.Config{}, err
	}
	if g.persistence == nil {
		return checkpoint.Config{}, ErrCheckpointerRequired
	}
	requested := checkpoint.Config{
		ThreadID:     config.ThreadID,
		Namespace:    config.CheckpointNamespace,
		CheckpointID: config.CheckpointID,
	}
	if err := requested.Validate(); err != nil {
		return checkpoint.Config{}, fmt.Errorf("%w: %w", ErrInvalidRunConfig, err)
	}
	return requested, nil
}

func (g *CompiledGraph[S, D]) stateSnapshot(
	ctx context.Context,
	tuple checkpoint.Tuple,
	applyPending bool,
) (StateSnapshot[S, D], error) {
	return g.stateSnapshotExpanded(ctx, tuple, applyPending, false)
}

func (g *CompiledGraph[S, D]) stateSnapshotExpanded(
	ctx context.Context,
	tuple checkpoint.Tuple,
	applyPending bool,
	includeSubgraphs bool,
) (StateSnapshot[S, D], error) {
	encoded, exists := tuple.Checkpoint.Values[checkpoint.StateChannel]
	if !exists {
		return StateSnapshot[S, D]{}, persistenceError(
			"decode-state",
			tuple.Config,
			fmt.Errorf("%w: state channel is missing", checkpoint.ErrInvalidCheckpoint),
		)
	}
	state, err := g.persistence.stateCodec.Decode(encoded)
	if err != nil {
		return StateSnapshot[S, D]{}, persistenceError("decode-state", tuple.Config, err)
	}

	completed := make(map[string]taskResult[D])
	controls := make(map[string]persistedTaskControl)
	subgraphControls := make(map[string]persistedSubgraphControl)
	scheduledTaskIDs := make(map[string]struct{}, len(tuple.Checkpoint.Next))
	for _, task := range tuple.Checkpoint.Next {
		scheduledTaskIDs[task.ID] = struct{}{}
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
				return StateSnapshot[S, D]{}, persistenceError(
					"decode-pending-write",
					tuple.Config,
					err,
				)
			}
			completed[write.TaskID] = result
		case checkpoint.InterruptChannel:
			if _, scheduled := scheduledTaskIDs[write.TaskID]; !scheduled {
				continue
			}
			control, err := g.persistence.decodeTaskControl(write.Value)
			if err != nil {
				return StateSnapshot[S, D]{}, persistenceError(
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
				return StateSnapshot[S, D]{}, persistenceError("decode-subgraph", tuple.Config, err)
			}
			subgraphControls[write.TaskID] = control
		}
	}

	next := make([]NodeID, 0, len(tuple.Checkpoint.Next))
	tasks := make([]StateTask[S, D], 0, len(tuple.Checkpoint.Next))
	interrupts := make([]Interrupt, 0)
	deltas := make([]D, 0, len(completed))
	for _, task := range tuple.Checkpoint.Next {
		result, done := completed[task.ID]
		stateTask := StateTask[S, D]{ID: task.ID, Node: NodeID(task.Name), Completed: done}
		if task.Input != nil {
			input, decodeErr := g.persistence.stateCodec.Decode(*task.Input)
			if decodeErr != nil {
				return StateSnapshot[S, D]{}, persistenceError("decode-task-input", tuple.Config, decodeErr)
			}
			stateTask.Input = &input
		}
		if control, exists := controls[task.ID]; exists && !done {
			for index, item := range control.Interrupts {
				if index < len(control.Resumes) && control.Resumes[index] != nil {
					continue
				}
				interrupt := Interrupt{
					ID:    item.ID,
					Value: append([]byte(nil), item.Value...),
				}
				stateTask.Interrupts = append(stateTask.Interrupts, interrupt)
				interrupts = append(interrupts, interrupt)
			}
		}
		if control, exists := subgraphControls[task.ID]; exists {
			if !done {
				for _, item := range control.Interrupts {
					interrupt := Interrupt{ID: item.ID, Value: append([]byte(nil), item.Value...), Namespace: control.Namespace}
					stateTask.Interrupts = append(stateTask.Interrupts, interrupt)
					interrupts = append(interrupts, interrupt)
				}
			}
			if includeSubgraphs {
				if spec, ok := g.subgraphs[NodeID(task.Name)]; ok {
					nested, nestedErr := spec.snapshot(
						ctx, g.persistence, tuple.Config.ThreadID, control.Namespace, control.CheckpointID,
					)
					if nestedErr != nil {
						return StateSnapshot[S, D]{}, persistenceError("get-subgraph-state", tuple.Config, nestedErr)
					}
					stateTask.State = nested
				}
			}
		}
		if done {
			command := result.cmd
			command.Goto = cloneNodeIDs(command.Goto)
			command.Sends = cloneTaskSends(command.Sends)
			stateTask.Result = &command
			if applyPending && command.HasUpdate {
				deltas = append(deltas, command.Update)
			}
		}
		if !applyPending || !done {
			next = append(next, NodeID(task.Name))
		}
		tasks = append(tasks, stateTask)
	}
	if applyPending && len(deltas) > 0 {
		state, err = g.reducer(ctx, state, deltas)
		if err != nil {
			return StateSnapshot[S, D]{}, persistenceError("apply-pending-writes", tuple.Config, err)
		}
	}

	return StateSnapshot[S, D]{
		Values:       state,
		Next:         next,
		Config:       tuple.Config,
		Metadata:     tuple.Metadata,
		CreatedAt:    tuple.Checkpoint.Timestamp,
		ParentConfig: tuple.ParentConfig,
		Tasks:        tasks,
		Interrupts:   interrupts,
	}, nil
}

func nestedSnapshot[S, D any](snapshot StateSnapshot[S, D]) *NestedStateSnapshot {
	tasks := make([]NestedStateTask, len(snapshot.Tasks))
	for index, task := range snapshot.Tasks {
		tasks[index] = NestedStateTask{
			ID: task.ID, Node: task.Node, Completed: task.Completed,
			Result: task.Result, Interrupts: cloneInterrupts(task.Interrupts),
			Input: task.Input, State: task.State,
		}
	}
	return &NestedStateSnapshot{
		Values: snapshot.Values, Next: cloneNodeIDs(snapshot.Next), Config: snapshot.Config,
		Metadata: snapshot.Metadata, CreatedAt: snapshot.CreatedAt,
		ParentConfig: snapshot.ParentConfig, Tasks: tasks,
		Interrupts: cloneInterrupts(snapshot.Interrupts),
	}
}
