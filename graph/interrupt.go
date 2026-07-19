package graph

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"sync"

	"github.com/ybszm/langgraph-go/checkpoint"
)

func durableInterruptID(namespace, taskID string, index int) string {
	// The checkpoint namespace is part of interrupt identity. Child graph task
	// IDs restart at step zero, so taskID alone collides when a subgraph is
	// fanned out with Send. A truncated SHA-256 gives the same 128-bit,
	// opaque/stable shape as LangGraph's namespace-derived IDs without exposing
	// checkpoint coordinates in the public value.
	digest := sha256.Sum256([]byte(fmt.Sprintf("%s\x00%s\x00%d", namespace, taskID, index)))
	return fmt.Sprintf("%x", digest[:16])
}

// Interrupt is a durable request for external input. Value is retained as
// JSON so callers can decode it into the appropriate prompt type.
type Interrupt struct {
	ID    string
	Value json.RawMessage
	// Namespace is empty for root interrupts and identifies the owning child
	// checkpoint namespace for nested interrupts.
	Namespace string
}

// DecodeInterrupt decodes an interrupt prompt into T.
func DecodeInterrupt[T any](interrupt Interrupt) (T, error) {
	var value T
	if err := json.Unmarshal(interrupt.Value, &value); err != nil {
		return value, fmt.Errorf("decode interrupt %q: %w", interrupt.ID, err)
	}
	return value, nil
}

// PendingInterruptCount reports how many task-local interrupts are carried by
// an error. It lets composite nodes retain the most complete control-flow
// signal after concurrent children have all reached an interrupt boundary.
func PendingInterruptCount(err error) int {
	if err == nil {
		return 0
	}
	var signal *interruptSignal
	if errors.As(err, &signal) {
		return len(signal.control.Interrupts)
	}
	var nested *subgraphInterruptSignal
	if errors.As(err, &nested) {
		return len(nested.interrupts)
	}
	var public *GraphInterruptError
	if errors.As(err, &public) {
		return len(public.Interrupts)
	}
	return 0
}

type interruptHandler func(value any, decode func(json.RawMessage) (any, error)) (any, error)

type interruptCoordinate struct {
	scope string
	index int
}

type scopedInterruptValue struct {
	coordinate interruptCoordinate
	value      any
}

func unwrapInterruptValue(value any) (interruptCoordinate, any, bool) {
	scoped, ok := value.(scopedInterruptValue)
	if !ok {
		return interruptCoordinate{}, value, false
	}
	return scoped.coordinate, scoped.value, true
}

// InterruptOrder coordinates a composite node's child runtimes so positional
// interrupt slots are assigned in stable child order, while work before the
// interrupt boundary remains concurrent.
type InterruptOrder struct {
	mu   sync.Mutex
	cond *sync.Cond
	done []bool
	next int
}

// NewInterruptOrder creates an order for a fixed number of child calls.
func NewInterruptOrder(children int) *InterruptOrder {
	if children < 0 {
		children = 0
	}
	order := &InterruptOrder{done: make([]bool, children)}
	order.cond = sync.NewCond(&order.mu)
	return order
}

// Runtime returns a copy whose AwaitResume boundary waits for every earlier
// unfinished child. A runtime without persistence is returned unchanged.
func (o *InterruptOrder) Runtime(runtime Runtime, child int) Runtime {
	if o == nil || runtime.interrupt == nil || child < 0 || child >= len(o.done) {
		return runtime
	}
	base := runtime.interrupt
	var sequenceMu sync.Mutex
	sequence := 0
	runtime.interrupt = func(value any, decode func(json.RawMessage) (any, error)) (any, error) {
		sequenceMu.Lock()
		localIndex := sequence
		sequence++
		sequenceMu.Unlock()
		o.mu.Lock()
		for child != o.next && !o.done[child] {
			o.cond.Wait()
		}
		o.mu.Unlock()
		return base(scopedInterruptValue{
			coordinate: interruptCoordinate{scope: fmt.Sprintf("child:%d", child), index: localIndex},
			value:      value,
		}, decode)
	}
	return runtime
}

// Done releases the next child interrupt boundary. It is safe to call once
// from a defer even when the child never requested an interrupt.
func (o *InterruptOrder) Done(child int) {
	if o == nil || child < 0 || child >= len(o.done) {
		return
	}
	o.mu.Lock()
	o.done[child] = true
	for o.next < len(o.done) && o.done[o.next] {
		o.next++
	}
	o.cond.Broadcast()
	o.mu.Unlock()
}

// AwaitResume pauses a persistent graph on first execution and returns the
// typed resume value when the same task is run again after Resume. The name
// differs from Python's interrupt() because Go uses Interrupt as the payload
// type and does not allow a type and function to share a package identifier.
func AwaitResume[T any](runtime Runtime, value any) (T, error) {
	var zero T
	if runtime.interrupt == nil {
		return zero, ErrCheckpointerRequired
	}
	decoded, err := runtime.interrupt(value, func(raw json.RawMessage) (any, error) {
		var result T
		if err := json.Unmarshal(raw, &result); err != nil {
			return nil, err
		}
		return result, nil
	})
	if err != nil {
		return zero, err
	}
	result, ok := decoded.(T)
	if !ok {
		return zero, fmt.Errorf("resume value for task %q has unexpected type %T", runtime.TaskID, decoded)
	}
	return result, nil
}

// ResumeCommand contains either one positional resume value or values keyed
// by interrupt ID. Construct it with Resume or ResumeByID.
type ResumeCommand struct {
	single       *json.RawMessage
	byID         map[string]json.RawMessage
	continueOnly bool
}

// MarshalJSON encodes the transport-safe resume envelope used by remote graph
// protocols. Exactly one of value, by_id, or continue is emitted.
func (c ResumeCommand) MarshalJSON() ([]byte, error) {
	switch {
	case c.single != nil && c.byID == nil && !c.continueOnly:
		return json.Marshal(struct {
			Value *json.RawMessage `json:"value"`
		}{Value: c.single})
	case c.single == nil && c.byID != nil && !c.continueOnly:
		return json.Marshal(struct {
			ByID map[string]json.RawMessage `json:"by_id"`
		}{ByID: c.byID})
	case c.single == nil && c.byID == nil && c.continueOnly:
		return []byte(`{"continue":true}`), nil
	default:
		return nil, fmt.Errorf("%w: resume envelope must select exactly one mode", ErrInvalidResume)
	}
}

// UnmarshalJSON decodes and validates a transport-safe resume envelope.
func (c *ResumeCommand) UnmarshalJSON(data []byte) error {
	if c == nil {
		return fmt.Errorf("%w: nil resume destination", ErrInvalidResume)
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(data, &fields); err != nil {
		return fmt.Errorf("%w: decode resume envelope: %w", ErrInvalidResume, err)
	}
	for field := range fields {
		if field != "value" && field != "by_id" && field != "continue" {
			return fmt.Errorf("%w: unknown resume field %q", ErrInvalidResume, field)
		}
	}
	modes := 0
	var result ResumeCommand
	if raw, ok := fields["value"]; ok {
		modes++
		value := append(json.RawMessage(nil), raw...)
		result.single = &value
	}
	if raw, ok := fields["by_id"]; ok {
		modes++
		var values map[string]json.RawMessage
		if err := json.Unmarshal(raw, &values); err != nil {
			return fmt.Errorf("%w: decode by_id: %w", ErrInvalidResume, err)
		}
		if len(values) == 0 {
			return fmt.Errorf("%w: resume map is empty", ErrInvalidResume)
		}
		result.byID = make(map[string]json.RawMessage, len(values))
		for id, value := range values {
			if id == "" {
				return fmt.Errorf("%w: interrupt ID is empty", ErrInvalidResume)
			}
			result.byID[id] = append(json.RawMessage(nil), value...)
		}
	}
	if raw, ok := fields["continue"]; ok {
		modes++
		if err := json.Unmarshal(raw, &result.continueOnly); err != nil || !result.continueOnly {
			return fmt.Errorf("%w: continue must be true", ErrInvalidResume)
		}
	}
	if modes != 1 {
		return fmt.Errorf("%w: resume envelope must select exactly one mode", ErrInvalidResume)
	}
	*c = result
	return nil
}

// Continue resumes a compile-time static interrupt without a business value.
func Continue() ResumeCommand { return ResumeCommand{continueOnly: true} }

// Resume creates a command for exactly one pending interrupt.
func Resume(value any) (ResumeCommand, error) {
	raw, err := json.Marshal(value)
	if err != nil {
		return ResumeCommand{}, fmt.Errorf("%w: encode value: %w", ErrInvalidResume, err)
	}
	message := json.RawMessage(raw)
	return ResumeCommand{single: &message}, nil
}

// ResumeByID creates a command for one or more interrupt IDs.
func ResumeByID(values map[string]any) (ResumeCommand, error) {
	if len(values) == 0 {
		return ResumeCommand{}, fmt.Errorf("%w: resume map is empty", ErrInvalidResume)
	}
	encoded := make(map[string]json.RawMessage, len(values))
	for id, value := range values {
		if id == "" {
			return ResumeCommand{}, fmt.Errorf("%w: interrupt ID is empty", ErrInvalidResume)
		}
		raw, err := json.Marshal(value)
		if err != nil {
			return ResumeCommand{}, fmt.Errorf("%w: encode %q: %w", ErrInvalidResume, id, err)
		}
		encoded[id] = raw
	}
	return ResumeCommand{byID: encoded}, nil
}

// ResumeAsCommand wraps a durable resume payload in the same Command envelope
// used for update and routing operations. It is accepted by InvokeCommand and
// is not a valid node return value.
func ResumeAsCommand[D any](resume ResumeCommand) Command[D] {
	copy := cloneResumeCommand(resume)
	return Command[D]{Resume: &copy}
}

// WithResume attaches an invocation-only durable resume payload to an update,
// goto, or Send command. The returned command owns a detached resume value and
// can be passed to InvokeCommand.
func WithResume[D any](command Command[D], resume ResumeCommand) Command[D] {
	copy := cloneResumeCommand(resume)
	command.Resume = &copy
	command.Goto = cloneNodeIDs(command.Goto)
	command.Sends = cloneTaskSends(command.Sends)
	return command
}

func cloneResumeCommand(source ResumeCommand) ResumeCommand {
	result := ResumeCommand{continueOnly: source.continueOnly}
	if source.single != nil {
		value := append(json.RawMessage(nil), (*source.single)...)
		result.single = &value
	}
	if source.byID != nil {
		result.byID = make(map[string]json.RawMessage, len(source.byID))
		for id, value := range source.byID {
			result.byID[id] = append(json.RawMessage(nil), value...)
		}
	}
	return result
}

type persistedInterrupt struct {
	ID    string          `json:"id"`
	Value json.RawMessage `json:"value"`
	Scope string          `json:"scope,omitempty"`
	Index int             `json:"index,omitempty"`
}

type persistedTaskControl struct {
	Interrupts []persistedInterrupt `json:"interrupts"`
	Resumes    []json.RawMessage    `json:"resumes"`
}

type interruptSignal struct {
	control   persistedTaskControl
	interrupt persistedInterrupt
}

func (e *interruptSignal) Error() string {
	return "node requested an interrupt"
}

func (e *interruptSignal) Unwrap() error { return ErrGraphInterrupt }

func (p *persistenceRuntime[S, D]) encodeTaskControl(
	control persistedTaskControl,
) (checkpoint.EncodedValue, error) {
	data, err := json.Marshal(control)
	if err != nil {
		return checkpoint.EncodedValue{}, err
	}
	return checkpoint.EncodedValue{Type: "langgraph.go/task-control", Version: 1, Data: data}, nil
}

func (p *persistenceRuntime[S, D]) decodeTaskControl(
	value checkpoint.EncodedValue,
) (persistedTaskControl, error) {
	if value.Type != "langgraph.go/task-control" || value.Version != 1 {
		return persistedTaskControl{}, fmt.Errorf("%w: task control has %s v%d", checkpoint.ErrCodecMismatch, value.Type, value.Version)
	}
	var control persistedTaskControl
	if err := json.Unmarshal(value.Data, &control); err != nil {
		return persistedTaskControl{}, err
	}
	return control, nil
}

// Resume stores resume values and continues the latest interrupted thread.
func (g *CompiledGraph[S, D]) Resume(
	ctx context.Context,
	config RunConfig,
	command ResumeCommand,
) (S, error) {
	return g.resumeInternal(ctx, config, command, nil, nil)
}

// ResumeFork resumes an exact interrupted checkpoint on a new branch even
// when that checkpoint is currently the namespace head. It is useful for
// direct child-coordinate continuation because the parent may still reference
// the original child checkpoint and its unresolved controls.
func (g *CompiledGraph[S, D]) ResumeFork(
	ctx context.Context,
	config RunConfig,
	command ResumeCommand,
) (S, error) {
	config.forceResumeFork = true
	return g.resumeInternal(ctx, config, command, nil, nil)
}

// InvokeCommand accepts the unified Command envelope for a durable resume.
// Optional update, goto, and Send fields are validated before a transformed
// interrupt fork is published, then applied before interrupted tasks rerun.
func (g *CompiledGraph[S, D]) InvokeCommand(
	ctx context.Context,
	command Command[D],
	config RunConfig,
) (S, error) {
	var zero S
	if command.Resume == nil {
		return zero, fmt.Errorf("%w: command has no resume payload", ErrInvalidResume)
	}
	if command.Target != CommandCurrent {
		return zero, fmt.Errorf("%w: invocation command cannot target a parent graph", ErrInvalidResume)
	}
	resume := cloneResumeCommand(*command.Resume)
	return g.resumeInternal(ctx, config, resume, nil, &command)
}

func (g *CompiledGraph[S, D]) resumeInternal(
	ctx context.Context,
	config RunConfig,
	command ResumeCommand,
	emit eventEmitter[S, D],
	invocation *Command[D],
) (S, error) {
	var zero S
	requested, err := g.persistenceConfig(ctx, config)
	if err != nil {
		return zero, err
	}
	unlock := g.runLocks.lock(requested.ThreadID + "\x00" + requested.Namespace)
	defer unlock()
	tuple, found, err := saverGetTuple(ctx, g.persistence.saver, requested)
	if err != nil {
		return zero, err
	}
	if !found {
		return zero, persistenceError("resume", requested, checkpoint.ErrNotFound)
	}
	if config.forceResumeFork {
		if requested.CheckpointID == "" {
			return zero, fmt.Errorf("%w: ResumeFork requires an exact checkpoint ID", ErrInvalidResume)
		}
		forked, forkErr := g.forkHistoricalInterrupt(ctx, tuple, config.RunID)
		if forkErr != nil {
			return zero, forkErr
		}
		tuple = forked
		config.CheckpointID = ""
	} else if requested.CheckpointID != "" {
		latestRequest := requested
		latestRequest.CheckpointID = ""
		latest, latestFound, latestErr := saverGetTuple(ctx, g.persistence.saver, latestRequest)
		if latestErr != nil {
			return zero, latestErr
		}
		if latestFound && latest.Config.CheckpointID != tuple.Config.CheckpointID {
			if invocation == nil || (!invocation.HasUpdate && invocation.Goto == nil && len(invocation.Sends) == 0) {
				forked, forkErr := g.forkHistoricalInterrupt(ctx, tuple, config.RunID)
				if forkErr != nil {
					return zero, forkErr
				}
				tuple = forked
				config.CheckpointID = ""
			}
		}
	}
	if invocation != nil && (invocation.HasUpdate || invocation.Goto != nil || len(invocation.Sends) > 0) {
		if err := g.validateResumeBeforeFork(ctx, tuple, command); err != nil {
			return zero, err
		}
		forked, forkErr := g.forkInterruptCommand(ctx, tuple, config.RunID, *invocation)
		if forkErr != nil {
			return zero, forkErr
		}
		tuple = forked
		config.CheckpointID = ""
	}

	type controlEntry struct {
		taskID  string
		control persistedTaskControl
		config  checkpoint.Config
	}
	entries := make([]controlEntry, 0)
	var staticGate *staticInterruptGate
	interruptIndex := make(map[string]struct {
		entry int
		index int
	})
	addControls := func(current checkpoint.Tuple) error {
		scheduled := checkpointScheduledTaskIDs(current.Checkpoint)
		for _, write := range current.PendingWrites {
			if write.Channel != checkpoint.InterruptChannel {
				continue
			}
			if _, exists := scheduled[write.TaskID]; !exists {
				// Functional API tasks share the checkpoint's reserved channels,
				// but their task IDs are not graph-scheduled tasks. Their envelopes
				// are consumed by the functional node adapter, not by graph resume.
				continue
			}
			control, decodeErr := g.persistence.decodeTaskControl(write.Value)
			if decodeErr != nil {
				return persistenceError("decode-interrupt", current.Config, decodeErr)
			}
			entryIndex := len(entries)
			entries = append(entries, controlEntry{taskID: write.TaskID, control: control, config: current.Config})
			for index, interrupt := range control.Interrupts {
				if index >= len(control.Resumes) || control.Resumes[index] == nil {
					interruptIndex[interrupt.ID] = struct{ entry, index int }{entryIndex, index}
				}
			}
		}
		return nil
	}
	visited := make(map[string]struct{})
	var walkControls func(checkpoint.Tuple) error
	walkControls = func(current checkpoint.Tuple) error {
		coordinate := current.Config.ThreadID + "\x00" + current.Config.Namespace + "\x00" + current.Config.CheckpointID
		if _, exists := visited[coordinate]; exists {
			return nil
		}
		visited[coordinate] = struct{}{}
		if err := addControls(current); err != nil {
			return err
		}
		for _, write := range current.PendingWrites {
			if write.Channel != checkpoint.SubgraphChannel {
				continue
			}
			control, decodeErr := decodeSubgraphControl(write.Value)
			if decodeErr != nil {
				return persistenceError("decode-subgraph", current.Config, decodeErr)
			}
			childConfig := checkpoint.Config{
				ThreadID: current.Config.ThreadID, Namespace: control.Namespace, CheckpointID: control.CheckpointID,
			}
			childTuple, childFound, getErr := saverGetTuple(ctx, g.persistence.saver, childConfig)
			if getErr != nil {
				return getErr
			}
			if !childFound {
				return persistenceError("resume-subgraph", childConfig, checkpoint.ErrNotFound)
			}
			if err := walkControls(childTuple); err != nil {
				return err
			}
		}
		return nil
	}
	if err := walkControls(tuple); err != nil {
		return zero, err
	}
	for _, write := range tuple.PendingWrites {
		if write.Channel != checkpoint.StaticInterruptChannel {
			continue
		}
		gate, decodeErr := decodeStaticInterrupt(write.Value)
		if decodeErr != nil {
			return zero, persistenceError("decode-static-interrupt", tuple.Config, decodeErr)
		}
		staticGate = &gate
	}
	if staticGate != nil && len(interruptIndex) == 0 {
		staticGate.Consumed = true
		encoded, encodeErr := encodeStaticInterrupt(*staticGate)
		if encodeErr != nil {
			return zero, persistenceError("encode-static-continue", tuple.Config, encodeErr)
		}
		if err := g.persistence.saver.PutWrites(ctx, tuple.Config, []checkpoint.PendingWrite{{
			TaskID: "__static_interrupt__", Index: -3, Channel: checkpoint.StaticInterruptChannel, Value: encoded,
		}}); err != nil {
			return zero, persistenceError("put-static-continue", tuple.Config, err)
		}
		config.resuming = true
		return g.runInternal(ctx, zero, config, emit, false)
	}
	if len(interruptIndex) == 0 {
		return zero, fmt.Errorf("%w: thread has no pending interrupts", ErrInvalidResume)
	}
	if command.continueOnly {
		return zero, fmt.Errorf("%w: Continue cannot satisfy a dynamic interrupt", ErrInvalidResume)
	}
	if command.single != nil {
		if len(interruptIndex) != 1 {
			return zero, fmt.Errorf("%w: %d interrupts are pending; resume by ID", ErrInvalidResume, len(interruptIndex))
		}
		for _, location := range interruptIndex {
			setResume(&entries[location.entry].control, location.index, *command.single)
		}
	} else {
		if len(command.byID) == 0 {
			return zero, fmt.Errorf("%w: resume command is empty", ErrInvalidResume)
		}
		for id, value := range command.byID {
			location, exists := interruptIndex[id]
			if !exists {
				return zero, fmt.Errorf("%w: interrupt %q is not pending", ErrInvalidResume, id)
			}
			setResume(&entries[location.entry].control, location.index, value)
		}
	}
	for _, entry := range entries {
		encoded, err := g.persistence.encodeTaskControl(entry.control)
		if err != nil {
			return zero, persistenceError("encode-resume", tuple.Config, err)
		}
		if err := g.persistence.saver.PutWrites(ctx, entry.config, []checkpoint.PendingWrite{{
			TaskID: entry.taskID, Index: -1, Channel: checkpoint.InterruptChannel, Value: encoded,
		}}); err != nil {
			return zero, persistenceError("put-resume", entry.config, err)
		}
	}
	config.resuming = true
	return g.runInternal(ctx, zero, config, emit, false)
}

// validateResumeBeforeFork performs the complete dynamic-resume selection
// check without publishing writes. Mixed invocation commands use it before a
// transformed interrupt fork so invalid IDs cannot mutate thread history.
func (g *CompiledGraph[S, D]) validateResumeBeforeFork(
	ctx context.Context,
	root checkpoint.Tuple,
	command ResumeCommand,
) error {
	pending := make(map[string]struct{})
	visited := make(map[string]struct{})
	var walk func(checkpoint.Tuple) error
	walk = func(current checkpoint.Tuple) error {
		coordinate := current.Config.ThreadID + "\x00" + current.Config.Namespace + "\x00" + current.Config.CheckpointID
		if _, exists := visited[coordinate]; exists {
			return nil
		}
		visited[coordinate] = struct{}{}
		scheduled := checkpointScheduledTaskIDs(current.Checkpoint)
		for _, write := range current.PendingWrites {
			switch write.Channel {
			case checkpoint.InterruptChannel:
				if _, exists := scheduled[write.TaskID]; !exists {
					continue
				}
				control, err := g.persistence.decodeTaskControl(write.Value)
				if err != nil {
					return persistenceError("decode-command-resume", current.Config, err)
				}
				for index, interrupt := range control.Interrupts {
					if index >= len(control.Resumes) || control.Resumes[index] == nil {
						pending[interrupt.ID] = struct{}{}
					}
				}
			case checkpoint.SubgraphChannel:
				control, err := decodeSubgraphControl(write.Value)
				if err != nil {
					return persistenceError("decode-command-subgraph", current.Config, err)
				}
				childConfig := checkpoint.Config{ThreadID: current.Config.ThreadID, Namespace: control.Namespace, CheckpointID: control.CheckpointID}
				child, found, err := saverGetTuple(ctx, g.persistence.saver, childConfig)
				if err != nil {
					return err
				}
				if !found {
					return persistenceError("validate-command-subgraph", childConfig, checkpoint.ErrNotFound)
				}
				if err := walk(child); err != nil {
					return err
				}
			}
		}
		return nil
	}
	if err := walk(root); err != nil {
		return err
	}
	if len(pending) == 0 {
		return fmt.Errorf("%w: thread has no pending dynamic interrupts", ErrInvalidResume)
	}
	if command.continueOnly {
		return fmt.Errorf("%w: Continue cannot satisfy a dynamic interrupt", ErrInvalidResume)
	}
	if command.single != nil {
		if len(pending) != 1 {
			return fmt.Errorf("%w: %d interrupts are pending; resume by ID", ErrInvalidResume, len(pending))
		}
		return nil
	}
	if len(command.byID) == 0 {
		return fmt.Errorf("%w: resume command is empty", ErrInvalidResume)
	}
	for id := range command.byID {
		if _, exists := pending[id]; !exists {
			return fmt.Errorf("%w: interrupt %q is not pending", ErrInvalidResume, id)
		}
	}
	return nil
}

func (g *CompiledGraph[S, D]) forkHistoricalInterrupt(
	ctx context.Context,
	source checkpoint.Tuple,
	runID string,
) (checkpoint.Tuple, error) {
	return g.forkInterrupt(ctx, source, runID, nil)
}

func (g *CompiledGraph[S, D]) forkInterruptCommand(
	ctx context.Context,
	source checkpoint.Tuple,
	runID string,
	command Command[D],
) (checkpoint.Tuple, error) {
	return g.forkInterrupt(ctx, source, runID, &command)
}

func (g *CompiledGraph[S, D]) forkInterrupt(
	ctx context.Context,
	source checkpoint.Tuple,
	runID string,
	command *Command[D],
) (checkpoint.Tuple, error) {
	controls := make(map[string]checkpoint.EncodedValue)
	subgraphControls := make(map[string]persistedSubgraphControl)
	sourceTasks := make(map[string]checkpoint.Task, len(source.Checkpoint.Next))
	for _, task := range source.Checkpoint.Next {
		sourceTasks[task.ID] = task
	}
	for _, write := range source.PendingWrites {
		if write.Channel == checkpoint.SubgraphChannel {
			control, decodeErr := decodeSubgraphControl(write.Value)
			if decodeErr != nil {
				return checkpoint.Tuple{}, persistenceError("decode-historical-subgraph", source.Config, decodeErr)
			}
			subgraphControls[write.TaskID] = control
			continue
		}
		if write.Channel != checkpoint.InterruptChannel {
			continue
		}
		if _, exists := sourceTasks[write.TaskID]; !exists {
			continue
		}
		control, decodeErr := g.persistence.decodeTaskControl(write.Value)
		if decodeErr != nil {
			return checkpoint.Tuple{}, persistenceError("decode-historical-interrupt", source.Config, decodeErr)
		}
		control.Resumes = nil
		encoded, encodeErr := g.persistence.encodeTaskControl(control)
		if encodeErr != nil {
			return checkpoint.Tuple{}, persistenceError("encode-historical-interrupt", source.Config, encodeErr)
		}
		controls[write.TaskID] = encoded
	}
	if len(controls) == 0 && len(subgraphControls) == 0 {
		return checkpoint.Tuple{}, fmt.Errorf("%w: historical checkpoint has no interrupts", ErrInvalidResume)
	}
	// Validate the full child checkpoint tree before publishing the root fork.
	// Saver has no rollback primitive, so failures discovered after Put would
	// otherwise leave a visible but unusable branch checkpoint.
	for oldTaskID, control := range subgraphControls {
		task, exists := sourceTasks[oldTaskID]
		if !exists {
			return checkpoint.Tuple{}, persistenceError("validate-historical-subgraph-task", source.Config, checkpoint.ErrInvalidCheckpoint)
		}
		spec, exists := g.subgraphs[NodeID(task.Name)]
		if !exists {
			return checkpoint.Tuple{}, persistenceError("validate-historical-subgraph-spec", source.Config, checkpoint.ErrInvalidCheckpoint)
		}
		if err := spec.validateInterruptFork(ctx, g.persistence, source.Config.ThreadID, control); err != nil {
			return checkpoint.Tuple{}, err
		}
	}
	restored, err := g.restoreRun(source)
	if err != nil {
		return checkpoint.Tuple{}, err
	}
	forkStep := source.Checkpoint.Step + 1
	state := restored.state
	if command != nil && command.HasUpdate {
		state, err = g.reducer(ctx, state, []D{command.Update})
		if err != nil {
			return checkpoint.Tuple{}, fmt.Errorf("%w: reduce invocation command: %w", ErrInvalidResume, err)
		}
	}
	tasks := rescheduleTasks(forkStep+1, restored.tasks)
	if command != nil {
		destinations, validateErr := g.validateExplicitDestinations(forkStep, START, command.Goto)
		if validateErr != nil {
			return checkpoint.Tuple{}, validateErr
		}
		byNode := make(map[NodeID]int, len(tasks))
		for index, task := range tasks {
			if !task.hasInput {
				byNode[task.node] = index
			}
		}
		for _, destination := range destinations {
			if destination == END {
				continue
			}
			trigger := branchToTrigger(destination)
			if index, exists := byNode[destination]; exists {
				tasks[index].triggers = appendUniqueNodeID(tasks[index].triggers, trigger)
				continue
			}
			byNode[destination] = len(tasks)
			tasks = append(tasks, scheduledTask{
				node: destination, triggers: []NodeID{trigger},
				taskID: fmt.Sprintf("step:%d:task:%d:node:%s", forkStep+1, len(tasks), destination),
			})
		}
		for _, send := range command.Sends {
			if send.Node == START || send.Node == END {
				return checkpoint.Tuple{}, fmt.Errorf("%w: invalid invocation Send target %q", ErrUnknownNode, send.Node)
			}
			if _, exists := g.nodes[send.Node]; !exists {
				return checkpoint.Tuple{}, fmt.Errorf("%w: invocation Send target %q", ErrUnknownNode, send.Node)
			}
			input, ok := send.State.(S)
			if !ok {
				return checkpoint.Tuple{}, fmt.Errorf("%w: invocation Send target %q state has type %T", ErrInvalidResume, send.Node, send.State)
			}
			tasks = append(tasks, scheduledTask{
				node: send.Node, input: input, hasInput: true, triggers: []NodeID{"__pregel_push"},
				taskID: fmt.Sprintf("step:%d:task:%d:node:%s", forkStep+1, len(tasks), send.Node),
			})
		}
	}
	sourceKind := checkpoint.SourceFork
	extra := checkpoint.Metadata{"resume_fork": true}
	if command != nil {
		sourceKind = checkpoint.SourceUpdate
		extra = checkpoint.Metadata{"command_resume": true}
	}
	forkConfig, err := g.putCheckpoint(
		ctx, source.Config, state, tasks, forkStep, sourceKind,
		runID, restored.waiting, nil, extra,
	)
	if err != nil {
		return checkpoint.Tuple{}, err
	}
	newByOld := make(map[string]scheduledTask, len(tasks))
	for index, oldTask := range source.Checkpoint.Next {
		if index < len(tasks) {
			newByOld[oldTask.ID] = tasks[index]
		}
	}
	writes := make([]checkpoint.PendingWrite, 0)
	for oldTaskID, encoded := range controls {
		newTask, exists := newByOld[oldTaskID]
		if !exists {
			continue
		}
		writes = append(writes, checkpoint.PendingWrite{
			TaskID: newTask.taskID, TaskPath: string(newTask.node), Index: -1,
			Channel: checkpoint.InterruptChannel, Value: encoded,
		})
	}
	for oldTaskID, control := range subgraphControls {
		newTask, exists := newByOld[oldTaskID]
		if !exists {
			return checkpoint.Tuple{}, persistenceError("map-historical-subgraph", source.Config, checkpoint.ErrInvalidCheckpoint)
		}
		spec, exists := g.subgraphs[newTask.node]
		if !exists {
			return checkpoint.Tuple{}, persistenceError("find-historical-subgraph", source.Config, checkpoint.ErrInvalidCheckpoint)
		}
		forkedControl, forkErr := spec.forkInterrupt(
			ctx,
			g.persistence,
			forkConfig,
			checkpoint.Task{ID: newTask.taskID, Name: string(newTask.node)},
			control,
		)
		if forkErr != nil {
			return checkpoint.Tuple{}, forkErr
		}
		encoded, encodeErr := encodeSubgraphControl(forkedControl)
		if encodeErr != nil {
			return checkpoint.Tuple{}, persistenceError("encode-historical-subgraph", forkConfig, encodeErr)
		}
		writes = append(writes, checkpoint.PendingWrite{
			TaskID: newTask.taskID, TaskPath: string(newTask.node), Index: -2,
			Channel: checkpoint.SubgraphChannel, Value: encoded,
		})
	}
	if err := g.persistence.saver.PutWrites(ctx, forkConfig, writes); err != nil {
		return checkpoint.Tuple{}, persistenceError("put-historical-interrupt", forkConfig, err)
	}
	forked, found, err := saverGetTuple(ctx, g.persistence.saver, forkConfig)
	if err != nil {
		return checkpoint.Tuple{}, err
	}
	if !found {
		return checkpoint.Tuple{}, persistenceError("get-resume-fork", forkConfig, checkpoint.ErrNotFound)
	}
	return forked, nil
}

func setResume(control *persistedTaskControl, index int, value json.RawMessage) {
	for len(control.Resumes) <= index {
		control.Resumes = append(control.Resumes, nil)
	}
	control.Resumes[index] = append(json.RawMessage(nil), value...)
}

func checkpointScheduledTaskIDs(value checkpoint.Checkpoint) map[string]struct{} {
	ids := make(map[string]struct{}, len(value.Next))
	for _, task := range value.Next {
		ids[task.ID] = struct{}{}
	}
	return ids
}
