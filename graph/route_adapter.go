package graph

import (
	"context"
	"fmt"

	"github.com/ybszm/langgraph-go/checkpoint"
)

// CompletedTask is one ordered node command supplied by an external engine.
type CompletedTask[D any] struct {
	Node    NodeID
	TaskID  string
	Command Command[D]
}

// RoutedTask is one next task resolved from compiled graph routing rules.
type RoutedTask[S any] struct {
	ID       string
	Node     NodeID
	Input    S
	HasInput bool
}

// RoutePlan is the storage-neutral output of compiled routing resolution.
type RoutePlan[S any] struct {
	Tasks   []RoutedTask[S]
	Waiting map[string][]string
}

// ResolveTasks applies this compiled graph's static, conditional, waiting,
// Command Goto/Send, and send-branch rules without executing nodes or writing
// checkpoints. External engines must supply completed tasks in deterministic
// checkpoint order.
func (g *CompiledGraph[S, D]) ResolveTasks(
	ctx context.Context,
	step int,
	state S,
	completed []CompletedTask[D],
	waiting map[string][]string,
) (RoutePlan[S], error) {
	if ctx == nil {
		return RoutePlan[S]{}, fmt.Errorf("%w: context is nil", ErrInvalidRunConfig)
	}
	if err := ctx.Err(); err != nil {
		return RoutePlan[S]{}, err
	}
	if step < 0 {
		return RoutePlan[S]{}, fmt.Errorf("%w: route step cannot be negative", ErrInvalidRunConfig)
	}
	results := make([]taskResult[D], len(completed))
	for index, item := range completed {
		if item.TaskID == "" {
			return RoutePlan[S]{}, &RouterError{Step: step, Source: item.Node, Err: fmt.Errorf("task ID is empty")}
		}
		if _, exists := g.nodes[item.Node]; !exists {
			return RoutePlan[S]{}, &RouterError{Step: step, Source: item.Node, Err: fmt.Errorf("%w: %q", ErrUnknownNode, item.Node)}
		}
		if item.Command.Target != CommandCurrent {
			return RoutePlan[S]{}, &RouterError{Step: step, Source: item.Node, Err: &ParentCommandError{Command: item.Command}}
		}
		results[index] = taskResult[D]{node: item.Node, taskID: item.TaskID, cmd: item.Command}
	}
	resolvedWaiting := waitingFromCheckpoint(waiting)
	tasks, err := g.resolveNextTasks(
		ctx, step, results, state, resolvedWaiting, DefaultRecursionLimit,
		RunConfig{}, checkpoint.Config{},
	)
	if err != nil {
		return RoutePlan[S]{}, err
	}
	routed := make([]RoutedTask[S], len(tasks))
	for index, task := range tasks {
		routed[index] = RoutedTask[S]{ID: task.taskID, Node: task.node, HasInput: task.hasInput}
		if task.hasInput {
			input, ok := task.input.(S)
			if !ok {
				return RoutePlan[S]{}, &RouterError{Step: step, Source: task.node, Err: fmt.Errorf("routed task input has type %T", task.input)}
			}
			routed[index].Input = input
		}
	}
	return RoutePlan[S]{Tasks: routed, Waiting: waitingCheckpoint(resolvedWaiting, g.waitingEdges)}, nil
}
