package distributed

import (
	"context"
	"fmt"

	"github.com/wahanbo/langgraph-go/checkpoint"
	"github.com/wahanbo/langgraph-go/graph"
)

// CompiledTaskRouter is implemented by graph.CompiledGraph ResolveTasks.
type CompiledTaskRouter[S, D any] interface {
	ResolveTasks(context.Context, int, S, []graph.CompletedTask[D], map[string][]string) (graph.RoutePlan[S], error)
}

// NewCompiledRouteResolver adapts a compiled graph's public routing seam to a
// distributed StepCoordinator RouteResolver.
func NewCompiledRouteResolver[S, D any](
	router CompiledTaskRouter[S, D],
	stateCodec checkpoint.Codec[S],
) (RouteResolver[S, D], error) {
	if nilLike(router) || nilLike(stateCodec) {
		return nil, fmt.Errorf("%w: compiled router and state codec are required", ErrInvalidRequest)
	}
	return func(ctx context.Context, input StepInput[S, D]) (StepPlan, error) {
		if len(input.Commands) != len(input.TaskIDs) || len(input.Commands) != len(input.Nodes) {
			return StepPlan{}, fmt.Errorf("%w: commands, task IDs, and nodes must have equal length", ErrInvalidRequest)
		}
		completed := make([]graph.CompletedTask[D], len(input.Commands))
		for index := range input.Commands {
			completed[index] = graph.CompletedTask[D]{
				Node: input.Nodes[index], TaskID: input.TaskIDs[index], Command: input.Commands[index],
			}
		}
		plan, err := router.ResolveTasks(ctx, input.Step, input.State, completed, input.Waiting)
		if err != nil {
			return StepPlan{}, err
		}
		next := make([]checkpoint.Task, len(plan.Tasks))
		for index, task := range plan.Tasks {
			next[index] = checkpoint.Task{ID: task.ID, Name: string(task.Node)}
			if !task.HasInput {
				continue
			}
			encoded, err := stateCodec.Encode(task.Input)
			if err != nil {
				return StepPlan{}, fmt.Errorf("encode routed task %q input: %w", task.ID, err)
			}
			if _, err := stateCodec.Decode(encoded); err != nil {
				return StepPlan{}, fmt.Errorf("decode routed task %q input: %w", task.ID, err)
			}
			copy := checkpoint.CloneEncodedValue(encoded)
			next[index].Input = &copy
		}
		return StepPlan{Next: next, Waiting: cloneWaiting(plan.Waiting)}, nil
	}, nil
}
