package graph

import (
	"context"
	"fmt"
	"reflect"
)

// NodeInputMapper projects internal graph state into one node-specific input
// schema for each actual attempt.
type NodeInputMapper[S, I any] func(context.Context, S) (I, error)

// NodeOutputMapper projects one node-specific update into the graph's
// canonical delta schema.
type NodeOutputMapper[O, D any] func(context.Context, O) (D, error)

// TypedNode is a node whose input schema differs from the graph's internal
// state schema while retaining the graph delta/output contract.
type TypedNode[I, D any] func(context.Context, I, Runtime) (Command[D], error)

// AddTypedNode registers a node-specific typed input mapper and node body.
// Projection runs inside the attempt boundary, so retries re-run the mapper
// with the same attempt state and current context.
func AddTypedNode[S, D, I any](
	builder *StateGraph[S, D],
	id NodeID,
	mapper NodeInputMapper[S, I],
	node TypedNode[I, D],
	options ...NodeOption,
) error {
	if builder == nil {
		return fmt.Errorf("%w: typed node builder is nil", ErrInvalidGraph)
	}
	if mapper == nil || node == nil {
		return fmt.Errorf("%w: typed node %q requires mapper and body", ErrInvalidGraph, id)
	}
	wrapper := func(ctx context.Context, state S, runtime Runtime) (Command[D], error) {
		input, err := mapper(ctx, state)
		if err != nil {
			return Command[D]{}, fmt.Errorf("%w: node %q: %w", ErrNodeInputSchema, id, err)
		}
		return node(ctx, input, runtime)
	}
	if err := builder.AddNode(id, wrapper, options...); err != nil {
		return err
	}
	if builder.nodeSchemas == nil {
		builder.nodeSchemas = make(map[NodeID]nodeSchemaInfo)
	}
	builder.nodeSchemas[id] = nodeSchemaInfo{
		input:      reflect.TypeOf((*I)(nil)).Elem().String(),
		output:     reflect.TypeOf((*D)(nil)).Elem().String(),
		inputType:  reflect.TypeOf((*I)(nil)).Elem(),
		outputType: reflect.TypeOf((*D)(nil)).Elem(),
	}
	return nil
}

// AddTypedNodeWithOutput registers independent node input and output schemas.
// The output mapper runs inside the attempt boundary, so a mapping failure
// participates in the node's normal retry and error-handler policy.
func AddTypedNodeWithOutput[S, D, I, O any](
	builder *StateGraph[S, D],
	id NodeID,
	inputMapper NodeInputMapper[S, I],
	node TypedNode[I, O],
	outputMapper NodeOutputMapper[O, D],
	options ...NodeOption,
) error {
	if builder == nil {
		return fmt.Errorf("%w: typed node builder is nil", ErrInvalidGraph)
	}
	if inputMapper == nil || node == nil || outputMapper == nil {
		return fmt.Errorf("%w: typed node %q requires input mapper, body, and output mapper", ErrInvalidGraph, id)
	}
	wrapper := func(ctx context.Context, state S, runtime Runtime) (Command[D], error) {
		input, err := inputMapper(ctx, state)
		if err != nil {
			return Command[D]{}, fmt.Errorf("%w: node %q: %w", ErrNodeInputSchema, id, err)
		}
		command, err := node(ctx, input, runtime)
		if err != nil {
			return Command[D]{}, err
		}
		mapped := Command[D]{
			HasUpdate: command.HasUpdate,
			Goto:      cloneNodeIDs(command.Goto),
			Sends:     cloneTaskSends(command.Sends),
			Target:    command.Target,
		}
		if command.Resume != nil {
			resume := cloneResumeCommand(*command.Resume)
			mapped.Resume = &resume
		}
		if command.HasUpdate {
			mapped.Update, err = outputMapper(ctx, command.Update)
			if err != nil {
				return Command[D]{}, fmt.Errorf("%w: node %q: %w", ErrNodeOutputSchema, id, err)
			}
		}
		return mapped, nil
	}
	if err := builder.AddNode(id, wrapper, options...); err != nil {
		return err
	}
	if builder.nodeSchemas == nil {
		builder.nodeSchemas = make(map[NodeID]nodeSchemaInfo)
	}
	builder.nodeSchemas[id] = nodeSchemaInfo{
		input:      reflect.TypeOf((*I)(nil)).Elem().String(),
		output:     reflect.TypeOf((*O)(nil)).Elem().String(),
		inputType:  reflect.TypeOf((*I)(nil)).Elem(),
		outputType: reflect.TypeOf((*O)(nil)).Elem(),
	}
	return nil
}
