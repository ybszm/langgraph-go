package graph

import (
	"context"
	"fmt"
)

const defaultErrorHandlerNode NodeID = "__default_error_handler__"

// NodeError is the failure context supplied to a node error handler after the
// source node has exhausted its retry policy.
type NodeError struct {
	Node NodeID
	Err  error
}

// RecoveredNodeError is the safe default representation of an error restored
// from a durable node-failure marker. Applications that require concrete error
// identity can supply PersistenceConfig.ErrorCodec.
type RecoveredNodeError struct {
	Type    string
	Message string
}

func (e *RecoveredNodeError) Error() string {
	if e.Type == "" {
		return e.Message
	}
	return e.Type + ": " + e.Message
}

// ErrorHandler receives the same state input as the failed node plus its
// terminal failure. Its command is reduced normally, but an update-only
// command does not continue along the failed node's static edges.
type ErrorHandler[S, D any] func(
	ctx context.Context,
	state S,
	failure NodeError,
	runtime Runtime,
) (Command[D], error)

type resolvedErrorHandler[S, D any] struct {
	node    NodeID
	handler ErrorHandler[S, D]
}

// WithErrorHandler assigns a non-recursive recovery handler to one node.
func WithErrorHandler[S, D any](handler ErrorHandler[S, D]) NodeOption {
	return func(options *nodeOptions) error {
		if handler == nil {
			return fmt.Errorf("%w: node error handler is nil", ErrInvalidGraph)
		}
		options.errorHandler = handler
		return nil
	}
}

func explicitErrorHandlerNode(source NodeID) NodeID {
	return NodeID("__error_handler__" + string(source))
}

func resolveErrorHandlers[S, D any](
	explicit map[NodeID]ErrorHandler[S, D],
	nodes map[NodeID]Node[S, D],
	defaultHandler ErrorHandler[S, D],
) (map[NodeID]resolvedErrorHandler[S, D], error) {
	result := make(map[NodeID]resolvedErrorHandler[S, D], len(nodes))
	if defaultHandler != nil {
		if _, collision := nodes[defaultErrorHandlerNode]; collision {
			return nil, fmt.Errorf("%w: generated error handler node %q collides with a user node", ErrInvalidGraph, defaultErrorHandlerNode)
		}
	}
	for source := range nodes {
		handler := explicit[source]
		handlerNode := explicitErrorHandlerNode(source)
		if handler == nil {
			handler = defaultHandler
			handlerNode = defaultErrorHandlerNode
		}
		if handler == nil {
			continue
		}
		if _, collision := nodes[handlerNode]; collision {
			return nil, fmt.Errorf("%w: generated error handler node %q collides with a user node", ErrInvalidGraph, handlerNode)
		}
		result[source] = resolvedErrorHandler[S, D]{node: handlerNode, handler: handler}
	}
	return result, nil
}
