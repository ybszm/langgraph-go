// Package managed defines values derived from the current Pregel task rather
// than stored in graph checkpoints.
package managed

import "context"

// Scope is the immutable execution coordinate used to resolve managed values.
// Stop is the exclusive recursion boundary for the current invocation.
type Scope struct {
	Step                int
	Stop                int
	Node                string
	TaskID              string
	ThreadID            string
	CheckpointNamespace string
	CheckpointID        string
}

// Projector builds the node-visible state from canonical persisted state and
// derived managed values. The returned state is used for node input and cache
// identity, but is never reduced or checkpointed directly.
//
// A projector must not mutate persisted. When S contains maps, slices, or
// pointers, configure a graph StateCloner or make an isolated copy here.
type Projector[S any] func(context.Context, S, Scope) (S, error)

// RemainingSteps returns the number of steps before the recursion boundary.
func RemainingSteps(scope Scope) int { return scope.Stop - scope.Step }

// IsLastStep reports whether this task occupies the final permitted step.
func IsLastStep(scope Scope) bool { return RemainingSteps(scope) == 1 }
