package graph

import (
	"context"
	"encoding/json"
	"fmt"
	"sync/atomic"
)

// InterruptRequest is the provider-neutral external resume lookup contract.
type InterruptRequest struct {
	Interrupt Interrupt
	Index     int
	// Scope identifies a composite child such as one parallel tool call.
	// Index is local to this scope.
	Scope        string
	TaskID       string
	ThreadID     string
	CheckpointID string
}

// ResumeProvider persists or looks up one interrupt and returns encoded resume data.
// Returning an error wrapping ErrGraphInterrupt pauses the external engine.
type ResumeProvider func(context.Context, InterruptRequest) (json.RawMessage, error)

// RuntimeServices attaches external-engine interrupt and stream callbacks.
type RuntimeServices struct {
	Resume       ResumeProvider
	WriteCustom  func(any) error
	WriteMessage func(any, map[string]any) error
}

// AttachRuntimeServices returns a Runtime with provider-neutral callbacks while
// preserving its public task metadata and deployment-local Store/Context.
func AttachRuntimeServices(ctx context.Context, runtime Runtime, services RuntimeServices) (Runtime, error) {
	if ctx == nil {
		return Runtime{}, fmt.Errorf("%w: runtime service context is nil", ErrInvalidRunConfig)
	}
	if err := ctx.Err(); err != nil {
		return Runtime{}, err
	}
	if services.Resume != nil {
		if runtime.TaskID == "" {
			return Runtime{}, fmt.Errorf("%w: runtime task ID is required for resume service", ErrInvalidRunConfig)
		}
		var sequence atomic.Int64
		runtime.interrupt = func(value any, decode func(json.RawMessage) (any, error)) (any, error) {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			coordinate, prompt, scoped := unwrapInterruptValue(value)
			encoded, err := json.Marshal(prompt)
			if err != nil {
				return nil, fmt.Errorf("encode interrupt prompt: %w", err)
			}
			index := int(sequence.Add(1) - 1)
			scope := ""
			taskIdentity := runtime.TaskID
			if scoped {
				index = coordinate.index
				scope = coordinate.scope
				taskIdentity += "\x00" + scope
			}
			interrupt := Interrupt{
				ID:    durableInterruptID(runtime.CheckpointNamespace, taskIdentity, index),
				Value: append(json.RawMessage(nil), encoded...), Namespace: runtime.CheckpointNamespace,
			}
			resume, err := services.Resume(ctx, InterruptRequest{
				Interrupt: interrupt, Index: index, Scope: scope, TaskID: runtime.TaskID,
				ThreadID: runtime.ThreadID, CheckpointID: runtime.CheckpointID,
			})
			if err != nil {
				return nil, err
			}
			return decode(append(json.RawMessage(nil), resume...))
		}
	}
	if services.WriteCustom != nil {
		runtime.writeCustom = services.WriteCustom
	}
	if services.WriteMessage != nil {
		runtime.writeMessage = services.WriteMessage
	}
	return runtime, nil
}
