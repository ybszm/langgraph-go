package graph

import (
	"context"
	"fmt"
)

// StreamEventsVersion identifies the public run-event protocol revision.
type StreamEventsVersion string

const StreamEventsV3 StreamEventsVersion = "v3"

// RunStreamEvent is the provider-neutral Go representation of one v3 run
// event. ParentIDs are ordered from the outermost known parent to the
// immediate parent.
type RunStreamEvent struct {
	Event     string
	Name      string
	RunID     string
	ParentIDs []string
	Tags      []string
	Metadata  map[string]any
	Data      map[string]any
	Err       error
}

// StreamEvents exposes graph, node, message, custom, update, interrupt, and
// terminal boundaries through one v3 run-event envelope.
func (g *CompiledGraph[S, D]) StreamEvents(
	ctx context.Context,
	input S,
	config RunConfig,
	version StreamEventsVersion,
) <-chan RunStreamEvent {
	output := make(chan RunStreamEvent)
	go func() {
		defer close(output)
		if version != StreamEventsV3 {
			sendRunEvent(ctx, output, RunStreamEvent{Err: fmt.Errorf("%w: %q", ErrUnsupportedStreamEventsVersion, version)})
			return
		}
		if config.RunID == "" {
			generated, err := randomMessageID()
			if err != nil {
				sendRunEvent(ctx, output, RunStreamEvent{Err: err})
				return
			}
			config.RunID = generated
		}
		rootParents := parentRunIDs(config.ParentRunID)
		if !sendRunEvent(ctx, output, RunStreamEvent{
			Event: "on_chain_start", Name: graphRunName(config), RunID: config.RunID,
			ParentIDs: rootParents, Tags: cloneTags(config.Tags), Metadata: cloneMessageMetadata(config.Metadata),
			Data: map[string]any{"input": input},
		}) {
			return
		}
		stream := g.StreamWithOptions(ctx, input, config, StreamOptions{Modes: []StreamMode{
			StreamDebug, StreamMessages, StreamCustom, StreamUpdates, StreamValues,
			StreamInterrupt, StreamDone, StreamError,
		}})
		for event := range stream {
			converted, emit := convertRunStreamEvent(event, config, rootParents)
			if emit && !sendRunEvent(ctx, output, converted) {
				return
			}
		}
	}()
	return output
}

func convertRunStreamEvent[S, D any](event StreamEvent[S, D], config RunConfig, rootParents []string) (RunStreamEvent, bool) {
	base := RunStreamEvent{Tags: cloneTags(config.Tags), Metadata: cloneMessageMetadata(config.Metadata)}
	switch event.Mode {
	case StreamDebug:
		if event.Debug == nil {
			return RunStreamEvent{}, false
		}
		debug := event.Debug
		switch debug.Kind {
		case DebugTask:
			base.Event, base.Name = "on_chain_start", string(debug.Node)
			base.RunID = nodeCallbackRunID(config.RunID, debug.TaskID, 1)
			base.ParentIDs = append(append([]string(nil), rootParents...), config.RunID)
			base.Metadata = cloneMessageMetadata(debug.Metadata)
			base.Data = map[string]any{"input": debug.Input, "triggers": cloneNodeIDs(debug.Triggers)}
			return base, true
		case DebugTaskResult:
			base.Name = string(debug.Node)
			base.RunID = nodeCallbackRunID(config.RunID, debug.TaskID, 1)
			base.ParentIDs = append(append([]string(nil), rootParents...), config.RunID)
			if debug.Err != nil {
				base.Event, base.Err = "on_chain_error", debug.Err
				base.Data = map[string]any{"error": debug.Err}
			} else {
				base.Event = "on_chain_end"
				base.Data = map[string]any{"output": debug.Result, "interrupts": cloneInterrupts(debug.Interrupts)}
			}
			return base, true
		default:
			return RunStreamEvent{}, false
		}
	case StreamMessages:
		if event.Message == nil {
			return RunStreamEvent{}, false
		}
		base.Event, base.Name = "on_chat_model_stream", metadataString(event.Message.Metadata, "langgraph_node")
		base.RunID = messageRunID(config.RunID, event.Message.Metadata)
		base.ParentIDs = append(append([]string(nil), rootParents...), config.RunID)
		base.Metadata = cloneMessageMetadata(event.Message.Metadata)
		if event.Message.ContentBlock != nil {
			base.Data = map[string]any{"chunk": cloneContentBlockStreamEvent(event.Message.ContentBlock)}
		} else {
			base.Data = map[string]any{"chunk": event.Message.Message}
		}
		return base, true
	case StreamCustom:
		base.Event, base.Name, base.RunID = "on_custom_event", graphRunName(config), config.RunID
		base.ParentIDs, base.Data = rootParents, map[string]any{"data": event.Custom}
		return base, true
	case StreamUpdates:
		base.Event, base.Name, base.RunID = "on_chain_stream", graphRunName(config), config.RunID
		base.ParentIDs, base.Data = rootParents, map[string]any{"chunk": event.Updates}
		return base, true
	case StreamInterrupt:
		base.Event, base.Name, base.RunID = "on_chain_stream", graphRunName(config), config.RunID
		base.ParentIDs, base.Data = rootParents, map[string]any{"interrupts": cloneInterrupts(event.Interrupts)}
		return base, true
	case StreamDone:
		base.Event, base.Name, base.RunID = "on_chain_end", graphRunName(config), config.RunID
		base.ParentIDs, base.Data = rootParents, map[string]any{"output": event.State}
		return base, true
	case StreamError:
		base.Event, base.Name, base.RunID, base.Err = "on_chain_error", graphRunName(config), config.RunID, event.Err
		base.ParentIDs, base.Data = rootParents, map[string]any{"error": event.Err}
		return base, true
	default:
		return RunStreamEvent{}, false
	}
}

func sendRunEvent(ctx context.Context, output chan<- RunStreamEvent, event RunStreamEvent) bool {
	select {
	case output <- event:
		return true
	case <-ctx.Done():
		return false
	}
}

func parentRunIDs(parent string) []string {
	if parent == "" {
		return nil
	}
	return []string{parent}
}

func metadataString(metadata map[string]any, key string) string {
	value, _ := metadata[key].(string)
	return value
}

func messageRunID(root string, metadata map[string]any) string {
	taskID := metadataString(metadata, "langgraph_task_id")
	if taskID == "" {
		return root
	}
	return nodeCallbackRunID(root, taskID, 1)
}
