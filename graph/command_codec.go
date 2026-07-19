package graph

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"reflect"

	"github.com/ybszm/langgraph-go/checkpoint"
)

const commandEnvelopeType = "langgraph.go/command"

type commandEnvelope struct {
	HasUpdate bool                     `json:"has_update"`
	Update    *checkpoint.EncodedValue `json:"update"`
	Goto      *[]string                `json:"goto"`
	Sends     []persistedTaskSend      `json:"sends,omitempty"`
	Target    CommandTarget            `json:"target"`
}

// CommandCodec is a versioned checkpoint codec for typed Command values and
// heterogeneous TaskSend state encoded through the graph state codec.
type CommandCodec[S, D any] struct {
	stateCodec checkpoint.Codec[S]
	deltaCodec checkpoint.Codec[D]
}

// NewCommandCodec validates and constructs a public durable command codec.
func NewCommandCodec[S, D any](stateCodec checkpoint.Codec[S], deltaCodec checkpoint.Codec[D]) (*CommandCodec[S, D], error) {
	if codecIsNil(stateCodec) || codecIsNil(deltaCodec) {
		return nil, fmt.Errorf("%w: command state and delta codecs are required", checkpoint.ErrCodecMismatch)
	}
	return &CommandCodec[S, D]{stateCodec: stateCodec, deltaCodec: deltaCodec}, nil
}

// Encode implements checkpoint.Codec.
func (c *CommandCodec[S, D]) Encode(command Command[D]) (checkpoint.EncodedValue, error) {
	if command.Resume != nil {
		return checkpoint.EncodedValue{}, fmt.Errorf("%w: invocation resume commands are not task results", checkpoint.ErrCodecMismatch)
	}
	if command.Target != CommandCurrent && command.Target != CommandParent {
		return checkpoint.EncodedValue{}, fmt.Errorf("%w: invalid command target %d", checkpoint.ErrCodecMismatch, command.Target)
	}
	envelope := commandEnvelope{HasUpdate: command.HasUpdate, Target: command.Target}
	if command.HasUpdate {
		encoded, err := c.deltaCodec.Encode(command.Update)
		if err != nil {
			return checkpoint.EncodedValue{}, fmt.Errorf("encode command update: %w", err)
		}
		copy := checkpoint.CloneEncodedValue(encoded)
		envelope.Update = &copy
	}
	if command.Goto != nil {
		gotoNodes := make([]string, len(command.Goto))
		for index, node := range command.Goto {
			if node == "" {
				return checkpoint.EncodedValue{}, fmt.Errorf("%w: command Goto contains an empty node", checkpoint.ErrCodecMismatch)
			}
			gotoNodes[index] = string(node)
		}
		envelope.Goto = &gotoNodes
	}
	if len(command.Sends) > 0 {
		envelope.Sends = make([]persistedTaskSend, len(command.Sends))
		for index, send := range command.Sends {
			if send.Node == "" {
				return checkpoint.EncodedValue{}, fmt.Errorf("%w: command Send contains an empty node", checkpoint.ErrCodecMismatch)
			}
			state, ok := send.State.(S)
			if !ok {
				return checkpoint.EncodedValue{}, fmt.Errorf("%w: Send target %q state has type %T", checkpoint.ErrCodecMismatch, send.Node, send.State)
			}
			encoded, err := c.stateCodec.Encode(state)
			if err != nil {
				return checkpoint.EncodedValue{}, fmt.Errorf("encode command Send %q: %w", send.Node, err)
			}
			envelope.Sends[index] = persistedTaskSend{Node: string(send.Node), State: checkpoint.CloneEncodedValue(encoded)}
		}
	}
	data, err := json.Marshal(envelope)
	if err != nil {
		return checkpoint.EncodedValue{}, fmt.Errorf("encode command envelope: %w", err)
	}
	return checkpoint.EncodedValue{Type: commandEnvelopeType, Version: 1, Data: data}, nil
}

// Decode implements checkpoint.Codec.
func (c *CommandCodec[S, D]) Decode(value checkpoint.EncodedValue) (Command[D], error) {
	var zero Command[D]
	if value.Type != commandEnvelopeType || value.Version != 1 {
		return zero, fmt.Errorf("%w: got %s v%d, want %s v1", checkpoint.ErrCodecMismatch, value.Type, value.Version, commandEnvelopeType)
	}
	decoder := json.NewDecoder(bytes.NewReader(value.Data))
	decoder.DisallowUnknownFields()
	var envelope commandEnvelope
	if err := decoder.Decode(&envelope); err != nil {
		return zero, fmt.Errorf("decode command envelope: %w", err)
	}
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		if err == nil {
			err = fmt.Errorf("multiple JSON values")
		}
		return zero, fmt.Errorf("decode command envelope: %w", err)
	}
	if envelope.Target != CommandCurrent && envelope.Target != CommandParent {
		return zero, fmt.Errorf("%w: invalid command target %d", checkpoint.ErrCodecMismatch, envelope.Target)
	}
	if envelope.HasUpdate != (envelope.Update != nil) {
		return zero, fmt.Errorf("%w: command update presence is inconsistent", checkpoint.ErrCodecMismatch)
	}
	command := Command[D]{HasUpdate: envelope.HasUpdate, Target: envelope.Target}
	if envelope.Update != nil {
		update, err := c.deltaCodec.Decode(*envelope.Update)
		if err != nil {
			return zero, fmt.Errorf("decode command update: %w", err)
		}
		command.Update = update
	}
	if envelope.Goto != nil {
		command.Goto = make([]NodeID, len(*envelope.Goto))
		for index, node := range *envelope.Goto {
			if node == "" {
				return zero, fmt.Errorf("%w: command Goto contains an empty node", checkpoint.ErrCodecMismatch)
			}
			command.Goto[index] = NodeID(node)
		}
	}
	if len(envelope.Sends) > 0 {
		command.Sends = make([]TaskSend, len(envelope.Sends))
		for index, send := range envelope.Sends {
			if send.Node == "" {
				return zero, fmt.Errorf("%w: command Send contains an empty node", checkpoint.ErrCodecMismatch)
			}
			state, err := c.stateCodec.Decode(send.State)
			if err != nil {
				return zero, fmt.Errorf("decode command Send %q: %w", send.Node, err)
			}
			command.Sends[index] = TaskSend{Node: NodeID(send.Node), State: state}
		}
	}
	return command, nil
}

func codecIsNil(value any) bool {
	if value == nil {
		return true
	}
	reflected := reflect.ValueOf(value)
	switch reflected.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return reflected.IsNil()
	default:
		return false
	}
}
