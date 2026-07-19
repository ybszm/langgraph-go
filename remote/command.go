package remote

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"

	"github.com/ybszm/langgraph-go/graph"
)

// CommandInvoker is implemented by graphs accepting invocation-time
// Command envelopes containing resume, update, goto, and Send operations.
type CommandInvoker[D, O any] interface {
	InvokeCommand(context.Context, graph.Command[D], graph.RunConfig) (O, error)
}

type commandController interface {
	invoke(context.Context, json.RawMessage, graph.RunConfig) (any, error)
}

var errInvalidCommandWire = errors.New("invalid remote Command wire envelope")

type typedCommandController[S, D, O any] struct {
	invoker CommandInvoker[D, O]
}

type commandSendWire[S any] struct {
	Node  graph.NodeID `json:"node"`
	State S            `json:"state"`
}

type commandWire[S, D any] struct {
	Update    D                    `json:"update,omitempty"`
	HasUpdate bool                 `json:"has_update,omitempty"`
	Goto      []graph.NodeID       `json:"goto,omitempty"`
	Sends     []commandSendWire[S] `json:"sends,omitempty"`
	Target    graph.CommandTarget  `json:"target,omitempty"`
	Resume    *graph.ResumeCommand `json:"resume,omitempty"`
}

type commandRequest struct {
	Command json.RawMessage `json:"command"`
	Config  RunConfig       `json:"config,omitempty"`
}

func (c typedCommandController[S, D, O]) invoke(
	ctx context.Context,
	raw json.RawMessage,
	config graph.RunConfig,
) (any, error) {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	var wire commandWire[S, D]
	if err := decoder.Decode(&wire); err != nil {
		return nil, fmt.Errorf("%w: %v", errInvalidCommandWire, err)
	}
	if err := ensureJSONEOF(decoder); err != nil {
		return nil, fmt.Errorf("%w: %v", errInvalidCommandWire, err)
	}
	command := graph.Command[D]{
		Update: wire.Update, HasUpdate: wire.HasUpdate, Goto: append([]graph.NodeID(nil), wire.Goto...),
		Target: wire.Target, Resume: wire.Resume,
	}
	if wire.Goto == nil {
		command.Goto = nil
	}
	command.Sends = make([]graph.TaskSend, len(wire.Sends))
	for index, send := range wire.Sends {
		command.Sends[index] = graph.SendTo(send.Node, send.State)
	}
	return c.invoker.InvokeCommand(ctx, command, config)
}

// NewCommandServer constructs a remote server with the typed Command
// invocation capability in addition to ordinary Invoke.
func NewCommandServer[I, O, S, D any](
	invoker Invoker[I, O],
	commander CommandInvoker[D, O],
	options ServerOptions,
) (*Server[I, O], error) {
	if isNil(commander) {
		return nil, fmt.Errorf("remote command server requires command invoker")
	}
	server, err := NewServer(invoker, options)
	if err != nil {
		return nil, err
	}
	server.command = typedCommandController[S, D, O]{invoker: commander}
	return server, nil
}

func (s *Server[I, O]) serveCommand(writer http.ResponseWriter, request *http.Request) {
	if s.command == nil {
		s.writeError(writer, http.StatusNotImplemented, CodeProtocol, "remote graph does not support Command invocation")
		return
	}
	request.Body = http.MaxBytesReader(writer, request.Body, s.options.MaxBodyBytes)
	decoder := json.NewDecoder(request.Body)
	decoder.DisallowUnknownFields()
	var payload commandRequest
	if err := decoder.Decode(&payload); err != nil {
		s.writeError(writer, http.StatusBadRequest, CodeInvalidRequest, err.Error())
		return
	}
	if err := ensureJSONEOF(decoder); err != nil {
		s.writeError(writer, http.StatusBadRequest, CodeInvalidRequest, err.Error())
		return
	}
	if len(payload.Command) == 0 {
		s.writeError(writer, http.StatusBadRequest, CodeInvalidRequest, "command is required")
		return
	}
	output, err := s.command.invoke(
		request.Context(), payload.Command, tracedGraphConfig(request.Context(), payload.Config.graphConfig()),
	)
	if err != nil {
		if errors.Is(err, errInvalidCommandWire) {
			s.writeError(writer, http.StatusBadRequest, CodeInvalidRequest, err.Error())
			return
		}
		s.writeError(writer, http.StatusInternalServerError, CodeExecution, err.Error())
		return
	}
	writer.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(writer).Encode(invokeResponse[any]{Output: &output})
}

// CommandClient is the typed Go SDK for remote Command invocation.
type CommandClient[S, D, O any] struct {
	baseURL string
	http    *http.Client
	headers http.Header
}

// NewCommandClient validates and constructs a typed Command client.
func NewCommandClient[S, D, O any](
	baseURL string,
	client *http.Client,
) (*CommandClient[S, D, O], error) {
	base, err := NewClientWithOptions[S, O](baseURL, client, ClientOptions{})
	if err != nil {
		return nil, err
	}
	return &CommandClient[S, D, O]{baseURL: base.baseURL, http: base.http, headers: base.headers}, nil
}

// InvokeCommand sends one detached typed Command envelope to the remote graph.
func (c *CommandClient[S, D, O]) InvokeCommand(
	ctx context.Context,
	command graph.Command[D],
	config RunConfig,
) (O, error) {
	var zero O
	wire := commandWire[S, D]{
		Update: command.Update, HasUpdate: command.HasUpdate,
		Goto: append([]graph.NodeID(nil), command.Goto...), Target: command.Target, Resume: command.Resume,
	}
	if command.Goto == nil {
		wire.Goto = nil
	}
	wire.Sends = make([]commandSendWire[S], len(command.Sends))
	for index, send := range command.Sends {
		state, ok := send.State.(S)
		if !ok {
			return zero, fmt.Errorf("encode remote Command Send %d: state has type %T", index, send.State)
		}
		wire.Sends[index] = commandSendWire[S]{Node: send.Node, State: state}
	}
	encodedCommand, err := json.Marshal(wire)
	if err != nil {
		return zero, fmt.Errorf("encode remote Command: %w", err)
	}
	payload, err := json.Marshal(commandRequest{Command: encodedCommand, Config: config})
	if err != nil {
		return zero, fmt.Errorf("encode remote Command request: %w", err)
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+CommandPath, bytes.NewReader(payload))
	if err != nil {
		return zero, err
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set(ProtocolHeader, ProtocolVersion)
	for key, values := range c.headers {
		request.Header[key] = append([]string(nil), values...)
	}
	response, err := c.http.Do(request)
	if err != nil {
		if ctx.Err() != nil {
			return zero, ctx.Err()
		}
		return zero, fmt.Errorf("remote Command invoke: %w", err)
	}
	defer response.Body.Close()
	if version := response.Header.Get(ProtocolHeader); version != ProtocolVersion {
		return zero, &Error{Code: CodeProtocol, Message: fmt.Sprintf("protocol version %q", version), Status: response.StatusCode}
	}
	decoder := json.NewDecoder(io.LimitReader(response.Body, 2<<20))
	var result invokeResponse[O]
	if err := decoder.Decode(&result); err != nil {
		return zero, &Error{Code: CodeProtocol, Message: err.Error(), Status: response.StatusCode}
	}
	if result.Error != nil {
		result.Error.Status = response.StatusCode
		return zero, result.Error
	}
	if response.StatusCode != http.StatusOK || result.Output == nil {
		return zero, &Error{Code: CodeProtocol, Message: fmt.Sprintf("unexpected HTTP status %d", response.StatusCode), Status: response.StatusCode}
	}
	return *result.Output, nil
}
