package remote

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"

	"github.com/ybszm/langgraph-go/checkpoint"
	"github.com/ybszm/langgraph-go/graph"
)

// StateGraph is the typed checkpoint state capability used by a state server.
type StateGraph[S, D any] interface {
	GetState(context.Context, graph.RunConfig, ...graph.GetStateOption) (graph.StateSnapshot[S, D], error)
	GetStateHistory(context.Context, graph.RunConfig, graph.StateHistoryOptions) ([]graph.StateSnapshot[S, D], error)
	UpdateState(context.Context, graph.RunConfig, graph.StateUpdate[D]) (checkpoint.Config, error)
}

type stateController interface {
	get(context.Context, graph.RunConfig, bool) (any, error)
	history(context.Context, graph.RunConfig, graph.StateHistoryOptions) (any, error)
	update(context.Context, graph.RunConfig, json.RawMessage) (checkpoint.Config, error)
}

type typedStateController[S, D any] struct{ graph StateGraph[S, D] }

func (c typedStateController[S, D]) get(ctx context.Context, config graph.RunConfig, subgraphs bool) (any, error) {
	if subgraphs {
		return c.graph.GetState(ctx, config, graph.WithSubgraphs())
	}
	return c.graph.GetState(ctx, config)
}

func (c typedStateController[S, D]) history(ctx context.Context, config graph.RunConfig, options graph.StateHistoryOptions) (any, error) {
	return c.graph.GetStateHistory(ctx, config, options)
}

func (c typedStateController[S, D]) update(ctx context.Context, config graph.RunConfig, raw json.RawMessage) (checkpoint.Config, error) {
	var update graph.StateUpdate[D]
	if err := json.Unmarshal(raw, &update); err != nil {
		return checkpoint.Config{}, err
	}
	return c.graph.UpdateState(ctx, config, update)
}

// NewStateServer constructs a remote server with typed state, history, and update capabilities.
func NewStateServer[I, O, S, D any](invoker Invoker[I, O], stateGraph StateGraph[S, D], options ServerOptions) (*Server[I, O], error) {
	if isNil(stateGraph) {
		return nil, fmt.Errorf("remote state server requires state graph")
	}
	server, err := NewServer(invoker, options)
	if err != nil {
		return nil, err
	}
	server.state = typedStateController[S, D]{graph: stateGraph}
	return server, nil
}

type stateServerEnvelope struct {
	State   any                `json:"state,omitempty"`
	History any                `json:"history,omitempty"`
	Config  *checkpoint.Config `json:"config,omitempty"`
	Error   *Error             `json:"error,omitempty"`
}

type stateClientEnvelope struct {
	State   json.RawMessage    `json:"state,omitempty"`
	History json.RawMessage    `json:"history,omitempty"`
	Config  *checkpoint.Config `json:"config,omitempty"`
	Error   *Error             `json:"error,omitempty"`
}

type historyRequest struct {
	Config  RunConfig                 `json:"config"`
	Options graph.StateHistoryOptions `json:"options"`
}

type updateStateRequest struct {
	Config RunConfig       `json:"config"`
	Update json.RawMessage `json:"update"`
}

func (s *Server[I, O]) requireState(ctx context.Context, writer http.ResponseWriter, threadID string) bool {
	threadExists, err := s.threadExists(ctx, threadID)
	if err != nil {
		s.writeStoreError(writer, err, "get thread")
		return false
	}
	if !threadExists {
		s.writeControlError(writer, http.StatusNotFound, CodeNotFound, "thread not found")
		return false
	}
	if s.state == nil {
		s.writeControlError(writer, http.StatusNotImplemented, CodeProtocol, "remote graph does not support state")
		return false
	}
	return true
}

func (s *Server[I, O]) getRemoteState(writer http.ResponseWriter, request *http.Request, threadID string) {
	if !s.requireState(request.Context(), writer, threadID) {
		return
	}
	query := request.URL.Query()
	config := RunConfig{
		ThreadID: threadID, CheckpointNamespace: query.Get("checkpoint_namespace"),
		CheckpointID: query.Get("checkpoint_id"), RunID: query.Get("run_id"),
	}
	snapshot, err := s.state.get(request.Context(), tracedGraphConfig(request.Context(), config.graphConfig()), query.Get("subgraphs") == "true")
	if err != nil {
		s.writeControlError(writer, http.StatusInternalServerError, CodeExecution, err.Error())
		return
	}
	_ = json.NewEncoder(writer).Encode(stateServerEnvelope{State: snapshot})
}

func (s *Server[I, O]) getRemoteStateHistory(writer http.ResponseWriter, request *http.Request, threadID string) {
	if !s.requireState(request.Context(), writer, threadID) {
		return
	}
	var payload historyRequest
	if err := s.decodeControl(writer, request, &payload); err != nil {
		s.writeControlError(writer, http.StatusBadRequest, CodeInvalidRequest, err.Error())
		return
	}
	payload.Config.ThreadID = threadID
	history, err := s.state.history(request.Context(), tracedGraphConfig(request.Context(), payload.Config.graphConfig()), payload.Options)
	if err != nil {
		s.writeControlError(writer, http.StatusInternalServerError, CodeExecution, err.Error())
		return
	}
	_ = json.NewEncoder(writer).Encode(stateServerEnvelope{History: history})
}

func (s *Server[I, O]) updateRemoteState(writer http.ResponseWriter, request *http.Request, threadID string) {
	if !s.requireState(request.Context(), writer, threadID) {
		return
	}
	var payload updateStateRequest
	if err := s.decodeControl(writer, request, &payload); err != nil {
		s.writeControlError(writer, http.StatusBadRequest, CodeInvalidRequest, err.Error())
		return
	}
	payload.Config.ThreadID = threadID
	config, err := s.state.update(request.Context(), tracedGraphConfig(request.Context(), payload.Config.graphConfig()), payload.Update)
	if err != nil {
		s.writeControlError(writer, http.StatusInternalServerError, CodeExecution, err.Error())
		return
	}
	_ = json.NewEncoder(writer).Encode(stateServerEnvelope{Config: &config})
}

// StateClient is a typed Go SDK with checkpoint state capabilities.
type StateClient[I, O, S, D any] struct{ *Client[I, O] }

// NewStateClient validates and constructs a typed state client.
func NewStateClient[I, O, S, D any](baseURL string, client *http.Client) (*StateClient[I, O, S, D], error) {
	base, err := NewClient[I, O](baseURL, client)
	if err != nil {
		return nil, err
	}
	return &StateClient[I, O, S, D]{Client: base}, nil
}

// GetState returns the latest or exact typed checkpoint snapshot.
func (c *StateClient[I, O, S, D]) GetState(ctx context.Context, config RunConfig, subgraphs bool) (graph.StateSnapshot[S, D], error) {
	query := url.Values{}
	query.Set("checkpoint_namespace", config.CheckpointNamespace)
	query.Set("checkpoint_id", config.CheckpointID)
	query.Set("run_id", config.RunID)
	query.Set("subgraphs", strconv.FormatBool(subgraphs))
	path := "/v1/threads/" + url.PathEscape(config.ThreadID) + "/state?" + query.Encode()
	var envelope stateClientEnvelope
	if err := c.stateRequest(ctx, http.MethodGet, path, nil, &envelope); err != nil {
		return graph.StateSnapshot[S, D]{}, err
	}
	var snapshot graph.StateSnapshot[S, D]
	if len(envelope.State) == 0 {
		return snapshot, &Error{Code: CodeProtocol, Message: "state response is missing state"}
	}
	if err := json.Unmarshal(envelope.State, &snapshot); err != nil {
		return snapshot, &Error{Code: CodeProtocol, Message: err.Error(), cause: err}
	}
	return snapshot, nil
}

// GetStateHistory returns typed snapshots in backend order.
func (c *StateClient[I, O, S, D]) GetStateHistory(ctx context.Context, config RunConfig, options graph.StateHistoryOptions) ([]graph.StateSnapshot[S, D], error) {
	path := "/v1/threads/" + url.PathEscape(config.ThreadID) + "/state/history"
	var envelope stateClientEnvelope
	if err := c.stateRequest(ctx, http.MethodPost, path, historyRequest{Config: config, Options: options}, &envelope); err != nil {
		return nil, err
	}
	var history []graph.StateSnapshot[S, D]
	if len(envelope.History) == 0 {
		return nil, &Error{Code: CodeProtocol, Message: "state response is missing history"}
	}
	if err := json.Unmarshal(envelope.History, &history); err != nil {
		return nil, &Error{Code: CodeProtocol, Message: err.Error(), cause: err}
	}
	return history, nil
}

// UpdateState applies one typed delta and returns the new checkpoint config.
func (c *StateClient[I, O, S, D]) UpdateState(ctx context.Context, config RunConfig, update graph.StateUpdate[D]) (checkpoint.Config, error) {
	raw, err := json.Marshal(update)
	if err != nil {
		return checkpoint.Config{}, err
	}
	path := "/v1/threads/" + url.PathEscape(config.ThreadID) + "/state/update"
	var envelope stateClientEnvelope
	if err := c.stateRequest(ctx, http.MethodPost, path, updateStateRequest{Config: config, Update: raw}, &envelope); err != nil {
		return checkpoint.Config{}, err
	}
	if envelope.Config == nil {
		return checkpoint.Config{}, &Error{Code: CodeProtocol, Message: "state response is missing config"}
	}
	return *envelope.Config, nil
}

func (c *StateClient[I, O, S, D]) stateRequest(ctx context.Context, method, path string, payload any, target *stateClientEnvelope) error {
	var body io.Reader
	if payload != nil {
		encoded, err := json.Marshal(payload)
		if err != nil {
			return err
		}
		body = bytes.NewReader(encoded)
	}
	request, err := http.NewRequestWithContext(ctx, method, c.baseURL+path, body)
	if err != nil {
		return err
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set(ProtocolHeader, ProtocolVersion)
	c.applyHeaders(request)
	response, err := c.http.Do(request)
	if err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return err
	}
	defer response.Body.Close()
	if version := response.Header.Get(ProtocolHeader); version != ProtocolVersion {
		return &Error{Code: CodeProtocol, Message: fmt.Sprintf("protocol version %q", version), Status: response.StatusCode}
	}
	if err := json.NewDecoder(io.LimitReader(response.Body, 2<<20)).Decode(target); err != nil {
		return &Error{Code: CodeProtocol, Message: err.Error(), Status: response.StatusCode, cause: err}
	}
	if target.Error != nil {
		target.Error.Status = response.StatusCode
		return target.Error
	}
	if response.StatusCode != http.StatusOK {
		return &Error{Code: CodeProtocol, Message: fmt.Sprintf("unexpected HTTP status %d", response.StatusCode), Status: response.StatusCode}
	}
	return nil
}
