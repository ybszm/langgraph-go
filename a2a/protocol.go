// Package a2a implements a minimal Agent-to-Agent HTTP subset for Go services.
//
// It is intentionally small and JSON-over-HTTP so agents built with langgraph-go
// can call each other without depending on a full multi-vendor A2A stack.
// Field names follow common A2A card/message shapes where practical.
package a2a

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

const (
	// ProtocolVersion is advertised by this package.
	ProtocolVersion = "langgraph-go-a2a/0.1"
	// HeaderProtocol carries ProtocolVersion.
	HeaderProtocol = "X-A2A-Protocol"
	// DefaultPath is the message send endpoint path suffix.
	DefaultPath = "/a2a/v1/message:send"
	// CardPath is the agent card discovery path.
	CardPath = "/a2a/v1/card"
)

// AgentCard describes one callable agent for discovery.
type AgentCard struct {
	Name        string            `json:"name"`
	Description string            `json:"description,omitempty"`
	URL         string            `json:"url,omitempty"`
	Version     string            `json:"version,omitempty"`
	Skills      []string          `json:"skills,omitempty"`
	Metadata    map[string]string `json:"metadata,omitempty"`
}

// Message is one agent turn payload.
type Message struct {
	Role     string         `json:"role"` // user | agent | system
	Content  string         `json:"content"`
	Metadata map[string]any `json:"metadata,omitempty"`
}

// SendRequest is the HTTP body for message:send.
type SendRequest struct {
	// SessionID correlates multi-turn collaboration.
	SessionID string    `json:"session_id,omitempty"`
	Message   Message   `json:"message"`
	History   []Message `json:"history,omitempty"`
}

// SendResponse is the HTTP body returned by message:send.
type SendResponse struct {
	SessionID string  `json:"session_id,omitempty"`
	Message   Message `json:"message"`
	// TraceID is an optional correlation identifier.
	TraceID string `json:"trace_id,omitempty"`
}

// HandlerFunc processes one A2A message.
type HandlerFunc func(context.Context, SendRequest) (SendResponse, error)

// Server exposes card + message endpoints.
type Server struct {
	Card    AgentCard
	Handler HandlerFunc
	// MaxBodyBytes defaults to 1 MiB.
	MaxBodyBytes int64
}

// ServeHTTP implements http.Handler.
func (s *Server) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	writer.Header().Set(HeaderProtocol, ProtocolVersion)
	switch {
	case request.Method == http.MethodGet && strings.HasSuffix(request.URL.Path, CardPath):
		writeJSON(writer, http.StatusOK, s.Card)
	case request.Method == http.MethodPost && strings.HasSuffix(request.URL.Path, DefaultPath):
		s.handleSend(writer, request)
	default:
		http.NotFound(writer, request)
	}
}

func (s *Server) handleSend(writer http.ResponseWriter, request *http.Request) {
	if s.Handler == nil {
		writeJSON(writer, http.StatusServiceUnavailable, map[string]string{"error": "handler not configured"})
		return
	}
	limit := s.MaxBodyBytes
	if limit <= 0 {
		limit = 1 << 20
	}
	body, err := io.ReadAll(io.LimitReader(request.Body, limit+1))
	if err != nil {
		writeJSON(writer, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	if int64(len(body)) > limit {
		writeJSON(writer, http.StatusRequestEntityTooLarge, map[string]string{"error": "body too large"})
		return
	}
	var req SendRequest
	if err := json.Unmarshal(body, &req); err != nil {
		writeJSON(writer, http.StatusBadRequest, map[string]string{"error": "invalid json"})
		return
	}
	if strings.TrimSpace(req.Message.Content) == "" {
		writeJSON(writer, http.StatusBadRequest, map[string]string{"error": "message.content is required"})
		return
	}
	if req.Message.Role == "" {
		req.Message.Role = "user"
	}
	resp, err := s.Handler(request.Context(), req)
	if err != nil {
		writeJSON(writer, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	if resp.Message.Role == "" {
		resp.Message.Role = "agent"
	}
	if resp.SessionID == "" {
		resp.SessionID = req.SessionID
	}
	writeJSON(writer, http.StatusOK, resp)
}

// Client calls a remote A2A agent.
type Client struct {
	BaseURL    string
	HTTPClient *http.Client
	// Headers are applied to every request.
	Headers map[string]string
}

// GetCard fetches the remote agent card.
func (c *Client) GetCard(ctx context.Context) (AgentCard, error) {
	var card AgentCard
	if err := c.doJSON(ctx, http.MethodGet, CardPath, nil, &card); err != nil {
		return AgentCard{}, err
	}
	return card, nil
}

// Send posts one message to the remote agent.
func (c *Client) Send(ctx context.Context, req SendRequest) (SendResponse, error) {
	var resp SendResponse
	if err := c.doJSON(ctx, http.MethodPost, DefaultPath, req, &resp); err != nil {
		return SendResponse{}, err
	}
	return resp, nil
}

func (c *Client) doJSON(ctx context.Context, method, path string, in any, out any) error {
	if c == nil || strings.TrimSpace(c.BaseURL) == "" {
		return fmt.Errorf("a2a client base URL is empty")
	}
	httpClient := c.HTTPClient
	if httpClient == nil {
		httpClient = &http.Client{Timeout: 60 * time.Second}
	}
	var body io.Reader
	if in != nil {
		encoded, err := json.Marshal(in)
		if err != nil {
			return err
		}
		body = bytes.NewReader(encoded)
	}
	request, err := http.NewRequestWithContext(ctx, method, strings.TrimRight(c.BaseURL, "/")+path, body)
	if err != nil {
		return err
	}
	request.Header.Set(HeaderProtocol, ProtocolVersion)
	request.Header.Set("Accept", "application/json")
	if in != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	for key, value := range c.Headers {
		request.Header.Set(key, value)
	}
	response, err := httpClient.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	payload, err := io.ReadAll(io.LimitReader(response.Body, 4<<20))
	if err != nil {
		return err
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return fmt.Errorf("a2a http %d: %s", response.StatusCode, strings.TrimSpace(string(payload)))
	}
	if out == nil {
		return nil
	}
	return json.Unmarshal(payload, out)
}

func writeJSON(writer http.ResponseWriter, status int, value any) {
	writer.Header().Set("Content-Type", "application/json; charset=utf-8")
	writer.WriteHeader(status)
	_ = json.NewEncoder(writer).Encode(value)
}
