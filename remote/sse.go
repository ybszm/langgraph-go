package remote

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"strings"
	"time"

	"github.com/wahanbo/langgraph-go/graph"
)

// ErrInvalidSSE identifies a malformed server-sent event response.
var ErrInvalidSSE = errors.New("invalid remote SSE")

const streamCompleteComment = "langgraph-stream-complete"

// StreamEvent is one ordered event in a remote graph stream.
type StreamEvent struct {
	ID    string          `json:"id,omitempty"`
	Mode  string          `json:"mode"`
	Data  json.RawMessage `json:"data,omitempty"`
	Error *Error          `json:"error,omitempty"`
}

// Streamer is implemented by graph abstractions that expose ordered events.
type Streamer[I any] interface {
	Stream(context.Context, I, graph.RunConfig) <-chan StreamEvent
}

// StreamOptions controls reconnect behavior after an incomplete SSE response.
type StreamOptions struct {
	// MaxReconnectAttempts is the number of requests after the initial request.
	MaxReconnectAttempts int
	// InitialReconnectDelay defaults to 100 milliseconds when reconnects are enabled.
	InitialReconnectDelay time.Duration
	// MaxReconnectDelay caps exponential backoff and defaults to 5 seconds.
	MaxReconnectDelay time.Duration
}

func (s *Server[I, O]) serveStream(writer http.ResponseWriter, request *http.Request) {
	streamer, ok := any(s.invoker).(Streamer[I])
	if !ok {
		writer.Header().Set("Content-Type", "application/json")
		s.writeError(writer, http.StatusNotImplemented, CodeProtocol, "remote graph does not support streaming")
		return
	}
	request.Body = http.MaxBytesReader(writer, request.Body, s.options.MaxBodyBytes)
	decoder := json.NewDecoder(request.Body)
	decoder.DisallowUnknownFields()
	var payload invokeRequest[I]
	if err := decoder.Decode(&payload); err != nil {
		writer.Header().Set("Content-Type", "application/json")
		s.writeError(writer, http.StatusBadRequest, CodeInvalidRequest, err.Error())
		return
	}
	if err := ensureJSONEOF(decoder); err != nil {
		writer.Header().Set("Content-Type", "application/json")
		s.writeError(writer, http.StatusBadRequest, CodeInvalidRequest, err.Error())
		return
	}
	flusher, ok := writer.(http.Flusher)
	if !ok {
		writer.Header().Set("Content-Type", "application/json")
		s.writeError(writer, http.StatusInternalServerError, CodeProtocol, "HTTP streaming is unavailable")
		return
	}
	writer.Header().Set("Content-Type", "text/event-stream")
	writer.Header().Set("Cache-Control", "no-cache")
	writer.Header().Set("Connection", "keep-alive")
	writer.WriteHeader(http.StatusOK)
	flusher.Flush()

	events := streamer.Stream(request.Context(), payload.Input, tracedGraphConfig(request.Context(), payload.Config.graphConfig()))
	sequence := 0
	cursor := request.Header.Get("Last-Event-ID")
	cursorFound := cursor == ""
	for {
		select {
		case <-request.Context().Done():
			return
		case event, open := <-events:
			if !open {
				if !cursorFound {
					event := StreamEvent{ID: fmt.Sprint(sequence + 1), Mode: "error", Error: &Error{Code: CodeProtocol, Message: fmt.Sprintf("event cursor %q was not found", cursor)}}
					encoded, _ := json.Marshal(event)
					_, _ = fmt.Fprintf(writer, "id: %s\nevent: %s\ndata: %s\n\n", event.ID, event.Mode, encoded)
				}
				_, _ = fmt.Fprintf(writer, ": %s\n\n", streamCompleteComment)
				flusher.Flush()
				return
			}
			sequence++
			if event.ID == "" {
				event.ID = fmt.Sprint(sequence)
			}
			if event.Mode == "" && event.Error != nil {
				event.Mode = "error"
			}
			if !cursorFound {
				if event.ID == cursor {
					cursorFound = true
				}
				continue
			}
			encoded, err := json.Marshal(event)
			if err != nil {
				event = StreamEvent{ID: event.ID, Mode: "error", Error: &Error{Code: CodeProtocol, Message: err.Error()}}
				encoded, _ = json.Marshal(event)
			}
			if _, err := fmt.Fprintf(writer, "id: %s\nevent: %s\ndata: %s\n\n", event.ID, event.Mode, encoded); err != nil {
				return
			}
			flusher.Flush()
		}
	}
}

// Stream calls the remote graph SSE endpoint. The returned channel closes on
// normal completion or cancellation and contains protocol failures as events.
func (c *Client[I, O]) Stream(ctx context.Context, input I, config RunConfig) <-chan StreamEvent {
	return c.StreamWithOptions(ctx, input, config, StreamOptions{})
}

// StreamWithOptions calls the SSE endpoint and optionally reconnects incomplete
// responses using Last-Event-ID. Delivered event IDs are suppressed on replay.
func (c *Client[I, O]) StreamWithOptions(ctx context.Context, input I, config RunConfig, options StreamOptions) <-chan StreamEvent {
	result := make(chan StreamEvent, 1)
	go func() {
		defer close(result)
		if err := validateStreamOptions(&options); err != nil {
			sendStreamError(ctx, result, &Error{Code: CodeInvalidRequest, Message: err.Error(), cause: err})
			return
		}
		payload, err := json.Marshal(invokeRequest[I]{Input: input, Config: config})
		if err != nil {
			sendStreamError(ctx, result, &Error{Code: CodeInvalidRequest, Message: err.Error(), cause: err})
			return
		}
		lastID := ""
		for attempt := 0; ; attempt++ {
			request, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+StreamPath, bytes.NewReader(payload))
			if err != nil {
				sendStreamError(ctx, result, &Error{Code: CodeInvalidRequest, Message: err.Error(), cause: err})
				return
			}
			request.Header.Set("Content-Type", "application/json")
			request.Header.Set("Accept", "text/event-stream")
			request.Header.Set(ProtocolHeader, ProtocolVersion)
			c.applyHeaders(request)
			if lastID != "" {
				request.Header.Set("Last-Event-ID", lastID)
			}
			response, err := c.http.Do(request)
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				if attempt < options.MaxReconnectAttempts {
					if !waitReconnect(ctx, reconnectDelay(options, attempt)) {
						return
					}
					continue
				}
				sendStreamError(ctx, result, &Error{Code: CodeProtocol, Message: err.Error(), cause: err})
				return
			}
			if response.StatusCode != http.StatusOK {
				sendStreamError(ctx, result, decodeStreamHTTPError(response))
				response.Body.Close()
				return
			}
			if version := response.Header.Get(ProtocolHeader); version != ProtocolVersion {
				response.Body.Close()
				sendStreamError(ctx, result, &Error{Code: CodeProtocol, Message: fmt.Sprintf("protocol version %q", version), Status: response.StatusCode})
				return
			}
			mediaType, _, parseErr := mime.ParseMediaType(response.Header.Get("Content-Type"))
			if parseErr != nil || mediaType != "text/event-stream" {
				response.Body.Close()
				sendStreamError(ctx, result, invalidSSE("unexpected content type"))
				return
			}
			complete, scanErr := scanSSE(ctx, response.Body, result, &lastID)
			response.Body.Close()
			if ctx.Err() != nil {
				return
			}
			if complete {
				return
			}
			if attempt < options.MaxReconnectAttempts {
				if !waitReconnect(ctx, reconnectDelay(options, attempt)) {
					return
				}
				continue
			}
			message := "stream ended without completion marker"
			if scanErr != nil {
				message = scanErr.Error()
			}
			sendStreamError(ctx, result, invalidSSE(message))
			return
		}
	}()
	return result
}

func scanSSE(ctx context.Context, reader io.Reader, output chan<- StreamEvent, lastID *string) (bool, error) {
	scanner := bufio.NewScanner(reader)
	scanner.Buffer(make([]byte, 4096), 2<<20)
	var id, mode string
	var data []string
	complete := false
	dispatch := func() error {
		if len(data) == 0 {
			id, mode = "", ""
			return nil
		}
		var event StreamEvent
		if err := json.Unmarshal([]byte(strings.Join(data, "\n")), &event); err != nil {
			return err
		}
		if event.ID == "" {
			event.ID = id
		}
		if event.Mode == "" {
			event.Mode = mode
		}
		if event.ID != "" && event.ID == *lastID {
			id, mode, data = "", "", nil
			return nil
		}
		select {
		case output <- event:
			if event.ID != "" {
				*lastID = event.ID
			}
			id, mode, data = "", "", nil
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	for scanner.Scan() {
		line := scanner.Text()
		if line == "" {
			if err := dispatch(); err != nil {
				return false, err
			}
			continue
		}
		if strings.HasPrefix(line, ":") {
			if strings.TrimSpace(strings.TrimPrefix(line, ":")) == streamCompleteComment {
				complete = true
			}
			continue
		}
		field, value, found := strings.Cut(line, ":")
		if found && strings.HasPrefix(value, " ") {
			value = value[1:]
		}
		switch field {
		case "id":
			id = value
		case "event":
			mode = value
		case "data":
			data = append(data, value)
		}
	}
	if err := scanner.Err(); err != nil {
		return false, err
	}
	if err := dispatch(); err != nil {
		return false, err
	}
	return complete, nil
}

func validateStreamOptions(options *StreamOptions) error {
	if options.MaxReconnectAttempts < 0 || options.InitialReconnectDelay < 0 || options.MaxReconnectDelay < 0 {
		return fmt.Errorf("remote stream reconnect options cannot be negative")
	}
	if options.MaxReconnectAttempts > 0 && options.InitialReconnectDelay == 0 {
		options.InitialReconnectDelay = 100 * time.Millisecond
	}
	if options.MaxReconnectDelay == 0 {
		options.MaxReconnectDelay = 5 * time.Second
	}
	if options.MaxReconnectDelay < options.InitialReconnectDelay {
		return fmt.Errorf("remote stream max reconnect delay cannot be less than initial delay")
	}
	return nil
}

func reconnectDelay(options StreamOptions, attempt int) time.Duration {
	delay := options.InitialReconnectDelay
	for i := 0; i < attempt && delay < options.MaxReconnectDelay/2; i++ {
		delay *= 2
	}
	if delay > options.MaxReconnectDelay {
		return options.MaxReconnectDelay
	}
	return delay
}

func waitReconnect(ctx context.Context, delay time.Duration) bool {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-timer.C:
		return true
	case <-ctx.Done():
		return false
	}
}

func decodeStreamHTTPError(response *http.Response) *Error {
	decoder := json.NewDecoder(io.LimitReader(response.Body, 2<<20))
	var envelope invokeResponse[json.RawMessage]
	if err := decoder.Decode(&envelope); err == nil && envelope.Error != nil {
		envelope.Error.Status = response.StatusCode
		return envelope.Error
	}
	return &Error{Code: CodeProtocol, Message: fmt.Sprintf("unexpected HTTP status %d", response.StatusCode), Status: response.StatusCode}
}

func invalidSSE(message string) *Error {
	return &Error{Code: CodeProtocol, Message: message, cause: ErrInvalidSSE}
}

func sendStreamError(ctx context.Context, output chan<- StreamEvent, err *Error) {
	select {
	case output <- StreamEvent{Mode: "error", Error: err}:
	case <-ctx.Done():
	}
}
