package remote

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"mime"
	"net/http"
	"net/url"

	"github.com/ybszm/langgraph-go/backend/distributed"
)

func (s *Server[I, O]) streamRemoteRun(writer http.ResponseWriter, request *http.Request, threadID, runID string) {
	exists := false
	if !isNil(s.options.ControlStore) {
		_, found, err := s.options.ControlStore.GetRun(request.Context(), threadID, runID)
		if err != nil {
			s.writeStoreError(writer, err, "get run")
			return
		}
		exists = found
	} else {
		s.control.mu.RLock()
		exists = s.lookupRunLocked(threadID, runID) != nil
		s.control.mu.RUnlock()
	}
	if !exists {
		s.writeControlError(writer, http.StatusNotFound, CodeNotFound, "run not found")
		return
	}
	if isNil(s.options.EventLog) {
		s.writeControlError(writer, http.StatusNotImplemented, CodeProtocol, "remote server does not support durable run streams")
		return
	}
	flusher, ok := writer.(http.Flusher)
	if !ok {
		s.writeControlError(writer, http.StatusInternalServerError, CodeProtocol, "HTTP streaming is unavailable")
		return
	}
	writer.Header().Set("Content-Type", "text/event-stream")
	writer.Header().Set("Cache-Control", "no-cache")
	writer.Header().Set("Connection", "keep-alive")
	writer.WriteHeader(http.StatusOK)
	flusher.Flush()
	tail := s.options.EventLog.Tail(request.Context(), distributed.EventQuery{
		ThreadID: threadID, RunID: runID, AfterID: request.Header.Get("Last-Event-ID"), Buffer: 1,
	})
	for result := range tail {
		if result.Error != nil {
			code := CodeProtocol
			if errors.Is(result.Error, distributed.ErrEventCursorNotFound) {
				code = CodeNotFound
			}
			event := StreamEvent{Mode: "error", Error: &Error{Code: code, Message: result.Error.Error()}}
			if err := writeRemoteSSE(writer, event); err != nil {
				return
			}
			flusher.Flush()
			return
		}
		event := StreamEvent{ID: result.Event.ID, Mode: result.Event.Mode, Data: append(json.RawMessage(nil), result.Event.Data...)}
		if err := writeRemoteSSE(writer, event); err != nil {
			return
		}
		flusher.Flush()
	}
	if request.Context().Err() == nil {
		_, _ = fmt.Fprintf(writer, ": %s\n\n", streamCompleteComment)
		flusher.Flush()
	}
}

func writeRemoteSSE(writer http.ResponseWriter, event StreamEvent) error {
	encoded, err := json.Marshal(event)
	if err != nil {
		return err
	}
	_, err = fmt.Fprintf(writer, "id: %s\nevent: %s\ndata: %s\n\n", event.ID, event.Mode, encoded)
	return err
}

// StreamRun tails a durable thread/run event log after an optional event ID.
func (c *Client[I, O]) StreamRun(ctx context.Context, threadID, runID, afterID string) <-chan StreamEvent {
	result := make(chan StreamEvent, 1)
	go func() {
		defer close(result)
		path := "/v1/threads/" + url.PathEscape(threadID) + "/runs/" + url.PathEscape(runID) + "/stream"
		request, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+path, nil)
		if err != nil {
			sendStreamError(ctx, result, &Error{Code: CodeInvalidRequest, Message: err.Error(), cause: err})
			return
		}
		request.Header.Set("Accept", "text/event-stream")
		if afterID != "" {
			request.Header.Set("Last-Event-ID", afterID)
		}
		c.applyHeaders(request)
		response, err := c.http.Do(request)
		if err != nil {
			if ctx.Err() == nil {
				sendStreamError(ctx, result, &Error{Code: CodeProtocol, Message: err.Error(), cause: err})
			}
			return
		}
		defer response.Body.Close()
		if response.StatusCode != http.StatusOK {
			sendStreamError(ctx, result, decodeStreamHTTPError(response))
			return
		}
		if version := response.Header.Get(ProtocolHeader); version != ProtocolVersion {
			sendStreamError(ctx, result, &Error{Code: CodeProtocol, Message: fmt.Sprintf("protocol version %q", version), Status: response.StatusCode})
			return
		}
		mediaType, _, err := mime.ParseMediaType(response.Header.Get("Content-Type"))
		if err != nil || mediaType != "text/event-stream" {
			sendStreamError(ctx, result, invalidSSE("unexpected content type"))
			return
		}
		lastID := afterID
		complete, err := scanSSE(ctx, response.Body, result, &lastID)
		if ctx.Err() != nil {
			return
		}
		if err != nil {
			sendStreamError(ctx, result, invalidSSE(err.Error()))
			return
		}
		if !complete {
			sendStreamError(ctx, result, invalidSSE("stream ended without completion marker"))
		}
	}()
	return result
}

// StreamRunWithOptions automatically reconnects incomplete durable run streams
// from the last event successfully delivered to the caller.
func (c *Client[I, O]) StreamRunWithOptions(
	ctx context.Context,
	threadID, runID, afterID string,
	options StreamOptions,
) <-chan StreamEvent {
	output := make(chan StreamEvent, 1)
	go func() {
		defer close(output)
		if err := validateStreamOptions(&options); err != nil {
			sendStreamError(ctx, output, &Error{Code: CodeInvalidRequest, Message: err.Error(), cause: err})
			return
		}
		lastID := afterID
		for attempt := 0; ; attempt++ {
			retry := false
			for event := range c.StreamRun(ctx, threadID, runID, lastID) {
				if event.Error != nil {
					if event.Error.Code == CodeProtocol && attempt < options.MaxReconnectAttempts {
						retry = true
						break
					}
					select {
					case output <- event:
					case <-ctx.Done():
					}
					return
				}
				select {
				case output <- event:
					if event.ID != "" {
						lastID = event.ID
					}
				case <-ctx.Done():
					return
				}
			}
			if ctx.Err() != nil || !retry {
				return
			}
			if !waitReconnect(ctx, reconnectDelay(options, attempt)) {
				return
			}
		}
	}()
	return output
}
