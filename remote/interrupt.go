package remote

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"

	"github.com/wahanbo/langgraph-go/backend/distributed"
)

// InterruptRecord is the remote alias of a distributed interrupt snapshot.
type InterruptRecord = distributed.InterruptRecord

// InterruptStatus is the remote alias of distributed interrupt status.
type InterruptStatus = distributed.InterruptStatus

const (
	// InterruptPending means a resume value has not been committed.
	InterruptPending = distributed.InterruptPending
	// InterruptResumed means the committed resume is available to task retry.
	InterruptResumed = distributed.InterruptResumed
)

type interruptEnvelope struct {
	Interrupts []InterruptRecord `json:"interrupts,omitempty"`
	Error      *Error            `json:"error,omitempty"`
}

type resumeInterruptRequest struct {
	Value json.RawMessage `json:"value"`
}

func (s *Server[I, O]) requireInterrupts(ctx context.Context, writer http.ResponseWriter, threadID string) bool {
	exists, err := s.threadExists(ctx, threadID)
	if err != nil {
		s.writeStoreError(writer, err, "get thread")
		return false
	}
	if !exists {
		s.writeControlError(writer, http.StatusNotFound, CodeNotFound, "thread not found")
		return false
	}
	if isNil(s.options.InterruptStore) {
		s.writeControlError(writer, http.StatusNotImplemented, CodeProtocol, "remote server does not support distributed interrupts")
		return false
	}
	return true
}

func (s *Server[I, O]) listRemoteInterrupts(writer http.ResponseWriter, request *http.Request, threadID string) {
	if !s.requireInterrupts(request.Context(), writer, threadID) {
		return
	}
	records, err := s.options.InterruptStore.List(request.Context(), threadID)
	if err != nil {
		s.writeControlError(writer, http.StatusInternalServerError, CodeExecution, err.Error())
		return
	}
	_ = json.NewEncoder(writer).Encode(interruptEnvelope{Interrupts: records})
}

func (s *Server[I, O]) resumeRemoteInterrupt(writer http.ResponseWriter, request *http.Request, threadID, interruptID string) {
	if !s.requireInterrupts(request.Context(), writer, threadID) {
		return
	}
	var payload resumeInterruptRequest
	if err := s.decodeControl(writer, request, &payload); err != nil || len(payload.Value) == 0 {
		if err == nil {
			err = fmt.Errorf("resume value is required")
		}
		s.writeControlError(writer, http.StatusBadRequest, CodeInvalidRequest, err.Error())
		return
	}
	err := s.options.InterruptStore.Resume(request.Context(), threadID, interruptID, payload.Value)
	if err != nil {
		switch {
		case errors.Is(err, distributed.ErrInterruptNotFound):
			s.writeControlError(writer, http.StatusNotFound, CodeNotFound, err.Error())
		case errors.Is(err, distributed.ErrInterruptConflict):
			s.writeControlError(writer, http.StatusConflict, CodeConflict, err.Error())
		default:
			s.writeControlError(writer, http.StatusInternalServerError, CodeExecution, err.Error())
		}
		return
	}
	writer.WriteHeader(http.StatusNoContent)
}

// ListInterrupts returns one thread's stable interrupt snapshots.
func (c *Client[I, O]) ListInterrupts(ctx context.Context, threadID string) ([]InterruptRecord, error) {
	path := "/v1/threads/" + url.PathEscape(threadID) + "/interrupts"
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+path, nil)
	if err != nil {
		return nil, err
	}
	c.applyHeaders(request)
	response, err := c.http.Do(request)
	if err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, err
	}
	defer response.Body.Close()
	if version := response.Header.Get(ProtocolHeader); version != ProtocolVersion {
		return nil, &Error{Code: CodeProtocol, Message: fmt.Sprintf("protocol version %q", version), Status: response.StatusCode}
	}
	var envelope interruptEnvelope
	if err := json.NewDecoder(io.LimitReader(response.Body, 2<<20)).Decode(&envelope); err != nil {
		return nil, &Error{Code: CodeProtocol, Message: err.Error(), Status: response.StatusCode, cause: err}
	}
	if envelope.Error != nil {
		envelope.Error.Status = response.StatusCode
		return nil, envelope.Error
	}
	if response.StatusCode != http.StatusOK {
		return nil, &Error{Code: CodeProtocol, Message: fmt.Sprintf("unexpected HTTP status %d", response.StatusCode), Status: response.StatusCode}
	}
	if envelope.Interrupts == nil {
		return []InterruptRecord{}, nil
	}
	return envelope.Interrupts, nil
}

// ResumeInterrupt commits one JSON-serializable resume value.
func (c *Client[I, O]) ResumeInterrupt(ctx context.Context, threadID, interruptID string, value any) error {
	raw, err := json.Marshal(value)
	if err != nil {
		return err
	}
	body, _ := json.Marshal(resumeInterruptRequest{Value: raw})
	path := "/v1/threads/" + url.PathEscape(threadID) + "/interrupts/" + url.PathEscape(interruptID) + "/resume"
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+path, bytes.NewReader(body))
	if err != nil {
		return err
	}
	request.Header.Set("Content-Type", "application/json")
	c.applyHeaders(request)
	response, err := c.http.Do(request)
	if err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return err
	}
	defer response.Body.Close()
	if response.StatusCode == http.StatusNoContent && response.Header.Get(ProtocolHeader) == ProtocolVersion {
		return nil
	}
	var envelope interruptEnvelope
	if err := json.NewDecoder(io.LimitReader(response.Body, 2<<20)).Decode(&envelope); err == nil && envelope.Error != nil {
		envelope.Error.Status = response.StatusCode
		return envelope.Error
	}
	return &Error{Code: CodeProtocol, Message: fmt.Sprintf("unexpected HTTP status %d", response.StatusCode), Status: response.StatusCode}
}
