// Package sse contains the strict, bounded SSE reader shared by HTTP model
// providers. It intentionally does not interpret provider JSON payloads.
package sse

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
)

// Scan reads one SSE stream with a total byte bound. Multi-line data fields
// are joined with newlines and delivered when an event terminator is read.
func Scan(ctx context.Context, reader io.Reader, maxBytes int64, handle func(event, data string) error) error {
	if ctx == nil {
		return errors.New("SSE context is nil")
	}
	if reader == nil || handle == nil {
		return errors.New("SSE reader and handler are required")
	}
	if maxBytes <= 0 {
		return errors.New("SSE max bytes must be positive")
	}
	limited := &io.LimitedReader{R: reader, N: maxBytes + 1}
	scanner := bufio.NewScanner(limited)
	scanner.Buffer(make([]byte, 32<<10), int(min(maxBytes, 16<<20)))
	var event string
	var data []string
	dispatch := func() error {
		if len(data) == 0 {
			event = ""
			return nil
		}
		payload := strings.Join(data, "\n")
		data = data[:0]
		name := event
		event = ""
		return handle(name, payload)
	}
	for scanner.Scan() {
		if err := ctx.Err(); err != nil {
			return err
		}
		line := scanner.Text()
		if line == "" {
			if err := dispatch(); err != nil {
				return err
			}
			continue
		}
		if strings.HasPrefix(line, ":") {
			continue
		}
		field, value, found := strings.Cut(line, ":")
		if !found {
			field, value = line, ""
		} else {
			value = strings.TrimPrefix(value, " ")
		}
		switch field {
		case "event":
			event = value
		case "data":
			data = append(data, value)
		}
	}
	if err := scanner.Err(); err != nil {
		return fmt.Errorf("read SSE stream: %w", err)
	}
	if limited.N <= 0 {
		return fmt.Errorf("SSE stream exceeds %d bytes", maxBytes)
	}
	return dispatch()
}
