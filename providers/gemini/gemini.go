// Package gemini provides a Google Gemini generateContent adapter.
package gemini

import (
	"context"
	"errors"
	"net/http"
	"net/url"
	"strings"

	"github.com/wahanbo/langgraph-go/graph"
	"github.com/wahanbo/langgraph-go/prebuilt"
	"github.com/wahanbo/langgraph-go/providers/internal/geminiapi"
	"github.com/wahanbo/langgraph-go/providers/openaicompat"
)

const DefaultBaseURL = "https://generativelanguage.googleapis.com/v1beta"

type Config[S any] struct {
	APIKey     string
	Model      string
	BaseURL    string
	HTTPClient *http.Client
	Messages   openaicompat.MessageReader[S]
	Streaming  bool
}

type Model[S any] struct{ inner *geminiapi.Model[S] }

func New[S any](config Config[S]) (*Model[S], error) {
	if config.BaseURL == "" {
		config.BaseURL = DefaultBaseURL
	}
	if strings.TrimSpace(config.Model) == "" {
		return nil, errors.New("Gemini model is required")
	}
	endpoint := strings.TrimRight(config.BaseURL, "/") + "/models/" + url.PathEscape(config.Model) + ":generateContent"
	inner, err := geminiapi.New(geminiapi.Config[S]{Provider: "Gemini", Endpoint: endpoint, APIKey: config.APIKey, HTTPClient: config.HTTPClient, Messages: config.Messages, Streaming: config.Streaming})
	if err != nil {
		return nil, err
	}
	return &Model[S]{inner: inner}, nil
}
func (m *Model[S]) Stream(ctx context.Context, state S, runtime graph.Runtime, emit func(prebuilt.AssistantMessageChunk) error) (prebuilt.AssistantMessage, error) {
	if m == nil || m.inner == nil {
		return prebuilt.AssistantMessage{}, errors.New("Gemini model is nil")
	}
	return m.inner.Stream(ctx, state, runtime, emit)
}

func (m *Model[S]) Invoke(ctx context.Context, state S, runtime graph.Runtime) (prebuilt.AssistantMessage, error) {
	if m == nil || m.inner == nil {
		return prebuilt.AssistantMessage{}, errors.New("Gemini model is nil")
	}
	return m.inner.Invoke(ctx, state, runtime)
}
func (m *Model[S]) BindTools(definitions []prebuilt.ToolDefinition) (prebuilt.ChatModel[S], error) {
	if m == nil || m.inner == nil {
		return nil, errors.New("Gemini model is nil")
	}
	return m.inner.BindTools(definitions)
}
