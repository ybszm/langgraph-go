// Package vertexai provides a Vertex AI Gemini generateContent adapter.
package vertexai

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"

	"github.com/ybszm/langgraph-go/graph"
	"github.com/ybszm/langgraph-go/prebuilt"
	"github.com/ybszm/langgraph-go/providers/internal/geminiapi"
	"github.com/ybszm/langgraph-go/providers/openaicompat"
)

type Config[S any] struct {
	ProjectID   string
	Location    string
	Model       string
	AccessToken string
	Endpoint    string
	HTTPClient  *http.Client
	Messages    openaicompat.MessageReader[S]
	EditRequest openaicompat.RequestEditor
	Streaming   bool
}

type Model[S any] struct{ inner *geminiapi.Model[S] }

func New[S any](config Config[S]) (*Model[S], error) {
	if config.ProjectID == "" || config.Location == "" || config.Model == "" {
		return nil, errors.New("Vertex AI project, location, and model are required")
	}
	endpoint := config.Endpoint
	if endpoint == "" {
		endpoint = fmt.Sprintf("https://%s-aiplatform.googleapis.com/v1/projects/%s/locations/%s/publishers/google/models/%s:generateContent", config.Location, url.PathEscape(config.ProjectID), url.PathEscape(config.Location), url.PathEscape(config.Model))
	}
	inner, err := geminiapi.New(geminiapi.Config[S]{Provider: "Vertex AI", Endpoint: strings.TrimSpace(endpoint), AccessToken: config.AccessToken, HTTPClient: config.HTTPClient, Messages: config.Messages, EditRequest: config.EditRequest, Streaming: config.Streaming})
	if err != nil {
		return nil, err
	}
	return &Model[S]{inner: inner}, nil
}
func (m *Model[S]) Stream(ctx context.Context, state S, runtime graph.Runtime, emit func(prebuilt.AssistantMessageChunk) error) (prebuilt.AssistantMessage, error) {
	if m == nil || m.inner == nil {
		return prebuilt.AssistantMessage{}, errors.New("Vertex AI model is nil")
	}
	return m.inner.Stream(ctx, state, runtime, emit)
}

func (m *Model[S]) Invoke(ctx context.Context, state S, runtime graph.Runtime) (prebuilt.AssistantMessage, error) {
	if m == nil || m.inner == nil {
		return prebuilt.AssistantMessage{}, errors.New("Vertex AI model is nil")
	}
	return m.inner.Invoke(ctx, state, runtime)
}
func (m *Model[S]) BindTools(definitions []prebuilt.ToolDefinition) (prebuilt.ChatModel[S], error) {
	if m == nil || m.inner == nil {
		return nil, errors.New("Vertex AI model is nil")
	}
	return m.inner.BindTools(definitions)
}
