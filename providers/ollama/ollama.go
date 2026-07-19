// Package ollama provides an Ollama OpenAI-compatible chat adapter.
package ollama

import (
	"net/http"

	"github.com/ybszm/langgraph-go/providers/openaicompat"
)

const DefaultBaseURL = "http://127.0.0.1:11434/v1"

type Config[S any] struct {
	Model       string
	BaseURL     string
	HTTPClient  *http.Client
	Messages    openaicompat.MessageReader[S]
	MaxTokens   int
	Temperature *float64
	Streaming   bool
}

func New[S any](config Config[S]) (*openaicompat.Model[S], error) {
	if config.BaseURL == "" {
		config.BaseURL = DefaultBaseURL
	}
	return openaicompat.New(openaicompat.Config[S]{Provider: "Ollama", BaseURL: config.BaseURL, Model: config.Model, HTTPClient: config.HTTPClient, Messages: config.Messages, MaxTokens: config.MaxTokens, LegacyMaxTokens: true, Temperature: config.Temperature, Streaming: config.Streaming})
}
