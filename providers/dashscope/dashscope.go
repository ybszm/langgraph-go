// Package dashscope provides a DashScope/Qwen OpenAI-compatible adapter.
package dashscope

import (
	"net/http"

	"github.com/ybszm/langgraph-go/providers/openaicompat"
)

const DefaultBaseURL = "https://dashscope.aliyuncs.com/compatible-mode/v1"

type Config[S any] struct {
	APIKey      string
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
	return openaicompat.New(openaicompat.Config[S]{Provider: "DashScope", BaseURL: config.BaseURL, APIKey: config.APIKey, Model: config.Model, HTTPClient: config.HTTPClient, Messages: config.Messages, MaxTokens: config.MaxTokens, LegacyMaxTokens: true, Temperature: config.Temperature, Streaming: config.Streaming})
}
