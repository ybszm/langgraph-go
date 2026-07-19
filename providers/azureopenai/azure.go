// Package azureopenai provides an Azure OpenAI v1 chat adapter.
package azureopenai

import (
	"net/http"
	"strings"

	"github.com/ybszm/langgraph-go/providers/openaicompat"
)

type Config[S any] struct {
	Endpoint    string
	APIKey      string
	AccessToken string
	Model       string
	HTTPClient  *http.Client
	Messages    openaicompat.MessageReader[S]
	MaxTokens   int
	Temperature *float64
	Streaming   bool
}

func New[S any](config Config[S]) (*openaicompat.Model[S], error) {
	headers := map[string]string{}
	apiKey := config.AccessToken
	if config.APIKey != "" {
		headers["api-key"] = config.APIKey
		apiKey = ""
	}
	baseURL := strings.TrimRight(config.Endpoint, "/")
	if !strings.HasSuffix(baseURL, "/openai/v1") {
		baseURL += "/openai/v1"
	}
	return openaicompat.New(openaicompat.Config[S]{Provider: "Azure OpenAI", BaseURL: baseURL, APIKey: apiKey, Model: config.Model, HTTPClient: config.HTTPClient, Messages: config.Messages, Headers: headers, MaxTokens: config.MaxTokens, Temperature: config.Temperature, Streaming: config.Streaming})
}
