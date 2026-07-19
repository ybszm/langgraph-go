// Package mcpclient exposes MCP server tools as prebuilt tools using the
// official Model Context Protocol Go SDK.
package mcpclient

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os/exec"
	"strings"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/wahanbo/langgraph-go/prebuilt"
)

type Options struct {
	Name    string
	Version string
}

// Client owns one initialized MCP client session.
type Client struct {
	session *mcp.ClientSession
}

func Connect(ctx context.Context, transport mcp.Transport, options Options) (*Client, error) {
	if transport == nil {
		return nil, errors.New("MCP transport is nil")
	}
	if options.Name == "" {
		options.Name = "langgraph-go"
	}
	if options.Version == "" {
		options.Version = "dev"
	}
	client := mcp.NewClient(&mcp.Implementation{Name: options.Name, Version: options.Version}, nil)
	session, err := client.Connect(ctx, transport, nil)
	if err != nil {
		return nil, fmt.Errorf("connect MCP client: %w", err)
	}
	return &Client{session: session}, nil
}

func ConnectHTTP(ctx context.Context, endpoint string, httpClient *http.Client, options Options) (*Client, error) {
	if strings.TrimSpace(endpoint) == "" {
		return nil, errors.New("MCP endpoint is empty")
	}
	return Connect(ctx, &mcp.StreamableClientTransport{Endpoint: endpoint, HTTPClient: httpClient}, options)
}

func ConnectCommand(ctx context.Context, command *exec.Cmd, options Options) (*Client, error) {
	if command == nil {
		return nil, errors.New("MCP command is nil")
	}
	return Connect(ctx, &mcp.CommandTransport{Command: command}, options)
}

func (client *Client) Close() error {
	if client == nil || client.session == nil {
		return nil
	}
	return client.session.Close()
}

// Tools discovers every paginated MCP tool and converts it to a typed prebuilt
// tool. Protocol errors remain Go errors; MCP tool errors become error-status
// ToolMessages so the model can correct its request.
func Tools[S, D any](ctx context.Context, client *Client) ([]prebuilt.Tool[S, D], error) {
	if client == nil || client.session == nil {
		return nil, errors.New("MCP client is nil")
	}
	var result []prebuilt.Tool[S, D]
	seen := make(map[string]struct{})
	for remote, err := range client.session.Tools(ctx, nil) {
		if err != nil {
			return nil, fmt.Errorf("list MCP tools: %w", err)
		}
		if remote == nil || remote.Name == "" {
			return nil, errors.New("MCP server returned a nil or unnamed tool")
		}
		if _, duplicate := seen[remote.Name]; duplicate {
			return nil, fmt.Errorf("MCP server returned duplicate tool %q", remote.Name)
		}
		seen[remote.Name] = struct{}{}
		schema, err := json.Marshal(remote.InputSchema)
		if err != nil || !json.Valid(schema) {
			return nil, fmt.Errorf("encode MCP tool %q schema: %w", remote.Name, err)
		}
		name := remote.Name
		base := prebuilt.ToolFunc[S, D]{ToolName: name, Run: func(ctx context.Context, call prebuilt.ToolCall, _ prebuilt.ToolRuntime[S]) (prebuilt.ToolResult[D], error) {
			var arguments map[string]any
			if err := json.Unmarshal(call.Arguments, &arguments); err != nil {
				return prebuilt.ToolResult[D]{}, fmt.Errorf("decode MCP tool %q arguments: %w", name, err)
			}
			response, err := client.session.CallTool(ctx, &mcp.CallToolParams{Name: name, Arguments: arguments})
			if err != nil {
				return prebuilt.ToolResult[D]{}, fmt.Errorf("call MCP tool %q: %w", name, err)
			}
			content, artifact, err := toolContent(response)
			if err != nil {
				return prebuilt.ToolResult[D]{}, fmt.Errorf("decode MCP tool %q result: %w", name, err)
			}
			if response.IsError {
				return prebuilt.MessageResult[D](prebuilt.ToolMessage{Content: content, Artifact: artifact, Status: prebuilt.ToolStatusError}), nil
			}
			if artifact != nil {
				return prebuilt.ArtifactResult[D](content, artifact), nil
			}
			return prebuilt.TextResult[D](content), nil
		}}
		adapted, err := prebuilt.WithToolDefinition[S, D](base, prebuilt.ToolDefinition{Name: name, Description: remote.Description, InputSchema: schema})
		if err != nil {
			return nil, err
		}
		result = append(result, adapted)
	}
	return result, nil
}

func toolContent(result *mcp.CallToolResult) (string, any, error) {
	if result == nil {
		return "", nil, errors.New("MCP tool returned nil result")
	}
	parts := make([]string, 0, len(result.Content))
	for _, item := range result.Content {
		if text, ok := item.(*mcp.TextContent); ok {
			parts = append(parts, text.Text)
			continue
		}
		encoded, err := item.MarshalJSON()
		if err != nil {
			return "", nil, err
		}
		parts = append(parts, string(encoded))
	}
	content := strings.Join(parts, "\n")
	if result.StructuredContent != nil {
		if content == "" {
			encoded, err := json.Marshal(result.StructuredContent)
			if err != nil {
				return "", nil, err
			}
			content = string(encoded)
		}
		return content, result.StructuredContent, nil
	}
	return content, nil, nil
}
