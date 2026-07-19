package retrieval

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/wahanbo/langgraph-go/prebuilt"
)

// ToolOptions configures the generated retriever tool schema and result limit.
type ToolOptions struct {
	Name, Description      string
	DefaultLimit, MaxLimit int
}

// AsTool exposes a Retriever as a schema-bearing provider-neutral Agent tool.
func AsTool[S, D any](retriever Retriever, options ToolOptions) (prebuilt.Tool[S, D], error) {
	if retriever == nil {
		return nil, errors.New("retriever tool requires a retriever")
	}
	if options.Name == "" {
		options.Name = "search_documents"
	}
	if options.Description == "" {
		options.Description = "Search relevant documents for a natural-language query."
	}
	if options.DefaultLimit == 0 {
		options.DefaultLimit = 5
	}
	if options.MaxLimit == 0 {
		options.MaxLimit = 20
	}
	if options.DefaultLimit < 1 || options.MaxLimit < options.DefaultLimit {
		return nil, errors.New("retriever tool limits are invalid")
	}
	base := prebuilt.ToolFunc[S, D]{ToolName: options.Name, Run: func(ctx context.Context, call prebuilt.ToolCall, _ prebuilt.ToolRuntime[S]) (prebuilt.ToolResult[D], error) {
		var arguments struct {
			Query  string         `json:"query"`
			Limit  int            `json:"limit,omitempty"`
			Filter map[string]any `json:"filter,omitempty"`
		}
		if err := json.Unmarshal(call.Arguments, &arguments); err != nil {
			return prebuilt.ToolResult[D]{}, fmt.Errorf("decode retriever tool arguments: %w", err)
		}
		if arguments.Limit == 0 {
			arguments.Limit = options.DefaultLimit
		}
		if arguments.Limit < 1 || arguments.Limit > options.MaxLimit {
			return prebuilt.ToolResult[D]{}, fmt.Errorf("retriever limit must be between 1 and %d", options.MaxLimit)
		}
		results, err := retriever.Retrieve(ctx, Query{Text: arguments.Query, Limit: arguments.Limit, Filter: arguments.Filter})
		if err != nil {
			return prebuilt.ToolResult[D]{}, err
		}
		return prebuilt.ValueResult[D](results)
	}}
	schema := json.RawMessage(fmt.Sprintf(`{"type":"object","properties":{"query":{"type":"string","minLength":1},"limit":{"type":"integer","minimum":1,"maximum":%d},"filter":{"type":"object"}},"required":["query"],"additionalProperties":false}`, options.MaxLimit))
	return prebuilt.WithToolDefinition[S, D](base, prebuilt.ToolDefinition{Name: options.Name, Description: options.Description, InputSchema: schema})
}
