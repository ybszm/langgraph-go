package prebuilt

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"runtime/debug"
	"sort"

	"github.com/ybszm/langgraph-go/graph"
	lgstore "github.com/ybszm/langgraph-go/store"
	"golang.org/x/sync/errgroup"
)

// ToolRuntime is trusted execution context injected by ToolNode rather than
// accepted from model-controlled JSON arguments.
type ToolRuntime[S any] struct {
	State   S
	Call    ToolCall
	Graph   graph.Runtime
	Store   lgstore.Store
	Context any
}

// ToolResult contains exactly one normal ToolMessage or graph Command.
type ToolResult[D any] struct {
	Message *ToolMessage
	Command *graph.Command[D]
}

// TextResult converts a regular tool value to a successful ToolMessage.
func TextResult[D any](content string) ToolResult[D] {
	return ToolResult[D]{Message: &ToolMessage{Content: content, Status: ToolStatusSuccess}}
}

// ArtifactResult returns user/model-visible content plus an application-owned
// artifact that is preserved separately on ToolMessage.
func ArtifactResult[D any](content string, artifact any) ToolResult[D] {
	return ToolResult[D]{Message: &ToolMessage{Content: content, Artifact: artifact, Status: ToolStatusSuccess}}
}

// ValueResult JSON-encodes non-string tool output into a ToolMessage.
func ValueResult[D any](value any) (ToolResult[D], error) {
	if content, ok := value.(string); ok {
		return TextResult[D](content), nil
	}
	encoded, err := json.Marshal(value)
	if err != nil {
		return ToolResult[D]{}, fmt.Errorf("encode tool result: %w", err)
	}
	return TextResult[D](string(encoded)), nil
}

// MessageResult returns a caller-customized ToolMessage.
func MessageResult[D any](message ToolMessage) ToolResult[D] {
	return ToolResult[D]{Message: &message}
}

// CommandResult returns graph control flow from a tool.
func CommandResult[D any](command graph.Command[D]) ToolResult[D] {
	return ToolResult[D]{Command: &command}
}

// Tool is one provider-neutral executable tool.
type Tool[S, D any] interface {
	Name() string
	Invoke(context.Context, ToolCall, ToolRuntime[S]) (ToolResult[D], error)
}

// ToolFunc adapts a Go function into Tool.
type ToolFunc[S, D any] struct {
	ToolName string
	Run      func(context.Context, ToolCall, ToolRuntime[S]) (ToolResult[D], error)
}

func (f ToolFunc[S, D]) Name() string { return f.ToolName }

func (f ToolFunc[S, D]) Invoke(
	ctx context.Context,
	call ToolCall,
	runtime ToolRuntime[S],
) (ToolResult[D], error) {
	if f.Run == nil {
		return ToolResult[D]{}, errors.New("tool function is nil")
	}
	return f.Run(ctx, call, runtime)
}

// ToolNodeAdapter binds ToolNode to an application's typed graph state/delta.
type ToolNodeAdapter[S, D any] struct {
	Calls    func(context.Context, S) ([]ToolCall, error)
	Messages func(context.Context, S, []ToolMessage) (D, error)
	// Combine is required when one super-step contains both regular messages
	// and Commands, or multiple Commands. It defines application-specific delta
	// and routing merge semantics without weakening graph's type safety.
	Combine func(context.Context, S, []ToolMessage, []graph.Command[D]) (graph.Command[D], error)
}

// ToolErrorHandler returns message content and whether an error is handled.
type ToolErrorHandler func(error, ToolCall) (string, bool)

// ToolNodeConfig controls parallelism, error conversion, and message streaming.
type ToolNodeConfig struct {
	MaxConcurrency    int
	HandleToolErrors  bool
	ErrorHandler      ToolErrorHandler
	StreamToolResults bool
}

// ToolNode executes model tool calls in parallel and returns deterministic,
// call-order graph output.
type ToolNode[S, D any] struct {
	tools   map[string]Tool[S, D]
	adapter ToolNodeAdapter[S, D]
	config  ToolNodeConfig
}

// NewToolNode validates and constructs a ToolNode.
func NewToolNode[S, D any](
	tools []Tool[S, D],
	adapter ToolNodeAdapter[S, D],
	config ToolNodeConfig,
) (*ToolNode[S, D], error) {
	if adapter.Calls == nil || adapter.Messages == nil {
		return nil, fmt.Errorf("tool node requires Calls and Messages adapters")
	}
	if config.MaxConcurrency < 0 {
		return nil, fmt.Errorf("tool node max concurrency cannot be negative")
	}
	registered := make(map[string]Tool[S, D], len(tools))
	for _, tool := range tools {
		if tool == nil || tool.Name() == "" {
			return nil, fmt.Errorf("tool node contains a nil or unnamed tool")
		}
		if _, duplicate := registered[tool.Name()]; duplicate {
			return nil, fmt.Errorf("duplicate tool %q", tool.Name())
		}
		registered[tool.Name()] = tool
	}
	return &ToolNode[S, D]{tools: registered, adapter: adapter, config: config}, nil
}

// Node returns a graph.Node suitable for StateGraph.AddNode.
func (n *ToolNode[S, D]) Node() graph.Node[S, D] {
	return n.Invoke
}

type indexedToolResult[D any] struct {
	message *ToolMessage
	command *graph.Command[D]
}

// Invoke implements graph.Node.
func (n *ToolNode[S, D]) Invoke(
	ctx context.Context,
	state S,
	runtime graph.Runtime,
) (graph.Command[D], error) {
	calls, err := n.adapter.Calls(ctx, state)
	if err != nil {
		return graph.Command[D]{}, fmt.Errorf("extract tool calls: %w", err)
	}
	if len(calls) == 0 {
		return graph.NoCommand[D](), nil
	}
	seen := make(map[string]struct{}, len(calls))
	for index, call := range calls {
		if call.ID == "" || call.Name == "" {
			return graph.Command[D]{}, fmt.Errorf("tool call %d has an empty id or name", index)
		}
		if _, duplicate := seen[call.ID]; duplicate {
			return graph.Command[D]{}, fmt.Errorf("duplicate tool call id %q", call.ID)
		}
		seen[call.ID] = struct{}{}
	}

	results := make([]indexedToolResult[D], len(calls))
	interruptErrors := make([]error, len(calls))
	interruptOrder := graph.NewInterruptOrder(len(calls))
	group, groupCtx := errgroup.WithContext(ctx)
	limit := n.config.MaxConcurrency
	if limit == 0 || limit > len(calls) {
		limit = len(calls)
	}
	group.SetLimit(limit)
	for index, call := range calls {
		index, call := index, call
		group.Go(func() error {
			defer interruptOrder.Done(index)
			callRuntime := interruptOrder.Runtime(runtime, index)
			result, invokeErr := n.invokeOne(groupCtx, state, callRuntime, call)
			if invokeErr != nil {
				if errors.Is(invokeErr, graph.ErrGraphInterrupt) {
					interruptErrors[index] = invokeErr
					return nil
				}
				return invokeErr
			}
			results[index] = result
			return nil
		})
	}
	if err := group.Wait(); err != nil {
		return graph.Command[D]{}, err
	}
	var interruptErr error
	mostPending := -1
	for _, candidate := range interruptErrors {
		if candidate == nil {
			continue
		}
		if pending := graph.PendingInterruptCount(candidate); pending > mostPending {
			interruptErr = candidate
			mostPending = pending
		}
	}
	if interruptErr != nil {
		return graph.Command[D]{}, interruptErr
	}

	messages := make([]ToolMessage, 0, len(results))
	commands := make([]graph.Command[D], 0, len(results))
	for _, result := range results {
		if result.message != nil {
			messages = append(messages, *result.message)
			if n.config.StreamToolResults {
				if err := runtime.WriteMessage(*result.message, map[string]any{
					"tool_call_id": result.message.ToolCallID,
					"tool_name":    result.message.Name,
				}); err != nil {
					return graph.Command[D]{}, err
				}
			}
		}
		if result.command != nil {
			commands = append(commands, *result.command)
		}
	}
	if len(commands) == 0 {
		delta, err := n.adapter.Messages(ctx, state, messages)
		if err != nil {
			return graph.Command[D]{}, fmt.Errorf("adapt tool messages: %w", err)
		}
		return graph.Update(delta), nil
	}
	if len(commands) == 1 && len(messages) == 0 {
		return commands[0], nil
	}
	if n.adapter.Combine == nil {
		return graph.Command[D]{}, fmt.Errorf(
			"tool outputs contain %d command(s) and %d message(s); Combine adapter is required",
			len(commands), len(messages),
		)
	}
	combined, err := n.adapter.Combine(ctx, state, messages, commands)
	if err != nil {
		return graph.Command[D]{}, fmt.Errorf("combine tool outputs: %w", err)
	}
	return combined, nil
}

func (n *ToolNode[S, D]) invokeOne(
	ctx context.Context,
	state S,
	runtime graph.Runtime,
	call ToolCall,
) (result indexedToolResult[D], err error) {
	tool, exists := n.tools[call.Name]
	if !exists {
		available := make([]string, 0, len(n.tools))
		for name := range n.tools {
			available = append(available, name)
		}
		sort.Strings(available)
		content := fmt.Sprintf("Error: %s is not a valid tool, try one of %v.", call.Name, available)
		message := ToolMessage{ToolCallID: call.ID, Name: call.Name, Content: content, Status: ToolStatusError}
		return indexedToolResult[D]{message: &message}, nil
	}
	output, err := invokeToolSafely(ctx, tool, call, ToolRuntime[S]{
		State: state, Call: call, Graph: runtime, Store: runtime.Store, Context: runtime.Context,
	})
	if err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) ||
			errors.Is(err, graph.ErrGraphInterrupt) || errors.Is(err, graph.ErrParentCommand) {
			return indexedToolResult[D]{}, err
		}
		content, handled := "", false
		if n.config.ErrorHandler != nil {
			content, handled = n.config.ErrorHandler(err, call)
		} else if n.config.HandleToolErrors {
			content, handled = "Error: "+err.Error()+"\n Please fix your mistakes.", true
		}
		if !handled {
			return indexedToolResult[D]{}, fmt.Errorf("tool %q call %q: %w", call.Name, call.ID, err)
		}
		message := ToolMessage{ToolCallID: call.ID, Name: call.Name, Content: content, Status: ToolStatusError}
		return indexedToolResult[D]{message: &message}, nil
	}
	if (output.Message == nil) == (output.Command == nil) {
		return indexedToolResult[D]{}, fmt.Errorf("tool %q must return exactly one Message or Command", call.Name)
	}
	if output.Message != nil {
		message := *output.Message
		if message.ToolCallID == "" {
			message.ToolCallID = call.ID
		}
		if message.ToolCallID != call.ID {
			return indexedToolResult[D]{}, fmt.Errorf(
				"tool %q returned message for call %q, want %q", call.Name, message.ToolCallID, call.ID,
			)
		}
		message.Name = call.Name
		if message.Status == "" {
			message.Status = ToolStatusSuccess
		}
		return indexedToolResult[D]{message: &message}, nil
	}
	command := *output.Command
	return indexedToolResult[D]{command: &command}, nil
}

func invokeToolSafely[S, D any](
	ctx context.Context,
	tool Tool[S, D],
	call ToolCall,
	runtime ToolRuntime[S],
) (result ToolResult[D], err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("tool %q panic: %v\n%s", call.Name, recovered, debug.Stack())
		}
	}()
	return tool.Invoke(ctx, call, runtime)
}

// ToolsCondition returns toolsNode when calls are pending, otherwise END.
func ToolsCondition(calls []ToolCall, toolsNode graph.NodeID) graph.NodeID {
	if len(calls) > 0 {
		return toolsNode
	}
	return graph.END
}
