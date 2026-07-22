package prebuilt

import (
	"context"
	"fmt"
	"strings"

	"github.com/ybszm/langgraph-go/graph"
)

// ToolGuardPolicy configures allow/deny checks applied through ToolMiddleware.
type ToolGuardPolicy struct {
	// Allowed is the explicit tool name allowlist. Empty means “all registered
	// tools are eligible unless Denied matches”.
	Allowed []string
	// Denied always blocks, even when listed in Allowed.
	Denied []string
	// RequireHuman names tools that may run only when the runtime context
	// carries RequireHumanApproval=true (via graph Runtime.Context).
	RequireHuman []string
	// OnDenied returns a tool message instead of failing the graph when set.
	// When nil, denied tools return a handled error ToolMessage with status error.
	OnDenied func(ToolCall, string) ToolResult[AgentDelta]
}

// RequireHumanApproval is the optional Runtime.Context contract for human gates.
type RequireHumanApproval interface {
	HumanApprovalGranted() bool
}

// HumanApprovalContext is a convenience Runtime.Context value.
type HumanApprovalContext struct {
	Granted bool
}

// HumanApprovalGranted implements RequireHumanApproval.
func (c HumanApprovalContext) HumanApprovalGranted() bool { return c.Granted }

// NewToolGuardMiddleware builds ToolMiddleware that enforces ToolGuardPolicy
// before invoking the base tool.
func NewToolGuardMiddleware[S, D any](policy ToolGuardPolicy) ToolMiddleware[S, D] {
	allowed := indexNames(policy.Allowed)
	denied := indexNames(policy.Denied)
	requireHuman := indexNames(policy.RequireHuman)
	return ToolMiddlewareFunc[S, D](func(
		ctx context.Context,
		call ToolCall,
		runtime ToolRuntime[S],
		next ToolHandler[S, D],
	) (ToolResult[D], error) {
		name := strings.TrimSpace(call.Name)
		if name == "" {
			return deniedResult[D](call, "tool call is missing a name", policy.OnDenied)
		}
		if _, blocked := denied[name]; blocked {
			return deniedResult[D](call, fmt.Sprintf("tool %q is denied by policy", name), policy.OnDenied)
		}
		if len(allowed) > 0 {
			if _, ok := allowed[name]; !ok {
				return deniedResult[D](call, fmt.Sprintf("tool %q is not in the allowlist", name), policy.OnDenied)
			}
		}
		if _, needsHuman := requireHuman[name]; needsHuman {
			if !humanApproved(runtime.Graph) {
				return deniedResult[D](call, fmt.Sprintf("tool %q requires human approval", name), policy.OnDenied)
			}
		}
		return next(ctx, call, runtime)
	})
}

// GuardTools wraps every tool with the same guard policy while preserving names.
func GuardTools[S, D any](tools []Tool[S, D], policy ToolGuardPolicy) ([]Tool[S, D], error) {
	if len(tools) == 0 {
		return nil, nil
	}
	middleware := NewToolGuardMiddleware[S, D](policy)
	result := make([]Tool[S, D], 0, len(tools))
	for _, tool := range tools {
		guarded, err := WrapTool(tool, middleware)
		if err != nil {
			return nil, err
		}
		result = append(result, guarded)
	}
	return result, nil
}

func indexNames(names []string) map[string]struct{} {
	if len(names) == 0 {
		return nil
	}
	result := make(map[string]struct{}, len(names))
	for _, name := range names {
		name = strings.TrimSpace(name)
		if name != "" {
			result[name] = struct{}{}
		}
	}
	return result
}

func humanApproved(rt graph.Runtime) bool {
	value := rt.Context
	if value == nil {
		return false
	}
	if approval, ok := value.(RequireHumanApproval); ok {
		return approval.HumanApprovalGranted()
	}
	if approval, ok := value.(*HumanApprovalContext); ok && approval != nil {
		return approval.HumanApprovalGranted()
	}
	return false
}

func deniedResult[D any](call ToolCall, reason string, onDenied func(ToolCall, string) ToolResult[AgentDelta]) (ToolResult[D], error) {
	if onDenied != nil {
		// Best-effort: only AgentDelta policies use OnDenied helpers today.
		if result, ok := any(onDenied(call, reason)).(ToolResult[D]); ok {
			return result, nil
		}
	}
	return ToolResult[D]{
		Message: &ToolMessage{
			ToolCallID: call.ID,
			Name:       call.Name,
			Content:    reason,
			Status:     ToolStatusError,
		},
	}, nil
}
