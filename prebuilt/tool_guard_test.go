package prebuilt_test

import (
	"context"
	"testing"

	"github.com/ybszm/langgraph-go/graph"
	"github.com/ybszm/langgraph-go/prebuilt"
)

type namedTool struct {
	name string
	ran  *bool
}

func (t namedTool) Name() string { return t.name }
func (t namedTool) Invoke(context.Context, prebuilt.ToolCall, prebuilt.ToolRuntime[prebuilt.AgentState]) (prebuilt.ToolResult[prebuilt.AgentDelta], error) {
	if t.ran != nil {
		*t.ran = true
	}
	return prebuilt.TextResult[prebuilt.AgentDelta]("ok"), nil
}

func TestToolGuardAllowlistBlocksUnknown(t *testing.T) {
	var ran bool
	tools, err := prebuilt.GuardTools[prebuilt.AgentState, prebuilt.AgentDelta](
		[]prebuilt.Tool[prebuilt.AgentState, prebuilt.AgentDelta]{namedTool{name: "search", ran: &ran}},
		prebuilt.ToolGuardPolicy{Allowed: []string{"other"}},
	)
	if err != nil {
		t.Fatal(err)
	}
	result, err := tools[0].Invoke(context.Background(), prebuilt.ToolCall{ID: "1", Name: "search"}, prebuilt.ToolRuntime[prebuilt.AgentState]{})
	if err != nil {
		t.Fatal(err)
	}
	if ran {
		t.Fatal("tool should not run")
	}
	if result.Message == nil || result.Message.Status != prebuilt.ToolStatusError {
		t.Fatalf("result=%+v", result)
	}
}

func TestToolGuardRequireHuman(t *testing.T) {
	var ran bool
	tools, err := prebuilt.GuardTools[prebuilt.AgentState, prebuilt.AgentDelta](
		[]prebuilt.Tool[prebuilt.AgentState, prebuilt.AgentDelta]{namedTool{name: "restart", ran: &ran}},
		prebuilt.ToolGuardPolicy{RequireHuman: []string{"restart"}},
	)
	if err != nil {
		t.Fatal(err)
	}
	// without approval
	result, err := tools[0].Invoke(context.Background(), prebuilt.ToolCall{ID: "1", Name: "restart"}, prebuilt.ToolRuntime[prebuilt.AgentState]{
		Graph: graph.Runtime{},
	})
	if err != nil {
		t.Fatal(err)
	}
	if ran || result.Message == nil || result.Message.Status != prebuilt.ToolStatusError {
		t.Fatalf("expected denial ran=%v result=%+v", ran, result)
	}
	// with approval
	result, err = tools[0].Invoke(context.Background(), prebuilt.ToolCall{ID: "1", Name: "restart"}, prebuilt.ToolRuntime[prebuilt.AgentState]{
		Graph: graph.Runtime{Context: prebuilt.HumanApprovalContext{Granted: true}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if !ran || result.Message == nil || result.Message.Status != prebuilt.ToolStatusSuccess {
		t.Fatalf("expected allow ran=%v result=%+v", ran, result)
	}
}
