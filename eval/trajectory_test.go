package eval_test

import (
	"testing"

	"github.com/ybszm/langgraph-go/eval"
)

func TestExpectToolsSubsequence(t *testing.T) {
	traj := eval.Trajectory{
		{Kind: "model"},
		{Kind: "tool", Name: "search"},
		{Kind: "model"},
		{Kind: "tool", Name: "calc"},
		{Kind: "final", Content: "answer 42"},
	}
	if err := eval.ExpectTools(traj, "search", "calc"); err != nil {
		t.Fatal(err)
	}
	if err := eval.ExpectTools(traj, "calc", "search"); err == nil {
		t.Fatal("expected order failure")
	}
	eval.RequireFinalContains(t, traj, "42")
	eval.RequireTools(t, traj, "search")
}

func TestExpectNoTool(t *testing.T) {
	traj := eval.FromAssistantAndTools("hi only")
	// FromAssistantAndTools with no tools still has no tool steps before final
	traj = eval.Trajectory{{Kind: "model", Content: "hi"}, {Kind: "final", Content: "hi"}}
	if err := eval.ExpectNoTool(traj); err != nil {
		t.Fatal(err)
	}
}
