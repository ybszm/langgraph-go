// Package eval provides minimal golden-trajectory assertions for agents and
// graphs. It stays dependency-free so CI can run without external model calls.
package eval

import (
	"fmt"
	"strings"
	"testing"
)

// Step records one observed model/tool/node boundary.
type Step struct {
	// Kind is free-form; common values: "model", "tool", "node", "final".
	Kind string `json:"kind"`
	// Name is a tool name, node ID, or model label.
	Name string `json:"name,omitempty"`
	// Content is optional textual payload (assistant text, tool args summary).
	Content string `json:"content,omitempty"`
}

// Trajectory is an ordered list of steps captured during a run.
type Trajectory []Step

// ExpectTools asserts that tool names appear in order as a subsequence of the
// trajectory (other steps may intervene).
func ExpectTools(traj Trajectory, tools ...string) error {
	if len(tools) == 0 {
		return nil
	}
	index := 0
	for _, step := range traj {
		if !strings.EqualFold(step.Kind, "tool") {
			continue
		}
		if strings.EqualFold(step.Name, tools[index]) {
			index++
			if index == len(tools) {
				return nil
			}
		}
	}
	return fmt.Errorf("eval: tool sequence %v not found in trajectory (matched %d)", tools, index)
}

// ExpectAnyTool asserts at least one of the tool names was invoked.
func ExpectAnyTool(traj Trajectory, tools ...string) error {
	set := map[string]struct{}{}
	for _, name := range tools {
		set[strings.ToLower(name)] = struct{}{}
	}
	for _, step := range traj {
		if strings.EqualFold(step.Kind, "tool") {
			if _, ok := set[strings.ToLower(step.Name)]; ok {
				return nil
			}
		}
	}
	return fmt.Errorf("eval: none of tools %v appeared", tools)
}

// ExpectNoTool asserts no tool step is present.
func ExpectNoTool(traj Trajectory) error {
	for _, step := range traj {
		if strings.EqualFold(step.Kind, "tool") {
			return fmt.Errorf("eval: unexpected tool %q", step.Name)
		}
	}
	return nil
}

// ExpectFinalContains asserts the last final/model content contains substr.
func ExpectFinalContains(traj Trajectory, substr string) error {
	for i := len(traj) - 1; i >= 0; i-- {
		step := traj[i]
		if strings.EqualFold(step.Kind, "final") || strings.EqualFold(step.Kind, "model") {
			if strings.Contains(step.Content, substr) {
				return nil
			}
			return fmt.Errorf("eval: final content %q does not contain %q", step.Content, substr)
		}
	}
	return fmt.Errorf("eval: no final/model step in trajectory")
}

// ToolNames returns tool step names in order.
func (traj Trajectory) ToolNames() []string {
	names := make([]string, 0)
	for _, step := range traj {
		if strings.EqualFold(step.Kind, "tool") {
			names = append(names, step.Name)
		}
	}
	return names
}

// RequireTools is a testing helper around ExpectTools.
func RequireTools(t testing.TB, traj Trajectory, tools ...string) {
	t.Helper()
	if err := ExpectTools(traj, tools...); err != nil {
		t.Fatal(err)
	}
}

// RequireFinalContains is a testing helper around ExpectFinalContains.
func RequireFinalContains(t testing.TB, traj Trajectory, substr string) {
	t.Helper()
	if err := ExpectFinalContains(traj, substr); err != nil {
		t.Fatal(err)
	}
}

// FromAssistantAndTools builds a simple trajectory for offline tests.
func FromAssistantAndTools(assistant string, tools ...string) Trajectory {
	traj := Trajectory{{Kind: "model", Name: "assistant", Content: assistant}}
	for _, name := range tools {
		traj = append(traj, Step{Kind: "tool", Name: name})
	}
	traj = append(traj, Step{Kind: "final", Content: assistant})
	return traj
}
