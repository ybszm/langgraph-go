// Package compat hosts executable behavioral scenarios used to track
// LangGraph 1.2.9 compatibility work. Scenarios are Go-native checks that
// document expected runtime behavior; they are not a full Python interop suite.
package compat

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"testing"
)

// Status mirrors COMPATIBILITY.md row meanings.
type Status string

const (
	StatusSupported  Status = "supported"
	StatusPartial    Status = "partial"
	StatusExtension  Status = "extension"
	StatusMissing    Status = "missing"
)

// Scenario is one named behavioral check.
type Scenario struct {
	ID          string
	Area        string
	Status      Status
	Description string
	// Run executes the check. It must be safe for parallel tests when possible.
	Run func(t *testing.T)
}

var registry = map[string]Scenario{}

// Register adds a scenario. Duplicate IDs panic to fail CI early.
func Register(scenario Scenario) {
	id := strings.TrimSpace(scenario.ID)
	if id == "" {
		panic("compat: scenario ID is empty")
	}
	if scenario.Run == nil {
		panic("compat: scenario " + id + " has nil Run")
	}
	if _, exists := registry[id]; exists {
		panic("compat: duplicate scenario " + id)
	}
	scenario.ID = id
	registry[id] = scenario
}

// List returns registered scenarios sorted by ID.
func List() []Scenario {
	ids := make([]string, 0, len(registry))
	for id := range registry {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	result := make([]Scenario, 0, len(ids))
	for _, id := range ids {
		result = append(result, registry[id])
	}
	return result
}

// RunAll executes every registered scenario as subtests of t.
func RunAll(t *testing.T) {
	t.Helper()
	for _, scenario := range List() {
		scenario := scenario
		t.Run(scenario.ID, func(t *testing.T) {
			t.Helper()
			t.Logf("[%s] %s — %s", scenario.Status, scenario.Area, scenario.Description)
			scenario.Run(t)
		})
	}
}

// Summary returns a short markdown table of registered scenarios.
func Summary(ctx context.Context) string {
	_ = ctx
	var b strings.Builder
	b.WriteString("| ID | Area | Status | Description |\n|---|---|---|---|\n")
	for _, scenario := range List() {
		fmt.Fprintf(&b, "| %s | %s | %s | %s |\n",
			scenario.ID, scenario.Area, scenario.Status, scenario.Description)
	}
	return b.String()
}
