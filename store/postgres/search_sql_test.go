package postgres

import (
	"strings"
	"testing"

	"github.com/ybszm/langgraph-go/store"
)

func TestBuildSearchQueryPushesNamespaceAndJSONBEquality(t *testing.T) {
	query, arguments, err := buildSearchQuery(store.SearchOp{
		NamespacePrefix: store.Namespace{"docs", "public"},
		Filter: store.Value{
			"active": true,
			"meta":   map[string]any{"kind": "guide"},
			"rank":   map[string]any{"$gte": 2},
			"status": map[string]any{"$ne": "draft"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, fragment := range []string{"AS MATERIALIZED", "jsonb_array_length", "namespace::jsonb ->>", "value #>", "::jsonb"} {
		if !strings.Contains(query, fragment) {
			t.Fatalf("query missing %q: %s", fragment, query)
		}
	}
	if strings.Contains(query, "$gte") || strings.Contains(query, "IS DISTINCT") {
		t.Fatalf("unsafe predicate was pushed: %s", query)
	}
	if len(arguments) < 7 || arguments[0] != 2 || arguments[2] != "docs" || arguments[4] != "public" {
		t.Fatalf("arguments=%v", arguments)
	}
}

func TestBuildSearchQueryRejectsUnsupportedOperator(t *testing.T) {
	_, _, err := buildSearchQuery(store.SearchOp{Filter: store.Value{"rank": map[string]any{"$bad": 1}}})
	if err == nil || !strings.Contains(err.Error(), "$bad") {
		t.Fatalf("err=%v", err)
	}
}
