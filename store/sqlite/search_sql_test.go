package sqlite

import (
	"reflect"
	"strings"
	"testing"

	"github.com/ybszm/langgraph-go/store"
)

func TestBuildSearchQueryPushesSafePredicates(t *testing.T) {
	query, arguments, err := buildSearchQuery(store.SearchOp{
		NamespacePrefix: store.Namespace{"docs", "public"},
		Filter: store.Value{
			"active": true,
			"meta":   map[string]any{"kind": "guide"},
			"rank":   map[string]any{"$gte": 2},
			"status": map[string]any{"$ne": "draft"},
		},
		Limit: 5, Offset: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, fragment := range []string{"json_array_length(namespace)", "json_extract(namespace", "json_extract(value"} {
		if !strings.Contains(query, fragment) {
			t.Fatalf("query missing %q: %s", fragment, query)
		}
	}
	if strings.Contains(query, "$gte") || strings.Contains(query, "CAST(") || strings.Contains(query, "IS NOT") {
		t.Fatalf("unsafe predicate was pushed: %s", query)
	}
	wantPrefix := []any{2, `$[0]`, "docs", `$[1]`, "public"}
	if len(arguments) < len(wantPrefix) || !reflect.DeepEqual(arguments[:len(wantPrefix)], wantPrefix) {
		t.Fatalf("arguments=%v", arguments)
	}
}

func TestBuildSearchQueryRejectsUnsupportedOperatorBeforeSQL(t *testing.T) {
	_, _, err := buildSearchQuery(store.SearchOp{Filter: store.Value{"rank": map[string]any{"$bad": 1}}})
	if err == nil || !strings.Contains(err.Error(), "$bad") {
		t.Fatalf("err=%v", err)
	}
}
