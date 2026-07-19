package indexed

import (
	"reflect"
	"testing"
)

func TestExtractTextsPathGrammar(t *testing.T) {
	value := map[string]any{
		"title":  "root",
		"nested": map[string]any{"name": "inside", "count": 3},
		"parts":  []map[string]any{{"body": "one"}, {"body": "two"}},
		"tags":   []string{"first", "last"},
	}
	tests := map[string][]string{
		"title":               {"root"},
		"nested.name":         {"inside"},
		"parts[*].body":       {"one", "two"},
		"parts[0].body":       {"one"},
		"tags[-1]":            {"last"},
		"nested.*":            {"3", "inside"},
		"{title,nested.name}": {"root", "inside"},
		"parts[*].missing":    nil,
		"missing":             nil,
	}
	for path, want := range tests {
		t.Run(path, func(t *testing.T) {
			tokens, err := tokenizePath(path)
			if err != nil {
				t.Fatal(err)
			}
			got, err := extractTexts(value, tokens)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("extractTexts=%v want=%v", got, want)
			}
		})
	}
	whole, err := extractTexts(value, nil)
	if err != nil || len(whole) != 1 || whole[0] != `{"nested":{"count":3,"name":"inside"},"parts":[{"body":"one"},{"body":"two"}],"tags":["first","last"],"title":"root"}` {
		t.Fatalf("whole=%q err=%v", whole, err)
	}
}

func TestTokenizePathRejectsMalformedDelimiters(t *testing.T) {
	for _, path := range []string{"parts[0", "{title,name", ""} {
		if _, err := tokenizePath(path); err == nil {
			t.Fatalf("tokenizePath(%q) succeeded", path)
		}
	}
}
