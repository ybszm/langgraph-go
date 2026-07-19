package postgres_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/ybszm/langgraph-go/store"
	storepostgres "github.com/ybszm/langgraph-go/store/postgres"
)

func pythonStoreFixture(t *testing.T, mode, dsn, scope string) []byte {
	t.Helper()
	pythonPath := os.Getenv("LANGGRAPH_PYTHON_POSTGRES_PATH")
	if pythonPath == "" {
		t.Skip("set LANGGRAPH_PYTHON_POSTGRES_PATH to langgraph-checkpoint-postgres 3.1.0 dependencies")
	}
	command := exec.Command("python", filepath.Join("testdata", "python_store_interop.py"), mode, dsn, scope)
	command.Env = append(os.Environ(), "PYTHONPATH="+pythonPath)
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("Python store %s fixture: %v output=%s", mode, err, output)
	}
	return output
}

func TestPythonGoPostgreSQLStorePhysicalBidirectionalInterop(t *testing.T) {
	dsn := os.Getenv("LANGGRAPH_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("set LANGGRAPH_POSTGRES_DSN to run Python↔Go Store integration")
	}
	ctx := context.Background()
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	adapter, err := storepostgres.NewPythonStoreAdapter(db)
	if err != nil {
		t.Fatal(err)
	}
	if err := adapter.Setup(ctx); err != nil {
		t.Fatal(err)
	}
	scope := "python-go-store-1-2-9"
	namespace := store.Namespace{"interop", scope}
	defer func() {
		_ = adapter.Delete(ctx, namespace, "python-item")
		_ = adapter.Delete(ctx, namespace, "go-item")
	}()

	pythonOutput := pythonStoreFixture(t, "write", dsn, scope)
	var written struct {
		Namespace []string       `json:"namespace"`
		Key       string         `json:"key"`
		Value     map[string]any `json:"value"`
		CreatedAt string         `json:"created_at"`
	}
	if err := json.Unmarshal(pythonOutput, &written); err != nil {
		t.Fatalf("Python output=%s err=%v", pythonOutput, err)
	}
	item, err := adapter.Get(ctx, namespace, "python-item")
	if err != nil || item == nil || item.Value["writer"] != "python" || item.Value["count"] != float64(2) ||
		item.Key != written.Key || item.CreatedAt.IsZero() {
		t.Fatalf("Go read Python item=%+v written=%+v err=%v", item, written, err)
	}
	results, err := adapter.Search(ctx, store.Namespace{"interop"}, store.SearchOptions{
		Filter: store.Value{"kind": "shared"}, Limit: 10,
	})
	if err != nil || len(results) != 1 || results[0].Key != "python-item" {
		t.Fatalf("Go search Python item=%+v err=%v", results, err)
	}

	if err := adapter.PutWithTTL(ctx, namespace, "go-item", store.Value{
		"writer": "go", "kind": "shared", "nested": map[string]any{"ok": true},
	}, 5*time.Minute); err != nil {
		t.Fatal(err)
	}
	pythonRead := pythonStoreFixture(t, "read", dsn, scope)
	var read struct {
		Item struct {
			Namespace []string       `json:"namespace"`
			Key       string         `json:"key"`
			Value     map[string]any `json:"value"`
		} `json:"item"`
		Matches []struct {
			Key string `json:"key"`
		} `json:"matches"`
		Namespaces [][]string `json:"namespaces"`
	}
	if err := json.Unmarshal(pythonRead, &read); err != nil {
		t.Fatalf("Python read=%s err=%v", pythonRead, err)
	}
	if read.Item.Key != "go-item" || read.Item.Value["writer"] != "go" || len(read.Matches) != 2 ||
		len(read.Namespaces) != 1 || len(read.Namespaces[0]) != 2 || read.Namespaces[0][1] != scope {
		t.Fatalf("Python read=%s", pythonRead)
	}
}
