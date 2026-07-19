package postgres_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/ybszm/langgraph-go/checkpoint"
	checkpointpostgres "github.com/ybszm/langgraph-go/checkpoint/postgres"
)

func pythonPostgresFixture(t *testing.T, mode, dsn, threadID string) []byte {
	t.Helper()
	pythonPath := os.Getenv("LANGGRAPH_PYTHON_POSTGRES_PATH")
	if pythonPath == "" {
		t.Skip("set LANGGRAPH_PYTHON_POSTGRES_PATH to checkpoint-postgres 3.1.0 dependencies")
	}
	script := filepath.Join("..", "testdata", "python_postgres_interop.py")
	command := exec.Command("python", script, mode, dsn, threadID)
	command.Env = append(os.Environ(), "PYTHONPATH="+pythonPath)
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("Python %s fixture: %v output=%s", mode, err, output)
	}
	return output
}

func TestPythonGoPostgreSQLPhysicalBidirectionalInterop(t *testing.T) {
	dsn := os.Getenv("LANGGRAPH_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("set LANGGRAPH_POSTGRES_DSN to run Python↔Go physical integration")
	}
	ctx := context.Background()
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	adapter, err := checkpointpostgres.NewPythonAdapter(db, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := adapter.Setup(ctx); err != nil {
		t.Fatal(err)
	}

	pythonThread := "python-to-go-1.2.9"
	defer adapter.DeleteThread(ctx, pythonThread)
	pythonOutput := pythonPostgresFixture(t, "write", dsn, pythonThread)
	var written struct {
		CheckpointID string `json:"checkpoint_id"`
	}
	if err := json.Unmarshal(pythonOutput, &written); err != nil {
		t.Fatalf("Python write output=%s err=%v", pythonOutput, err)
	}
	tuple, found, err := adapter.GetTuple(ctx, checkpoint.Config{
		ThreadID: pythonThread, CheckpointID: written.CheckpointID,
	})
	if err != nil || !found {
		t.Fatalf("Go GetTuple found=%v err=%v", found, err)
	}
	if string(tuple.Checkpoint.ChannelValues["text"]) != `"python"` {
		t.Fatalf("Python inline value=%s", tuple.Checkpoint.ChannelValues["text"])
	}
	var object map[string]any
	if err := json.Unmarshal(tuple.Checkpoint.ChannelValues["object"], &object); err != nil ||
		object["writer"] != "python" || object["count"] != float64(2) {
		t.Fatalf("Python blob=%v err=%v", object, err)
	}
	if len(tuple.PendingWrites) != 1 || tuple.PendingWrites[0].TaskPath != "pull/python" {
		t.Fatalf("Python writes=%+v", tuple.PendingWrites)
	}
	decodedWrite, err := (checkpoint.DefaultPythonSerializer{}).DecodeValue(
		tuple.PendingWrites[0].Value.Type, tuple.PendingWrites[0].Value.Data,
	)
	if err != nil {
		t.Fatal(err)
	}
	var write map[string]any
	_ = json.Unmarshal(decodedWrite, &write)
	if write["writer"] != "python" || write["ok"] != true {
		t.Fatalf("Python write=%v", write)
	}

	goThread := "go-to-python-1.2.9"
	defer adapter.DeleteThread(ctx, goThread)
	goCheckpoint, err := checkpoint.DecodePythonCheckpoint([]byte(`{
		"v":2,
		"id":"1f000000-0000-6000-8000-000000000202",
		"ts":"2026-07-19T08:31:45+00:00",
		"channel_values":{"text":"go","object":{"writer":"go","count":3}},
		"channel_versions":{"text":"v3","object":"v4"},
		"versions_seen":{"worker":{"object":"v4"}},
		"pending_sends":[],
		"updated_channels":["text","object"]
	}`))
	if err != nil {
		t.Fatal(err)
	}
	goConfig, err := adapter.Put(ctx, checkpoint.Config{ThreadID: goThread}, goCheckpoint,
		checkpoint.Metadata{"source": "loop", "step": 3, "fixture": "go"},
		map[string]checkpoint.PythonChannelVersion{
			"text": goCheckpoint.ChannelVersions["text"], "object": goCheckpoint.ChannelVersions["object"],
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	writeBlob, _ := checkpoint.EncodePythonMessagePackValue(json.RawMessage(`{"ok":true,"writer":"go"}`))
	if err := adapter.PutWrites(ctx, goConfig, []checkpoint.PythonPendingWrite{{
		TaskID: "go-task", TaskPath: "pull/go", Index: 0, Channel: "result",
		Value: checkpoint.PythonTypedValue{Type: "msgpack", Data: writeBlob},
	}}); err != nil {
		t.Fatal(err)
	}
	pythonRead := pythonPostgresFixture(t, "read", dsn, goThread)
	var read struct {
		Checkpoint struct {
			ChannelValues map[string]json.RawMessage `json:"channel_values"`
		} `json:"checkpoint"`
		Metadata map[string]any `json:"metadata"`
		Writes   []struct {
			TaskID  string         `json:"task_id"`
			Channel string         `json:"channel"`
			Value   map[string]any `json:"value"`
		} `json:"writes"`
	}
	if err := json.Unmarshal(pythonRead, &read); err != nil {
		t.Fatalf("Python read output=%s err=%v", pythonRead, err)
	}
	if string(read.Checkpoint.ChannelValues["text"]) != `"go"` ||
		read.Metadata["fixture"] != "go" || len(read.Writes) != 1 ||
		read.Writes[0].TaskID != "go-task" || read.Writes[0].Value["writer"] != "go" {
		t.Fatalf("Python read=%s", pythonRead)
	}
}
