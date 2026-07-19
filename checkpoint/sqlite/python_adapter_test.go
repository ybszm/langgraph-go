package sqlite_test

import (
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"path/filepath"
	"testing"

	"github.com/ybszm/langgraph-go/checkpoint"
	checkpointsqlite "github.com/ybszm/langgraph-go/checkpoint/sqlite"
)

func TestPythonAdapterReadsAndWritesUpstream12_9Rows(t *testing.T) {
	db, err := sql.Open("sqlite", filepath.Join(t.TempDir(), "python.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	adapter, err := checkpointsqlite.NewPythonAdapter(db, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := adapter.Setup(context.Background()); err != nil {
		t.Fatal(err)
	}
	fixture := loadPythonSQLiteFixture(t)
	if _, err := db.Exec(`INSERT INTO checkpoints
		(thread_id, checkpoint_ns, checkpoint_id, parent_checkpoint_id, type, checkpoint, metadata)
		VALUES (?, ?, ?, ?, ?, ?, ?)`, fixture.threadID, fixture.namespace, fixture.checkpointID,
		fixture.parentID, fixture.typeName, fixture.payload, fixture.metadata); err != nil {
		t.Fatal(err)
	}
	for _, write := range fixture.writes {
		if _, err := db.Exec(`INSERT INTO writes
			(thread_id, checkpoint_ns, checkpoint_id, task_id, idx, channel, type, value)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?)`, fixture.threadID, fixture.namespace,
			fixture.checkpointID, write.TaskID, write.Index, write.Channel, write.Value.Type, write.Value.Data); err != nil {
			t.Fatal(err)
		}
	}

	tuple, ok, err := adapter.GetTuple(context.Background(), checkpoint.Config{ThreadID: "thread", Namespace: "ns"})
	if err != nil || !ok {
		t.Fatalf("GetTuple() ok=%v err=%v", ok, err)
	}
	if tuple.Checkpoint.ID != fixture.checkpointID || string(tuple.Checkpoint.ChannelValues["text"]) != `"hello"` {
		t.Fatalf("checkpoint = %#v", tuple.Checkpoint)
	}
	if len(tuple.PendingWrites) != 2 || tuple.PendingWrites[0].Index != -3 || tuple.PendingWrites[1].Index != 0 {
		t.Fatalf("pending writes = %#v", tuple.PendingWrites)
	}
	if tuple.Metadata["source"] != "loop" {
		t.Fatalf("metadata = %#v", tuple.Metadata)
	}

	child := tuple.Checkpoint
	child.ID = "1f000000-0000-6000-8000-000000000002"
	config, err := adapter.Put(context.Background(), tuple.Config, child, checkpoint.Metadata{"source": "update", "step": 2})
	if err != nil {
		t.Fatal(err)
	}
	if config.CheckpointID != child.ID {
		t.Fatalf("Put() config = %#v", config)
	}
	if err := adapter.PutWrites(context.Background(), config, fixture.writes); err != nil {
		t.Fatal(err)
	}
	written, ok, err := adapter.GetTuple(context.Background(), config)
	if err != nil || !ok {
		t.Fatalf("GetTuple(child) ok=%v err=%v", ok, err)
	}
	if written.ParentConfig == nil || written.ParentConfig.CheckpointID != fixture.checkpointID {
		t.Fatalf("parent = %#v", written.ParentConfig)
	}
	if len(written.PendingWrites) != 2 || string(written.PendingWrites[1].Value.Data) != string(fixture.writes[1].Value.Data) {
		t.Fatalf("written pending writes = %#v", written.PendingWrites)
	}
	var typeName string
	if err := db.QueryRow(`SELECT type FROM checkpoints WHERE checkpoint_id = ?`, child.ID).Scan(&typeName); err != nil {
		t.Fatal(err)
	}
	if typeName != "msgpack" {
		t.Fatalf("stored checkpoint type = %q", typeName)
	}
	listed, err := adapter.List(context.Background(), checkpoint.ListOptions{
		Config: &checkpoint.Config{ThreadID: "thread", Namespace: "ns"},
		Filter: checkpoint.Metadata{"source": "update"}, Limit: 1,
	})
	if err != nil || len(listed) != 1 || listed[0].Config.CheckpointID != child.ID {
		t.Fatalf("List() = %#v, %v", listed, err)
	}
	if err := adapter.DeleteThread(context.Background(), "thread"); err != nil {
		t.Fatal(err)
	}
	if _, ok, err := adapter.GetTuple(context.Background(), config); err != nil || ok {
		t.Fatalf("GetTuple() after delete ok=%v err=%v", ok, err)
	}
}

type pythonSQLiteFixture struct {
	threadID, namespace, checkpointID, typeName string
	parentID                                    any
	payload, metadata                           []byte
	writes                                      []checkpointsqlite.PythonPendingWrite
}

func loadPythonSQLiteFixture(t *testing.T) pythonSQLiteFixture {
	t.Helper()
	data := []byte(`{"checkpoint":["thread","ns","1f000000-0000-6000-8000-000000000001",null,"msgpack","iKF2AqJpZNkkMWYwMDAwMDAtMDAwMC02MDAwLTgwMDAtMDAwMDAwMDAwMDAxonRz2SAyMDI2LTA3LTE5VDA4OjMwOjQ1LjEyMzQ1NiswMDowMK5jaGFubmVsX3ZhbHVlc4KkdGV4dKVoZWxsb6Vjb3VudAOwY2hhbm5lbF92ZXJzaW9uc4KkdGV4dKJ2MaVjb3VudAKtdmVyc2lvbnNfc2VlboGmd29ya2VygaR0ZXh0onYxrXBlbmRpbmdfc2VuZHOQsHVwZGF0ZWRfY2hhbm5lbHOSpHRleHSlY291bnQ=","eyJzb3VyY2UiOiAibG9vcCIsICJzdGVwIjogMSwgInBhcmVudHMiOiB7fX0="],"writes":[["task-1",-3,"__interrupt__","msgpack","gaZwcm9tcHSpY29udGludWU/"],["task-1",0,"result","msgpack","gaJva8M="]]}`)
	var raw struct {
		Checkpoint []json.RawMessage   `json:"checkpoint"`
		Writes     [][]json.RawMessage `json:"writes"`
	}
	if err := json.Unmarshal(data, &raw); err != nil {
		t.Fatal(err)
	}
	stringAt := func(row []json.RawMessage, index int) string {
		var value string
		if err := json.Unmarshal(row[index], &value); err != nil {
			t.Fatal(err)
		}
		return value
	}
	decodeAt := func(row []json.RawMessage, index int) []byte {
		value, err := base64.StdEncoding.DecodeString(stringAt(row, index))
		if err != nil {
			t.Fatal(err)
		}
		return value
	}
	fixture := pythonSQLiteFixture{
		threadID: stringAt(raw.Checkpoint, 0), namespace: stringAt(raw.Checkpoint, 1),
		checkpointID: stringAt(raw.Checkpoint, 2), typeName: stringAt(raw.Checkpoint, 4),
		payload: decodeAt(raw.Checkpoint, 5), metadata: decodeAt(raw.Checkpoint, 6),
	}
	for _, row := range raw.Writes {
		var index int
		if err := json.Unmarshal(row[1], &index); err != nil {
			t.Fatal(err)
		}
		fixture.writes = append(fixture.writes, checkpointsqlite.PythonPendingWrite{
			TaskID: stringAt(row, 0), Index: index, Channel: stringAt(row, 2),
			Value: checkpointsqlite.PythonTypedValue{Type: stringAt(row, 3), Data: decodeAt(row, 4)},
		})
	}
	return fixture
}
