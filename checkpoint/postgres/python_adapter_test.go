package postgres_test

import (
	"context"
	"encoding/json"
	"regexp"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/wahanbo/langgraph-go/checkpoint"
	checkpointpostgres "github.com/wahanbo/langgraph-go/checkpoint/postgres"
)

func TestPythonAdapterHydratesJSONBBlobsAndWrites(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	adapter, err := checkpointpostgres.NewPythonAdapter(db, nil)
	if err != nil {
		t.Fatal(err)
	}
	checkpointJSON := []byte(`{"v":2,"id":"cp-2","ts":"2026-07-19T08:30:45Z","channel_values":{"text":"inline"},"channel_versions":{"text":"v1","object":"v2","missing":"v3"},"versions_seen":{},"pending_sends":[],"updated_channels":["object"]}`)
	metadataJSON := []byte(`{"source":"loop","step":2}`)
	objectBlob, err := checkpoint.EncodePythonMessagePackValue(json.RawMessage(`{"nested":true}`))
	if err != nil {
		t.Fatal(err)
	}
	writeBlob, err := checkpoint.EncodePythonMessagePackValue(json.RawMessage(`{"ok":true}`))
	if err != nil {
		t.Fatal(err)
	}
	mock.ExpectQuery(regexp.QuoteMeta(checkpointpostgres.PythonSelectCheckpointSQL)).
		WithArgs("thread", "ns", "cp-2").
		WillReturnRows(sqlmock.NewRows([]string{"thread_id", "checkpoint_ns", "checkpoint_id", "parent_checkpoint_id", "checkpoint", "metadata"}).
			AddRow("thread", "ns", "cp-2", nil, checkpointJSON, metadataJSON))
	mock.ExpectQuery(regexp.QuoteMeta(checkpointpostgres.PythonSelectBlobsSQL)).
		WithArgs("thread", "ns").
		WillReturnRows(sqlmock.NewRows([]string{"channel", "version", "type", "blob"}).
			AddRow("object", "v2", "msgpack", objectBlob).
			AddRow("missing", "v3", "empty", nil))
	mock.ExpectQuery(regexp.QuoteMeta(checkpointpostgres.PythonSelectWritesSQL)).
		WithArgs("thread", "ns", "cp-2").
		WillReturnRows(sqlmock.NewRows([]string{"task_id", "task_path", "idx", "channel", "type", "blob"}).
			AddRow("task", "pull/node", 0, "result", "msgpack", writeBlob))

	tuple, ok, err := adapter.GetTuple(context.Background(), checkpoint.Config{ThreadID: "thread", Namespace: "ns", CheckpointID: "cp-2"})
	if err != nil || !ok {
		t.Fatalf("GetTuple() ok=%v err=%v", ok, err)
	}
	if string(tuple.Checkpoint.ChannelValues["text"]) != `"inline"` || string(tuple.Checkpoint.ChannelValues["object"]) != `{"nested":true}` {
		t.Fatalf("channel values = %#v", tuple.Checkpoint.ChannelValues)
	}
	if _, exists := tuple.Checkpoint.ChannelValues["missing"]; exists {
		t.Fatal("empty blob was surfaced as a channel value")
	}
	if len(tuple.PendingWrites) != 1 || tuple.PendingWrites[0].TaskPath != "pull/node" || string(tuple.PendingWrites[0].Value.Data) != string(writeBlob) {
		t.Fatalf("pending writes = %#v", tuple.PendingWrites)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestPythonAdapterMigratesParentPendingSends(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	adapter, err := checkpointpostgres.NewPythonAdapter(db, nil)
	if err != nil {
		t.Fatal(err)
	}
	checkpointJSON := []byte(`{"v":2,"id":"child","ts":"2026-07-19T08:30:45Z","channel_values":{},"channel_versions":{"state":"v9"},"versions_seen":{},"pending_sends":[],"updated_channels":null}`)
	sendOne, _ := checkpoint.EncodePythonMessagePackValue(json.RawMessage(`{"node":"one"}`))
	sendTwo, _ := checkpoint.EncodePythonMessagePackValue(json.RawMessage(`{"node":"two"}`))
	mock.ExpectQuery(regexp.QuoteMeta(checkpointpostgres.PythonSelectCheckpointSQL)).
		WithArgs("thread", "ns", "child").
		WillReturnRows(sqlmock.NewRows([]string{"thread_id", "checkpoint_ns", "checkpoint_id", "parent_checkpoint_id", "checkpoint", "metadata"}).
			AddRow("thread", "ns", "child", "parent", checkpointJSON, []byte(`{}`)))
	mock.ExpectQuery(regexp.QuoteMeta(checkpointpostgres.PythonSelectBlobsSQL)).WithArgs("thread", "ns").
		WillReturnRows(sqlmock.NewRows([]string{"channel", "version", "type", "blob"}))
	mock.ExpectQuery(regexp.QuoteMeta(checkpointpostgres.PythonSelectSendsSQL)).
		WithArgs("thread", "ns", "parent", checkpoint.PythonTasksChannel).
		WillReturnRows(sqlmock.NewRows([]string{"type", "blob"}).AddRow("msgpack", sendOne).AddRow("msgpack", sendTwo))
	mock.ExpectQuery(regexp.QuoteMeta(checkpointpostgres.PythonSelectWritesSQL)).WithArgs("thread", "ns", "child").
		WillReturnRows(sqlmock.NewRows([]string{"task_id", "task_path", "idx", "channel", "type", "blob"}))

	tuple, ok, err := adapter.GetTuple(context.Background(), checkpoint.Config{ThreadID: "thread", Namespace: "ns", CheckpointID: "child"})
	if err != nil || !ok {
		t.Fatalf("GetTuple() ok=%v err=%v", ok, err)
	}
	assertJSON := string(tuple.Checkpoint.ChannelValues[checkpoint.PythonTasksChannel])
	if assertJSON != `[{"node":"one"},{"node":"two"}]` {
		t.Fatalf("pending sends = %s", assertJSON)
	}
	if got := tuple.Checkpoint.ChannelVersions[checkpoint.PythonTasksChannel].String(); got != "v9" {
		t.Fatalf("pending sends version = %q", got)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestPythonAdapterPutSplitsPrimitiveBlobAndTombstone(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	adapter, err := checkpointpostgres.NewPythonAdapter(db, nil)
	if err != nil {
		t.Fatal(err)
	}
	value, err := checkpoint.DecodePythonCheckpoint([]byte(`{"v":2,"id":"cp-2","ts":"2026-07-19T08:30:45Z","channel_values":{"text":"inline","object":{"nested":true}},"channel_versions":{"text":"v1","object":"v2","missing":"v3"},"versions_seen":{},"pending_sends":[],"updated_channels":["object","missing"]}`))
	if err != nil {
		t.Fatal(err)
	}
	mock.ExpectBegin()
	mock.ExpectExec(regexp.QuoteMeta(checkpointpostgres.PythonInsertBlobSQL)).
		WithArgs("thread", "ns", "missing", "v3", "empty", nil).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(regexp.QuoteMeta(checkpointpostgres.PythonInsertBlobSQL)).
		WithArgs("thread", "ns", "object", "v2", "msgpack", sqlmock.AnyArg()).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(regexp.QuoteMeta(checkpointpostgres.PythonUpsertCheckpointSQL)).
		WithArgs("thread", "ns", "cp-2", "cp-1", sqlmock.AnyArg(), sqlmock.AnyArg()).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()
	config, err := adapter.Put(context.Background(), checkpoint.Config{ThreadID: "thread", Namespace: "ns", CheckpointID: "cp-1"}, value,
		checkpoint.Metadata{"source": "loop"}, map[string]checkpoint.PythonChannelVersion{
			"object": value.ChannelVersions["object"], "missing": value.ChannelVersions["missing"],
		})
	if err != nil || config.CheckpointID != "cp-2" {
		t.Fatalf("Put() config=%#v err=%v", config, err)
	}

	writeBlob, _ := checkpoint.EncodePythonMessagePackValue(json.RawMessage(`{"ok":true}`))
	writes := []checkpoint.PythonPendingWrite{
		{TaskID: "task", TaskPath: "pull/node", Index: -3, Channel: "__interrupt__", Value: checkpoint.PythonTypedValue{Type: "msgpack", Data: writeBlob}},
		{TaskID: "task", TaskPath: "pull/node", Index: 0, Channel: "result", Value: checkpoint.PythonTypedValue{Type: "msgpack", Data: writeBlob}},
	}
	mock.ExpectBegin()
	mock.ExpectExec(regexp.QuoteMeta(checkpointpostgres.PythonUpsertWriteSQL)).
		WithArgs("thread", "ns", "cp-2", "task", "pull/node", -3, "__interrupt__", "msgpack", writeBlob).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(regexp.QuoteMeta(checkpointpostgres.PythonInsertWriteSQL)).
		WithArgs("thread", "ns", "cp-2", "task", "pull/node", 0, "result", "msgpack", writeBlob).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()
	if err := adapter.PutWrites(context.Background(), config, writes); err != nil {
		t.Fatal(err)
	}
	mock.ExpectBegin()
	mock.ExpectExec(regexp.QuoteMeta(`DELETE FROM checkpoint_writes WHERE thread_id = $1`)).WithArgs("thread").WillReturnResult(sqlmock.NewResult(0, 2))
	mock.ExpectExec(regexp.QuoteMeta(`DELETE FROM checkpoints WHERE thread_id = $1`)).WithArgs("thread").WillReturnResult(sqlmock.NewResult(0, 2))
	mock.ExpectExec(regexp.QuoteMeta(`DELETE FROM checkpoint_blobs WHERE thread_id = $1`)).WithArgs("thread").WillReturnResult(sqlmock.NewResult(0, 2))
	mock.ExpectCommit()
	if err := adapter.DeleteThread(context.Background(), "thread"); err != nil {
		t.Fatal(err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestPythonAdapterListAppliesMetadataFilterBeforeLimit(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	adapter, err := checkpointpostgres.NewPythonAdapter(db, nil)
	if err != nil {
		t.Fatal(err)
	}
	query := `SELECT thread_id, checkpoint_ns, checkpoint_id, metadata FROM checkpoints WHERE thread_id = $1 AND checkpoint_ns = $2 AND checkpoint_id < $3 ORDER BY checkpoint_id DESC, thread_id ASC, checkpoint_ns ASC`
	mock.ExpectQuery(regexp.QuoteMeta(query)).WithArgs("thread", "ns", "cp-9").
		WillReturnRows(sqlmock.NewRows([]string{"thread_id", "checkpoint_ns", "checkpoint_id", "metadata"}).
			AddRow("thread", "ns", "cp-8", []byte(`{"source":"input"}`)).
			AddRow("thread", "ns", "cp-7", []byte(`{"source":"loop"}`)))
	checkpointJSON := []byte(`{"v":2,"id":"cp-7","ts":"2026-07-19T08:30:45Z","channel_values":{},"channel_versions":{},"versions_seen":{},"pending_sends":[],"updated_channels":null}`)
	mock.ExpectQuery(regexp.QuoteMeta(checkpointpostgres.PythonSelectCheckpointSQL)).WithArgs("thread", "ns", "cp-7").
		WillReturnRows(sqlmock.NewRows([]string{"thread_id", "checkpoint_ns", "checkpoint_id", "parent_checkpoint_id", "checkpoint", "metadata"}).
			AddRow("thread", "ns", "cp-7", nil, checkpointJSON, []byte(`{"source":"loop"}`)))
	mock.ExpectQuery(regexp.QuoteMeta(checkpointpostgres.PythonSelectBlobsSQL)).WithArgs("thread", "ns").
		WillReturnRows(sqlmock.NewRows([]string{"channel", "version", "type", "blob"}))
	mock.ExpectQuery(regexp.QuoteMeta(checkpointpostgres.PythonSelectWritesSQL)).WithArgs("thread", "ns", "cp-7").
		WillReturnRows(sqlmock.NewRows([]string{"task_id", "task_path", "idx", "channel", "type", "blob"}))
	before := checkpoint.Config{CheckpointID: "cp-9"}
	listed, err := adapter.List(context.Background(), checkpoint.ListOptions{
		Config: &checkpoint.Config{ThreadID: "thread", Namespace: "ns"}, Before: &before,
		Filter: checkpoint.Metadata{"source": "loop"}, Limit: 1,
	})
	if err != nil || len(listed) != 1 || listed[0].Config.CheckpointID != "cp-7" {
		t.Fatalf("List() = %#v, %v", listed, err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}
