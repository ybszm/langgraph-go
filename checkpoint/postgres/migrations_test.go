package postgres

import (
	"context"
	"regexp"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
)

func TestSchemaMigrationsAreContiguousAndAddTaskPath(t *testing.T) {
	t.Parallel()

	if len(postgresSchemaMigrations) != schemaVersion {
		t.Fatalf("migration count = %d, schema version = %d", len(postgresSchemaMigrations), schemaVersion)
	}
	for i, migration := range postgresSchemaMigrations {
		if migration.version != i+1 {
			t.Fatalf("migration[%d].version = %d", i, migration.version)
		}
		if len(migration.statements) == 0 {
			t.Fatalf("migration v%d has no statements", migration.version)
		}
	}
	joined := strings.Join(postgresSchemaMigrations[1].statements, "\n")
	if !strings.Contains(joined, "ADD COLUMN IF NOT EXISTS task_path") {
		t.Fatalf("v2 migration does not add task_path: %s", joined)
	}
}

func TestPythonAdapterSetupAppliesEveryMigrationInOneTransaction(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	adapter, err := NewPythonAdapter(db, nil)
	if err != nil {
		t.Fatal(err)
	}
	mock.ExpectBegin()
	mock.ExpectExec(regexp.QuoteMeta(`SELECT pg_advisory_xact_lock(hashtext('langgraph-python-checkpoint-schema'))`)).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(regexp.QuoteMeta(pythonPostgresMigrations[0])).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT max(v) FROM checkpoint_migrations`)).WillReturnRows(sqlmock.NewRows([]string{"max"}).AddRow(nil))
	for version, statement := range pythonPostgresMigrations {
		mock.ExpectExec(regexp.QuoteMeta(statement)).WillReturnResult(sqlmock.NewResult(0, 1))
		mock.ExpectExec(regexp.QuoteMeta(`INSERT INTO checkpoint_migrations(v) VALUES ($1) ON CONFLICT DO NOTHING`)).WithArgs(version).WillReturnResult(sqlmock.NewResult(0, 1))
	}
	mock.ExpectCommit()
	if err := adapter.Setup(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestPythonSchemaMigrationsMatchUpstream12_9(t *testing.T) {
	t.Parallel()

	if len(pythonPostgresMigrations) != 10 {
		t.Fatalf("Python migration count = %d, want 10", len(pythonPostgresMigrations))
	}
	if !strings.Contains(pythonPostgresMigrations[0], "checkpoint_migrations (v") {
		t.Fatalf("migration zero = %s", pythonPostgresMigrations[0])
	}
	if !strings.Contains(pythonPostgresMigrations[9], "task_path") {
		t.Fatalf("migration nine = %s", pythonPostgresMigrations[9])
	}
}

func TestDecodePythonPostgresCheckpointToleratesMissingChannelValues(t *testing.T) {
	t.Parallel()

	value, err := decodePythonPostgresCheckpoint([]byte(`{"v":2,"id":"cp","ts":"2026-07-19T08:30:45Z","channel_versions":{},"versions_seen":{},"pending_sends":[],"updated_channels":null}`))
	if err != nil {
		t.Fatal(err)
	}
	if value.ChannelValues == nil || len(value.ChannelValues) != 0 {
		t.Fatalf("channel values = %#v", value.ChannelValues)
	}
}
