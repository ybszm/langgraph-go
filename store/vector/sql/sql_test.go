package sql_test

import (
	"context"
	stdsql "database/sql"
	"path/filepath"
	"testing"

	"github.com/wahanbo/langgraph-go/store"
	vectorsql "github.com/wahanbo/langgraph-go/store/vector/sql"
	"github.com/wahanbo/langgraph-go/store/vectortest"
	_ "modernc.org/sqlite"
)

func TestSQLiteVectorIndexContract(t *testing.T) {
	vectortest.Run(t, func(t *testing.T, dimensions int, metric store.VectorMetric) store.VectorIndex {
		db, err := stdsql.Open("sqlite", filepath.Join(t.TempDir(), "vectors.db")+"?_pragma=busy_timeout(5000)")
		if err != nil {
			t.Fatal(err)
		}
		db.SetMaxOpenConns(8)
		t.Cleanup(func() { _ = db.Close() })
		index, err := vectorsql.New(db, vectorsql.SQLite, dimensions, metric)
		if err != nil {
			t.Fatal(err)
		}
		if err := index.Setup(context.Background()); err != nil {
			t.Fatal(err)
		}
		return index
	})
}

func TestSQLiteVectorIndexPersistsAcrossHandles(t *testing.T) {
	path := filepath.Join(t.TempDir(), "persistent-vectors.db")
	open := func() (*stdsql.DB, *vectorsql.Index) {
		db, err := stdsql.Open("sqlite", path+"?_pragma=busy_timeout(5000)")
		if err != nil {
			t.Fatal(err)
		}
		index, err := vectorsql.New(db, vectorsql.SQLite, 2, store.VectorCosine)
		if err != nil {
			t.Fatal(err)
		}
		if err := index.Setup(context.Background()); err != nil {
			t.Fatal(err)
		}
		return db, index
	}
	firstDB, first := open()
	if err := first.Upsert(context.Background(), []store.VectorDocument{{
		Namespace: store.Namespace{"docs"}, Key: "one", Field: "text", Vector: []float32{1, 0},
	}}); err != nil {
		t.Fatal(err)
	}
	_ = firstDB.Close()
	secondDB, second := open()
	defer secondDB.Close()
	matches, err := second.Search(context.Background(), store.VectorQuery{
		NamespacePrefix: store.Namespace{"docs"}, Vector: []float32{1, 0}, Limit: 1,
	})
	if err != nil || len(matches) != 1 || matches[0].Key != "one" {
		t.Fatalf("matches=%+v err=%v", matches, err)
	}
}
