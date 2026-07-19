package sql_test

import (
	"context"
	stdsql "database/sql"
	"os"
	"testing"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/ybszm/langgraph-go/store"
	vectorsql "github.com/ybszm/langgraph-go/store/vector/sql"
	"github.com/ybszm/langgraph-go/store/vectortest"
)

func TestPostgresVectorIndexContract(t *testing.T) {
	dsn := os.Getenv("LANGGRAPH_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("set LANGGRAPH_POSTGRES_DSN to run PostgreSQL vector integration")
	}
	db, err := stdsql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := db.PingContext(context.Background()); err != nil {
		t.Fatal(err)
	}
	vectortest.Run(t, func(t *testing.T, dimensions int, metric store.VectorMetric) store.VectorIndex {
		index, err := vectorsql.New(db, vectorsql.Postgres, dimensions, metric)
		if err != nil {
			t.Fatal(err)
		}
		if err := index.Setup(context.Background()); err != nil {
			t.Fatal(err)
		}
		if _, err := db.ExecContext(context.Background(), "TRUNCATE TABLE langgraph_vector_index"); err != nil {
			t.Fatal(err)
		}
		return index
	})
}
