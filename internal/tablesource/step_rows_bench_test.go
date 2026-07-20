//go:build cgo

package tablesource

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"
)

// goivm_col is defined in step_rows_shim.go (cgo build)

// step_rows_bench_test.go — A/B microbench: database/sql Rows.Next path
// vs the goivm_step_rows C shim, on the same query and same data.
//
// The query mimics the snapshotter's GetRow path: SELECT * FROM <table>
// WHERE pk=? — a point lookup that returns a single row with ~20 columns.
// We run it in a tight loop to measure the per-call CGO overhead difference.
//
// Run: go test -bench=StepRows -benchmem -count=5 -timeout=120s -tags libsqlite3

// setupStepRowsBench creates a test DB with a 20-column table and 1000 rows.
func setupStepRowsBench(t testing.TB) (*sql.DB, string) {
	// Use file-based DB to ensure all conns see the same data
	dbPath := fmt.Sprintf("/tmp/step_rows_bench_%d.db", time.Now().UnixNano())
	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		t.Fatal(err)
	}
	// Create a table with ~20 columns (mimics the users table shape)
	cols := "id INTEGER PRIMARY KEY"
	for i := 1; i <= 18; i++ {
		cols += fmt.Sprintf(", col%d TEXT", i)
	}
	cols += ", created_at INTEGER"
	if _, err := db.Exec(fmt.Sprintf("CREATE TABLE bench_users (%s)", cols)); err != nil {
		t.Fatal(err)
	}
	// Insert 1000 rows
	for i := 0; i < 1000; i++ {
		args := []any{i}
		for j := 1; j <= 18; j++ {
			args = append(args, fmt.Sprintf("value-%d-%d", i, j))
		}
		args = append(args, i*1000)
		placeholders := "?"
		for j := 0; j < 18; j++ {
			placeholders += ",?"
		}
		placeholders += ",?"
		if _, err := db.Exec(fmt.Sprintf("INSERT INTO bench_users VALUES (%s)", placeholders), args...); err != nil {
			t.Fatal(err)
		}
	}
	return db, "bench_users"
}

// BenchmarkStepRows_DatabaseSQL measures the current path:
// *sql.Conn → *sql.Rows → Next + Scan per row.
func BenchmarkStepRows_DatabaseSQL(b *testing.B) {
	db, table := setupStepRowsBench(b)
	defer db.Close()

	conn, err := db.Conn(context.Background())
	if err != nil {
		b.Fatal(err)
	}
	defer conn.Close()

	query := fmt.Sprintf("SELECT * FROM %s WHERE id=?", table)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		rows, err := conn.QueryContext(context.Background(), query, i%1000)
		if err != nil {
			b.Fatal(err)
		}
		for rows.Next() {
			cols, _ := rows.Columns()
			vals := make([]any, len(cols))
			ptrs := make([]any, len(cols))
			for j := range vals {
				ptrs[j] = &vals[j]
			}
			rows.Scan(ptrs...)
		}
		rows.Close()
	}
}

// BenchmarkStepRows_Shim measures the C shim path:
// goivm_step_rows batches step+extract in 1 CGO crossing.
func BenchmarkStepRows_Shim(b *testing.B) {
	db, table := setupStepRowsBench(b)
	defer db.Close()

	conn, err := db.Conn(context.Background())
	if err != nil {
		b.Fatal(err)
	}
	defer conn.Close()

	query := fmt.Sprintf("SELECT * FROM %s WHERE id=?", table)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		var got int
		err := stepRowsShim(conn, query, []any{i % 1000}, func(colNames []string, rowVals []any) bool {
			got++
			return true
		})
		if err != nil {
			b.Fatal(err)
		}
		if got != 1 {
			b.Fatalf("expected 1 row, got %d", got)
		}
	}
}

// BenchmarkStepRows_MultiRow_DatabaseSQL measures multi-row scan via database/sql
// (mimics GetRows which can return multiple rows on unique-key conflicts).
func BenchmarkStepRows_MultiRow_DatabaseSQL(b *testing.B) {
	db, _ := setupStepRowsBench(b)
	defer db.Close()

	conn, err := db.Conn(context.Background())
	if err != nil {
		b.Fatal(err)
	}
	defer conn.Close()

	query := `SELECT * FROM bench_users ORDER BY id`
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		rows, err := conn.QueryContext(context.Background(), query)
		if err != nil {
			b.Fatal(err)
		}
		count := 0
		for rows.Next() {
			cols, _ := rows.Columns()
			vals := make([]any, len(cols))
			ptrs := make([]any, len(cols))
			for j := range vals {
				ptrs[j] = &vals[j]
			}
			rows.Scan(ptrs...)
			count++
		}
		rows.Close()
		if count != 1000 {
			b.Fatalf("expected 1000 rows, got %d", count)
		}
	}
}

// BenchmarkStepRows_MultiRow_Shim measures multi-row scan via the C shim.
func BenchmarkStepRows_MultiRow_Shim(b *testing.B) {
	db, _ := setupStepRowsBench(b)
	defer db.Close()

	conn, err := db.Conn(context.Background())
	if err != nil {
		b.Fatal(err)
	}
	defer conn.Close()

	query := `SELECT * FROM bench_users ORDER BY id`
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		count := 0
		err := stepRowsShim(conn, query, nil, func(colNames []string, rowVals []any) bool {
			count++
			return true
		})
		if err != nil {
			b.Fatal(err)
		}
		if count != 1000 {
			b.Fatalf("expected 1000 rows, got %d", count)
		}
	}
}
