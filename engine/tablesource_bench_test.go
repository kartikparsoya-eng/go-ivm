package engine

// Benchmarks for the REAL Go-primary read path: a tablesource.Source backed by
// a SQLite replica (the snapshotter-era leaf), driven through the engine exactly
// as production does (RegisterSource → AddQueries hydrate → Advance push). The
// existing testharness/bench_test.go benchmarks use the in-memory MemorySource,
// which does NOT reflect the fetchForConn SQL read path that the 2026-06-04
// reprofile identified as 59% of allocations. A MemorySource variant on the
// SAME data + query is included so the SQL-leaf overhead is directly visible.

import (
	"database/sql"
	"fmt"
	"path/filepath"
	"sync"
	"testing"

	"github.com/kartikparsoya-eng/go-ivm/builder"
	"github.com/kartikparsoya-eng/go-ivm/internal/tablesource"
	"github.com/kartikparsoya-eng/go-ivm/ivm"
	"github.com/kartikparsoya-eng/go-ivm/sqlite"

	_ "github.com/mattn/go-sqlite3"
)

// seedBenchReplica builds a WAL-mode replica with `rows` rows of a 3-column
// table (id INTEGER PK, name TEXT, val INTEGER).
func seedBenchReplica(tb testing.TB, rows int) string {
	tb.Helper()
	path := filepath.Join(tb.TempDir(), "replica.sqlite")
	w, err := sql.Open("sqlite3", path)
	if err != nil {
		tb.Fatalf("seed open: %v", err)
	}
	defer w.Close()
	for _, stmt := range []string{
		"PRAGMA journal_mode=WAL",
		"PRAGMA synchronous=NORMAL",
		"CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT, val INTEGER)",
	} {
		if _, err := w.Exec(stmt); err != nil {
			tb.Fatalf("seed exec %q: %v", stmt, err)
		}
	}
	tx, _ := w.Begin()
	stmt, _ := tx.Prepare("INSERT INTO t (id, name, val) VALUES (?, ?, ?)")
	for i := 1; i <= rows; i++ {
		if _, err := stmt.Exec(i, fmt.Sprintf("row-%d", i), i%500); err != nil {
			tb.Fatalf("seed insert %d: %v", i, err)
		}
	}
	stmt.Close()
	if err := tx.Commit(); err != nil {
		tb.Fatalf("seed commit: %v", err)
	}
	return path
}

func benchColumns() map[string]sqlite.ColumnSchema {
	return map[string]sqlite.ColumnSchema{
		"id":   {Type: "number"},
		"name": {Type: "string"},
		"val":  {Type: "number"},
	}
}

// benchAST: a representative dashboard-shaped query — filter + orderBy + limit.
func benchAST() builder.AST {
	limit := 50
	return builder.AST{
		Table: "t",
		Where: &builder.Condition{
			Type:  "simple",
			Op:    ">",
			Left:  &builder.ValuePos{Type: "column", Name: "val"},
			Right: &builder.ValuePos{Type: "literal", Value: float64(100)},
		},
		OrderBy: ivm.Ordering{{"id", "asc"}},
		Limit:   &limit,
	}
}

func newTableSourceEngine(tb testing.TB, rows int) *Engine {
	tb.Helper()
	path := seedBenchReplica(tb, rows)
	db, err := tablesource.Open(path, tablesource.OpenOptions{})
	if err != nil {
		tb.Fatalf("Open: %v", err)
	}
	wdb, err := tablesource.OpenWritable(path, tablesource.OpenOptions{})
	if err != nil {
		tb.Fatalf("OpenWritable: %v", err)
	}
	eng, err := NewEngine(EngineConfig{StoragePath: filepath.Join(tb.TempDir(), "storage.db")})
	if err != nil {
		tb.Fatalf("NewEngine: %v", err)
	}
	tb.Cleanup(func() { eng.Close(); db.Close(); wdb.Close() })
	src, err := tablesource.New(db, wdb, "t", benchColumns(), []string{"id"})
	if err != nil {
		tb.Fatalf("tablesource.New: %v", err)
	}
	eng.RegisterSource(src)
	return eng
}

func newMemSourceEngine(tb testing.TB, rows int) *Engine {
	tb.Helper()
	eng, err := NewEngine(EngineConfig{StoragePath: filepath.Join(tb.TempDir(), "storage.db")})
	if err != nil {
		tb.Fatalf("NewEngine: %v", err)
	}
	tb.Cleanup(func() { eng.Close() })
	ms := ivm.NewMemorySourceWithConverter("t",
		map[string]string{"id": "number", "name": "string", "val": "number"},
		[]string{"id"},
		func(v interface{}, colType string) ivm.Value { return sqlite.FromSQLiteType(v, colType) },
	)
	seed := make([]ivm.Row, rows)
	for i := 1; i <= rows; i++ {
		seed[i-1] = ivm.Row{"id": float64(i), "name": fmt.Sprintf("row-%d", i), "val": float64(i % 500)}
	}
	ms.BulkInsert(seed)
	eng.RegisterMemorySource(ms)
	return eng
}

// --- Hydrate: the fetchForConn hot path (re-add the query each iteration so
//     every op pays a full hydrate read of the leaf). ---

func benchHydrate(b *testing.B, eng *Engine) {
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err := eng.AddQueries([]QuerySpec{{QueryID: "q", AST: benchAST()}}); err != nil {
			b.Fatalf("AddQueries: %v", err)
		}
	}
}

func BenchmarkTableSourceHydrate_1k(b *testing.B)  { benchHydrate(b, newTableSourceEngine(b, 1000)) }
func BenchmarkTableSourceHydrate_10k(b *testing.B) { benchHydrate(b, newTableSourceEngine(b, 10000)) }
func BenchmarkMemSourceHydrate_1k(b *testing.B)    { benchHydrate(b, newMemSourceEngine(b, 1000)) }
func BenchmarkMemSourceHydrate_10k(b *testing.B)   { benchHydrate(b, newMemSourceEngine(b, 10000)) }

// --- Advance: push one ADD per op (new unique id) through the registered query.
//     Exercises genPushAndWrite → writeChange (SQLite) + pipeline fanout. ---

func benchAdvance(b *testing.B, eng *Engine, startID int) {
	if _, err := eng.AddQueries([]QuerySpec{{QueryID: "q", AST: benchAST()}}); err != nil {
		b.Fatalf("AddQueries: %v", err)
	}
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		id := startID + i + 1
		eng.Advance([]SnapshotChange{{
			Table:     "t",
			NextValue: ivm.Row{"id": float64(id), "name": "x", "val": float64(200)},
		}})
	}
}

func BenchmarkTableSourceAdvance_1k(b *testing.B) {
	benchAdvance(b, newTableSourceEngine(b, 1000), 1000)
}
func BenchmarkMemSourceAdvance_1k(b *testing.B) { benchAdvance(b, newMemSourceEngine(b, 1000), 1000) }

// --- Cross-engine (cross-CG) parallelism on the REAL tablesource path. N
//     independent engines (each its own SQLite replica) hydrate sequentially
//     (the TS single-event-loop model) vs concurrently (Go's win). This is the
//     allocation/SQLite-cgo-bound path, NOT the in-memory microbench — so it
//     shows whether the parallel scaling survives GC + cgo contention. ---

func benchTableSourceMultiEngine(b *testing.B, n int, parallel bool) {
	engines := make([]*Engine, n)
	for i := 0; i < n; i++ {
		engines[i] = newTableSourceEngine(b, 1000)
	}
	b.ResetTimer()
	for iter := 0; iter < b.N; iter++ {
		if parallel {
			var wg sync.WaitGroup
			for i := 0; i < n; i++ {
				wg.Add(1)
				go func(e *Engine) {
					defer wg.Done()
					_, _ = e.AddQueries([]QuerySpec{{QueryID: "q", AST: benchAST()}})
				}(engines[i])
			}
			wg.Wait()
		} else {
			for i := 0; i < n; i++ {
				_, _ = engines[i].AddQueries([]QuerySpec{{QueryID: "q", AST: benchAST()}})
			}
		}
	}
}

func BenchmarkTableSourceMulti_1_Sequential(b *testing.B)  { benchTableSourceMultiEngine(b, 1, false) }
func BenchmarkTableSourceMulti_4_Sequential(b *testing.B)  { benchTableSourceMultiEngine(b, 4, false) }
func BenchmarkTableSourceMulti_8_Sequential(b *testing.B)  { benchTableSourceMultiEngine(b, 8, false) }
func BenchmarkTableSourceMulti_16_Sequential(b *testing.B) { benchTableSourceMultiEngine(b, 16, false) }
func BenchmarkTableSourceMulti_1_Parallel(b *testing.B)    { benchTableSourceMultiEngine(b, 1, true) }
func BenchmarkTableSourceMulti_4_Parallel(b *testing.B)    { benchTableSourceMultiEngine(b, 4, true) }
func BenchmarkTableSourceMulti_8_Parallel(b *testing.B)    { benchTableSourceMultiEngine(b, 8, true) }
func BenchmarkTableSourceMulti_16_Parallel(b *testing.B)   { benchTableSourceMultiEngine(b, 16, true) }

// --- Within-CG parallel hydration (a SEPARATE axis from cross-CG): one engine,
//     M queries. Parallel = AddQueries([M]) fans out M goroutines (engine.go).
//     Serial = M single-query AddQueries calls. Shows whether the per-query
//     fan-out helps when a single CG hydrates many queries. ---

func benchASTn(i int) builder.AST {
	limit := 50
	return builder.AST{
		Table: "t",
		Where: &builder.Condition{
			Type: "simple", Op: ">",
			Left:  &builder.ValuePos{Type: "column", Name: "val"},
			Right: &builder.ValuePos{Type: "literal", Value: float64(50 + i)},
		},
		OrderBy: ivm.Ordering{{"id", "asc"}},
		Limit:   &limit,
	}
}

func benchWithinCG(b *testing.B, m int, parallel bool) {
	eng := newTableSourceEngine(b, 1000)
	b.ResetTimer()
	for it := 0; it < b.N; it++ {
		if parallel {
			qs := make([]QuerySpec, m)
			for i := 0; i < m; i++ {
				qs[i] = QuerySpec{QueryID: fmt.Sprintf("q%d", i), AST: benchASTn(i)}
			}
			_, _ = eng.AddQueries(qs)
		} else {
			for i := 0; i < m; i++ {
				_, _ = eng.AddQueries([]QuerySpec{{QueryID: fmt.Sprintf("q%d", i), AST: benchASTn(i)}})
			}
		}
	}
}

func BenchmarkWithinCGHydrate_8_Serial(b *testing.B)   { benchWithinCG(b, 8, false) }
func BenchmarkWithinCGHydrate_8_Parallel(b *testing.B) { benchWithinCG(b, 8, true) }

// --- Within-CG hydration across DISTINCT tables: 8 queries, each on its own
//     tablesource (own s.mu, own pinned conn from the SHARED pool). If s.mu is
//     the same-table serializer, THIS should parallelize. ---

func seedMultiTableReplica(tb testing.TB, nTables, rows int) string {
	tb.Helper()
	path := filepath.Join(tb.TempDir(), "replica.sqlite")
	w, _ := sql.Open("sqlite3", path)
	defer w.Close()
	w.Exec("PRAGMA journal_mode=WAL")
	w.Exec("PRAGMA synchronous=NORMAL")
	for t := 0; t < nTables; t++ {
		w.Exec(fmt.Sprintf("CREATE TABLE t%d (id INTEGER PRIMARY KEY, name TEXT, val INTEGER)", t))
		tx, _ := w.Begin()
		st, _ := tx.Prepare(fmt.Sprintf("INSERT INTO t%d (id,name,val) VALUES (?,?,?)", t))
		for i := 1; i <= rows; i++ {
			st.Exec(i, fmt.Sprintf("r%d", i), i%500)
		}
		st.Close()
		tx.Commit()
	}
	return path
}

func benchMultiTable(b *testing.B, nTables int, parallel bool) {
	path := seedMultiTableReplica(b, nTables, 1000)
	db, _ := tablesource.Open(path, tablesource.OpenOptions{})
	wdb, _ := tablesource.OpenWritable(path, tablesource.OpenOptions{})
	eng, _ := NewEngine(EngineConfig{StoragePath: filepath.Join(b.TempDir(), "s.db")})
	b.Cleanup(func() { eng.Close(); db.Close(); wdb.Close() })
	for t := 0; t < nTables; t++ {
		src, err := tablesource.New(db, wdb, fmt.Sprintf("t%d", t), benchColumns(), []string{"id"})
		if err != nil {
			b.Fatalf("New t%d: %v", t, err)
		}
		eng.RegisterSource(src)
	}
	mkAST := func(t int) builder.AST {
		limit := 50
		return builder.AST{
			Table: fmt.Sprintf("t%d", t),
			Where: &builder.Condition{Type: "simple", Op: ">",
				Left: &builder.ValuePos{Type: "column", Name: "val"}, Right: &builder.ValuePos{Type: "literal", Value: float64(100)}},
			OrderBy: ivm.Ordering{{"id", "asc"}}, Limit: &limit,
		}
	}
	b.ResetTimer()
	for it := 0; it < b.N; it++ {
		if parallel {
			qs := make([]QuerySpec, nTables)
			for t := 0; t < nTables; t++ {
				qs[t] = QuerySpec{QueryID: fmt.Sprintf("q%d", t), AST: mkAST(t)}
			}
			_, _ = eng.AddQueries(qs)
		} else {
			for t := 0; t < nTables; t++ {
				_, _ = eng.AddQueries([]QuerySpec{{QueryID: fmt.Sprintf("q%d", t), AST: mkAST(t)}})
			}
		}
	}
}

func BenchmarkMultiTableHydrate_8_Serial(b *testing.B)   { benchMultiTable(b, 8, false) }
func BenchmarkMultiTableHydrate_8_Parallel(b *testing.B) { benchMultiTable(b, 8, true) }
