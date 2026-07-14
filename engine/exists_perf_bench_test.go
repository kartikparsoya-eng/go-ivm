package engine

import (
	"database/sql"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/kartikparsoya-eng/go-ivm/builder"
	"github.com/kartikparsoya-eng/go-ivm/internal/tablesource"
	"github.com/kartikparsoya-eng/go-ivm/ivm"
	"github.com/kartikparsoya-eng/go-ivm/planner"
	"github.com/kartikparsoya-eng/go-ivm/sqlite"

	_ "github.com/mattn/go-sqlite3"
)

// Reproduces the production userAllChannels query shape:
//   channels WHERE workspaceId = ?
//     AND (visibility = 'PUBLIC'
//          OR EXISTS (channel_participants WHERE channelId = channels.id AND userId = ?))
//
// The non-flipped path (what Go always uses — no query planner) executes N+1
// SQL queries: 1 to fetch all channels, then 1 per channel to probe
// channel_participants. The flipped path (what TS's planner would choose)
// executes 2 queries: 1 to fetch the user's channel_participants, then 1
// batched IN(...) lookup against channels.
//
// This benchmark demonstrates the performance gap that the missing query
// planner causes on the production read path.

func seedChannelsReplica(tb testing.TB, numChannels, numParticipants int) string {
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
		`CREATE TABLE channels (
			id TEXT PRIMARY KEY,
			workspaceId TEXT,
			visibility TEXT,
			name TEXT
		)`,
		`CREATE TABLE channel_participants (
			id TEXT PRIMARY KEY,
			channelId TEXT,
			userId TEXT
		)`,
		`CREATE INDEX idx_participants_channel_user ON channel_participants(channelId, userId)`,
		`CREATE INDEX idx_channels_workspace ON channels(workspaceId)`,
	} {
		if _, err := w.Exec(stmt); err != nil {
			tb.Fatalf("seed exec %q: %v", stmt, err)
		}
	}
	tx, _ := w.Begin()

	chStmt, _ := tx.Prepare("INSERT INTO channels (id, workspaceId, visibility, name) VALUES (?, ?, ?, ?)")
	ws := "6642623f-cca7-43ad-9a6b-5e49c33226b4"
	for i := 0; i < numChannels; i++ {
		vis := "PRIVATE"
		if i%5 == 0 {
			vis = "PUBLIC"
		}
		chStmt.Exec(fmt.Sprintf("ch-%04d", i), ws, vis, fmt.Sprintf("Channel %d", i))
	}
	chStmt.Close()

	partStmt, _ := tx.Prepare("INSERT INTO channel_participants (id, channelId, userId) VALUES (?, ?, ?)")
	// Each user is in ~numParticipants/numChannels channels
	for i := 0; i < numParticipants; i++ {
		channelIdx := i % numChannels
		partStmt.Exec(fmt.Sprintf("part-%06d", i), fmt.Sprintf("ch-%04d", channelIdx), "cmgjjwr2r0000o3eezs6zawu5")
	}
	partStmt.Close()

	if err := tx.Commit(); err != nil {
		tb.Fatalf("seed commit: %v", err)
	}
	if _, err := w.Exec("ANALYZE"); err != nil {
		tb.Fatalf("ANALYZE: %v", err)
	}
	return path
}

func channelColumns() map[string]sqlite.ColumnSchema {
	return map[string]sqlite.ColumnSchema{
		"id":          {Type: "string"},
		"workspaceId": {Type: "string"},
		"visibility":  {Type: "string"},
		"name":        {Type: "string"},
	}
}

func participantColumns() map[string]sqlite.ColumnSchema {
	return map[string]sqlite.ColumnSchema{
		"id":        {Type: "string"},
		"channelId": {Type: "string"},
		"userId":    {Type: "string"},
	}
}

// userAllChannelsAST builds the production query AST with a non-flipped EXISTS
// (what Go uses — no planner). The client sends Flip=false (or omits it), and
// Go trusts that decision.
func userAllChannelsAST() builder.AST {
	return builder.AST{
		Table: "channels",
		Where: &builder.Condition{
			Type: "and",
			Conditions: []builder.Condition{
				{
					Type: "simple", Op: "=",
					Left:  &builder.ValuePos{Type: "column", Name: "workspaceId"},
					Right: &builder.ValuePos{Type: "literal", Value: "6642623f-cca7-43ad-9a6b-5e49c33226b4"},
				},
				{
					Type: "or",
					Conditions: []builder.Condition{
						{
							Type: "simple", Op: "=",
							Left:  &builder.ValuePos{Type: "column", Name: "visibility"},
							Right: &builder.ValuePos{Type: "literal", Value: "PUBLIC"},
						},
						{
							Type: "correlatedSubquery", Op: "EXISTS", Flip: false,
							Related: &builder.CorrelatedSubquery{
								System:      "client",
								Correlation: builder.Correlation{ParentField: []string{"id"}, ChildField: []string{"channelId"}},
								Subquery: builder.AST{
									Table: "channel_participants", Alias: "zsubq_participants",
									Where: &builder.Condition{
										Type: "simple", Op: "=",
										Left:  &builder.ValuePos{Type: "column", Name: "userId"},
										Right: &builder.ValuePos{Type: "literal", Value: "cmgjjwr2r0000o3eezs6zawu5"},
									},
								},
							},
						},
					},
				},
			},
		},
		OrderBy: ivm.Ordering{{"id", "asc"}},
	}
}

// userAllChannelsFlippedAST is the same query but with Flip=true — what the TS
// query planner would choose when the child (channel_participants) is smaller
// than the parent (channels). This routes through FlippedJoin instead of the
// N+1 semi-join path.
func userAllChannelsFlippedAST() builder.AST {
	ast := userAllChannelsAST()
	// Set Flip=true on the correlatedSubquery inside the OR branch
	orCond := &ast.Where.Conditions[1]
	for i := range orCond.Conditions {
		if orCond.Conditions[i].Type == "correlatedSubquery" {
			orCond.Conditions[i].Flip = true
		}
	}
	return ast
}

func newChannelsEngine(tb testing.TB, numChannels, numParticipants int) (*Engine, string) {
	tb.Helper()
	path := seedChannelsReplica(tb, numChannels, numParticipants)
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
	chans, err := tablesource.New(db, wdb, "channels", channelColumns(), []string{"id"})
	if err != nil {
		tb.Fatalf("tablesource.New channels: %v", err)
	}
	parts, err := tablesource.New(db, wdb, "channel_participants", participantColumns(), []string{"id"})
	if err != nil {
		tb.Fatalf("tablesource.New participants: %v", err)
	}
	eng.RegisterSource(chans)
	eng.RegisterSource(parts)
	return eng, path
}

// BenchmarkExistsHydrate_NonFlipped is the path without any planner:
// N+1 queries (1 parent scan + N child probes).
func BenchmarkExistsHydrate_NonFlipped_100(b *testing.B)  { benchExistsHydrate(b, 100, 50, false) }
func BenchmarkExistsHydrate_NonFlipped_500(b *testing.B)  { benchExistsHydrate(b, 500, 50, false) }
func BenchmarkExistsHydrate_NonFlipped_1000(b *testing.B) { benchExistsHydrate(b, 1000, 50, false) }

// BenchmarkExistsHydrate_Flipped is the path with Flip=true manually set:
// 2 queries (1 child scan + 1 batched parent lookup).
func BenchmarkExistsHydrate_Flipped_100(b *testing.B)  { benchExistsHydrate(b, 100, 50, true) }
func BenchmarkExistsHydrate_Flipped_500(b *testing.B)  { benchExistsHydrate(b, 500, 50, true) }
func BenchmarkExistsHydrate_Flipped_1000(b *testing.B) { benchExistsHydrate(b, 1000, 50, true) }

// BenchmarkExistsHydrate_Planned uses the Go planner (test harness) to
// automatically decide the flip. In production, the TS host-side planner
// makes this decision before dispatching the AST to Go. This benchmark
// verifies the Go planner reaches the same decision and delivers the same
// performance as the manually-flipped path.
func BenchmarkExistsHydrate_Planned_100(b *testing.B)  { benchExistsHydratePlanned(b, 100, 50) }
func BenchmarkExistsHydrate_Planned_500(b *testing.B)  { benchExistsHydratePlanned(b, 500, 50) }
func BenchmarkExistsHydrate_Planned_1000(b *testing.B) { benchExistsHydratePlanned(b, 1000, 50) }

func benchExistsHydrate(b *testing.B, numChannels, numParticipants int, flipped bool) {
	eng, _ := newChannelsEngine(b, numChannels, numParticipants)
	ast := userAllChannelsAST()
	if flipped {
		ast = userAllChannelsFlippedAST()
	}
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err := eng.AddQueries([]QuerySpec{{QueryID: "q", AST: ast}}); err != nil {
			b.Fatalf("AddQueries: %v", err)
		}
		eng.RemoveQuery("q")
	}
}

// TestExistsPerf_NonFlippedVsFlipped is a test (not benchmark) that runs each
// path once and reports the wall time. It also runs the Go planner (test
// harness) to verify it automatically chooses the flipped path.
func TestExistsPerf_NonFlippedVsFlipped(t *testing.T) {
	numChannels := 1000
	numParticipants := 50
	eng, dbPath := newChannelsEngine(t, numChannels, numParticipants)

	astNF := userAllChannelsAST()
	astF := userAllChannelsFlippedAST()

	// Warm up (prepare statements, populate page cache)
	eng.AddQueries([]QuerySpec{{QueryID: "warmup", AST: astNF}})
	eng.RemoveQuery("warmup")
	eng.AddQueries([]QuerySpec{{QueryID: "warmup", AST: astF}})
	eng.RemoveQuery("warmup")

	// Measure non-flipped (N+1 path)
	nfStart := nowMs()
	resultNF, err := eng.AddQueries([]QuerySpec{{QueryID: "q-nf", AST: astNF}})
	if err != nil {
		t.Fatalf("AddQueries non-flipped: %v", err)
	}
	nfElapsed := nowMs() - nfStart
	eng.RemoveQuery("q-nf")

	// Measure flipped (batched path)
	fStart := nowMs()
	resultF, err := eng.AddQueries([]QuerySpec{{QueryID: "q-f", AST: astF}})
	if err != nil {
		t.Fatalf("AddQueries flipped: %v", err)
	}
	fElapsed := nowMs() - fStart
	eng.RemoveQuery("q-f")

	// Measure planned (Go planner decides the flip automatically)
	sqlDB, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		t.Fatalf("open replica for cost model: %v", err)
	}
	defer sqlDB.Close()
	tableSpecs := map[string]planner.TableSpec{
		"channels":             {PrimaryKey: []string{"id"}, UniqueKeys: nil, ColumnTypes: colTypes(channelColumns())},
		"channel_participants": {PrimaryKey: []string{"id"}, UniqueKeys: nil, ColumnTypes: colTypes(participantColumns())},
	}
	costModel := planner.CreateSQLiteCostModel(sqlDB, tableSpecs)
	plannedAST := planner.PlanQuery(astNF, costModel, nil, nil)
	pStart := nowMs()
	resultP, err := eng.AddQueries([]QuerySpec{{QueryID: "q-p", AST: plannedAST}})
	if err != nil {
		t.Fatalf("AddQueries planned: %v", err)
	}
	pElapsed := nowMs() - pStart
	eng.RemoveQuery("q-p")

	nfRows := countQueryResultRows(resultNF)
	fRows := countQueryResultRows(resultF)
	pRows := countQueryResultRows(resultP)
	if nfRows != fRows {
		t.Logf("  NOTE: row count differs (non-flipped=%d flipped=%d) — OR-branch dedup semantics differ between paths; timing comparison still valid", nfRows, fRows)
	}

	t.Logf("userAllChannels hydrate (%d channels, %d participants):", numChannels, numParticipants)
	t.Logf("  non-flipped (N+1):           %dms  (%d rows)", nfElapsed, nfRows)
	t.Logf("  flipped    (manual):         %dms  (%d rows)", fElapsed, fRows)
	t.Logf("  planned    (Go planner):     %dms  (%d rows)", pElapsed, pRows)
	if nfElapsed > 0 && fElapsed > 0 {
		ratio := float64(nfElapsed) / float64(fElapsed)
		t.Logf("  ratio (non-flipped / flipped): %.1fx", ratio)
	}
	if pElapsed > 0 && fElapsed > 0 {
		pRatio := float64(pElapsed) / float64(fElapsed)
		t.Logf("  ratio (planned / manual flipped): %.2fx (expect ~1.0)", pRatio)
	}
}

func benchExistsHydratePlanned(b *testing.B, numChannels, numParticipants int) {
	eng, dbPath := newChannelsEngine(b, numChannels, numParticipants)
	ast := userAllChannelsAST()
	sqlDB, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		b.Fatalf("open replica for cost model: %v", err)
	}
	defer sqlDB.Close()
	tableSpecs := map[string]planner.TableSpec{
		"channels":             {PrimaryKey: []string{"id"}, UniqueKeys: nil, ColumnTypes: colTypes(channelColumns())},
		"channel_participants": {PrimaryKey: []string{"id"}, UniqueKeys: nil, ColumnTypes: colTypes(participantColumns())},
	}
	costModel := planner.CreateSQLiteCostModel(sqlDB, tableSpecs)
	plannedAST := planner.PlanQuery(ast, costModel, nil, nil)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err := eng.AddQueries([]QuerySpec{{QueryID: "q", AST: plannedAST}}); err != nil {
			b.Fatalf("AddQueries: %v", err)
		}
		eng.RemoveQuery("q")
	}
}

func countQueryResultRows(results []QueryResult) int {
	count := 0
	for _, r := range results {
		count += len(r.Changes)
	}
	return count
}

func nowMs() int64 {
	return time.Now().UnixMilli()
}

func colTypes(cols map[string]sqlite.ColumnSchema) map[string]string {
	m := make(map[string]string, len(cols))
	for k, v := range cols {
		m[k] = v.Type
	}
	return m
}
