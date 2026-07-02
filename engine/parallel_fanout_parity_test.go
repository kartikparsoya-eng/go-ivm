package engine

// End-to-end parity test for the parallel advance fanout
// (tablesource.ParallelAdvance / GO_IVM_PARALLEL_ADVANCE): the SAME advance
// batch, run through the full engine + operator stack with FIVE queries
// subscribed to the same tables, must produce identical per-query RowChange
// sequences with the fanout serial and parallel.
//
// Per-query comparison (not whole-slice DeepEqual) is deliberate: the
// streamer's documented ordering contract (D8) makes cross-query interleave
// non-deterministic under concurrent pushes, while rows WITHIN a query keep
// their push order — which is exactly what downstream consumers rely on (TS
// routes by queryID). This test pins that contract through the real stack:
// plain-filter queries, an unfiltered query, and two compound-EXISTS shapes
// whose push processing does nested lazy fetches (Join parent fetch +
// Exists re-check) against the shared prev-tx conn from CONCURRENT group
// goroutines — the checkout stmt cache + interleaved-cursor path under real
// parallelism. Run with -race in CI.
//
// Fresh engine + replica per run (same constraint as
// lazy_advance_parity_test.go: the mattn test build has no BEGIN
// CONCURRENT, so multi-advance chains on one engine can't be made
// self-consistent; one seeded fixture per run sidesteps it).

import (
	"database/sql"
	"path/filepath"
	"reflect"
	"sort"
	"testing"

	"github.com/kartikparsoya-eng/go-ivm/builder"
	"github.com/kartikparsoya-eng/go-ivm/internal/tablesource"
	"github.com/kartikparsoya-eng/go-ivm/ivm"
	"github.com/kartikparsoya-eng/go-ivm/sqlite"

	_ "github.com/mattn/go-sqlite3"
)

func fanoutParityCPQuery(userID string) builder.AST {
	return builder.AST{
		Table: "channel_participants",
		Where: &builder.Condition{
			Type:  "simple",
			Left:  &builder.ValuePos{Type: "column", Name: "userId"},
			Op:    "=",
			Right: &builder.ValuePos{Type: "literal", Value: userID},
		},
		OrderBy: ivm.Ordering{{"id", "asc"}},
	}
}

func fanoutParityExistsQuery(alias, filterCol string, filterVal string) builder.AST {
	return builder.AST{
		Table: "channels",
		Where: &builder.Condition{
			Type: "correlatedSubquery", Op: "EXISTS",
			Related: &builder.CorrelatedSubquery{
				System: "client",
				Correlation: builder.Correlation{
					ParentField: []string{"id"},
					ChildField:  []string{"channelId"},
				},
				Subquery: builder.AST{
					Table: "channel_participants",
					Alias: alias,
					Where: &builder.Condition{
						Type:  "simple",
						Left:  &builder.ValuePos{Type: "column", Name: filterCol},
						Op:    "=",
						Right: &builder.ValuePos{Type: "literal", Value: filterVal},
					},
				},
			},
		},
		OrderBy: ivm.Ordering{{"id", "asc"}},
	}
}

// runFanoutParityAdvance seeds a fresh replica, registers five queries on
// the shared sources, runs ONE advance batch, and returns the RowChanges
// grouped by queryID (sequence order preserved within each query).
func runFanoutParityAdvance(t *testing.T) map[string][]RowChange {
	t.Helper()

	path := filepath.Join(t.TempDir(), "replica.sqlite")
	w, err := sql.Open("sqlite3", path)
	if err != nil {
		t.Fatalf("seed open: %v", err)
	}
	seed := []string{
		"PRAGMA journal_mode=WAL",
		`CREATE TABLE channels (id TEXT PRIMARY KEY, name TEXT, visibility TEXT)`,
		`CREATE TABLE channel_participants (id TEXT PRIMARY KEY, channelId TEXT, userId TEXT, joinedAt INTEGER, role TEXT)`,
		`INSERT INTO channels VALUES ('chan-A', 'alpha', 'PRIVATE'), ('chan-B', 'beta', 'PUBLIC'), ('chan-C', 'gamma', 'PUBLIC')`,
		// Seeded participants: user-A in chan-A (edited by the batch),
		// user-B in chan-B (removed by the batch), an ADMIN in chan-C
		// (existing EXISTS=true for q-exists-admin).
		`INSERT INTO channel_participants VALUES ('cp-a1', 'chan-A', 'user-A', 0, 'MEMBER')`,
		`INSERT INTO channel_participants VALUES ('cp-b1', 'chan-B', 'user-B', 0, 'MEMBER')`,
		`INSERT INTO channel_participants VALUES ('cp-c1', 'chan-C', 'user-C', 0, 'ADMIN')`,
	}
	for _, stmt := range seed {
		if _, err := w.Exec(stmt); err != nil {
			t.Fatalf("seed exec %q: %v", stmt, err)
		}
	}
	w.Close()

	db, err := tablesource.Open(path, tablesource.OpenOptions{})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	wdb, err := tablesource.OpenWritable(path, tablesource.OpenOptions{})
	if err != nil {
		t.Fatalf("OpenWritable: %v", err)
	}
	t.Cleanup(func() { wdb.Close() })

	chanSrc, err := tablesource.New(db, wdb, "channels",
		map[string]sqlite.ColumnSchema{
			"id": {Type: "string"}, "name": {Type: "string"}, "visibility": {Type: "string"},
		},
		[]string{"id"},
	)
	if err != nil {
		t.Fatalf("channels New: %v", err)
	}
	cpSrc, err := tablesource.New(db, wdb, "channel_participants",
		map[string]sqlite.ColumnSchema{
			"id": {Type: "string"}, "channelId": {Type: "string"}, "userId": {Type: "string"},
			"joinedAt": {Type: "number"}, "role": {Type: "string"},
		},
		[]string{"id"},
	)
	if err != nil {
		t.Fatalf("channel_participants New: %v", err)
	}

	eng, err := NewEngine(EngineConfig{StoragePath: tempStoragePath(t)})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { eng.Close() })
	eng.RegisterSource(chanSrc)
	eng.RegisterSource(cpSrc)
	eng.SetTableUniqueKeys("channels", [][]string{{"id"}})
	eng.SetTableUniqueKeys("channel_participants", [][]string{{"id"}})

	queries := map[string]builder.AST{
		"q-cp-userA":     fanoutParityCPQuery("user-A"),
		"q-cp-userX":     fanoutParityCPQuery("user-X"),
		"q-cp-all":       {Table: "channel_participants", OrderBy: ivm.Ordering{{"id", "asc"}}},
		"q-exists-userX": fanoutParityExistsQuery("zsubq_cp_px", "userId", "user-X"),
		"q-exists-admin": fanoutParityExistsQuery("zsubq_cp_pa", "role", "ADMIN"),
	}
	// Deterministic registration order (map ranges are randomized).
	names := make([]string, 0, len(queries))
	for name := range queries {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		if _, _, err := eng.AddQuery(name, queries[name]); err != nil {
			t.Fatalf("AddQuery %s: %v", name, err)
		}
	}

	// One batch touching every query: an Add that flips q-exists-userX on
	// for chan-A and lands in q-cp-userX + q-cp-all; an Edit moving user-A
	// off its filter (Remove for q-cp-userA, Edit for q-cp-all); a Remove
	// (Remove for q-cp-all; user-B had no dedicated query); and an ADMIN
	// add on chan-B flipping q-exists-admin on for a second channel.
	batch := []SnapshotChange{
		{
			Table: "channel_participants",
			NextValue: ivm.Row{
				"id": "cp-x1", "channelId": "chan-A", "userId": "user-X",
				"joinedAt": int64(1), "role": "MEMBER",
			},
		},
		{
			Table: "channel_participants",
			PrevValues: []ivm.Row{{
				"id": "cp-a1", "channelId": "chan-A", "userId": "user-A",
				"joinedAt": int64(0), "role": "MEMBER",
			}},
			NextValue: ivm.Row{
				"id": "cp-a1", "channelId": "chan-A", "userId": "user-A2",
				"joinedAt": int64(0), "role": "MEMBER",
			},
		},
		{
			Table: "channel_participants",
			PrevValues: []ivm.Row{{
				"id": "cp-b1", "channelId": "chan-B", "userId": "user-B",
				"joinedAt": int64(0), "role": "MEMBER",
			}},
		},
		{
			Table: "channel_participants",
			NextValue: ivm.Row{
				"id": "cp-b2", "channelId": "chan-B", "userId": "user-D",
				"joinedAt": int64(2), "role": "ADMIN",
			},
		},
	}

	r := eng.Advance(batch)
	if r.Drift != nil {
		t.Fatalf("advance drifted: %+v", r.Drift)
	}

	byQuery := make(map[string][]RowChange)
	for _, c := range r.Changes {
		byQuery[c.QueryID] = append(byQuery[c.QueryID], c)
	}
	return byQuery
}

func TestParallelAdvanceFanoutParity(t *testing.T) {
	prevP, prevW := tablesource.ParallelAdvance, tablesource.ParallelAdvanceWorkers
	defer func() {
		tablesource.ParallelAdvance, tablesource.ParallelAdvanceWorkers = prevP, prevW
	}()

	tablesource.ParallelAdvance = false
	base := runFanoutParityAdvance(t)

	tablesource.ParallelAdvance = true
	tablesource.ParallelAdvanceWorkers = 4
	par := runFanoutParityAdvance(t)

	if len(base) == 0 {
		t.Fatal("degenerate scenario: serial advance produced no changes")
	}
	if len(par) != len(base) {
		t.Fatalf("query sets differ: serial=%d queries, parallel=%d", len(base), len(par))
	}
	for q, want := range base {
		got, ok := par[q]
		if !ok {
			t.Errorf("query %s: emitted serially but not in parallel", q)
			continue
		}
		if !reflect.DeepEqual(got, want) {
			t.Errorf("query %s diverged:\n serial   (%d): %+v\n parallel (%d): %+v",
				q, len(want), want, len(got), got)
		}
	}

	// Non-degeneracy: the batch is constructed to touch every query class —
	// the two EXISTS queries must both flip a channel on, and the plain
	// filter queries must see their add/remove.
	for _, q := range []string{"q-cp-userA", "q-cp-userX", "q-cp-all", "q-exists-userX", "q-exists-admin"} {
		if len(base[q]) == 0 {
			t.Errorf("scenario degenerate: query %s emitted nothing serially", q)
		}
	}
}
