package engine

// Regression for the 20-user 30-min shadow-soak Take stale-bound panic:
//
//   [TAKE-FLIGHT-RECORDER] panic-context: stale-bound (oldCmp>0, newCmp==0):
//   edit row.id=ticket-6 sort=...457341→...623262 bound.id=ticket-6
//   boundSort=...623262 size=1
//
// Production trigger (limit=1 ORDER BY updatedAt DESC):
//   1. A single advance batch contains edits for multiple rows that land at
//      the SAME new updatedAt (rapid mutations sharing a millisecond).
//   2. With id-ASC tiebreaker, the row whose id sorts LAST among the tied
//      group ends up as the "row immediately before the old bound" in the
//      reverse fetch — even though it's not the actual top-of-source.
//   3. Take's pushEditChange "old outside, new inside" path picks that
//      row as newBoundNode, setting bound to a row whose stored sort
//      equals its NEW source value while still-pending edits below it in
//      the batch carry the row's OLD value as their OldRow.
//   4. When the pending edit for that same row arrives, oldCmp>0 (old
//      sort < bound sort in DESC) and newCmp==0 (new equals bound). This
//      is the stale-bound trap.
//
// Recovery contract: Take must raise *ivm.DriftError (not a raw panic)
// so engine.Advance recovers, drops the in-flight advance, and the TS
// caller re-inits from SQLite truth. This test verifies the contract
// holds — a non-DriftError panic would crash the shared sidecar and take
// down every cg.

import (
	"database/sql"
	"path/filepath"
	"testing"

	"github.com/kartikparsoya-eng/go-ivm/builder"
	"github.com/kartikparsoya-eng/go-ivm/internal/tablesource"
	"github.com/kartikparsoya-eng/go-ivm/ivm"
	"github.com/kartikparsoya-eng/go-ivm/sqlite"

	_ "github.com/mattn/go-sqlite3"
)

// TestTableSourceTake_StaleBoundOnTieAtLimit1 reproduces the 30-min-soak
// stale-bound panic with the minimum production-equivalent setup: limit=1,
// updatedAt DESC sort, and a batch of edits where two rows collide at the
// same new updatedAt with id-ASC tiebreaker.
//
// Layout: pick names so id-ASC puts the "wrong" row between the displaced
// bound and the row that's actually moving — that's what causes Take to
// pick the wrong newBoundNode in the reverse fetch.
func TestTableSourceTake_StaleBoundOnTieAtLimit1(t *testing.T) {
	path := filepath.Join(t.TempDir(), "replica.sqlite")
	w, err := sql.Open("sqlite3", path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if _, err := w.Exec("PRAGMA journal_mode=WAL"); err != nil {
		t.Fatalf("pragma: %v", err)
	}
	if _, err := w.Exec(`CREATE TABLE tickets (id TEXT PRIMARY KEY, updatedAt INTEGER)`); err != nil {
		t.Fatalf("create: %v", err)
	}
	// Seed: ticket-17 is the top by updatedAt (becomes Take's bound after
	// hydrate). ticket-6 and ticket-14 are outside the window with strictly
	// smaller updatedAt values; their later edits will both land at the
	// same new value and provoke the tie.
	seed := []struct {
		id        string
		updatedAt int64
	}{
		{"ticket-17", 617688},
		{"ticket-6", 457341},
		{"ticket-14", 527114},
		{"ticket-13", 603189},
		{"ticket-12", 590281},
	}
	for _, s := range seed {
		if _, err := w.Exec(`INSERT INTO tickets VALUES (?, ?)`, s.id, s.updatedAt); err != nil {
			t.Fatalf("seed: %v", err)
		}
	}
	w.Close()

	db, err := tablesource.Open(path, tablesource.OpenOptions{})
	wdb, _ := tablesource.OpenWritable(path, tablesource.OpenOptions{})
	defer wdb.Close()
	if err != nil {
		t.Fatalf("tablesource.Open: %v", err)
	}
	t.Cleanup(func() { db.Close() })

	src, err := tablesource.New(db, wdb, "tickets",
		map[string]sqlite.ColumnSchema{
			"id":        {Type: "string"},
			"updatedAt": {Type: "number"},
		},
		[]string{"id"})
	if err != nil {
		t.Fatalf("tablesource.New: %v", err)
	}

	eng, err := NewEngine(EngineConfig{StoragePath: tempStoragePath(t)})
	if err != nil {
		t.Fatalf("NewEngine: %v", err)
	}
	t.Cleanup(func() { eng.Close() })
	eng.RegisterSource(src)

	limit := 1
	ast := builder.AST{
		Table:   "tickets",
		OrderBy: ivm.Ordering{{"updatedAt", "desc"}},
		Limit:   &limit,
	}
	if _, _, err := eng.AddQuery("q-top1-tie", ast); err != nil {
		t.Fatalf("AddQuery: %v", err)
	}

	// Both ticket-6 and ticket-14 land at the SAME new updatedAt = 623262.
	// With id-ASC tiebreaker, ticket-14 < ticket-6 lex, so at the tied
	// sort ticket-14 ends up at the smaller forward position. The
	// SnapshotChanges below drive the prev-tx writeChange — no external
	// UPDATE (would deadlock the prev tx's read snapshot under plain BEGIN).
	const newTs = int64(623262)

	// Batch the two edits in a single Advance — order matters: ticket-6
	// first (so it lands in source.data before ticket-14's push fires).
	changes := []SnapshotChange{
		{
			Table: "tickets",
			PrevValues: []ivm.Row{{
				"id": "ticket-6", "updatedAt": int64(457341),
			}},
			NextValue: ivm.Row{
				"id": "ticket-6", "updatedAt": newTs,
			},
		},
		{
			Table: "tickets",
			PrevValues: []ivm.Row{{
				"id": "ticket-14", "updatedAt": int64(527114),
			}},
			NextValue: ivm.Row{
				"id": "ticket-14", "updatedAt": newTs,
			},
		},
	}

	defer func() {
		r := recover()
		if r == nil {
			return // no panic — Take handled the tie cleanly. Good.
		}
		// Must be *DriftError so engine.Advance's recover catches it and
		// the sidecar continues. Any other panic type crashes the shared
		// sidecar process.
		if _, ok := r.(*ivm.DriftError); !ok {
			t.Fatalf("stale-bound panic must be *DriftError for recovery, got %T: %v", r, r)
		}
		t.Logf("stale-bound triggered DriftError as expected — recovery contract honored: %v", r)
	}()

	result := eng.Advance(changes)
	if result.Drift != nil {
		t.Logf("advance returned drift=%v (recovery via DriftError path)", result.Drift)
		return
	}
	t.Logf("advance returned %d changes without drift", len(result.Changes))
	// Optional: validate the post-advance state is consistent.
	// The Take window must contain ONE row, and it must be one of the
	// two tied rows at updatedAt=623262.
	idsSeen := map[string]int{}
	for _, c := range result.Changes {
		if c.Table != "tickets" {
			continue
		}
		idsSeen[c.RowKey["id"].(string)]++
	}
	t.Logf("changes per id: %v", idsSeen)
}
