package tablesource

// Regression: a REAL DB error from the exists probe must not read as "row
// absent" (scale review). Pre-fix, existsLocked collapsed EVERY error to
// false, so under I/O pressure (SQLITE_BUSY, closed conn, disk fault):
//
//   - driftCheckLocked fabricated a missing-row DriftError for every
//     Remove/Edit — spurious pipeline resets exactly when the system was
//     already struggling (drift storm), and
//   - a dup-Add sailed PAST the drift check into writeChange's UNIQUE
//     violation — the crash-grade panic the drift check exists to prevent.
//
// TS's exists closure (table-source.ts:399-413, better-sqlite3) THROWS on
// statement error. The port now propagates the error and Push panics with
// a plain (transient, -32000 → reset) error AFTER releasing s.mu — never a
// DriftError, never a silent wrong answer.

import (
	"strings"
	"testing"

	"github.com/kartikparsoya-eng/go-ivm/ivm"
)

func TestExistsProbeErrorIsNotDrift(t *testing.T) {
	src, db := newUserSource(t)
	defer db.Close()

	// Sanity: a benign push works before the sabotage.
	src.Push(ivm.MakeSourceChangeAdd(ivm.Row{
		"id": float64(60), "name": "wc", "score": float64(1), "active": true,
	}))

	// Sabotage the exists probe only: point checkExists at a missing table
	// so QueryRow fails at prepare — the shape of any real statement
	// failure (I/O error, closed conn), without touching other statements.
	src.mu.Lock()
	goodSQL := src.checkExistsSQL
	src.checkExistsSQL = `SELECT 1 FROM no_such_table_exists_probe WHERE id = ? LIMIT 1`
	src.mu.Unlock()

	// Remove of a row that REALLY exists (id=1 is seeded). Pre-fix: the
	// probe error read as "absent" → fabricated missing-row DriftError.
	var recovered any
	func() {
		defer func() { recovered = recover() }()
		src.Push(ivm.MakeSourceChangeRemove(ivm.Row{
			"id": float64(1), "name": "alice", "score": float64(90), "active": true,
		}))
	}()

	if recovered == nil {
		t.Fatal("Push with a failing exists probe must panic (matching TS's throwing exists closure)")
	}
	if _, isDrift := recovered.(*ivm.DriftError); isDrift {
		t.Fatalf("probe failure surfaced as a DriftError (spurious drift → reset storm): %v", recovered)
	}
	msg, ok := recovered.(string)
	if !ok || !strings.Contains(msg, "checkExists") {
		t.Fatalf("panic should carry the checkExists probe error, got %T: %v", recovered, recovered)
	}

	// s.mu must have been released before the panic (the file-wide
	// unlock-then-panic discipline): the source stays usable after repair.
	src.mu.Lock()
	src.checkExistsSQL = goodSQL
	src.mu.Unlock()
	src.Push(ivm.MakeSourceChangeRemove(ivm.Row{
		"id": float64(1), "name": "alice", "score": float64(90), "active": true,
	}))
}
