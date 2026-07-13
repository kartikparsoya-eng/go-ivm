package sqlite

import (
	"strings"
	"testing"

	"github.com/kartikparsoya-eng/go-ivm/ivm"
)

// A Start cursor whose Row lacks the FIRST order column (with a non-"at" basis)
// produces no OR clauses. Pre-fix gatherStartConstraints returned the literal
// "()", a SQL syntax error that panics every Fetch. The guard must instead emit
// a no-op TRUE constraint (a cursor pinning no position imposes no lower bound).
func TestBuildSelectQuery_EmptyStartCursorDoesNotEmitParenParen(t *testing.T) {
	cols := map[string]ColumnSchema{
		"createdAt": {Type: "number"},
		"id":        {Type: "string"},
	}
	order := ivm.Ordering{{"createdAt", "asc"}, {"id", "asc"}}
	start := &ivm.Start{Row: ivm.Row{}, Basis: "after"} // no order columns present

	q := BuildSelectQuery("t", cols, nil, nil, order, false, start, nil)

	if strings.Contains(q.SQL, "()") {
		t.Fatalf("generated SQL contains invalid empty parens %q", q.SQL)
	}
	if !strings.Contains(q.SQL, "TRUE") {
		t.Fatalf("expected a TRUE no-op cursor constraint, got %q", q.SQL)
	}
	// Sanity: still a well-formed SELECT with the cursor folded into the WHERE.
	if !strings.HasPrefix(q.SQL, "SELECT ") || !strings.Contains(q.SQL, "WHERE ") {
		t.Fatalf("malformed SQL: %q", q.SQL)
	}
}

// A cursor value that is PRESENT but NULL on an OPTIONAL order column
// previously generated "(? IS NULL OR col > ?)" with a NULL bind, which
// collapsed to TRUE (NULL IS NULL → TRUE) — admitting every row including
// the cursor row itself. This caused an infinite loop in FlippedJoin's
// chunked fetch (fetchChunkedSequential re-fetched the same head forever).
// The fix short-circuits: "strictly after NULL" (> in ascending) emits
// "col IS NOT NULL" (SQLite sorts NULL first, so everything after NULL =
// all non-NULL rows). This intentionally diverges from TS, which has the
// same latent bug. The divergence is safe: the push-path comparator
// (CompareWithPartialBound, nil sorts first) already treats "after NULL"
// as "all non-NULL" — the SQL now matches that semantics instead of
// contradicting it.
//
// Divergence from TS: at a NULL-valued cursor boundary on an Optional
// sort column, TS over-includes (its identical SQL shape collapses to
// TRUE) while Go now returns the SQL-correct set (col IS NOT NULL).
// Shadow-compare / G8 oracle triage should classify this as
// go=correct, ts=over-include, not a Go bug.
func TestBuildSelectQuery_PresentNullCursorDivergesFromTS(t *testing.T) {
	cols := map[string]ColumnSchema{
		"score": {Type: "number", Optional: true},
		"id":    {Type: "string"},
	}
	order := ivm.Ordering{{"score", "asc"}, {"id", "asc"}}
	start := &ivm.Start{Row: ivm.Row{"score": nil, "id": "x"}, Basis: "after"}

	q := BuildSelectQuery("t", cols, nil, nil, order, false, start, nil)

	// Fixed: NULL cursor on optional ">" column emits "col IS NOT NULL"
	// instead of the old "(? IS NULL OR col > ?)" which collapsed to TRUE.
	if !strings.Contains(q.SQL, `"score" IS NOT NULL`) {
		t.Fatalf("expected IS NOT NULL for NULL cursor on optional >, got %q", q.SQL)
	}
	// The old broken form "(? IS NULL OR score > ?)" must NOT appear.
	if strings.Contains(q.SQL, `? IS NULL OR "score" > ?`) {
		t.Fatalf("old broken nullable-aware form still present in %q", q.SQL)
	}
}
