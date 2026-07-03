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

// User's-audit item "present-but-null cursor over-include", RESOLVED AS
// TS-MIRRORED (no behavior change): a cursor value that is PRESENT but NULL
// on an OPTIONAL order column generates the nullable-aware form
// "(? IS NULL OR col > ?)" — with a NULL bind, "? IS NULL" is TRUE, so the
// clause admits EVERY row including ones whose col IS NULL. The push-path
// comparator (ivm.CompareWithPartialBound: nil sorts first) admits only
// non-null rows for the same bound — an internal hydrate-vs-push
// over-include. Verified against TS before touching anything: TS emits the
// IDENTICAL SQL shape (zqlite/query-builder.ts nullableAwareRangeComparison
// binds the null cursor value into the same "(value IS NULL OR …)" form)
// while ITS push comparator (zql skip.ts #comparator, nulls-first) has the
// same asymmetry. Under the faithful-to-TS rule the divergence must be
// PRESERVED, not fixed — this test pins the SQL shape so a future
// "correction" here can't silently split Go from TS under shadow-compare.
func TestBuildSelectQuery_PresentNullCursorMirrorsTS(t *testing.T) {
	cols := map[string]ColumnSchema{
		"score": {Type: "number", Optional: true},
		"id":    {Type: "string"},
	}
	order := ivm.Ordering{{"score", "asc"}, {"id", "asc"}}
	start := &ivm.Start{Row: ivm.Row{"score": nil, "id": "x"}, Basis: "after"}

	q := BuildSelectQuery("t", cols, nil, nil, order, false, start, nil)

	// TS shape: the optional ">" form with the cursor value bound twice.
	if !strings.Contains(q.SQL, `(? IS NULL OR "score" > ?)`) {
		t.Fatalf("optional-column cursor form missing from %q (TS emits it verbatim)", q.SQL)
	}
	// The null cursor value must actually be BOUND (twice for the ">" form),
	// exactly as TS binds `value` into both placeholders.
	nils := 0
	for _, p := range q.Params {
		if p == nil {
			nils++
		}
	}
	if nils < 2 {
		t.Fatalf("params %v: want the null cursor value bound into both placeholders", q.Params)
	}
}
