package sqlite

import (
	"strings"
	"testing"

	"github.com/kartikparsoya-eng/go-ivm/ivm"
)

// TestBuildSelectQuery_AtBasisPartialCursorPinsPrefixOnlyEquality pins the
// basis='at' + partial-cursor sub-case the porting-correctness review flagged
// as real-but-unpinned. This is a DELIBERATE divergence in the ed7a302
// partial-cursor family, documented in gatherStartConstraints:
//
//   - TS (query-builder.ts:377-388) maps its 'at' equality clause over ALL
//     order columns, so a cursor missing the trailing sort column emits a
//     dead clause — the missing column binds NULL and `id = NULL` never
//     matches — EXCLUDING the boundary row from the SQL result.
//   - Go (query_builder.go:492-506) stops at the first order column absent
//     from the cursor, so the 'at' arm is equality on the SPECIFIED PREFIX
//     only — INCLUDING the boundary row (and every row sharing the prefix).
//
// Go's include is self-consistent with its push path
// (ivm.CompareWithPartialBound: an 'at' cursor admits rows equal on the
// specified prefix), whereas TS's SQL contradicts TS's own push comparator
// for the same cursor. Matching TS's dead clause would reintroduce the
// hydrate-vs-push disagreement the partial-cursor family of fixes removed.
// Pinned so a future "TS-alignment" pass can't silently flip it.
func TestBuildSelectQuery_AtBasisPartialCursorPinsPrefixOnlyEquality(t *testing.T) {
	cols := map[string]ColumnSchema{
		"createdAt": {Type: "number"},
		"id":        {Type: "string"},
	}
	order := ivm.Ordering{{"createdAt", "asc"}, {"id", "asc"}}
	// Partial cursor: only the first sort column is pinned.
	start := &ivm.Start{Row: ivm.Row{"createdAt": float64(5)}, Basis: "at"}

	q := BuildSelectQuery("t", cols, nil, nil, order, false, start, nil)

	// Range arm: strictly-after on the specified prefix.
	if !strings.Contains(q.SQL, `("createdAt" > ?)`) {
		t.Fatalf("range arm missing from %q", q.SQL)
	}
	// 'at' arm: equality on the specified prefix ONLY — no dead "id" clause.
	if !strings.Contains(q.SQL, `("createdAt" = ?)`) {
		t.Fatalf("'at' arm must be prefix-only equality; got %q", q.SQL)
	}
	if strings.Contains(q.SQL, `"id" = ?`) || strings.Contains(q.SQL, `"id" IS ?`) {
		t.Fatalf("cursor constraint must not reference the unspecified trailing sort column; got %q", q.SQL)
	}
	// Exactly the two prefix binds — one for the range arm, one for the 'at' arm.
	if len(q.Params) != 2 || q.Params[0] != float64(5) || q.Params[1] != float64(5) {
		t.Fatalf("params = %v, want [5 5]", q.Params)
	}
}

// TestBuildSelectQuery_EmptyStartCursorAtBasisDoesNotEmitParenParen closes the
// 'at' twin of the existing empty-cursor guard: a Basis:"at" cursor whose Row
// lacks the FIRST sort column used to make the 'at' block append the literal
// "()" (its per-field loop broke immediately, then unconditionally appended
// "(" + join(nothing) + ")"), a SQL syntax error that panics every Fetch. The
// defensive TRUE guard below the loop never fired because orClauses was
// non-empty — it held the "()" itself. Same disposition as the pinned
// basis:"after" case: a cursor pinning no position imposes no constraint.
func TestBuildSelectQuery_EmptyStartCursorAtBasisDoesNotEmitParenParen(t *testing.T) {
	cols := map[string]ColumnSchema{
		"createdAt": {Type: "number"},
		"id":        {Type: "string"},
	}
	order := ivm.Ordering{{"createdAt", "asc"}, {"id", "asc"}}
	start := &ivm.Start{Row: ivm.Row{}, Basis: "at"} // no order columns present

	q := BuildSelectQuery("t", cols, nil, nil, order, false, start, nil)

	if strings.Contains(q.SQL, "()") {
		t.Fatalf("generated SQL contains invalid empty parens %q", q.SQL)
	}
	if !strings.Contains(q.SQL, "TRUE") {
		t.Fatalf("expected a TRUE no-op cursor constraint, got %q", q.SQL)
	}
	if len(q.Params) != 0 {
		t.Fatalf("no cursor binds expected, got %v", q.Params)
	}
}
