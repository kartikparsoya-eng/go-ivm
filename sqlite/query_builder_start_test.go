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

	q := BuildSelectQuery("t", cols, nil, nil, order, false, start)

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
