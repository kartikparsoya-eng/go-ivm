package sqlite

import (
	"strings"
	"testing"
)

// TestLikeConditionToSQL pins the SQL emitted for LIKE/ILIKE to the zero
// 1.7.0 contract (zqlite/query-builder.ts likeConditionToSQL): explicit
// `ESCAPE '\'` (Postgres' default escape; SQLite has none), and lower() on
// both ILIKE operands (with case_sensitive_like=ON making bare LIKE
// case-sensitive). Pre-1.7.0 the port mapped ILIKE→LIKE and emitted no
// ESCAPE clause — asserting the exact SQL here fails against that.
func TestLikeConditionToSQL(t *testing.T) {
	mkCond := func(op string) *Condition {
		return &Condition{
			Type:  "simple",
			Op:    op,
			Left:  ValuePos{Type: "column", Name: "title"},
			Right: ValuePos{Type: "literal", Value: "a%"},
		}
	}
	cases := []struct {
		op      string
		wantSQL string
	}{
		{"LIKE", `"title" LIKE ? ESCAPE '\'`},
		{"NOT LIKE", `"title" NOT LIKE ? ESCAPE '\'`},
		{"ILIKE", `lower("title") LIKE lower(?) ESCAPE '\'`},
		{"NOT ILIKE", `lower("title") NOT LIKE lower(?) ESCAPE '\'`},
	}
	for _, c := range cases {
		t.Run(c.op, func(t *testing.T) {
			gotSQL, params := simpleConditionToSQL(mkCond(c.op))
			if gotSQL != c.wantSQL {
				t.Fatalf("simpleConditionToSQL(%s) SQL = %q, want %q", c.op, gotSQL, c.wantSQL)
			}
			if len(params) != 1 || params[0] != "a%" {
				t.Fatalf("simpleConditionToSQL(%s) params = %v, want [a%%]", c.op, params)
			}
		})
	}
}

// TestLikeSQLInsideFilterTree: the ESCAPE form must survive and/or nesting.
func TestLikeSQLInsideFilterTree(t *testing.T) {
	cond := &Condition{
		Type: "and",
		Conditions: []*Condition{
			{
				Type:  "simple",
				Op:    "ILIKE",
				Left:  ValuePos{Type: "column", Name: "name"},
				Right: ValuePos{Type: "literal", Value: "%é%"},
			},
			{
				Type:  "simple",
				Op:    "=",
				Left:  ValuePos{Type: "column", Name: "kind"},
				Right: ValuePos{Type: "literal", Value: "x"},
			},
		},
	}
	gotSQL, params := filtersToSQL(cond)
	if !strings.Contains(gotSQL, `lower("name") LIKE lower(?) ESCAPE '\'`) {
		t.Fatalf("filtersToSQL(and) = %q, want ILIKE lower()/ESCAPE form inside", gotSQL)
	}
	if len(params) != 2 {
		t.Fatalf("filtersToSQL(and) params = %v, want 2", params)
	}
}
