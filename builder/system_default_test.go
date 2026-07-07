package builder

// Regression test for faithfulness #6: an omitted correlated-subquery
// `system` defaults to "client" — TS: `system: sq.system ?? 'client'` at both
// join construction sites (builder.ts:508, builder.ts:682). Pre-fix Go passed
// the zero value "" through to the relationship subschema.

import (
	"testing"

	"github.com/kartikparsoya-eng/go-ivm/ivm"
)

func TestRelatedSystemDefaultsToClient(t *testing.T) {
	delegate := &mockDelegate{
		sources: map[string]*mockSource{
			"users": {tableName: "users"},
			"posts": {tableName: "posts"},
		},
	}

	ast := AST{
		Table:   "users",
		OrderBy: ivm.Ordering{{"id", "asc"}},
		Related: []CorrelatedSubquery{
			{
				Correlation: Correlation{
					ParentField: []string{"id"},
					ChildField:  []string{"userId"},
				},
				// System deliberately omitted — the wire AST omits it for
				// ordinary client queries.
				Subquery: AST{
					Table:   "posts",
					Alias:   "posts",
					OrderBy: ivm.Ordering{{"id", "asc"}},
				},
			},
		},
	}

	p := BuildPipeline(ast, delegate)
	rel := p.Input.GetSchema().Relationships["posts"]
	if rel == nil {
		t.Fatal("expected posts relationship subschema")
	}
	if rel.System != "client" {
		t.Fatalf("relationship subschema System = %q, want %q (TS: sq.system ?? 'client')", rel.System, "client")
	}
}
