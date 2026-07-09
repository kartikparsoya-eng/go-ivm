package engine

import (
	"testing"

	"github.com/kartikparsoya-eng/go-ivm/builder"
	"github.com/kartikparsoya-eng/go-ivm/ivm"
)

// scalarUsersIssuesAST builds the canonical scalar-EXISTS AST used across the
// companion tests: issues WHERE EXISTS(users WHERE id='u1', scalar) correlated
// on ownerId = users.name.
func scalarUsersIssuesAST() builder.AST {
	return builder.AST{
		Table: "issues",
		Where: &builder.Condition{
			Type:   "correlatedSubquery",
			Op:     "EXISTS",
			Scalar: true,
			Related: &builder.CorrelatedSubquery{
				Correlation: builder.Correlation{
					ParentField: []string{"ownerId"},
					ChildField:  []string{"name"},
				},
				Subquery: builder.AST{
					Table: "users",
					Where: &builder.Condition{
						Type:  "simple",
						Op:    "=",
						Left:  &builder.ValuePos{Type: "column", Name: "id"},
						Right: &builder.ValuePos{Type: "literal", Value: "u1"},
					},
				},
			},
		},
	}
}

// TestCompanionScalar_NoMatchToMatchedNullResets pins the F1 fix from the
// streaming/advance audit: TS keeps a TRI-STATE resolvedValue — "`null` if a
// row matched but the field was `NULL`, or `undefined` if no row matched"
// (resolve-scalar-subqueries.ts:21-23; executor returns undefined at
// pipeline-driver.ts:1195-1199, `?? null` at :1203) — and its companion push
// compares with strict `===` where null !== undefined (scalarValuesEqual,
// pipeline-driver.ts:3025-3034). So a subquery that matched NO row at resolve
// time (resolvedValue=undefined) MUST reset when an INSERT later produces a
// matching row whose child field is NULL: scalarValuesEqual(null, undefined)
// is false → ResetPipelinesSignal (pipeline-driver.ts:1717-1722).
//
// Go conflated both states into nil (engine.go's old single resolvedValue),
// so nil==nil suppressed exactly this reset. The end-state rows happened to
// be identical (the re-baked scalar is still ALWAYS_FALSE), but the reset
// LIFECYCLE diverged: TS rebuilds the pipeline and re-resolves; Go kept
// serving from a stale companion whose resolvedValue no longer described the
// subquery's true state ("no row" vs "row with NULL").
func TestCompanionScalar_NoMatchToMatchedNullResets(t *testing.T) {
	// users starts EMPTY — the scalar subquery (id='u1') matches nothing:
	// TS resolvedValue = undefined.
	users := ivm.NewMemorySource("users",
		map[string]string{"id": "string", "name": "string"},
		[]string{"id"})
	issues := ivm.NewMemorySource("issues",
		map[string]string{"id": "string", "ownerId": "string"},
		[]string{"id"})
	issues.BulkInsert([]ivm.Row{{"id": "i1", "ownerId": "Alice"}})

	eng, err := NewEngine(EngineConfig{StoragePath: tempStoragePath(t)})
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()
	eng.RegisterMemorySource(users)
	eng.RegisterMemorySource(issues)
	eng.SetTableUniqueKeys("users", [][]string{{"id"}})

	changes, _, err := eng.AddQuery("q", scalarUsersIssuesAST())
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range changes {
		if c.Table == "issues" {
			t.Fatalf("no-match hydrate must emit zero issue rows (ALWAYS_FALSE); got %+v", c)
		}
	}

	// INSERT users{id:u1, name:NULL}: the scalar moves from "no row matched"
	// (undefined) to "row matched, field NULL" (null). TS: newValue = null ??
	// null = null; scalarValuesEqual(null, undefined) = false → RESET with
	// message `${String(undefined)} -> ${String(null)}`.
	sre := advanceScalarResetPanic(t, eng, []SnapshotChange{
		{Table: "users", NextValue: ivm.Row{"id": "u1", "name": nil}},
	})
	if got := sre.Error(); got != "Scalar subquery value changed for users: undefined -> null" {
		t.Fatalf("unexpected reset message: %q", got)
	}
}

// TestScalarValuesEqual_TSIdentityMatrix pins the full === matrix of TS's
// scalarValuesEqual (pipeline-driver.ts:3029-3034) over the tri-state
// LiteralValue | null | undefined domain: undefined === undefined (no row
// matched), null === null (row matched but field was NULL), and undefined
// !== null !== value in every mixed pairing.
func TestScalarValuesEqual_TSIdentityMatrix(t *testing.T) {
	const undef = true
	cases := []struct {
		name   string
		a      ivm.Value
		aUndef bool
		b      ivm.Value
		bUndef bool
		want   bool
	}{
		{"undefined === undefined", nil, undef, nil, undef, true},
		{"null === null", nil, false, nil, false, true},
		{"undefined !== null", nil, undef, nil, false, false},
		{"null !== undefined", nil, false, nil, undef, false},
		{"value === value", "x", false, "x", false, true},
		{"value !== other value", "x", false, "y", false, false},
		{"value !== null", "x", false, nil, false, false},
		{"value !== undefined", "x", false, nil, undef, false},
		// A stray value alongside the undefined flag must not defeat the
		// flag comparison (flag wins, values ignored when both undefined).
		{"undefined(with stray value) === undefined", "x", undef, "y", undef, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := scalarValuesEqual(c.a, c.aUndef, c.b, c.bUndef); got != c.want {
				t.Fatalf("scalarValuesEqual(%v,%v, %v,%v) = %v, want %v",
					c.a, c.aUndef, c.b, c.bUndef, got, c.want)
			}
		})
	}
}
