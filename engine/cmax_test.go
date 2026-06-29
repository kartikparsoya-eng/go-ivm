package engine

import (
	"testing"

	"github.com/kartikparsoya-eng/go-ivm/builder"
	"github.com/kartikparsoya-eng/go-ivm/ivm"
)

// TestComputeCmax_ThreeDeepJoin verifies that concurrencyOfNode computes
// Cmax=3 for a 3-deep join A→B→C (Join(Join(A→B)→C)). This is the
// theoretical max concurrent cursors when the leaf is lazy (holding a
// cursor open while yielding rows). With the current eager leaf, Cmax=1
// in practice, but the computation correctly reflects the operator tree
// structure for when the leaf becomes lazy.
func TestComputeCmax_ThreeDeepJoin(t *testing.T) {
	// Build sources
	aCols := map[string]string{"id": "string"}
	aSrc := ivm.NewMemorySource("a", aCols, []string{"id"})
	bCols := map[string]string{"id": "string", "aId": "string"}
	bSrc := ivm.NewMemorySource("b", bCols, []string{"id"})
	cCols := map[string]string{"id": "string", "bId": "string"}
	cSrc := ivm.NewMemorySource("c", cCols, []string{"id"})

	sort := ivm.Ordering{{"id", "asc"}}
	aConn := aSrc.Connect(sort, nil, nil)
	bConn := bSrc.Connect(sort, nil, nil)
	cConn := cSrc.Connect(sort, nil, nil)

	// Join1: B→C
	join1 := ivm.NewJoin(ivm.JoinArgs{
		Parent:           bConn,
		Child:            cConn,
		ParentKey:        ivm.CompoundKey{"id"},
		ChildKey:         ivm.CompoundKey{"bId"},
		RelationshipName: "cRel",
	})

	// Join2: A→Join1
	join2 := ivm.NewJoin(ivm.JoinArgs{
		Parent:           aConn,
		Child:            join1,
		ParentKey:        ivm.CompoundKey{"id"},
		ChildKey:         ivm.CompoundKey{"aId"},
		RelationshipName: "bRel",
	})

	join2.SetOutput(&testOutput{})

	// Build inputsOf map: for each operator, what are its inputs?
	// Tree: join2 ← aConn, join1
	//       join1 ← bConn, cConn
	inputsOf := map[ivm.InputBase][]ivm.InputBase{
		join2: {aConn, join1},
		join1: {bConn, cConn},
	}

	cmax := concurrencyOfNode(join2, inputsOf, make(map[ivm.InputBase]int))
	if cmax != 3 {
		t.Errorf("3-deep join Cmax: got %d, want 3 (A_cursor + B_cursor + C_cursor)", cmax)
	}
}

// TestComputeCmax_TwoDeepJoin verifies Cmax=2 for a simple A→B join.
func TestComputeCmax_TwoDeepJoin(t *testing.T) {
	aCols := map[string]string{"id": "string"}
	aSrc := ivm.NewMemorySource("a", aCols, []string{"id"})
	bCols := map[string]string{"id": "string", "aId": "string"}
	bSrc := ivm.NewMemorySource("b", bCols, []string{"id"})

	sort := ivm.Ordering{{"id", "asc"}}
	aConn := aSrc.Connect(sort, nil, nil)
	bConn := bSrc.Connect(sort, nil, nil)

	join := ivm.NewJoin(ivm.JoinArgs{
		Parent:           aConn,
		Child:            bConn,
		ParentKey:        ivm.CompoundKey{"id"},
		ChildKey:         ivm.CompoundKey{"aId"},
		RelationshipName: "bRel",
	})

	inputsOf := map[ivm.InputBase][]ivm.InputBase{
		join: {aConn, bConn},
	}

	cmax := concurrencyOfNode(join, inputsOf, make(map[ivm.InputBase]int))
	if cmax != 2 {
		t.Errorf("2-deep join Cmax: got %d, want 2", cmax)
	}
}

// TestComputeCmax_SimpleQuery verifies Cmax=1 for a query with no joins.
func TestComputeCmax_SimpleQuery(t *testing.T) {
	aCols := map[string]string{"id": "string"}
	aSrc := ivm.NewMemorySource("a", aCols, []string{"id"})
	sort := ivm.Ordering{{"id", "asc"}}
	aConn := aSrc.Connect(sort, nil, nil)

	cmax := concurrencyOfNode(aConn, map[ivm.InputBase][]ivm.InputBase{}, make(map[ivm.InputBase]int))
	if cmax != 1 {
		t.Errorf("simple query Cmax: got %d, want 1", cmax)
	}
}

// testOutput is a stub Output for wiring joins in tests.
type testOutput struct{}

func (o *testOutput) Push(change ivm.Change, pusher ivm.InputBase) []ivm.Change {
	return nil
}

// TestComputeCmax_FlippedJoin verifies Cmax=2 for a FlippedJoin (C_parent + C_child).
func TestComputeCmax_FlippedJoin(t *testing.T) {
	aCols := map[string]string{"id": "string"}
	aSrc := ivm.NewMemorySource("a", aCols, []string{"id"})
	bCols := map[string]string{"id": "string", "aId": "string"}
	bSrc := ivm.NewMemorySource("b", bCols, []string{"id"})

	sort := ivm.Ordering{{"id", "asc"}}
	aConn := aSrc.Connect(sort, nil, nil)
	bConn := bSrc.Connect(sort, nil, nil)

	fj := ivm.NewFlippedJoin(ivm.FlippedJoinArgs{
		Parent:           aConn,
		Child:            bConn,
		ParentKey:        ivm.CompoundKey{"id"},
		ChildKey:         ivm.CompoundKey{"aId"},
		RelationshipName: "bRel",
	})

	inputsOf := map[ivm.InputBase][]ivm.InputBase{
		fj: {aConn, bConn},
	}

	cmax := concurrencyOfNode(fj, inputsOf, make(map[ivm.InputBase]int))
	if cmax != 2 {
		t.Errorf("FlippedJoin Cmax: got %d, want 2", cmax)
	}
}

// TestComputeConcurrency_PipelineIntegration verifies computeConcurrency
// on a builder.Pipeline built with a 2-deep join.
func TestComputeConcurrency_PipelineIntegration(t *testing.T) {
	aCols := map[string]string{"id": "string"}
	aSrc := ivm.NewMemorySource("a", aCols, []string{"id"})
	bCols := map[string]string{"id": "string", "aId": "string"}
	bSrc := ivm.NewMemorySource("b", bCols, []string{"id"})

	sort := ivm.Ordering{{"id", "asc"}}
	aConn := aSrc.Connect(sort, nil, nil)
	bConn := bSrc.Connect(sort, nil, nil)

	join := ivm.NewJoin(ivm.JoinArgs{
		Parent:           aConn,
		Child:            bConn,
		ParentKey:        ivm.CompoundKey{"id"},
		ChildKey:         ivm.CompoundKey{"aId"},
		RelationshipName: "bRel",
	})

	// Build a minimal Pipeline with edges: aConn→join, bConn→join
	p := &builder.Pipeline{
		Input: join,
		Edges: [][2]ivm.InputBase{
			{aConn, join},
			{bConn, join},
		},
	}

	cmax := computeConcurrency(p)
	if cmax != 2 {
		t.Errorf("pipeline 2-deep join Cmax: got %d, want 2", cmax)
	}
}

// Ensure builder.Pipeline is used (import guard)
var _ = builder.Pipeline{}
