package engine

// Regression test for faithfulness #4: node-level relationship wire order.
//
// TS's wire emitter walks Object.entries of the NODE's own relationships
// object (pipeline-driver.ts:2861). When union fan-in merges accumulated
// same-type pushes from multiple branches, mergeRelationships builds
// {...right, ...left} (push-accumulated.ts:271-278) — the LATER branch's
// relationship names come FIRST. Pre-fix Go walked schema.RelationshipOrder
// (branch-declaration order) instead, emitting the merged node's sibling
// child rows in the opposite order.

import (
	"testing"

	"github.com/kartikparsoya-eng/go-ivm/ivm"
)

// streamerShim forwards UFI output pushes into a Streamer, like the engine's
// pipelineOutput does.
type streamerShim struct {
	str    *Streamer
	schema *ivm.SourceSchema
}

func (s *streamerShim) Push(change ivm.Change, _ ivm.InputBase) {
	s.str.Accumulate("q", s.schema, []ivm.Change{change})
}

func TestUnionFanInAccumulatedMergeWireOrder(t *testing.T) {
	src := ivm.NewMemorySource("t", map[string]string{"id": "string"}, []string{"id"})
	conn := src.Connect(ivm.Ordering{{"id", "asc"}}, nil, nil)
	ufo := ivm.NewUnionFanOut(conn)

	childZ := ivm.NewMemorySource("cz",
		map[string]string{"id": "string", "tid": "string"}, []string{"id"})
	childZ.BulkInsert([]ivm.Row{{"id": "c1", "tid": "p1"}})
	childA := ivm.NewMemorySource("ca",
		map[string]string{"id": "string", "tid": "string"}, []string{"id"})
	childA.BulkInsert([]ivm.Row{{"id": "c2", "tid": "p1"}})

	branch := func(child *ivm.MemorySource, name string) ivm.Input {
		fs := ivm.NewFilterStart(ufo)
		f := ivm.NewFilter(fs, func(ivm.Row) bool { return true })
		end := ivm.NewFilterEnd(fs, f)
		return ivm.NewJoin(ivm.JoinArgs{
			Parent:           end,
			Child:            child.Connect(ivm.Ordering{{"id", "asc"}}, nil, nil),
			ParentKey:        ivm.CompoundKey{"id"},
			ChildKey:         ivm.CompoundKey{"tid"},
			RelationshipName: name,
			System:           "client",
		})
	}
	// Branch 1 attaches "zzz" (childZ), branch 2 attaches "aaa" (childA) —
	// schema order is [zzz, aaa] (branch order, union-fan-in.ts:52-88).
	b1 := branch(childZ, "zzz")
	b2 := branch(childA, "aaa")
	ufi := ivm.NewUnionFanIn(ufo, []ivm.Input{b1, b2})

	str := NewStreamer()
	ufi.SetOutput(&streamerShim{str: str, schema: ufi.GetSchema()})

	// An ADD entering the fan-out reaches both branches; each Join attaches
	// its own relationship; the fan-in merges the two accumulated ADDs for
	// the same row. TS's merged node iterates {...b2, ...b1} → [aaa, zzz]:
	// the wire carries ca's child row BEFORE cz's. Pre-fix Go emitted schema
	// order [zzz, aaa] → cz first.
	src.Push(ivm.SourceChange{Type: ivm.ChangeTypeAdd, Row: ivm.Row{"id": "p1"}})

	rows := str.Stream()
	tables := make([]string, len(rows))
	for i, rc := range rows {
		tables[i] = rc.Table
	}
	want := []string{"t", "ca", "cz"}
	if len(tables) != len(want) {
		t.Fatalf("got %d row changes %v, want %d %v", len(tables), tables, len(want), want)
	}
	for i := range want {
		if tables[i] != want[i] {
			t.Fatalf("wire table order = %v, want %v (later-branch relationships first, TS {...right, ...left})", tables, want)
		}
	}
}
