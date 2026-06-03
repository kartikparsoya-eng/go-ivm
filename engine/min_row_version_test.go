package engine

import (
	"testing"

	"github.com/kartikparsoya-eng/go-ivm/ivm"
)

// TestBumpRowVersions verifies the audit-K minRowVersion bump: non-REMOVE
// rows whose _0_version is below the table's minRowVersion are rewritten up
// to it; everything else is left untouched, and the source row map is never
// mutated in place (copy-on-bump). Port of TS streamNodes
// (pipeline-driver.ts:3172-3178).
func TestBumpRowVersions(t *testing.T) {
	mrv := map[string]string{"messages": "0e"}

	belowRow := ivm.Row{"id": "m1", zeroVersionColumn: "0a"} // below → bump
	aboveRow := ivm.Row{"id": "m2", zeroVersionColumn: "0z"} // above → keep
	rmRow := ivm.Row{"id": "m3", zeroVersionColumn: "0a"}    // REMOVE → keep
	otherTbl := ivm.Row{"id": "t1", zeroVersionColumn: "0a"} // no mrv → keep
	noVerCol := ivm.Row{"id": "m4"}                          // no version col → keep

	changes := []RowChange{
		{Type: RowChangeAdd, Table: "messages", Row: belowRow},
		{Type: RowChangeEdit, Table: "messages", Row: aboveRow},
		{Type: RowChangeRemove, Table: "messages", Row: rmRow},
		{Type: RowChangeAdd, Table: "tickets", Row: otherTbl},
		{Type: RowChangeAdd, Table: "messages", Row: noVerCol},
	}

	out := bumpRowVersions(changes, mrv)

	if got := out[0].Row[zeroVersionColumn]; got != "0e" {
		t.Fatalf("below-min ADD: expected bump to 0e, got %v", got)
	}
	if got := out[1].Row[zeroVersionColumn]; got != "0z" {
		t.Fatalf("above-min EDIT: expected unchanged 0z, got %v", got)
	}
	if got := out[2].Row[zeroVersionColumn]; got != "0a" {
		t.Fatalf("REMOVE: expected unchanged 0a, got %v", got)
	}
	if got := out[3].Row[zeroVersionColumn]; got != "0a" {
		t.Fatalf("no-minRowVersion table: expected unchanged 0a, got %v", got)
	}
	if _, ok := out[4].Row[zeroVersionColumn]; ok {
		t.Fatalf("row without version column: expected no version added")
	}

	// Copy-on-bump: the original source row map must not have been mutated.
	if got := belowRow[zeroVersionColumn]; got != "0a" {
		t.Fatalf("source row mutated in place: expected 0a, got %v", got)
	}

	// Empty minRowVersions is a pass-through no-op.
	in := []RowChange{{Type: RowChangeAdd, Table: "messages", Row: ivm.Row{zeroVersionColumn: "0a"}}}
	if out := bumpRowVersions(in, nil); out[0].Row[zeroVersionColumn] != "0a" {
		t.Fatalf("nil minRowVersions should be a no-op")
	}
}
