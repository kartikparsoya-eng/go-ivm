package main

import (
	"reflect"
	"testing"

	"github.com/kartikparsoya-eng/go-ivm/engine"
	"github.com/kartikparsoya-eng/go-ivm/ivm"
)

// Positional (rev 9) sends column keys ONCE per (queryID,table) group, so the
// dangerous inputs are rows in one group with DIFFERENT key sets. Pins the
// encoder's documented behavior:
//   - Cols is the UNION across the group's add/edit rows;
//   - a row missing a union column round-trips with that column = nil
//     (absent→null conflation — safe because TS normalizes undefined to null;
//     rows from FromSQLiteType are schema-complete anyway);
//   - the group's PK is taken from the FIRST change seen, including when that
//     first change is a Remove (whose RowKey carries only PK columns);
//   - JSON-typed column values (nested map/array) survive the round-trip.
func TestPositional_SparseRowsAndRemoveFirst(t *testing.T) {
	in := []engine.RowChange{
		// Remove FIRST in the group → dict PK derived from a remove's RowKey.
		{
			Type: engine.RowChangeRemove, QueryID: "q1", Table: "tickets",
			RowKey: map[string]interface{}{"id": "t0"},
		},
		// Sparse add: no "assignee", no "meta".
		{
			Type: engine.RowChangeAdd, QueryID: "q1", Table: "tickets",
			RowKey: map[string]interface{}{"id": "t1"},
			Row:    ivm.Row{"id": "t1", "title": "a"},
		},
		// Wider add: carries "assignee" + a JSON object column.
		{
			Type: engine.RowChangeAdd, QueryID: "q1", Table: "tickets",
			RowKey: map[string]interface{}{"id": "t2"},
			Row: ivm.Row{
				"id": "t2", "title": "b", "assignee": "u9",
				"meta": map[string]interface{}{"tags": []interface{}{"x", float64(1)}},
			},
		},
	}

	got := fromPositional(toPositional(in))
	if len(got) != 3 {
		t.Fatalf("round-trip length %d, want 3", len(got))
	}

	// Remove: PK rowKey intact, Row nil.
	if got[0].Type != engine.RowChangeRemove || got[0].Row != nil ||
		!reflect.DeepEqual(got[0].RowKey, in[0].RowKey) {
		t.Fatalf("remove mangled: %+v", got[0])
	}

	// Sparse row: original columns intact; union columns it lacked come back
	// as EXPLICIT nils (never as garbage or another row's values).
	r1 := got[1].Row
	if r1["id"] != "t1" || r1["title"] != "a" {
		t.Fatalf("sparse row lost data: %+v", r1)
	}
	if v, present := r1["assignee"]; present && v != nil {
		t.Fatalf("absent column must round-trip as nil, got %v", v)
	}
	if v, present := r1["meta"]; present && v != nil {
		t.Fatalf("absent JSON column must round-trip as nil, got %v", v)
	}

	// Wide row: everything preserved, including the nested JSON value.
	r2 := got[2].Row
	if r2["assignee"] != "u9" {
		t.Fatalf("wide row lost scalar: %+v", r2)
	}
	if !reflect.DeepEqual(r2["meta"], in[2].Row["meta"]) {
		t.Fatalf("JSON column mangled: %#v vs %#v", r2["meta"], in[2].Row["meta"])
	}
	// rowKey re-derived from PK columns.
	if !reflect.DeepEqual(got[2].RowKey, in[2].RowKey) {
		t.Fatalf("rowKey mangled: %+v", got[2].RowKey)
	}
}
