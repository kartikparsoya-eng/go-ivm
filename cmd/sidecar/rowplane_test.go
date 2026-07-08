package main

// Regression tests for the row plane's ALL-OR-NOTHING partial contract
// (REVIEW-napi-transport B2). Before the fix, emitChanges delivered
// encodable rows as records IMMEDIATELY while unencodable ones waited for
// the trailing fallback frame — reordering changes WITHIN a partial: [add X
// (fallback), remove X (record)] arrived at the client as remove-then-add →
// net phantom row → drift. (The unencodable trigger in these tests is a
// heterogeneous row — a column outside the group's canonical order; the
// original remove-first trigger now encodes via replacement defs.)
//
// Drives rowPlane directly with a sinkCollector (no engine, no addon):
// the contract under test is purely the record/frame routing.

import (
	"testing"

	"github.com/kartikparsoya-eng/go-ivm/engine"
	"github.com/kartikparsoya-eng/go-ivm/ivm"
)

func rowPlaneForTest(t *testing.T, col *sinkCollector, reqID float64) *rowPlane {
	t.Helper()
	rp := newRowPlane(&Server{abiDeliver: col.sink}, reqID, true, "cg-test", nil)
	if rp == nil {
		t.Fatal("newRowPlane returned nil with abiDeliver set + numeric id")
	}
	return rp
}

func rcAdd(q, id string) engine.RowChange {
	return engine.RowChange{
		Type: engine.RowChangeAdd, QueryID: q, Table: "t",
		RowKey: map[string]interface{}{"id": id},
		Row:    ivm.Row{"id": id, "n": float64(1)},
	}
}

func rcRemove(q, id string) engine.RowChange {
	return engine.RowChange{
		Type: engine.RowChangeRemove, QueryID: q, Table: "t",
		RowKey: map[string]interface{}{"id": id},
	}
}

// countKinds tallies the collector's entries by delivery kind.
func countKinds(col *sinkCollector) (defs, rows, frames int) {
	col.mu.Lock()
	defer col.mu.Unlock()
	for _, e := range col.entries {
		switch e.kind {
		case abiKindGroupDef:
			defs++
		case abiKindRow:
			rows++
		case abiKindFrame:
			frames++
		}
	}
	return
}

// TestRowPlane_MixedPartialAllOrNothing is the B2 repro: a partial holding
// one unencodable add (foreign column outside the group's canonical order)
// and one encodable remove must ship ENTIRELY as one frame, in original
// change order, with ZERO records. (This originally used a remove-first
// group as the unencodable trigger; the user's-audit fix made those encode
// via replacement defs, so the trigger is now a heterogeneous row — the
// remaining unencodable shape.)
func TestRowPlane_MixedPartialAllOrNothing(t *testing.T) {
	col := newSinkCollector()
	rp := rowPlaneForTest(t, col, 5)

	// Partial 1: an ordinary add interns group (q1,t) with cols {id,n}.
	rp.emitAdvanceToHeadPartial(engine.AdvanceStreamPartial{
		Changes: []engine.RowChange{rcAdd("q1", "seed")}, ChunkIndex: 0,
	}, "", 0)
	defs, rows, frames := countKinds(col)
	if defs != 1 || rows != 1 || frames != 0 {
		t.Fatalf("after partial1: defs=%d rows=%d frames=%d, want 1/1/0", defs, rows, frames)
	}

	// Partial 2: [add X (UNencodable — carries a column outside the
	// canonical order), remove X (encodable)]. Pre-fix: remove left as a
	// record BEFORE the add's fallback frame → client applied
	// remove-then-add → phantom X. Post-fix: zero new records; ONE frame
	// carrying both changes in original order.
	heteroAdd := rcAdd("q1", "x")
	heteroAdd.Row = ivm.Row{"id": "x", "zz": float64(9)} // zz ∉ {id,n}
	rp.emitAdvanceToHeadPartial(engine.AdvanceStreamPartial{
		Changes:    []engine.RowChange{heteroAdd, rcRemove("q1", "x")},
		ChunkIndex: 1,
	}, "", 0)
	defs, rows, frames = countKinds(col)
	if rows != 1 {
		t.Fatalf("mixed partial leaked %d row record(s) — must be all-or-nothing", rows-1+1)
	}
	if frames != 1 {
		t.Fatalf("mixed partial: frames=%d, want exactly 1", frames)
	}

	// Decode the frame: both changes present, ORIGINAL order (add then
	// remove — positional rows carry [dictIdx, type, ...]).
	col.mu.Lock()
	var framePayload []byte
	for _, e := range col.entries {
		if e.kind == abiKindFrame {
			framePayload = e.payload
		}
	}
	col.mu.Unlock()
	resp := decodeResp(t, framePayload)
	m, ok := resp.Result.(map[string]interface{})
	if !ok {
		t.Fatalf("frame result not a map: %#v", resp.Result)
	}
	rowsAny, _ := m["r"].([]interface{})
	if len(rowsAny) != 2 {
		t.Fatalf("frame rows = %d, want 2 (the WHOLE partial)", len(rowsAny))
	}
	typeAt := func(i int) float64 {
		row := rowsAny[i].([]interface{})
		v, _ := toFloat(row[1])
		return v
	}
	if typeAt(0) != float64(engine.RowChangeAdd) || typeAt(1) != float64(engine.RowChangeRemove) {
		t.Fatalf("frame change order = [%v, %v], want [add=0, remove=1] (original order)",
			typeAt(0), typeAt(1))
	}
}

// TestRowPlane_AllEncodablePartialStaysOnRecordPlane pins the fast path:
// a fully encodable multi-change partial delivers every row as a record,
// in order, with NO frame (non-final partials produce no frame — that's
// the win), and a later encodable partial for the same group still flows
// as records (the eager def from partial 1 is reused).
func TestRowPlane_AllEncodablePartialStaysOnRecordPlane(t *testing.T) {
	col := newSinkCollector()
	rp := rowPlaneForTest(t, col, 7)

	rp.emitAdvanceToHeadPartial(engine.AdvanceStreamPartial{
		Changes: []engine.RowChange{rcAdd("q1", "a"), rcAdd("q1", "b"), rcRemove("q1", "a")},
	}, "", 0)
	defs, rows, frames := countKinds(col)
	if defs != 1 || rows != 3 || frames != 0 {
		t.Fatalf("defs=%d rows=%d frames=%d, want 1/3/0", defs, rows, frames)
	}

	// Same group again — def must NOT re-deliver; rows keep flowing.
	rp.emitAdvanceToHeadPartial(engine.AdvanceStreamPartial{
		Changes: []engine.RowChange{rcAdd("q1", "c")}, ChunkIndex: 1,
	}, "", 0)
	defs, rows, frames = countKinds(col)
	if defs != 1 || rows != 4 || frames != 0 {
		t.Fatalf("after partial2: defs=%d rows=%d frames=%d, want 1/4/0", defs, rows, frames)
	}
}

// TestRowPlane_MixedFinalPartialCarriesEverything: a FINAL partial with
// mixed encodability ships one Final frame with all its changes and zero
// records — the terminal signal and the fallback rows must not split.
func TestRowPlane_MixedFinalPartialCarriesEverything(t *testing.T) {
	col := newSinkCollector()
	rp := rowPlaneForTest(t, col, 9)

	// Ordinary group, then a final partial mixing a heterogeneous
	// (unencodable) add with an encodable remove.
	rp.emitAdvanceToHeadPartial(engine.AdvanceStreamPartial{
		Changes: []engine.RowChange{rcAdd("q2", "seed")},
	}, "", 0)
	heteroAdd := rcAdd("q2", "y")
	heteroAdd.Row = ivm.Row{"id": "y", "zz": float64(1)} // zz ∉ canonical cols
	rp.emitAdvanceToHeadPartial(engine.AdvanceStreamPartial{
		Changes:    []engine.RowChange{heteroAdd, rcRemove("q2", "z")},
		ChunkIndex: 1,
		Final:      true,
	}, "v1", 2)
	_, rows, frames := countKinds(col)
	if rows != 1 || frames != 1 {
		t.Fatalf("rows=%d frames=%d, want 1 (seed only) / 1 (final)", rows, frames)
	}
	col.mu.Lock()
	var framePayload []byte
	for _, e := range col.entries {
		if e.kind == abiKindFrame {
			framePayload = e.payload
		}
	}
	col.mu.Unlock()
	m := decodeResp(t, framePayload).Result.(map[string]interface{})
	if fin, _ := m["final"].(bool); !fin {
		t.Fatal("final flag lost on the mixed final frame")
	}
	if rowsAny, _ := m["r"].([]interface{}); len(rowsAny) != 2 {
		t.Fatalf("final frame rows = %d, want 2", len(rowsAny))
	}
}
