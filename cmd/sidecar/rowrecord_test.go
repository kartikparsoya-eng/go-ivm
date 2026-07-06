package main

// Row-record decode mirror + row-mode end-to-end tests.
//
// decodeGroupDef/decodeRowRecord mirror EXACTLY what the TS side's record
// parser (napi-records.ts) does with a DataView — they exist to lock the
// binary layout with an executable spec on the Go side and to back the
// round-trip tests below. Any layout change must update both in lockstep.

import (
	"encoding/binary"
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/kartikparsoya-eng/go-ivm/engine"
	"github.com/kartikparsoya-eng/go-ivm/ivm"
	"github.com/kartikparsoya-eng/go-ivm/sqlite"
)

type decodedGroupDef struct {
	reqID   float64
	groupID uint32
	queryID string
	table   string
	cols    []string
	pk      []string
}

type decodedRow struct {
	reqID      float64
	groupID    uint32
	changeType int
	values     []interface{}
}

type recReader struct {
	buf []byte
	off int
}

func (r *recReader) u16() uint16 {
	v := binary.LittleEndian.Uint16(r.buf[r.off:])
	r.off += 2
	return v
}
func (r *recReader) u32() uint32 {
	v := binary.LittleEndian.Uint32(r.buf[r.off:])
	r.off += 4
	return v
}
func (r *recReader) f64() float64 {
	v := math.Float64frombits(binary.LittleEndian.Uint64(r.buf[r.off:]))
	r.off += 8
	return v
}
func (r *recReader) u8() byte {
	v := r.buf[r.off]
	r.off++
	return v
}
func (r *recReader) shortStr() string {
	n := int(r.u16())
	s := string(r.buf[r.off : r.off+n])
	r.off += n
	return s
}
func (r *recReader) longBytes() []byte {
	n := int(r.u32())
	b := r.buf[r.off : r.off+n]
	r.off += n
	return b
}

func decodeGroupDef(t *testing.T, rec []byte) decodedGroupDef {
	t.Helper()
	r := &recReader{buf: rec}
	d := decodedGroupDef{reqID: r.f64(), groupID: r.u32()}
	d.queryID = r.shortStr()
	d.table = r.shortStr()
	ncols := int(r.u16())
	for i := 0; i < ncols; i++ {
		d.cols = append(d.cols, r.shortStr())
	}
	npk := int(r.u16())
	for i := 0; i < npk; i++ {
		_ = r.u16() // pk column index (0xFFFF when not a column reference)
		d.pk = append(d.pk, r.shortStr())
	}
	if r.off != len(rec) {
		t.Fatalf("groupDef: %d trailing bytes", len(rec)-r.off)
	}
	return d
}

func decodeRowRecord(t *testing.T, rec []byte, nvalues int) decodedRow {
	t.Helper()
	r := &recReader{buf: rec}
	d := decodedRow{reqID: r.f64(), groupID: r.u32(), changeType: int(r.u8())}
	for i := 0; i < nvalues; i++ {
		switch tag := r.u8(); tag {
		case rowValNull:
			d.values = append(d.values, nil)
		case rowValFalse:
			d.values = append(d.values, false)
		case rowValTrue:
			d.values = append(d.values, true)
		case rowValF64:
			d.values = append(d.values, r.f64())
		case rowValI64:
			d.values = append(d.values, int64(binary.LittleEndian.Uint64(r.buf[r.off:])))
			r.off += 8
		case rowValStr:
			d.values = append(d.values, string(r.longBytes()))
		case rowValBlob:
			var v interface{}
			if err := mpUnmarshal(r.longBytes(), &v); err != nil {
				t.Fatalf("blob unmarshal: %v", err)
			}
			d.values = append(d.values, v)
		default:
			t.Fatalf("unknown value tag %d at offset %d", tag, r.off-1)
		}
	}
	if r.off != len(rec) {
		t.Fatalf("row: %d trailing bytes (decoded %d values)", len(rec)-r.off, nvalues)
	}
	return d
}

// TestRowRecordEncoder_RoundTrip locks the record layout: groupDef + rows
// for add/remove/edit with every value tag, decoded by the TS-mirroring
// reader above.
func TestRowRecordEncoder_RoundTrip(t *testing.T) {
	enc := newRowRecordEncoder(42)

	add := engine.RowChange{
		Type:    engine.RowChangeAdd,
		QueryID: "q1",
		Table:   "users",
		RowKey:  map[string]interface{}{"id": "u1"},
		Row: ivm.Row{
			"id":       "u1",
			"age":      float64(30),
			"count":    int64(7),
			"active":   true,
			"disabled": false,
			"note":     nil,
			"meta":     map[string]interface{}{"k": "v"},
		},
	}
	g, def := enc.groupFor(&add)
	if def == nil {
		t.Fatal("first sight must emit a groupDef")
	}
	dd := decodeGroupDef(t, def)
	if dd.reqID != 42 || dd.groupID != 0 || dd.queryID != "q1" || dd.table != "users" {
		t.Fatalf("groupDef header mismatch: %+v", dd)
	}
	wantCols := []string{"active", "age", "count", "disabled", "id", "meta", "note"}
	if fmt.Sprint(dd.cols) != fmt.Sprint(wantCols) {
		t.Fatalf("cols: got %v want %v (sorted first-row keys)", dd.cols, wantCols)
	}
	if fmt.Sprint(dd.pk) != fmt.Sprint([]string{"id"}) {
		t.Fatalf("pk: got %v", dd.pk)
	}

	rec, ok := enc.encodeRow(g, &add)
	if !ok {
		t.Fatal("add row must encode")
	}
	dr := decodeRowRecord(t, rec, len(dd.cols))
	if dr.changeType != engine.RowChangeAdd || dr.groupID != 0 || dr.reqID != 42 {
		t.Fatalf("row header mismatch: %+v", dr)
	}
	// Values arrive in sorted column order: active,age,count,disabled,id,meta,note
	if dr.values[0] != true || dr.values[3] != false || dr.values[6] != nil {
		t.Fatalf("bool/null values wrong: %v", dr.values)
	}
	if dr.values[1] != float64(30) || dr.values[2] != int64(7) || dr.values[4] != "u1" {
		t.Fatalf("scalar values wrong: %v", dr.values)
	}
	if m, ok := dr.values[5].(map[string]interface{}); !ok || m["k"] != "v" {
		t.Fatalf("blob value wrong: %#v", dr.values[5])
	}

	// Second row, same group: NO def re-emitted.
	if _, def2 := enc.groupFor(&add); def2 != nil {
		t.Fatal("second sight re-emitted groupDef")
	}

	// Remove: PK values only.
	rm := engine.RowChange{
		Type:    engine.RowChangeRemove,
		QueryID: "q1",
		Table:   "users",
		RowKey:  map[string]interface{}{"id": "u1"},
	}
	gRm, defRm := enc.groupFor(&rm)
	if defRm != nil || gRm != g {
		t.Fatal("remove must reuse the interned group")
	}
	recRm, ok := enc.encodeRow(gRm, &rm)
	if !ok {
		t.Fatal("remove must encode")
	}
	drm := decodeRowRecord(t, recRm, len(gRm.pk))
	if drm.changeType != engine.RowChangeRemove || drm.values[0] != "u1" {
		t.Fatalf("remove decode wrong: %+v", drm)
	}

	// Remove-FIRST group: def emitted with PK-only cols; a later add/edit
	// mints a REPLACEMENT group with full columns (user's-audit fix —
	// pre-fix the (queryID,table) was pinned to the frame plane forever).
	rmFirst := engine.RowChange{
		Type:    engine.RowChangeRemove,
		QueryID: "q2",
		Table:   "posts",
		RowKey:  map[string]interface{}{"pid": int64(9)},
	}
	g2, def2 := enc.groupFor(&rmFirst)
	if def2 == nil {
		t.Fatal("new group must emit def")
	}
	d2 := decodeGroupDef(t, def2)
	if len(d2.cols) != 0 || fmt.Sprint(d2.pk) != fmt.Sprint([]string{"pid"}) {
		t.Fatalf("remove-first def wrong: %+v", d2)
	}
	if rec, ok := enc.encodeRow(g2, &rmFirst); !ok || len(rec) == 0 {
		t.Fatal("remove-first row must encode")
	}
	addLater := engine.RowChange{
		Type:    engine.RowChangeAdd,
		QueryID: "q2",
		Table:   "posts",
		RowKey:  map[string]interface{}{"pid": int64(9)},
		Row:     ivm.Row{"pid": int64(9), "title": "x"},
	}
	gL, defL := enc.groupFor(&addLater)
	if defL == nil {
		t.Fatal("first add after a remove-first def must mint a REPLACEMENT def")
	}
	dL := decodeGroupDef(t, defL)
	if dL.groupID == d2.groupID {
		t.Fatalf("replacement def must carry a FRESH groupID (got %d twice) — "+
			"defs are immutable JS-side", dL.groupID)
	}
	if fmt.Sprint(dL.cols) != fmt.Sprint([]string{"pid", "title"}) {
		t.Fatalf("replacement def cols = %v, want [pid title]", dL.cols)
	}
	if _, ok := enc.encodeRow(gL, &addLater); !ok {
		t.Fatal("add after the replacement def must encode as a record " +
			"(pre-fix it fell back to the frame plane forever)")
	}
}

// TestEncodeRow_HomogeneityIsMembershipNotLength pins the homogeneity guard
// as a MEMBERSHIP check. The original guard was length-only
// (len(c.Row) > len(g.cols)), so a row carrying a column OUTSIDE the
// group's canonical order but with equal-or-smaller size ({a,b,x} vs
// canonical {a,b,c}) encoded silently wrong: x dropped, c fabricated as
// null — while the file header claimed the defensive fallback covered
// extra columns. The frame path (positional.go per-chunk sorted union) is
// immune, so falling back is always correct.
func TestEncodeRow_HomogeneityIsMembershipNotLength(t *testing.T) {
	enc := newRowRecordEncoder(7)

	first := engine.RowChange{
		Type:    engine.RowChangeAdd,
		QueryID: "qh",
		Table:   "t1",
		RowKey:  map[string]interface{}{"a": "k1"},
		Row:     ivm.Row{"a": "k1", "b": float64(1), "c": float64(2)},
	}
	g, def := enc.groupFor(&first)
	if def == nil {
		t.Fatal("first sight must emit a groupDef")
	}
	if fmt.Sprint(g.cols) != fmt.Sprint([]string{"a", "b", "c"}) {
		t.Fatalf("canonical cols = %v, want [a b c]", g.cols)
	}

	mkAdd := func(row ivm.Row) engine.RowChange {
		return engine.RowChange{
			Type: engine.RowChangeAdd, QueryID: "qh", Table: "t1",
			RowKey: map[string]interface{}{"a": "k"}, Row: row,
		}
	}

	// EQUAL size, foreign column: {a,b,x} vs {a,b,c}. The length-only
	// guard waved this through (x dropped, c encoded null); membership
	// must reject it to the frame path.
	sameSize := mkAdd(ivm.Row{"a": "k2", "b": float64(3), "x": float64(9)})
	if _, ok := enc.encodeRow(g, &sameSize); ok {
		t.Fatal("row with a foreign column (same size as canonical) must fall back to the frame path")
	}

	// SMALLER size, foreign column: {a,x} vs {a,b,c} — same hole.
	smaller := mkAdd(ivm.Row{"a": "k3", "x": float64(9)})
	if _, ok := enc.encodeRow(g, &smaller); ok {
		t.Fatal("row with a foreign column (smaller than canonical) must fall back to the frame path")
	}

	// SUBSET row {a,b}: intended leniency preserved — encodes with c=null.
	subset := mkAdd(ivm.Row{"a": "k4", "b": float64(5)})
	rec, ok := enc.encodeRow(g, &subset)
	if !ok {
		t.Fatal("subset row (keys ⊆ canonical cols) must still encode")
	}
	dr := decodeRowRecord(t, rec, len(g.cols))
	if dr.values[0] != "k4" || dr.values[1] != float64(5) || dr.values[2] != nil {
		t.Fatalf("subset row values = %v, want [k4 5 <nil>] (missing canonical col encodes null)", dr.values)
	}

	// Exact canonical row: still encodes.
	exact := mkAdd(ivm.Row{"a": "k5", "b": float64(6), "c": float64(7)})
	if _, ok := enc.encodeRow(g, &exact); !ok {
		t.Fatal("exact canonical row must encode")
	}

	// Removes get the same discipline against the interned PK (pk = [a]).
	// A RowKey keyed differently would otherwise encode null for the
	// missing PK column and silently drop the foreign one — a remove
	// targeting the wrong key at the client.
	badRm := engine.RowChange{
		Type: engine.RowChangeRemove, QueryID: "qh", Table: "t1",
		RowKey: map[string]interface{}{"z": "k1"},
	}
	if _, ok := enc.encodeRow(g, &badRm); ok {
		t.Fatal("remove whose RowKey is keyed outside the interned PK must fall back")
	}
	// PK-SUBSET RowKey (review C4): group interned pk=[a]; craft a group
	// with pk=[a,b] and remove keyed {a} only — found==len(RowKey) passes
	// the one-sided check but the missing PK col would encode null.
	firstAB := engine.RowChange{
		Type: engine.RowChangeAdd, QueryID: "qpk2", Table: "t2",
		RowKey: map[string]interface{}{"a": "x", "b": "y"},
		Row:    ivm.Row{"a": "x", "b": "y", "c": float64(1)},
	}
	g2, _ := enc.groupFor(&firstAB)
	subsetRm := engine.RowChange{
		Type: engine.RowChangeRemove, QueryID: "qpk2", Table: "t2",
		RowKey: map[string]interface{}{"a": "x"},
	}
	if _, ok := enc.encodeRow(g2, &subsetRm); ok {
		t.Fatal("remove whose RowKey is a strict SUBSET of the interned PK must fall back (would encode null PK)")
	}
	goodRm := engine.RowChange{
		Type: engine.RowChangeRemove, QueryID: "qh", Table: "t1",
		RowKey: map[string]interface{}{"a": "k1"},
	}
	recRm, ok := enc.encodeRow(g, &goodRm)
	if !ok {
		t.Fatal("well-keyed remove must encode")
	}
	if drm := decodeRowRecord(t, recRm, len(g.pk)); drm.values[0] != "k1" {
		t.Fatalf("remove PK value = %v, want k1", drm.values)
	}
}

// TestABIHost_RowModeHydrateEndToEnd drives the full stack in-process:
// init (table mode) → addQueriesStream with rowMode → asserts per-row
// records arrive (kind 2/3), the terminal frame arrives (kind 1, final),
// rows decode to the replica content, and the ordering invariant (defs
// before their rows, rows before the query's final frame, final before
// done) holds on the single delivery queue.
func TestABIHost_RowModeHydrateEndToEnd(t *testing.T) {
	col := newSinkCollector()
	path, db := makeReplica(t)
	mustExec(t, db, `CREATE TABLE "users" ("id" TEXT PRIMARY KEY, "name" TEXT, "age" INTEGER, "_0_version" TEXT)`)
	mustExec(t, db, `INSERT INTO "users" VALUES ('u1','alice',30,'0000000001')`)
	mustExec(t, db, `INSERT INTO "users" VALUES ('u2','bob',25,'0000000001')`)
	mustExec(t, db, `INSERT INTO "users" VALUES ('u3','carol',35,'0000000001')`)

	h := startABIHostWithServer(NewServer(path), col.sink, nil)
	defer h.Shutdown()

	send := func(id float64, method string, params interface{}) {
		t.Helper()
		if err := h.Send(encodeReq(t, method, id, params)); err != nil {
			t.Fatalf("send %s: %v", method, err)
		}
	}

	send(1, "init", initParams{
		ClientGroupID: "cg-rows",
		Tables: map[string]tableSchemaParams{
			"users": {
				Columns: map[string]sqlite.ColumnSchema{
					"id":         {Type: "string"},
					"name":       {Type: "string"},
					"age":        {Type: "number"},
					"_0_version": {Type: "string"},
				},
				PrimaryKey: []string{"id"},
			},
		},
	})
	send(2, "addQueriesStream", map[string]interface{}{
		"clientGroupID": "cg-rows",
		"initEpoch":     1,
		"rowMode":       true,
		"queries": []map[string]interface{}{
			{"queryID": "q-all", "ast": map[string]interface{}{
				"table":   "users",
				"orderBy": [][]string{{"id", "asc"}},
			}},
		},
	})

	// Expected deliveries for req id 2: 1 groupDef + 3 rows + 1 final frame,
	// then the done frame. Plus the init response frame (id 1).
	deadline := time.Now().Add(15 * time.Second)
	var entries []sinkEntry
	for {
		col.mu.Lock()
		entries = append(entries[:0], col.entries...)
		col.mu.Unlock()
		var defs, rows, frames2 int
		for _, e := range entries {
			switch e.kind {
			case abiKindGroupDef:
				defs++
			case abiKindRow:
				rows++
			case abiKindFrame:
				resp := decodeResp(t, e.payload)
				if id, ok := toFloat(resp.ID); ok && id == 2 {
					frames2++
				}
			}
		}
		if defs >= 1 && rows >= 3 && frames2 >= 2 { // final partial + done
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out: defs=%d rows=%d frames(id=2)=%d entries=%d",
				defs, rows, frames2, len(entries))
		}
		time.Sleep(10 * time.Millisecond)
	}

	// Decode + assert content and per-queue ordering.
	var def *decodedGroupDef
	var gotNames []string
	sawFinalFrame := false
	sawDone := false
	for _, e := range entries {
		switch e.kind {
		case abiKindGroupDef:
			d := decodeGroupDef(t, e.payload)
			if d.reqID != 2 {
				t.Fatalf("groupDef for wrong req: %v", d.reqID)
			}
			if def != nil {
				t.Fatal("duplicate groupDef for single-group query")
			}
			def = &d
			if d.queryID != "q-all" || d.table != "users" {
				t.Fatalf("groupDef content: %+v", d)
			}
		case abiKindRow:
			if def == nil {
				t.Fatal("ORDERING VIOLATION: row record before its groupDef")
			}
			if sawFinalFrame {
				t.Fatal("ORDERING VIOLATION: row record after final frame")
			}
			dr := decodeRowRecord(t, e.payload, len(def.cols))
			if dr.reqID != 2 || dr.changeType != engine.RowChangeAdd {
				t.Fatalf("row header: %+v", dr)
			}
			for i, c := range def.cols {
				if c == "name" {
					gotNames = append(gotNames, dr.values[i].(string))
				}
			}
		case abiKindFrame:
			resp := decodeResp(t, e.payload)
			id, _ := toFloat(resp.ID)
			if id != 2 {
				continue
			}
			if s, ok := resp.Result.(string); ok && s == "done" {
				if !sawFinalFrame {
					t.Fatal("ORDERING VIOLATION: done before final partial")
			}
			sawDone = true
				continue
			}
			m, ok := resp.Result.(map[string]interface{})
			if !ok {
				t.Fatalf("unexpected id-2 frame result: %#v", resp.Result)
			}
			if fin, _ := m["final"].(bool); !fin {
				t.Fatalf("non-final id-2 frame in row mode (fallback unexpected here): %#v", m)
			}
			sawFinalFrame = true
		}
	}
	if !sawDone {
		t.Fatal("done sentinel missing")
	}
	if fmt.Sprint(gotNames) != fmt.Sprint([]string{"alice", "bob", "carol"}) {
		t.Fatalf("row content/order wrong: %v", gotNames)
	}
}

// TestABIHost_RowModeAdvanceEndToEnd: advanceToHeadStream with rowMode ships
// each RowChange as a record and the terminal frame as kind-1, in order.
// The advance reads from the replica's changeLog (table mode), not from
// pushed changes.
func TestABIHost_RowModeAdvanceEndToEnd(t *testing.T) {
	col := newSinkCollector()
	path, db := makeReplica(t)
	mustExec(t, db, `CREATE TABLE "users" ("id" TEXT PRIMARY KEY, "name" TEXT, "_0_version" TEXT)`)
	if !beginConcurrentSupported(t, db) {
		t.Skip("drive mode writes into a past-pinned snapshot — requires BEGIN CONCURRENT (wal2/libsqlite3 build); validated via the rust-test soak")
	}

	h := startABIHostWithServer(NewServer(path), col.sink, nil)
	defer h.Shutdown()

	send := func(id float64, method string, params interface{}) {
		t.Helper()
		if err := h.Send(encodeReq(t, method, id, params)); err != nil {
			t.Fatalf("send %s: %v", method, err)
		}
	}
	send(1, "init", initParams{
		ClientGroupID: "cg-adv",
		Tables: map[string]tableSchemaParams{
			"users": {
				Columns: map[string]sqlite.ColumnSchema{
					"id":         {Type: "string"},
					"name":       {Type: "string"},
					"_0_version": {Type: "string"},
				},
				PrimaryKey: []string{"id"},
			},
		},
	})
	send(2, "addQueriesStream", map[string]interface{}{
		"clientGroupID": "cg-adv",
		"initEpoch":     1,
		"queries": []map[string]interface{}{
			{"queryID": "q-adv", "ast": map[string]interface{}{
				"table":   "users",
				"orderBy": [][]string{{"id", "asc"}},
			}},
		},
	})
	// Wait for hydrate to finish (0 rows in users table → done for req 2).
	waitReqDone := func(reqID float64) {
		t.Helper()
		deadline := time.Now().Add(15 * time.Second)
		for {
			col.mu.Lock()
			for _, e := range col.entries {
				if e.kind == abiKindFrame {
					resp := decodeResp(t, e.payload)
					if id, ok := toFloat(resp.ID); ok && id == reqID {
						if s, ok := resp.Result.(string); ok && s == "done" {
							col.mu.Unlock()
							return
						}
					}
				}
			}
			col.mu.Unlock()
			if time.Now().After(deadline) {
				t.Fatalf("timed out waiting for done (req %v)", reqID)
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
	waitReqDone(2)

	// V2: insert u9/zed into users + changeLog + replicationState.
	mustExec(t, db, `INSERT INTO "users" VALUES ('u9','zed','0000000002')`)
	mustExec(t, db, `INSERT OR REPLACE INTO "_zero.changeLog2" ("stateVersion","pos","table","rowKey","op") VALUES ('0000000002',0,'users','{"id":"u9"}','s')`)
	mustExec(t, db, `INSERT OR REPLACE INTO "_zero.replicationState" (stateVersion, lock) VALUES ('0000000002', 1)`)

	// Advance in row mode: the diff v1→v2 delivers one Add RowChange.
	send(3, "advanceToHeadStream", advanceToHeadParams{
		ClientGroupID: "cg-adv",
		InitEpoch:     1,
		RowMode:       true,
	})

	deadline := time.Now().Add(15 * time.Second)
	for {
		col.mu.Lock()
		var defs, rows int
		doneSeen := false
		for _, e := range col.entries {
			switch e.kind {
			case abiKindGroupDef:
				defs++
			case abiKindRow:
				rows++
			case abiKindFrame:
				resp := decodeResp(t, e.payload)
				if id, ok := toFloat(resp.ID); ok && id == 3 {
					if s, ok := resp.Result.(string); ok && s == "done" {
						doneSeen = true
					}
				}
			}
		}
		entriesCopy := append([]sinkEntry(nil), col.entries...)
		col.mu.Unlock()
		if doneSeen {
			if defs != 1 || rows != 1 {
				t.Fatalf("advance row mode: defs=%d rows=%d (want 1/1)", defs, rows)
			}
			// Verify the row record decodes to the advanced change.
			for _, e := range entriesCopy {
				if e.kind == abiKindGroupDef {
					d := decodeGroupDef(t, e.payload)
					if d.queryID != "q-adv" || d.table != "users" {
						t.Fatalf("advance groupDef: %+v", d)
					}
				}
				if e.kind == abiKindRow {
					var def decodedGroupDef
					for _, e2 := range entriesCopy {
						if e2.kind == abiKindGroupDef {
							def = decodeGroupDef(t, e2.payload)
						}
					}
					dr := decodeRowRecord(t, e.payload, len(def.cols))
					vals := map[string]interface{}{}
					for i, c := range def.cols {
						vals[c] = dr.values[i]
					}
					if vals["id"] != "u9" || vals["name"] != "zed" {
						t.Fatalf("advance row content: %v", vals)
					}
				}
			}
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("timed out waiting for advance done")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// TestRowRecord_IntWidthValueTags (REVIEW-napi-transport pass-2 minor): every
// integer width the engine could conceivably carry must cross as the i64 tag
// (symmetric with ivm's toFloat64 matrix), EXCEPT uint64/uint above MaxInt64 —
// the wire tag is signed (JS reads BigInt64), so those ship as msgpack blobs
// to avoid decoding negative.
func TestRowRecord_IntWidthValueTags(t *testing.T) {
	enc := newRowRecordEncoder(1)
	row := ivm.Row{
		"id":     "k1",
		"i8":     int8(-5),
		"i16":    int16(-1234),
		"u8":     uint8(200),
		"u16":    uint16(60000),
		"u32":    uint32(4000000000),
		"u64ok":  uint64(9223372036854775807),
		"u64big": uint64(9223372036854775808), // MaxInt64+1 → blob
	}
	c := &engine.RowChange{
		Type: engine.RowChangeAdd, QueryID: "q", Table: "t",
		RowKey: map[string]interface{}{"id": "k1"}, Row: row,
	}
	g, def := enc.groupFor(c)
	if def == nil {
		t.Fatal("expected a groupDef")
	}
	gd := decodeGroupDef(t, append([]byte(nil), def...))
	rec, ok := enc.encodeRow(g, c)
	if !ok {
		t.Fatal("encodeRow failed")
	}
	dr := decodeRowRecord(t, append([]byte(nil), rec...), len(gd.cols))
	byCol := map[string]interface{}{}
	for i, col := range gd.cols {
		byCol[col] = dr.values[i]
	}
	for col, want := range map[string]float64{
		"i8": -5, "i16": -1234, "u8": 200, "u16": 60000,
		"u32": 4000000000, "u64ok": 9223372036854775807, "u64big": 9223372036854775808,
	} {
		if got := toF64(t, byCol[col]); got != want {
			t.Errorf("%s = %v (%T), want %v", col, byCol[col], byCol[col], want)
		}
	}
}

// TestRowRecord_OversizedIdentifierFailsLoud: an identifier beyond the u16
// length space must pin the group to the frame plane (NO def delivered, every
// record rejected — removes included), not silently truncate. A truncated def
// would mismatch every record's column order at the client.
func TestRowRecord_OversizedIdentifierFailsLoud(t *testing.T) {
	enc := newRowRecordEncoder(2)
	giant := string(make([]byte, 70000))
	c := &engine.RowChange{
		Type: engine.RowChangeAdd, QueryID: giant, Table: "t",
		RowKey: map[string]interface{}{"id": "a"}, Row: ivm.Row{"id": "a"},
	}
	g, def := enc.groupFor(c)
	if def != nil {
		t.Fatal("oversized identifier must NOT produce a def")
	}
	if !g.frameOnly {
		t.Fatal("group must be pinned frameOnly")
	}
	if _, ok := enc.encodeRow(g, c); ok {
		t.Fatal("frameOnly group must reject add records")
	}
	// Removes too: the def was never delivered, so a remove record would
	// reference an unknown group on the JS side.
	rc := &engine.RowChange{
		Type: engine.RowChangeRemove, QueryID: giant, Table: "t",
		RowKey: map[string]interface{}{"id": "a"},
	}
	g2, def2 := enc.groupFor(rc)
	if def2 != nil || g2 != g {
		t.Fatal("interned frameOnly group must be reused, still defless")
	}
	if _, ok := enc.encodeRow(g2, rc); ok {
		t.Fatal("remove against a frameOnly group must fall back to frames")
	}
}

// TestRowRecord_OversizedRecordFallsBackToFrame is the R1 regression guard:
// kind-3 records carry u32 value lengths with no cap of their own, so a value
// pushing the record past the 64MB frame cap must make encodeRow reject it
// (the caller then falls back to the capped frame path) rather than emit an
// unbounded record that malloc+memcpy's into the addon.
func TestRowRecord_OversizedRecordFallsBackToFrame(t *testing.T) {
	enc := newRowRecordEncoder(1)
	big := string(make([]byte, maxFrameSize+16)) // one value over the frame cap
	c := &engine.RowChange{
		Type: engine.RowChangeAdd, QueryID: "q", Table: "t",
		RowKey: map[string]interface{}{"id": "a"},
		Row:    ivm.Row{"id": "a", "blob": big},
	}
	g, def := enc.groupFor(c)
	if def == nil {
		t.Fatal("expected a groupDef")
	}
	if _, ok := enc.encodeRow(g, c); ok {
		t.Fatal("oversized record must be rejected so the caller falls back to the frame path")
	}
	// A normal row for the same group still encodes (the reject didn't
	// wedge the encoder / group state).
	small := &engine.RowChange{
		Type: engine.RowChangeAdd, QueryID: "q", Table: "t",
		RowKey: map[string]interface{}{"id": "b"},
		Row:    ivm.Row{"id": "b", "blob": "ok"},
	}
	if _, ok := enc.encodeRow(g, small); !ok {
		t.Fatal("a normal row after an oversized one must still encode")
	}
}

// TestNumericReqID_ExactnessGuard (scale review): integer RPC ids beyond
// f64's exact-integer range (|id| > 2^53) must DECLINE row mode (frame
// fallback) instead of silently rounding. A rounded reqID can collide with
// a different RPC's id, delivering this stream's row records into that
// RPC's decode — cross-RPC record bleed.
func TestNumericReqID_ExactnessGuard(t *testing.T) {
	exact := int64(1) << 53

	// In-range ids (all widths) convert exactly.
	for _, tc := range []struct {
		id   interface{}
		want float64
	}{
		{float64(42), 42},
		{int(7), 7},
		{int64(exact), float64(exact)},        // 2^53 itself is exact
		{int64(-exact), float64(-exact)},      // and its negative
		{uint64(uint64(exact)), float64(exact)},
		{int32(-5), -5},
		{uint32(9), 9},
	} {
		got, ok := numericReqID(tc.id)
		if !ok || got != tc.want {
			t.Fatalf("numericReqID(%T %v) = (%v,%v), want (%v,true)", tc.id, tc.id, got, ok, tc.want)
		}
	}

	// Beyond 2^53: int64(2^53+1) is NOT representable in f64 — float64()
	// rounds it to 2^53, colliding with the id above. Must return false.
	for _, id := range []interface{}{
		int64(exact + 1),
		int64(-(exact + 1)),
		uint64(uint64(exact) + 1),
		int64(1) << 60,
	} {
		if got, ok := numericReqID(id); ok {
			t.Fatalf("numericReqID(%T %v) = (%v,true) — inexact id accepted (record-bleed risk)", id, id, got)
		}
	}

	// Non-numeric ids still decline.
	if _, ok := numericReqID("req-1"); ok {
		t.Fatal("string id accepted")
	}
}
