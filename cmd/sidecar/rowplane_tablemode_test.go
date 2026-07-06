package main

// Table-mode (PRODUCTION path) row-plane tests for the NAPI transport.
//
// The rowrecord E2E tests in rowrecord_test.go run the abi host in MEMORY
// mode, so the values crossing the boundary are msgpack-provenance
// (float64/string/bool from loadRows). Production is TABLE mode: values
// originate in SQLite and pass through sqlite.FromSQLiteType coercion
// (INTEGER 0/1 → bool for boolean columns, time.Time → epoch-ms for
// nullable temporal decltypes, strict JSON.parse at the read boundary for
// json columns). These tests pin that the record encoder ships the
// COERCED values — i.e. the row plane is provenance-faithful — and that
// PARALLEL hydrate lanes (P>1) emitting per-row records (chunkSize=1)
// through the shared rowPlane mutex preserve the two ordering invariants
// TS's RowGroupRegistry depends on:
//
//  1. a group's def precedes its first row on the delivery queue, and
//  2. each query's rows arrive in that query's ORDER BY order
//     (cross-QUERY interleaving is allowed — TS routes by groupID).
//
// Reuses mustExec/mustMarshal from advance_to_head_test.go and the
// sinkCollector/decode helpers from abi_test.go / rowrecord_test.go.

import (
	"database/sql"
	"fmt"
	"math"
	"path/filepath"
	"testing"
	"time"

	"github.com/kartikparsoya-eng/go-ivm/sqlite"
)

// makeTypedReplica builds a replica whose `things` table exercises every
// coercion class: boolean-typed INTEGER, number-typed INTEGER (safe-max),
// number-typed REAL, number-typed nullable `timestamp` decltype (mattn
// auto-converts to time.Time), json-typed TEXT, and strings (empty +
// 4-byte-UTF-8 emoji), plus NULLs.
func makeTypedReplica(t *testing.T, rows int) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "typed-replica.db")
	// Same DSN shape as makeReplica: the sidecar's table mode expects a WAL
	// replica (prev-tx pinning + co-read pools are WAL-frame-based).
	db, err := sql.Open("sqlite3", "file:"+path+"?_busy_timeout=5000&_journal_mode=WAL")
	if err != nil {
		t.Fatalf("open replica: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	mustExec(t, db, `CREATE TABLE "things" (
		"id" TEXT NOT NULL,
		"flag" INTEGER,
		"big" INTEGER,
		"ratio" REAL,
		"seenAt" timestamp,
		"meta" TEXT,
		"label" TEXT,
		"_0_version" TEXT NOT NULL,
		PRIMARY KEY ("id")
	)`)
	for i := 0; i < rows; i++ {
		// Alternate booleans; NULL every 5th meta/seenAt; emoji labels.
		var meta, seenAt interface{}
		if i%5 != 0 {
			meta = fmt.Sprintf(`{"n":%d,"tags":["a","b"]}`, i)
			seenAt = int64(1700000000000 + i) // epoch ms, > 1e12 → mattn ms heuristic
		}
		label := ""
		if i%2 == 1 {
			label = fmt.Sprintf("row-%d-🎯", i)
		}
		mustExec(t, db,
			`INSERT INTO "things" VALUES (?,?,?,?,?,?,?,?)`,
			fmt.Sprintf("id%04d", i), i%2, int64(9007199254740991), 0.5+float64(i),
			seenAt, meta, label, "0000000001")
	}
	mustExec(t, db, `CREATE TABLE "_zero.replicationState" (stateVersion TEXT NOT NULL, writeTimeMs INTEGER, lock INTEGER PRIMARY KEY DEFAULT 1 CHECK (lock=1))`)
	mustExec(t, db, `CREATE TABLE "_zero.changeLog2" ("stateVersion" TEXT NOT NULL,"pos" INT NOT NULL,"table" TEXT NOT NULL,"rowKey" TEXT NOT NULL,"op" TEXT NOT NULL,"backfillingColumnVersions" TEXT DEFAULT '{}',PRIMARY KEY("stateVersion","pos"),UNIQUE("table","rowKey"))`)
	mustExec(t, db, `INSERT OR REPLACE INTO "_zero.replicationState" (stateVersion, lock) VALUES ('0000000001', 1)`)
	return path
}

func typedInitParams(cg string) initParams {
	return initParams{
		ClientGroupID: cg,
		Tables: map[string]tableSchemaParams{
			"things": {
				Columns: map[string]sqlite.ColumnSchema{
					"id":         {Type: "string"},
					"flag":       {Type: "boolean"},
					"big":        {Type: "number"},
					"ratio":      {Type: "number"},
					"seenAt":     {Type: "number"},
					"meta":       {Type: "json"},
					"label":      {Type: "string"},
					"_0_version": {Type: "string"},
				},
				PrimaryKey: []string{"id"},
				UniqueKeys: [][]string{{"id"}},
			},
		},
	}
}

// TestABIHost_RowModeTableModeCoercion drives the FULL production stack —
// abi host → handleConnection → table-mode hydrate with parallel lanes →
// row plane — and asserts the decoded record values carry SQLite
// coercion results, with per-query ordering intact under lane parallelism.
func TestABIHost_RowModeTableModeCoercion(t *testing.T) {
	const nRows = 60
	path := makeTypedReplica(t, nRows)

	srv := NewServer(path)
	srv.hydrateLanes = 4
	srv.hydrateReaders = 8

	col := newSinkCollector()
	h := startABIHostWithServer(srv, col.sink, nil)
	defer h.Shutdown()

	send := func(id float64, method string, params interface{}) {
		t.Helper()
		if err := h.Send(encodeReq(t, method, id, params)); err != nil {
			t.Fatalf("send %s: %v", method, err)
		}
	}

	send(1, "init", typedInitParams("cg-typed"))

	// 4 queries over the same table — one per lane at P=4. Same ORDER BY
	// so every query's expected row order is identical and checkable.
	queries := make([]map[string]interface{}, 4)
	for i := range queries {
		queries[i] = map[string]interface{}{
			"queryID": fmt.Sprintf("q%d", i),
			"ast": map[string]interface{}{
				"table":   "things",
				"orderBy": [][]string{{"id", "asc"}},
			},
		}
	}
	send(2, "addQueriesStream", map[string]interface{}{
		"clientGroupID": "cg-typed",
		"initEpoch":     1,
		"rowMode":       true,
		"queries":       queries,
	})

	// Wait until every query delivered all rows + its final frame.
	deadline := time.Now().Add(30 * time.Second)
	var entries []sinkEntry
	for {
		col.mu.Lock()
		entries = append(entries[:0], col.entries...)
		col.mu.Unlock()
		rows := 0
		finals := 0
		for _, e := range entries {
			switch e.kind {
			case abiKindRow:
				rows++
			case abiKindFrame:
				resp := decodeResp(t, e.payload)
				if id, ok := toFloat(resp.ID); ok && id == 2 {
					if m, ok := resp.Result.(map[string]interface{}); ok {
						if fin, _ := m["final"].(bool); fin {
							finals++
						}
					}
				}
			}
		}
		if rows >= 4*nRows && finals >= 4 {
			break
		}
		if time.Now().After(deadline) {
			// Dump every kind-1 frame for diagnosability (init errors etc.).
			for _, e := range entries {
				if e.kind == abiKindFrame {
					resp := decodeResp(t, e.payload)
					t.Logf("frame id=%v err=%+v result=%.200v", resp.ID, resp.Error, resp.Result)
				}
			}
			t.Fatalf("timed out: rows=%d finals=%d", rows, finals)
		}
		time.Sleep(20 * time.Millisecond)
	}

	// Replay the queue asserting ordering + content.
	defs := map[uint32]decodedGroupDef{} // groupID → def
	rowsPerQuery := map[string][]map[string]interface{}{}
	finalSeen := map[string]bool{}
	for _, e := range entries {
		switch e.kind {
		case abiKindGroupDef:
			d := decodeGroupDef(t, e.payload)
			if d.reqID != 2 || d.table != "things" {
				t.Fatalf("unexpected groupDef: %+v", d)
			}
			defs[d.groupID] = d
		case abiKindRow:
			r := new(recordReader)
			r.buf = e.payload
			reqID := r.f64()
			groupID := r.u32()
			typ := r.u8()
			def, ok := defs[groupID]
			if !ok {
				t.Fatal("ORDERING VIOLATION: row before its groupDef")
			}
			if reqID != 2 || typ != 0 {
				t.Fatalf("row header: req=%v type=%d", reqID, typ)
			}
			if finalSeen[def.queryID] {
				t.Fatalf("ORDERING VIOLATION: row for %s after its final frame", def.queryID)
			}
			row := map[string]interface{}{}
			for _, c := range def.cols {
				row[c] = r.value(t)
			}
			rowsPerQuery[def.queryID] = append(rowsPerQuery[def.queryID], row)
		case abiKindFrame:
			resp := decodeResp(t, e.payload)
			if id, ok := toFloat(resp.ID); !ok || id != 2 {
				continue
			}
			if m, ok := resp.Result.(map[string]interface{}); ok {
				if fin, _ := m["final"].(bool); fin {
					qid, _ := m["queryID"].(string)
					finalSeen[qid] = true
					if rows, exists := m["r"]; exists && rows != nil {
						t.Errorf("final frame for %s carries fallback rows — every typed value should row-encode: %#v", qid, rows)
					}
				}
			}
		}
	}

	if len(rowsPerQuery) != 4 {
		t.Fatalf("queries with rows = %d, want 4", len(rowsPerQuery))
	}
	for qid, rows := range rowsPerQuery {
		if len(rows) != nRows {
			t.Fatalf("%s: %d rows, want %d", qid, len(rows), nRows)
		}
		for i, row := range rows {
			// (1) per-query ORDER BY order under parallel lanes.
			wantID := fmt.Sprintf("id%04d", i)
			if row["id"] != wantID {
				t.Fatalf("%s row %d: id=%v, want %s (per-query order violated)", qid, i, row["id"], wantID)
			}
			// (2) SQLite coercion fidelity through the record plane.
			wantFlag := i%2 != 0
			if row["flag"] != wantFlag {
				t.Errorf("%s row %d: flag=%#v (%T), want %v — boolean INTEGER must cross as bool", qid, i, row["flag"], row["flag"], wantFlag)
			}
			// Safe-max int64: exact through either f64 or i64 tag.
			if got := toF64(t, row["big"]); got != 9007199254740991 {
				t.Errorf("%s row %d: big=%v, want 9007199254740991 exact", qid, i, row["big"])
			}
			if got := toF64(t, row["ratio"]); got != 0.5+float64(i) {
				t.Errorf("%s row %d: ratio=%v, want %v", qid, i, row["ratio"], 0.5+float64(i))
			}
			if i%5 == 0 {
				if row["seenAt"] != nil || row["meta"] != nil {
					t.Errorf("%s row %d: want NULL seenAt/meta, got %#v/%#v", qid, i, row["seenAt"], row["meta"])
				}
			} else {
				// Nullable `timestamp` decltype → mattn time.Time → epoch ms.
				if got := toF64(t, row["seenAt"]); got != float64(1700000000000+i) {
					t.Errorf("%s row %d: seenAt=%v (%T), want %d — time.Time must normalize to epoch ms", qid, i, row["seenAt"], row["seenAt"], 1700000000000+i)
				}
				// json TEXT → strict parse at read boundary → blob tag → map.
				meta, ok := row["meta"].(map[string]interface{})
				if !ok {
					t.Fatalf("%s row %d: meta=%#v (%T), want parsed object — json must NOT cross as raw string", qid, i, row["meta"], row["meta"])
				}
				if n := toF64(t, meta["n"]); n != float64(i) {
					t.Errorf("%s row %d: meta.n=%v, want %d", qid, i, meta["n"], i)
				}
			}
			wantLabel := ""
			if i%2 == 1 {
				wantLabel = fmt.Sprintf("row-%d-🎯", i)
			}
			if row["label"] != wantLabel {
				t.Errorf("%s row %d: label=%q, want %q (empty + 4-byte UTF-8)", qid, i, row["label"], wantLabel)
			}
		}
	}
}

// recordReader is a minimal decode cursor over a row record's value
// section (mirrors napi-records.ts readValue).
type recordReader struct {
	buf []byte
	off int
}

func (r *recordReader) f64() float64 {
	bits := uint64(0)
	for i := 0; i < 8; i++ {
		bits |= uint64(r.buf[r.off+i]) << (8 * i)
	}
	r.off += 8
	return mathFloat64frombits(bits)
}

func (r *recordReader) u32() uint32 {
	v := uint32(r.buf[r.off]) | uint32(r.buf[r.off+1])<<8 | uint32(r.buf[r.off+2])<<16 | uint32(r.buf[r.off+3])<<24
	r.off += 4
	return v
}

func (r *recordReader) u8() byte {
	b := r.buf[r.off]
	r.off++
	return b
}

func (r *recordReader) value(t *testing.T) interface{} {
	t.Helper()
	tag := r.u8()
	switch tag {
	case rowValNull:
		return nil
	case rowValFalse:
		return false
	case rowValTrue:
		return true
	case rowValF64:
		return r.f64()
	case rowValI64:
		bits := uint64(0)
		for i := 0; i < 8; i++ {
			bits |= uint64(r.buf[r.off+i]) << (8 * i)
		}
		r.off += 8
		return int64(bits)
	case rowValStr:
		n := int(r.u32())
		s := string(r.buf[r.off : r.off+n])
		r.off += n
		return s
	case rowValBlob:
		n := int(r.u32())
		var v interface{}
		if err := mpUnmarshal(r.buf[r.off:r.off+n], &v); err != nil {
			t.Fatalf("blob unmarshal: %v", err)
		}
		r.off += n
		return normalizeBlob(v)
	default:
		t.Fatalf("unknown value tag %d", tag)
		return nil
	}
}

// normalizeBlob converts msgpack map[interface{}]interface{} decode shapes
// to map[string]interface{} for assertion convenience.
func normalizeBlob(v interface{}) interface{} {
	switch m := v.(type) {
	case map[string]interface{}:
		for k, vv := range m {
			m[k] = normalizeBlob(vv)
		}
		return m
	case map[interface{}]interface{}:
		out := make(map[string]interface{}, len(m))
		for k, vv := range m {
			out[fmt.Sprintf("%v", k)] = normalizeBlob(vv)
		}
		return out
	case []interface{}:
		for i := range m {
			m[i] = normalizeBlob(m[i])
		}
		return m
	}
	return v
}

// toF64 accepts the numeric types the record/msgpack planes may produce.
func toF64(t *testing.T, v interface{}) float64 {
	t.Helper()
	switch x := v.(type) {
	case float64:
		return x
	case int64:
		return float64(x)
	case int8:
		return float64(x)
	case int16:
		return float64(x)
	case int32:
		return float64(x)
	case uint64:
		return float64(x)
	case int:
		return float64(x)
	default:
		t.Fatalf("not numeric: %#v (%T)", v, v)
		return 0
	}
}

func mathFloat64frombits(b uint64) float64 { return math.Float64frombits(b) }
