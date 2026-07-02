package main

// Row-plane record encoding for the NAPI transport (GO_IVM row mode).
//
// When the TS client runs in-process (NAPI) and opts into rowMode on a
// streaming RPC, each RowChange crosses the Go↔JS boundary as ONE flat
// little-endian binary record instead of being batched into a msgpack
// frame. This kills the per-row msgpack encode (Go), the frame decode
// (msgpackr), and the positional rebuild (JS object churn): the addon
// hands the record bytes to JS, which reads them with a DataView and
// assembles the RowChange directly.
//
// Delivery kinds (the addon's single ordered TSFN queue carries all three,
// so cross-kind ordering is preserved end-to-end):
//
//	kind 1 = msgpack RPC frame (control plane; unchanged wire bytes)
//	kind 2 = groupDef record   (row plane; one per (queryID,table) per RPC)
//	kind 3 = row record        (row plane; one per RowChange)
//
// Record layouts (all integers little-endian; str = u16 len + UTF-8 bytes,
// except value strings/blobs which use u32 len):
//
//	groupDef: [f64 reqID][u32 groupID][str queryID][str table]
//	          [u16 ncols]([str col])*[u16 npk]([u16 pkIdx])*
//	row:      [f64 reqID][u32 groupID][u8 changeType][values...]
//	          changeType 1 (remove): npk values in PK order
//	          else:                  ncols values in groupDef column order
//	value:    [u8 tag] + payload:
//	          0=null 1=false 2=true 3=f64(8B) 4=i64(8B)
//	          5=string(u32+bytes) 6=msgpack blob(u32+bytes)
//
// Column-order contract mirrors positional.go: rows of a replicated table
// are homogeneous (same column set incl. _0_version, NULLs as nil values),
// so the FIRST row's sorted keys are the group's canonical column order.
// positional.go banks on the same invariant (its per-chunk sorted union
// equals each row's key set); shadow mode content-validates it. A later row
// missing a column encodes null for it; a later row with an EXTRA column
// would be silently dropped under this contract — defensively, encodeRow
// returns false in that case and the caller falls back to the msgpack
// frame path for that partial (correct over fast).
//
// reqID rides every record as f64 because TS RPC ids are JS numbers
// (#nextID counter) — msgpack may deliver them to Go as any int width, but
// they are exact in f64 by construction.

import (
	"encoding/binary"
	"math"
	"sort"

	"github.com/kartikparsoya-eng/go-ivm/engine"
	"github.com/kartikparsoya-eng/go-ivm/ivm"
)

const (
	abiKindFrame    = 1
	abiKindGroupDef = 2
	abiKindRow      = 3
)

const (
	rowValNull  = 0
	rowValFalse = 1
	rowValTrue  = 2
	rowValF64   = 3
	rowValI64   = 4
	rowValStr   = 5
	rowValBlob  = 6
)

// rowRecordEncoder interns (queryID,table) groups for ONE streaming RPC and
// encodes groupDef/row records into a reusable buffer. Not goroutine-safe —
// hydrate lanes call onResult concurrently, so callers wrap uses in their
// own mutex (the abiDeliver contract is synchronous-copy, so handing out
// e.buf between locked calls is safe).
type rowRecordEncoder struct {
	reqID  float64
	groups map[pgKey]*rowGroup
	buf    []byte
}

type rowGroup struct {
	id   uint32
	cols []string
	pk   []string
}

func newRowRecordEncoder(reqID float64) *rowRecordEncoder {
	return &rowRecordEncoder{reqID: reqID, groups: make(map[pgKey]*rowGroup)}
}

// numericReqID converts a decoded RPC id to f64. Returns false for
// non-numeric ids (string ids are legal JSON-RPC; row mode requires numeric
// — the caller falls back to frame mode for such requests).
func numericReqID(id interface{}) (float64, bool) {
	switch v := id.(type) {
	case float64:
		return v, true
	case float32:
		return float64(v), true
	case int:
		return float64(v), true
	case int8:
		return float64(v), true
	case int16:
		return float64(v), true
	case int32:
		return float64(v), true
	case int64:
		return float64(v), true
	case uint8:
		return float64(v), true
	case uint16:
		return float64(v), true
	case uint32:
		return float64(v), true
	case uint64:
		return float64(v), true
	}
	return 0, false
}

func (e *rowRecordEncoder) putU16(v uint16) {
	e.buf = binary.LittleEndian.AppendUint16(e.buf, v)
}

func (e *rowRecordEncoder) putU32(v uint32) {
	e.buf = binary.LittleEndian.AppendUint32(e.buf, v)
}

func (e *rowRecordEncoder) putF64(v float64) {
	e.buf = binary.LittleEndian.AppendUint64(e.buf, math.Float64bits(v))
}

func (e *rowRecordEncoder) putShortStr(s string) {
	if len(s) > math.MaxUint16 {
		s = s[:math.MaxUint16] // identifiers; never legitimately this long
	}
	e.putU16(uint16(len(s)))
	e.buf = append(e.buf, s...)
}

// groupFor interns the (queryID,table) group, encoding a groupDef record
// into e.buf on first sight. Returns (group, defRecord) where defRecord is
// nil if the group was already known. The returned slice aliases e.buf and
// must be consumed (copied by abiDeliver's sink) before the next encode.
func (e *rowRecordEncoder) groupFor(c *engine.RowChange) (*rowGroup, []byte) {
	k := pgKey{c.QueryID, c.Table}
	if g, ok := e.groups[k]; ok {
		return g, nil
	}
	g := &rowGroup{id: uint32(len(e.groups)), pk: sortedMapKeys(c.RowKey)}
	// Canonical column order: first row's sorted keys (homogeneity
	// contract — see file comment). Removes carry no Row; their group's
	// cols stay empty until an add/edit arrives... except a group whose
	// FIRST change is a remove: encode its def with PK only; a later
	// add/edit for the same group would then find cols empty. Guard: only
	// intern column order when we have a Row; remove-first groups defer
	// column fixing to the first add/edit by re-encoding a fresh def is
	// NOT possible (defs are immutable JS-side). Instead: remove-only
	// groups never need cols (removes encode PK values), and a mixed
	// group starting with a remove takes the FIRST ADD/EDIT's keys via
	// lazy fill below.
	if c.Type != engine.RowChangeRemove && c.Row != nil {
		g.cols = sortedRowKeys(c.Row)
	}
	e.groups[k] = g

	e.buf = e.buf[:0]
	e.putF64(e.reqID)
	e.putU32(g.id)
	e.putShortStr(c.QueryID)
	e.putShortStr(c.Table)
	e.putU16(uint16(len(g.cols)))
	for _, col := range g.cols {
		e.putShortStr(col)
	}
	e.putU16(uint16(len(g.pk)))
	for _, pkCol := range g.pk {
		idx := -1
		for i, col := range g.cols {
			if col == pkCol {
				idx = i
				break
			}
		}
		// For remove-only groups (no cols) the index is unused JS-side
		// (PK NAMES follow); encode 0xFFFF as "not a column reference".
		if idx < 0 {
			e.putU16(math.MaxUint16)
		} else {
			e.putU16(uint16(idx))
		}
		e.putShortStr(pkCol)
	}
	return g, e.buf
}

// encodeRow encodes one RowChange as a row record into e.buf. Returns
// (record, true) on success; (nil, false) when the change violates the
// group's column contract (extra column not in the canonical order, or an
// add/edit against a remove-first group that never fixed columns) — the
// caller must fall back to the msgpack frame path for this change.
// The returned slice aliases e.buf; consume before the next encode.
func (e *rowRecordEncoder) encodeRow(g *rowGroup, c *engine.RowChange) ([]byte, bool) {
	if c.Type != engine.RowChangeRemove {
		if c.Row == nil {
			return nil, false
		}
		if g.cols == nil {
			// Remove-first group: fix columns now from this first add/edit.
			// The JS side learns them from a SUPPLEMENTARY def... which the
			// format doesn't support. Fall back for this group entirely.
			return nil, false
		}
		if len(c.Row) > len(g.cols) {
			return nil, false // extra column — homogeneity violated
		}
	}

	e.buf = e.buf[:0]
	e.putF64(e.reqID)
	e.putU32(g.id)
	e.buf = append(e.buf, byte(c.Type))

	if c.Type == engine.RowChangeRemove {
		for _, pkCol := range g.pk {
			if !e.putValue(c.RowKey[pkCol]) {
				return nil, false
			}
		}
		return e.buf, true
	}
	for _, col := range g.cols {
		if !e.putValue(c.Row[col]) {
			return nil, false
		}
	}
	return e.buf, true
}

// putValue appends one tagged value. Returns false only when a blob
// fallback fails to marshal (practically never — mpMarshal handles any
// value the engine produces).
func (e *rowRecordEncoder) putValue(v interface{}) bool {
	switch x := v.(type) {
	case nil:
		e.buf = append(e.buf, rowValNull)
	case bool:
		if x {
			e.buf = append(e.buf, rowValTrue)
		} else {
			e.buf = append(e.buf, rowValFalse)
		}
	case float64:
		e.buf = append(e.buf, rowValF64)
		e.putF64(x)
	case float32:
		e.buf = append(e.buf, rowValF64)
		e.putF64(float64(x))
	case int64:
		e.buf = append(e.buf, rowValI64)
		e.buf = binary.LittleEndian.AppendUint64(e.buf, uint64(x))
	case int:
		e.buf = append(e.buf, rowValI64)
		e.buf = binary.LittleEndian.AppendUint64(e.buf, uint64(int64(x)))
	case int32:
		e.buf = append(e.buf, rowValI64)
		e.buf = binary.LittleEndian.AppendUint64(e.buf, uint64(int64(x)))
	case string:
		e.buf = append(e.buf, rowValStr)
		e.putU32(uint32(len(x)))
		e.buf = append(e.buf, x...)
	default:
		// Nested JSON (maps/slices), []byte, and any exotic value ride as
		// a msgpack blob; TS decodes those rare values with msgpackr.
		blob, err := mpMarshal(v)
		if err != nil {
			return false
		}
		e.buf = append(e.buf, rowValBlob)
		e.putU32(uint32(len(blob)))
		e.buf = append(e.buf, blob...)
	}
	return true
}

func sortedRowKeys(m ivm.Row) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
