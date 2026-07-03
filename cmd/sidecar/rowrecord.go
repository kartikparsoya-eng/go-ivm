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
// equals each row's key set); shadow mode content-validates it. Defensively,
// encodeRow verifies MEMBERSHIP, not just size: a later row encodes only if
// every one of its keys is a canonical column (keys it lacks encode null);
// any row carrying a column outside the canonical order — larger, equal, or
// smaller — returns false and the caller falls back to the msgpack frame
// path for that partial (correct over fast). A length-only guard misses the
// equal-or-smaller shapes ({a,b,x} vs canonical {a,b,c}): x would be
// silently dropped AND c fabricated as null.
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
	// frameOnly pins the group to the msgpack frame plane forever: set when
	// the groupDef could not be encoded (identifier over 64KB — see
	// putShortStr). The def was never delivered, so NO record may reference
	// this group (removes included) or the JS side throws "unknown group".
	frameOnly bool
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

// putShortStr appends a u16-length identifier. Returns false when the
// identifier exceeds the u16 length space — FAIL LOUD (the caller pins the
// group to the frame plane) instead of silently truncating: a truncated
// def would mismatch every subsequent record's column order, delivering
// wrong values under wrong keys at the client (REVIEW-napi-transport
// pass-2 minor).
func (e *rowRecordEncoder) putShortStr(s string) bool {
	if len(s) > math.MaxUint16 {
		return false // identifiers are never legitimately this long
	}
	e.putU16(uint16(len(s)))
	e.buf = append(e.buf, s...)
	return true
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
	ok := e.putShortStr(c.QueryID)
	ok = e.putShortStr(c.Table) && ok
	e.putU16(uint16(len(g.cols)))
	for _, col := range g.cols {
		ok = e.putShortStr(col) && ok
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
		ok = e.putShortStr(pkCol) && ok
	}
	if !ok {
		// Oversized identifier — the def cannot be represented. Pin the
		// group to the frame plane (encodeRow rejects everything for it)
		// and deliver NO def; the partially-encoded buf is discarded.
		g.frameOnly = true
		return g, nil
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
	if g.frameOnly {
		// Def was never delivered (oversized identifier) — no record may
		// reference this group, removes included.
		return nil, false
	}
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
			// Fast-path reject before any encoding work; the membership
			// count after the encode loop below is the complete check.
			return nil, false
		}
	}

	e.buf = e.buf[:0]
	e.putF64(e.reqID)
	e.putU32(g.id)
	e.buf = append(e.buf, byte(c.Type))

	if c.Type == engine.RowChangeRemove {
		found := 0
		for _, pkCol := range g.pk {
			v, ok := c.RowKey[pkCol]
			if ok {
				found++
			}
			if !e.putValue(v) {
				return nil, false
			}
		}
		// Membership must hold in BOTH directions against the interned PK:
		// found == len(c.RowKey) rejects foreign keys (RowKey ⊄ pk), and
		// found == len(g.pk) rejects PK-SUBSET RowKeys ({a} vs pk=[a,b]) —
		// which the one-sided check waved through, encoding the missing PK
		// column as null: a remove targeting (a, null) at the client.
		if found != len(c.RowKey) || found != len(g.pk) {
			return nil, false
		}
		if len(e.buf) > maxFrameSize {
			return nil, false // oversize — fall back to the (capped) frame path
		}
		return e.buf, true
	}
	found := 0
	for _, col := range g.cols {
		v, ok := c.Row[col]
		if ok {
			found++
		}
		if !e.putValue(v) {
			return nil, false
		}
	}
	// Homogeneity is a MEMBERSHIP check, not a size check: found counts the
	// row's keys that are canonical columns, so found != len(c.Row) iff the
	// row carries a column outside g.cols — including the equal-or-smaller
	// shapes ({a,b,x} vs {a,b,c}) that a length-only guard waves through,
	// silently dropping x and fabricating c as null. Missing canonical
	// columns (row ⊂ cols) still encode null above — intended leniency.
	// Comma-ok on lookups already paid for; this costs one int compare.
	if found != len(c.Row) {
		return nil, false
	}
	// R1 (REVIEW-napi-transport): kind-3 records carry u32 value lengths (up
	// to 4GB) with NO cap of their own — unlike frames, which capFrameBytes
	// guards. A single fat JSON/blob value would malloc+memcpy an unbounded
	// buffer into the addon. Above the frame cap, fall back to the msgpack
	// frame path, whose capFrameBytes turns a truly-oversized payload into a
	// loud error frame instead of an OOM.
	if len(e.buf) > maxFrameSize {
		return nil, false
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
	case int8:
		e.buf = append(e.buf, rowValI64)
		e.buf = binary.LittleEndian.AppendUint64(e.buf, uint64(int64(x)))
	case int16:
		e.buf = append(e.buf, rowValI64)
		e.buf = binary.LittleEndian.AppendUint64(e.buf, uint64(int64(x)))
	case uint8:
		e.buf = append(e.buf, rowValI64)
		e.buf = binary.LittleEndian.AppendUint64(e.buf, uint64(int64(x)))
	case uint16:
		e.buf = append(e.buf, rowValI64)
		e.buf = binary.LittleEndian.AppendUint64(e.buf, uint64(int64(x)))
	case uint32:
		e.buf = append(e.buf, rowValI64)
		e.buf = binary.LittleEndian.AppendUint64(e.buf, uint64(int64(x)))
	case uint64:
		// i64 tag is SIGNED on the wire (JS reads BigInt64): a value above
		// MaxInt64 would decode negative — ship it as a blob instead.
		if x <= math.MaxInt64 {
			e.buf = append(e.buf, rowValI64)
			e.buf = binary.LittleEndian.AppendUint64(e.buf, x)
		} else {
			return e.putBlobValue(v)
		}
	case uint:
		if uint64(x) <= math.MaxInt64 {
			e.buf = append(e.buf, rowValI64)
			e.buf = binary.LittleEndian.AppendUint64(e.buf, uint64(x))
		} else {
			return e.putBlobValue(v)
		}
	case string:
		e.buf = append(e.buf, rowValStr)
		e.putU32(uint32(len(x)))
		e.buf = append(e.buf, x...)
	default:
		return e.putBlobValue(v)
	}
	return true
}

// putBlobValue appends a msgpack-blob-tagged value (nested JSON maps/slices,
// []byte, over-range uint64, and any exotic value). TS decodes with msgpackr.
func (e *rowRecordEncoder) putBlobValue(v interface{}) bool {
	blob, err := mpMarshal(v)
	if err != nil {
		return false
	}
	e.buf = append(e.buf, rowValBlob)
	e.putU32(uint32(len(blob)))
	e.buf = append(e.buf, blob...)
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
