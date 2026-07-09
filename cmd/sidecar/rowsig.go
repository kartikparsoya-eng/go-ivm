package main

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"strconv"
	"sync"

	"github.com/kartikparsoya-eng/go-ivm/engine"
)

// Row-set signature accumulation (protocolRev 11).
//
// TS tracks each query's row-set signature as an XOR of per-row
// rowIDSignatureUnit hashes (row-set-signature.ts). The unit is
// h64(rowIDString({schema:'', table, rowKey})), where h64 is two xxHash32
// calls with seeds 0 and 1 (hash.ts:11-16), and rowIDString is
// stringify(["", table, ...tuples(rowKey)]) (row-key.ts:64-72).
//
// Previously the TS side computed this per-row inside
// #trackRowSetSignatures (pipeline-driver.ts:1795-1810), which wraps every
// Go-delivered change stream. For each Go row that meant: allocate a fresh
// RowID object, build a JSON string, run two pure-JS xxHash32 calls, and
// ~4 BigInt XOR ops — all on the JS thread.
//
// Go now accumulates the same XOR per queryID as it emits rows, and ships
// one hex-encoded delta on the Final frame. The TS side applies the delta
// (one BigInt XOR per query) and skips the per-row wrapper entirely on Go
// paths. The delta-on-Final design also simplifies the mid-stream-death
// failure mode (Finding 9, pipeline-driver.ts:1537-1539): a dead stream
// never ships its Final, so the delta is never applied — the signature
// stays at its pre-stream value (0 after removeQuery) instead of being
// partially accumulated.
//
// Byte-identical parity constraints:
//   - h64 = (uint64(xxh32(s, 0)) << 32) | uint64(xxh32(s, 1))
//   - xxHash32 matches js-xxhash@4.0.0 exactly (UTF-8 input)
//   - rowIDString matches stringify from bigint-json.ts:
//       - strings: '"'+s+'"' (or JSON.stringify for control chars, NO HTML escaping)
//       - numbers: String(n) — Go encoding/json matches for float64 (incl. -0→"0", 1e21→"1e+21", 1e-7→"1e-7")
//       - booleans: "true"/"false"
//       - null: "null"
//   - Column sort: JS string comparison (<) = UTF-16 code-unit order

const (
	xxPrime1 uint32 = 2654435761
	xxPrime2 uint32 = 2246822519
	xxPrime3 uint32 = 3266489917
	xxPrime4 uint32 = 668265263
	xxPrime5 uint32 = 374761393
)

func xxHash32(data []byte, seed uint32) uint32 {
	n := len(data)
	var h uint32

	if n >= 16 {
		acc1 := seed + xxPrime1 + xxPrime2
		acc2 := seed + xxPrime2
		acc3 := seed
		acc4 := seed - xxPrime1

		i := 0
		for ; i+16 <= n; i += 16 {
			acc1 = xxRound(acc1, binary.LittleEndian.Uint32(data[i:]))
			acc2 = xxRound(acc2, binary.LittleEndian.Uint32(data[i+4:]))
			acc3 = xxRound(acc3, binary.LittleEndian.Uint32(data[i+8:]))
			acc4 = xxRound(acc4, binary.LittleEndian.Uint32(data[i+12:]))
		}

		h = rotl32(acc1, 1) + rotl32(acc2, 7) + rotl32(acc3, 12) + rotl32(acc4, 18)
	} else {
		h = seed + xxPrime5
	}

	h += uint32(n)

	i := n &^ 15
	for ; i+4 <= n; i += 4 {
		h += binary.LittleEndian.Uint32(data[i:]) * xxPrime3
		h = rotl32(h, 17) * xxPrime4
	}
	for ; i < n; i++ {
		h += uint32(data[i]) * xxPrime5
		h = rotl32(h, 11) * xxPrime1
	}

	h ^= h >> 15
	h *= xxPrime2
	h ^= h >> 13
	h *= xxPrime3
	h ^= h >> 16
	return h
}

func xxRound(acc, lane uint32) uint32 {
	acc += lane * xxPrime2
	acc = rotl32(acc, 13)
	acc *= xxPrime1
	return acc
}

func rotl32(x uint32, r uint) uint32 {
	return (x << r) | (x >> (32 - r))
}

// h64 matches TS hash.ts h64: (xxHash32(s,0) << 32) + xxHash32(s,1).
func h64(s string) uint64 {
	b := []byte(s)
	return uint64(xxHash32(b, 0))<<32 | uint64(xxHash32(b, 1))
}

// rowIDString builds the same JSON array TS's rowIDString produces:
// stringify(["", table, ...tuples(rowKey)]) where tuples is the sorted
// [col, val, col, val, ...] flattening of the row key.
func rowIDString(table string, rowKey map[string]interface{}) string {
	cols := make([]string, 0, len(rowKey))
	for k := range rowKey {
		cols = append(cols, k)
	}
	sort.Slice(cols, func(i, j int) bool {
		return jsStringLess(cols[i], cols[j])
	})

	var buf bytes.Buffer
	buf.WriteByte('[')
	jsonString(&buf, "")
	buf.WriteByte(',')
	jsonString(&buf, table)
	for _, col := range cols {
		buf.WriteByte(',')
		jsonString(&buf, col)
		buf.WriteByte(',')
		jsonValue(&buf, rowKey[col])
	}
	buf.WriteByte(']')
	return buf.String()
}

// jsonString matches json-custom-numbers stringify for strings:
// '"'+s+'"' when no control chars, JSON.stringify(s) otherwise.
// Does NOT escape <, >, & (Go's encoding/json does — divergence avoided).
func jsonString(buf *bytes.Buffer, s string) {
	if !needsEscape(s) {
		buf.WriteByte('"')
		buf.WriteString(s)
		buf.WriteByte('"')
		return
	}
	buf.WriteByte('"')
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c == '"':
			buf.WriteString(`\"`)
		case c == '\\':
			buf.WriteString(`\\`)
		case c == '\n':
			buf.WriteString(`\n`)
		case c == '\r':
			buf.WriteString(`\r`)
		case c == '\t':
			buf.WriteString(`\t`)
		case c == '\b':
			buf.WriteString(`\b`)
		case c == '\f':
			buf.WriteString(`\f`)
		case c < 0x20:
			fmt.Fprintf(buf, `\u%04x`, c)
		default:
			buf.WriteByte(c)
		}
	}
	buf.WriteByte('"')
}

func needsEscape(s string) bool {
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c == '"' || c == '\\' || c < 0x20 {
			return true
		}
	}
	return false
}

// jsonValue matches json-custom-numbers stringify for the scalar types
// that appear in PK columns. Use json.Marshal for finite non-zero float64s,
// with explicit JS String(n) fixes for -0 and NaN/Inf.
func jsonValue(buf *bytes.Buffer, v interface{}) {
	switch v := v.(type) {
	case nil:
		buf.WriteString("null")
	case string:
		jsonString(buf, v)
	case float64:
		if math.IsNaN(v) || math.IsInf(v, 0) {
			buf.WriteString("null")
			return
		}
		if v == 0 {
			buf.WriteByte('0')
			return
		}
		b, _ := json.Marshal(v)
		buf.Write(b)
	case bool:
		if v {
			buf.WriteString("true")
		} else {
			buf.WriteString("false")
		}
	case int64:
		buf.WriteString(strconv.FormatInt(v, 10))
	case int:
		buf.WriteString(strconv.Itoa(v))
	default:
		b, _ := json.Marshal(v)
		buf.Write(b)
	}
}

func jsStringLess(a, b string) bool {
	ar := []rune(a)
	br := []rune(b)
	ai, bi := 0, 0
	for ai < len(ar) && bi < len(br) {
		au := utf16Units(ar[ai])
		bu := utf16Units(br[bi])
		for i := 0; i < len(au) && i < len(bu); i++ {
			if au[i] != bu[i] {
				return au[i] < bu[i]
			}
		}
		if len(au) != len(bu) {
			return len(au) < len(bu)
		}
		ai++
		bi++
	}
	return len(ar) < len(br)
}

func utf16Units(r rune) []uint16 {
	if r < 0x10000 {
		return []uint16{uint16(r)}
	}
	r -= 0x10000
	return []uint16{
		uint16(0xD800 + (r >> 10)),
		uint16(0xDC00 + (r & 0x3FF)),
	}
}

// RowSigAccumulator tracks per-queryID XOR deltas for row-set signatures.
// Only ADD (type 0) and REMOVE (type 1) contribute; EDIT (type 2) is a
// no-op, matching TS #trackRowSetSignatures (pipeline-driver.ts:1799).
type RowSigAccumulator struct {
	mu      sync.Mutex
	deltas  map[string]uint64
	touched map[string]struct{}
	invalid map[string]struct{}
}

func NewRowSigAccumulator() *RowSigAccumulator {
	return &RowSigAccumulator{
		deltas:  make(map[string]uint64),
		touched: make(map[string]struct{}),
		invalid: make(map[string]struct{}),
	}
}

func (a *RowSigAccumulator) accumulateChanges(changes []engine.RowChange) {
	a.mu.Lock()
	defer a.mu.Unlock()
	for i := range changes {
		a.accumulateLocked(changes[i])
	}
}

func (a *RowSigAccumulator) accumulate(c engine.RowChange) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.accumulateLocked(c)
}

func (a *RowSigAccumulator) accumulateLocked(c engine.RowChange) {
	if c.Type == engine.RowChangeEdit {
		return
	}
	a.touched[c.QueryID] = struct{}{}
	if _, bad := a.invalid[c.QueryID]; bad {
		return
	}
	sig, ok := rowIDSignatureUnit(c.Table, c.RowKey)
	if !ok {
		a.invalid[c.QueryID] = struct{}{}
		delete(a.deltas, c.QueryID)
		return
	}
	a.deltas[c.QueryID] ^= sig
}

func (a *RowSigAccumulator) deltaHex(queryID string) (string, bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if _, bad := a.invalid[queryID]; bad {
		return "", false
	}
	d := a.deltas[queryID]
	return strconv.FormatUint(d, 16), true
}

func (a *RowSigAccumulator) allDeltasHex() map[string]string {
	a.mu.Lock()
	defer a.mu.Unlock()
	if len(a.touched) == 0 {
		return nil
	}
	out := make(map[string]string, len(a.touched))
	for qid := range a.touched {
		if _, bad := a.invalid[qid]; bad {
			continue
		}
		out[qid] = strconv.FormatUint(a.deltas[qid], 16)
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func rowIDSignatureUnit(table string, rowKey map[string]interface{}) (uint64, bool) {
	for _, v := range rowKey {
		if !isRowKeyScalar(v) {
			return 0, false
		}
	}
	return h64(rowIDString(table, rowKey)), true
}

func isRowKeyScalar(v interface{}) bool {
	switch v.(type) {
	case nil, string, float64, bool, int64, int:
		return true
	default:
		return false
	}
}
