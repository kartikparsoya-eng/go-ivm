package main

import (
	"sort"

	"github.com/kartikparsoya-eng/go-ivm/engine"
	"github.com/kartikparsoya-eng/go-ivm/ivm"
)

// Positional (protocolRev 9) wire encoding for streamed RowChange chunks.
//
// The legacy form encoded each RowChange's Row as a msgpack map, re-sending the
// column-name keys on every row. Since both sides share the schema, those keys
// are pure waste — ~2/3 of the bytes and the bulk of encode/decode work. The
// positional form sends the column-name keys ONCE per (queryID,table) group in
// a dictionary, and each row as a value-only array referencing its group.
//
// Frame:  { d: []dictEntry, r: [][]any }
//   - each r[i] is [dictId, type, ...values]
//   - add/edit (type 0/2): values are dictEntry.Cols in order (nil-filled)
//   - remove   (type 1):   values are dictEntry.PK   in order (Row is absent)
//
// The TS client (go-ivm-client.ts decodePositionalChanges) rebuilds RowChange[]:
// for add/edit it derives rowKey from the row's PK columns (pkValue is a pure
// lookup, so rowKey[pk] == row[pk]); for remove it reads the PK values directly.
//
// Order-preserving: rows keep their input order (each tagged with its group's
// dict index), so the existing wire ordering is unchanged — only the framing of
// each row differs.

type dictEntry struct {
	Q    string   `json:"q"` // queryID
	T    string   `json:"t"` // table
	Cols []string `json:"c"` // canonical column order for add/edit rows
	PK   []string `json:"k"` // primary-key column names
}

type positionalChanges struct {
	Dict []dictEntry     `json:"d"`
	Rows [][]interface{} `json:"r"`
}

type pgKey struct{ q, t string }

// toPositional flattens a RowChange chunk into the positional wire form.
//
// Column order per group is the SORTED UNION of the group's add/edit row keys
// (including the synthetic _0_version, which is always present). Under the
// homogeneity that holds for replicated tables — every row of a table carries
// the same column set, NULLs included as nil values — the union equals each
// row's key set, so the nil-fill never introduces a spurious column. Shadow
// mode validates this directly (it content-compares the rebuilt rows).
func toPositional(changes []engine.RowChange) positionalChanges {
	if len(changes) == 0 {
		return positionalChanges{}
	}

	idx := make(map[pgKey]int)
	var dict []dictEntry
	var colSets []map[string]struct{}

	for i := range changes {
		c := &changes[i]
		k := pgKey{c.QueryID, c.Table}
		id, ok := idx[k]
		if !ok {
			id = len(dict)
			idx[k] = id
			dict = append(dict, dictEntry{Q: c.QueryID, T: c.Table, PK: sortedMapKeys(c.RowKey)})
			colSets = append(colSets, map[string]struct{}{})
		}
		if c.Type != engine.RowChangeRemove && c.Row != nil {
			for col := range c.Row {
				colSets[id][col] = struct{}{}
			}
		}
	}
	for id := range dict {
		dict[id].Cols = sortedSetKeys(colSets[id])
	}

	rows := make([][]interface{}, len(changes))
	for i := range changes {
		c := &changes[i]
		id := idx[pgKey{c.QueryID, c.Table}]
		if c.Type == engine.RowChangeRemove {
			pk := dict[id].PK
			row := make([]interface{}, 2+len(pk))
			row[0], row[1] = id, c.Type
			for j, col := range pk {
				row[2+j] = c.RowKey[col]
			}
			rows[i] = row
		} else {
			cols := dict[id].Cols
			row := make([]interface{}, 2+len(cols))
			row[0], row[1] = id, c.Type
			for j, col := range cols {
				row[2+j] = c.Row[col] // nil for any absent column
			}
			rows[i] = row
		}
	}
	return positionalChanges{Dict: dict, Rows: rows}
}

// fromPositional is the inverse of toPositional: it rebuilds []engine.RowChange
// from a positional frame. The Go side never decodes its own wire (only the TS
// client does), but this mirrors go-ivm-client.ts decodePositionalChanges and
// backs the round-trip unit test. For add/edit, rowKey is derived from the row's
// PK columns (matching pkValue's pure-lookup behavior); for remove, the PK
// values are read directly and Row stays nil.
func fromPositional(pc positionalChanges) []engine.RowChange {
	out := make([]engine.RowChange, len(pc.Rows))
	for i, r := range pc.Rows {
		e := pc.Dict[toInt(r[0])]
		typ := toInt(r[1])
		rc := engine.RowChange{Type: typ, QueryID: e.Q, Table: e.T}
		if typ == engine.RowChangeRemove {
			rk := make(map[string]interface{}, len(e.PK))
			for j, col := range e.PK {
				rk[col] = r[2+j]
			}
			rc.RowKey = rk
		} else {
			row := make(ivm.Row, len(e.Cols))
			for j, col := range e.Cols {
				row[col] = r[2+j]
			}
			rc.Row = row
			rk := make(map[string]interface{}, len(e.PK))
			for _, col := range e.PK {
				rk[col] = row[col]
			}
			rc.RowKey = rk
		}
		out[i] = rc
	}
	return out
}

// toInt coerces a positional row's leading int fields (dictId, type) back to an
// int, tolerating the int/int64/float64 forms a msgpack round-trip can produce.
func toInt(v interface{}) int {
	switch n := v.(type) {
	case int:
		return n
	case int64:
		return int(n)
	case uint64:
		return int(n)
	case float64:
		return int(n)
	}
	return 0
}

func sortedMapKeys(m map[string]interface{}) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func sortedSetKeys(m map[string]struct{}) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
