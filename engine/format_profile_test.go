package engine

// Throwaway: compares candidate wire encodings for advance/hydrate RowChange
// frames against the current map-keyed baseline. Measures Go-side encode,
// decode, and wire bytes. The TS-side (msgpackr) decode win is measured
// separately in a node script. Delete after reading.
//
//	baseline    — []RowChange, Row = map[string]Value (keys repeated per row)
//	positional  — one group header (queryID, table, column order) + rows as
//	              value-only arrays. Keys sent ONCE per group, not per row.
//	columnar    — same header + struct-of-arrays (one array per column).
//
// Run: go test ./engine -run TestFormatProfile -v

import (
	"fmt"
	"os"
	"testing"
	"time"
)

// column order shared by both sides (derived from schema in production).
var convCols = []string{
	"conversationId", "channelId", "workspaceId", "createdBy", "title",
	"createdAt", "updatedAt", "lastMessageAt", "participantCount", "unreadCount",
	"messageCount", "isPinned", "isArchived", "parentMessageId", "conversationSeenCutoffAt",
}

type posGroup struct {
	QueryID string          `json:"q"`
	Table   string          `json:"t"`
	Cols    []string        `json:"c"`
	Rows    [][]interface{} `json:"r"` // each: [type, v0, v1, ...] in Cols order
}

type colGroup struct {
	QueryID string          `json:"q"`
	Table   string          `json:"t"`
	Cols    []string        `json:"c"`
	Types   []int           `json:"y"`
	Columns [][]interface{} `json:"d"` // Columns[i] = all values for Cols[i]
}

func toPositional(ch []RowChange) posGroup {
	g := posGroup{QueryID: ch[0].QueryID, Table: ch[0].Table, Cols: convCols, Rows: make([][]interface{}, len(ch))}
	for i, c := range ch {
		row := make([]interface{}, 1+len(convCols))
		row[0] = c.Type
		for j, col := range convCols {
			row[1+j] = c.Row[col]
		}
		g.Rows[i] = row
	}
	return g
}

func toColumnar(ch []RowChange) colGroup {
	g := colGroup{QueryID: ch[0].QueryID, Table: ch[0].Table, Cols: convCols,
		Types: make([]int, len(ch)), Columns: make([][]interface{}, len(convCols))}
	for j := range convCols {
		g.Columns[j] = make([]interface{}, len(ch))
	}
	for i, c := range ch {
		g.Types[i] = c.Type
		for j, col := range convCols {
			g.Columns[j][i] = c.Row[col]
		}
	}
	return g
}

func TestFormatProfile(t *testing.T) {
	if os.Getenv("GO_IVM_BENCH") == "" {
		t.Skip("wire-format microbench; set GO_IVM_BENCH=1 to run")
	}
	sizes := []int{100, 1000, 10000}
	const iters = 200

	timeEncDec := func(build func() interface{}, decTarget func() interface{}) (encNs, decNs int64, nbytes int) {
		v := build()
		enc := mpEnc(v)
		nbytes = len(enc)
		tE := time.Now()
		for i := 0; i < iters; i++ {
			_ = mpEnc(v)
		}
		encNs = time.Since(tE).Nanoseconds() / iters
		tD := time.Now()
		for i := 0; i < iters; i++ {
			dst := decTarget()
			mpDec(enc, dst)
		}
		decNs = time.Since(tD).Nanoseconds() / iters
		return
	}

	fmt.Printf("\n%-10s %-12s %-12s %-12s %-12s\n", "format", "N", "encode_us", "decode_us", "bytes")
	for _, n := range sizes {
		ch := makeChanges(n)

		be, bd, bb := timeEncDec(
			func() interface{} { return ch },
			func() interface{} { var d []RowChange; return &d })
		fmt.Printf("%-10s %-12d %-12.1f %-12.1f %-12d\n", "baseline", n, us(be), us(bd), bb)

		pg := toPositional(ch)
		pe, pd, pb := timeEncDec(
			func() interface{} { return pg },
			func() interface{} { var d posGroup; return &d })
		fmt.Printf("%-10s %-12d %-12.1f %-12.1f %-12d  (enc %.2fx, dec %.2fx, %.2fx bytes)\n",
			"positional", n, us(pe), us(pd), pb,
			float64(be)/float64(pe), float64(bd)/float64(pd), float64(bb)/float64(pb))

		cg := toColumnar(ch)
		ce, cd, cb := timeEncDec(
			func() interface{} { return cg },
			func() interface{} { var d colGroup; return &d })
		fmt.Printf("%-10s %-12d %-12.1f %-12.1f %-12d  (enc %.2fx, dec %.2fx, %.2fx bytes)\n",
			"columnar", n, us(ce), us(cd), cb,
			float64(be)/float64(ce), float64(bd)/float64(cd), float64(bb)/float64(cb))
	}
	fmt.Println()
}

func us(ns int64) float64 { return float64(ns) / 1000 }
