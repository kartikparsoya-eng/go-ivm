package main

// Review item #2: the timed-out / abandoned-RPC divergence window.
//
// When the TS client's RPC budget expires it classifies the advance
// 'unclassified' → ResetPipelinesSignal → re-registration — but it CANNOT
// cancel the RPC: the Go side has no cancellation on the wire, so the
// abandoned advanceStream keeps running and its frames keep arriving with
// the old request ID (the TS demux drops frames for unknown IDs).
//
// The contract that keeps this safe, pinned here end-to-end over one socket:
//   1. FIFO isolation — the re-registration (removeQuery + addQuery) queued
//      behind the abandoned call executes strictly AFTER it completes; no
//      interleaving of its frames into the re-registration's responses.
//   2. Convergence — the re-hydrate reads post-advance engine state, i.e.
//      the world INCLUDING the advance TS gave up on. (TS's CVR reconcile
//      takes min(V_ts, V_go); a Go re-hydrate that missed the abandoned
//      advance's writes would under-claim and permanently stale the client.)
//   3. ID hygiene — every frame of the abandoned call carries the abandoned
//      call's ID, never the re-registration's (a cross-ID frame would make
//      the TS accumulator adopt rows into the wrong query result).
//
// Wire-order finding (documented, not a bug): STREAM partials are enqueued
// to flushCh directly from the worker goroutine, while non-streaming/done
// responses ride a per-response goroutine (respCh → dispatcher → flushCh).
// Across calls those two paths can reorder on the wire (goroutine wake order
// isn't FIFO), so this test asserts per-call ordering and processing-order
// CONSEQUENCES (state convergence) — never cross-call wire order. The TS
// client demuxes strictly by request ID, so it must never assume cross-call
// ordering either.

import (
	"bufio"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/kartikparsoya-eng/go-ivm/builder"
	"github.com/kartikparsoya-eng/go-ivm/engine"
	"github.com/kartikparsoya-eng/go-ivm/ivm"
	"github.com/kartikparsoya-eng/go-ivm/sqlite"
)

func TestAbandonedAdvanceStream_FIFOIsolationAndConvergence(t *testing.T) {
	server := NewServer(0, "") // memory mode: loadRows-backed sources persist across advances
	cliConn, srvConn := net.Pipe()
	connDone := make(chan struct{})
	go func() {
		handleConnection(srvConn, server)
		close(connDone)
	}()
	t.Cleanup(func() {
		cliConn.Close()
		<-connDone
	})
	cli := bufio.NewReaderSize(cliConn, 256*1024)

	send := func(method string, id float64, params interface{}) {
		t.Helper()
		if err := writeFrame(cliConn, encodeReq(t, method, id, params)); err != nil {
			t.Fatalf("write %s(%v): %v", method, id, err)
		}
	}
	read := func() RPCResponse {
		t.Helper()
		_ = cliConn.SetReadDeadline(time.Now().Add(10 * time.Second))
		frame, err := readFrame(cli)
		if err != nil {
			t.Fatalf("read: %v", err)
		}
		var resp RPCResponse
		if err := mpUnmarshal(frame, &resp); err != nil {
			t.Fatalf("decode: %v", err)
		}
		return resp
	}
	readOK := func(wantID float64) RPCResponse {
		t.Helper()
		resp := read()
		if resp.ID != interface{}(wantID) {
			t.Fatalf("response ID = %v, want %v (FIFO order broken)", resp.ID, wantID)
		}
		if resp.Error != nil {
			t.Fatalf("call %v failed: %+v", wantID, resp.Error)
		}
		return resp
	}

	// --- Setup: init + loadRows + a live query. ---
	send("init", 1, initParams{
		ClientGroupID: "cg-T",
		Storage:       t.TempDir() + "/storage.db",
		Tables: map[string]tableSchemaParams{
			"tickets": {
				Columns: map[string]sqlite.ColumnSchema{
					"id":        {Type: "string"},
					"updatedAt": {Type: "number"},
				},
				PrimaryKey: []string{"id"},
			},
		},
	})
	readOK(1)

	const seed = 10
	seedRows := make([]ivm.Row, seed)
	for i := range seedRows {
		seedRows[i] = ivm.Row{"id": fmt.Sprintf("t-%03d", i), "updatedAt": float64(1000 + i)}
	}
	send("loadRows", 2, loadRowsParams{ClientGroupID: "cg-T", Table: "tickets", Rows: seedRows, InitEpoch: 1})
	readOK(2)

	ast := builder.AST{Table: "tickets", OrderBy: ivm.Ordering{{"id", "asc"}}}
	send("addQuery", 3, addQueryParams{ClientGroupID: "cg-T", QueryID: "q1", AST: ast, InitEpoch: 1})
	readOK(3)

	// --- The abandoned call + everything TS queues behind it, written
	// back-to-back so all four sit in the CG FIFO together. ---
	const (
		abandonedID = 50.0 // advanceStream TS will "time out" on
		removeID    = 51.0
		readdID     = 52.0
		afterID     = 53.0
	)
	const advanced = 300
	changes := make([]engine.SnapshotChange, advanced)
	for i := range changes {
		changes[i] = engine.SnapshotChange{
			Table:     "tickets",
			NextValue: ivm.Row{"id": fmt.Sprintf("n-%04d", i), "updatedAt": float64(5000 + i)},
		}
	}
	send("advanceStream", abandonedID, advanceParams{ClientGroupID: "cg-T", InitEpoch: 1, Changes: changes})
	// TS gives up NOW (does not await) and resets: remove + re-add.
	send("removeQuery", removeID, removeQueryParams{ClientGroupID: "cg-T", QueryID: "q1", InitEpoch: 1})
	send("addQuery", readdID, addQueryParams{ClientGroupID: "cg-T", QueryID: "q1", AST: ast, InitEpoch: 1})
	// And one more advance AFTER the reset — the re-wired world keeps moving.
	send("advanceStream", afterID, advanceParams{
		ClientGroupID: "cg-T", InitEpoch: 1,
		Changes: []engine.SnapshotChange{{
			Table:     "tickets",
			NextValue: ivm.Row{"id": "post-reset", "updatedAt": float64(9999)},
		}},
	})

	// --- Read until every call has resolved; verify isolation + convergence. ---
	var (
		abandonedRows int
		abandonedDone bool
		removeReplied bool
		rehydrateSeen bool
		rehydrateRows int
		afterRows     int
		afterDone     bool
	)
	deadline := time.Now().Add(30 * time.Second)
	for !(abandonedDone && removeReplied && rehydrateSeen && afterDone) {
		if time.Now().After(deadline) {
			t.Fatalf("stream incomplete: abandonedDone=%v remove=%v readd=%v after=%v",
				abandonedDone, removeReplied, rehydrateSeen, afterDone)
		}
		resp := read()
		id, _ := resp.ID.(float64)
		if resp.Error != nil {
			t.Fatalf("call %v failed: %+v", id, resp.Error)
		}
		isDone := false
		if s, ok := resp.Result.(string); ok && s == "done" {
			isDone = true
		}
		switch id {
		case abandonedID:
			// Partials are pushed to flushCh directly from the worker, so an
			// abandoned PARTIAL after the removeQuery response would mean the
			// worker was still mid-advance when it processed the remove —
			// genuine FIFO breakage. (The abandoned DONE rides the goroutine
			// path and may lawfully land after; only partials are asserted.)
			if !isDone && removeReplied {
				t.Fatal("abandoned call emitted a PARTIAL after the removeQuery response — FIFO isolation broken")
			}
			if isDone {
				abandonedDone = true
			} else {
				abandonedRows += countPositionalRows(t, resp.Result)
			}
		case removeID:
			removeReplied = true
		case readdID:
			rehydrateSeen = true
			rehydrateRows = countAddQueryRows(t, resp.Result)
		case afterID:
			if isDone {
				afterDone = true
			} else {
				afterRows += countPositionalRows(t, resp.Result)
			}
		default:
			t.Fatalf("frame with unexpected ID %v", resp.ID)
		}
	}

	// 1. The abandoned advance fully computed (TS just ignores the frames).
	if abandonedRows != advanced {
		t.Fatalf("abandoned advance emitted %d rows, want %d", abandonedRows, advanced)
	}

	// 2. Convergence: the reset re-hydrate sees the world INCLUDING the
	// abandoned advance — the FIFO forced it to run after. Missing rows here
	// = the permanent-staleness bug class.
	if rehydrateRows != seed+advanced {
		t.Fatalf("re-hydrate returned %d rows, want %d (abandoned advance's writes missing → permanent staleness)",
			rehydrateRows, seed+advanced)
	}

	// 3. Post-reset advance flows to the re-wired query — exactly one row,
	// which also proves the remove+re-add executed between the two advances
	// (processing order), whatever the wire order of their responses.
	if afterRows != 1 {
		t.Fatalf("post-reset advance emitted %d rows, want 1", afterRows)
	}
}

// countPositionalRows extracts the row count from a decoded advanceStream
// partial (positional rev-9 wire keys: "d" = dict, "r" = rows).
func countPositionalRows(t *testing.T, result interface{}) int {
	t.Helper()
	m, ok := result.(map[string]interface{})
	if !ok {
		t.Fatalf("partial frame Result is %T, want map", result)
	}
	rows, _ := m["r"].([]interface{})
	return len(rows)
}

// countAddQueryRows extracts len(changes) from an addQuery response.
func countAddQueryRows(t *testing.T, result interface{}) int {
	t.Helper()
	m, ok := result.(map[string]interface{})
	if !ok {
		t.Fatalf("addQuery Result is %T, want map", result)
	}
	changes, _ := m["changes"].([]interface{})
	return len(changes)
}
