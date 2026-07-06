package main

// Review item #2: the abandoned-RPC divergence window, ported to the
// surviving advance (advanceToHeadStream).
//
// Under the follow-TS failure model the client has no in-process RPC
// timeout, but an in-flight advance can still be ABANDONED: a CG teardown
// (unclassified error elsewhere, client disconnect) queues removeQuery /
// re-registration / destroy behind it, and the Go side has no cancellation
// on the wire — the advance keeps running and its frames keep arriving with
// the old request ID (the TS demux drops frames for unknown IDs).
//
// The contract that keeps this safe, pinned here end-to-end over one pipe
// (handleConnection — the exact code the NAPI host pumps through):
//  1. FIFO isolation — the re-registration (removeQuery + addQueriesStream)
//     queued behind the abandoned call executes strictly AFTER it completes;
//     no interleaving of its frames into the re-registration's responses.
//  2. Convergence — the re-hydrate reads post-advance engine state, i.e.
//     the world INCLUDING the advance TS gave up on. (TS's CVR reconcile
//     takes min(V_ts, V_go); a Go re-hydrate that missed the abandoned
//     advance's writes would under-claim and permanently stale the client.)
//  3. ID hygiene — every frame of the abandoned call carries the abandoned
//     call's ID, never the re-registration's (a cross-ID frame would make
//     the TS accumulator adopt rows into the wrong query result).
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
	"net"
	"testing"
	"time"

	"github.com/kartikparsoya-eng/go-ivm/builder"
	"github.com/kartikparsoya-eng/go-ivm/ivm"
)

func TestAbandonedAdvanceToHeadStream_FIFOIsolationAndConvergence(t *testing.T) {
	path, db := makeReplica(t)
	if !beginConcurrentSupported(t, db) {
		t.Skip("the abandoned advance COMPLETES its drive apply — requires BEGIN CONCURRENT (wal2/libsqlite3 build)")
	}
	server := NewServer(path)
	server.appID = "myapp"
	t.Cleanup(server.closeAll)

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

	// --- Setup: init + a live query (the replica seeds 1 issue row). ---
	send("init", 1, issueInitParams("cg-T"))
	readOK(1)
	const seed = 1

	ast := builder.AST{Table: "issue", OrderBy: ivm.Ordering{{"id", "asc"}}}
	send("addQueriesStream", 3, oneQueryStreamParams("cg-T", "q1", ast, 1))
	readOK(3) // final partial (all rows)
	readOK(3) // terminal "done"

	// --- Stage v2 (the bulk the abandoned advance consumes) and v3 (one
	// more row) up-front. advanceToHead consumes the changelog to HEAD, so
	// the abandoned call applies BOTH versions; the post-reset advance then
	// settles cleanly on an already-at-head replica. ---
	const advanced = 300
	mustExec(t, db, `INSERT INTO issue (id, title, number, _0_version)
		SELECT 'n-'||printf('%04d', value), 't', value, '0000000002'
		FROM (WITH RECURSIVE c(value) AS (SELECT 1 UNION ALL SELECT value+1 FROM c WHERE value < ?1+1) SELECT value FROM c WHERE value <= ?1)`, advanced)
	mustExec(t, db, `INSERT OR REPLACE INTO "_zero.changeLog2" ("stateVersion","pos","table","rowKey","op")
		SELECT '0000000002', value, 'issue', '{"id":"n-'||printf('%04d', value)||'"}', 's'
		FROM (WITH RECURSIVE c(value) AS (SELECT 1 UNION ALL SELECT value+1 FROM c WHERE value < ?1+1) SELECT value FROM c WHERE value <= ?1)`, advanced)
	mustExec(t, db, `INSERT INTO issue (id, title, number, _0_version) VALUES ('post-reset','t',9999,'0000000003')`)
	mustExec(t, db, `INSERT OR REPLACE INTO "_zero.changeLog2" ("stateVersion","pos","table","rowKey","op") VALUES ('0000000003',0,'issue','{"id":"post-reset"}','s')`)
	mustExec(t, db, `INSERT OR REPLACE INTO "_zero.replicationState" (stateVersion, lock) VALUES ('0000000003', 1)`)

	// --- The abandoned call + everything TS queues behind it, written
	// back-to-back so all four sit in the CG FIFO together. ---
	const (
		abandonedID = 50.0 // advanceToHeadStream TS abandons mid-teardown
		removeID    = 51.0
		readdID     = 52.0
		afterID     = 53.0
	)
	send("advanceToHeadStream", abandonedID, advanceToHeadParams{ClientGroupID: "cg-T", InitEpoch: 1})
	// TS gives up NOW (does not await) and resets: remove + re-add (re-add
	// through the streaming hydrate — the unary addQuery RPC is gone).
	send("removeQuery", removeID, removeQueryParams{ClientGroupID: "cg-T", QueryID: "q1", InitEpoch: 1})
	send("addQueriesStream", readdID, oneQueryStreamParams("cg-T", "q1", ast, 1))
	// And one more advance AFTER the reset — already at head, so it settles
	// with an empty Final (proves the FIFO drained cleanly post-reset).
	send("advanceToHeadStream", afterID, advanceToHeadParams{ClientGroupID: "cg-T", InitEpoch: 1})

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
			// Streaming re-hydrate: rows arrive on partial frames, completion
			// on the terminal "done" frame (same positional wire shape as the
			// advance partials).
			if isDone {
				rehydrateSeen = true
			} else {
				rehydrateRows += countPositionalRows(t, resp.Result)
			}
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

	// 1. The abandoned advance fully computed (TS just ignores the frames):
	// the v2 bulk + the v3 row — advanceToHead consumes the changelog to head.
	if abandonedRows != advanced+1 {
		t.Fatalf("abandoned advance emitted %d rows, want %d", abandonedRows, advanced+1)
	}

	// 2. Convergence: the reset re-hydrate sees the world INCLUDING the
	// abandoned advance — the FIFO forced it to run after. Missing rows here
	// = the permanent-staleness bug class.
	if rehydrateRows != seed+advanced+1 {
		t.Fatalf("re-hydrate returned %d rows, want %d (abandoned advance's writes missing → permanent staleness)",
			rehydrateRows, seed+advanced+1)
	}

	// 3. Post-reset advance settles with zero rows (already at head) —
	// which also proves the remove+re-add executed between the two advances
	// (processing order), whatever the wire order of their responses.
	if afterRows != 0 {
		t.Fatalf("post-reset advance emitted %d rows, want 0 (replica already at head)", afterRows)
	}
}

// countPositionalRows extracts the row count from a decoded streaming
// partial (positional rev-9 wire keys: "d" = dict, "r" = rows — both the
// advanceToHeadStream and addQueriesStream partial shapes).
func countPositionalRows(t *testing.T, result interface{}) int {
	t.Helper()
	m, ok := result.(map[string]interface{})
	if !ok {
		t.Fatalf("partial frame Result is %T, want map", result)
	}
	rows, _ := m["r"].([]interface{})
	return len(rows)
}
