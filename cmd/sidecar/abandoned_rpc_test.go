package main

import (
	"fmt"
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

	col := newSinkCollector()
	h := startABIHostWithServer(server, col.sink, nil)
	defer h.Shutdown()

	send := func(method string, id float64, params interface{}) {
		t.Helper()
		if err := h.Send(encodeReq(t, method, id, params)); err != nil {
			t.Fatalf("send %s(%v): %v", method, id, err)
		}
	}
	entries := func() []sinkEntry {
		col.mu.Lock()
		defer col.mu.Unlock()
		out := append([]sinkEntry(nil), col.entries...)
		return out
	}
	waitDone := func(id float64) {
		t.Helper()
		waitFor(t, 10*time.Second, fmt.Sprintf("done for req %.0f", id), func() bool {
			return frameDoneOrFail(t, entries(), id)
		})
	}

	send("init", 1, issueInitParams("cg-T"))
	waitFor(t, 10*time.Second, "init response", func() bool {
		return frameSuccessOrFail(t, entries(), 1)
	})
	const seed = 1

	ast := builder.AST{Table: "issue", OrderBy: ivm.Ordering{{"id", "asc"}}}
	send("addQueriesStream", 3, oneQueryStreamParams("cg-T", "q1", ast, 1))
	waitDone(3)

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

	const (
		abandonedID = 50.0
		removeID    = 51.0
		readdID     = 52.0
		afterID     = 53.0
	)
	send("advanceToHeadStream", abandonedID, prodAdvanceParams("cg-T", 1))
	send("removeQuery", removeID, removeQueryParams{ClientGroupID: "cg-T", QueryID: "q1", InitEpoch: 1})
	send("addQueriesStream", readdID, oneQueryStreamParams("cg-T", "q1", ast, 1))
	send("advanceToHeadStream", afterID, prodAdvanceParams("cg-T", 1))

	waitFor(t, 30*time.Second, "all queued abandoned/reset calls done", func() bool {
		es := entries()
		return frameDoneOrFail(t, es, abandonedID) &&
			frameDoneOrFail(t, es, removeID) &&
			frameDoneOrFail(t, es, readdID) &&
			frameDoneOrFail(t, es, afterID)
	})
	es := entries()
	if abandonedRowsAfterRemove(t, es, abandonedID, removeID) {
		t.Fatal("abandoned call emitted row records after the removeQuery response — FIFO isolation broken")
	}
	if got := countRowRecordsForReq(es, abandonedID); got != advanced+1 {
		t.Fatalf("abandoned advance emitted %d rows, want %d", got, advanced+1)
	}
	if got := countRowRecordsForReq(es, readdID); got != seed+advanced+1 {
		t.Fatalf("re-hydrate returned %d rows, want %d", got, seed+advanced+1)
	}
	if got := countRowRecordsForReq(es, afterID); got != 0 {
		t.Fatalf("post-reset advance emitted %d rows, want 0", got)
	}
}

func frameDoneOrFail(t *testing.T, entries []sinkEntry, id float64) bool {
	t.Helper()
	for _, e := range entries {
		if e.kind != abiKindFrame {
			continue
		}
		resp := decodeResp(t, e.payload)
		rid, ok := toFloat(resp.ID)
		if !ok || rid != id {
			continue
		}
		if resp.Error != nil {
			t.Fatalf("call %.0f failed: %+v", id, resp.Error)
		}
		if s, ok := resp.Result.(string); ok && (s == "done" || s == "ok" || s == "pong") {
			return true
		}
	}
	return false
}

func frameSuccessOrFail(t *testing.T, entries []sinkEntry, id float64) bool {
	t.Helper()
	for _, e := range entries {
		if e.kind != abiKindFrame {
			continue
		}
		resp := decodeResp(t, e.payload)
		rid, ok := toFloat(resp.ID)
		if !ok || rid != id {
			continue
		}
		if resp.Error != nil {
			t.Fatalf("call %.0f failed: %+v", id, resp.Error)
		}
		return true
	}
	return false
}

func countRowRecordsForReq(entries []sinkEntry, id float64) int {
	n := 0
	for _, e := range entries {
		if e.kind == abiKindRow && rowRecordReqID(e.payload) == id {
			n++
		}
	}
	return n
}

func abandonedRowsAfterRemove(t *testing.T, entries []sinkEntry, abandonedID, removeID float64) bool {
	t.Helper()
	removeReplied := false
	for _, e := range entries {
		switch e.kind {
		case abiKindFrame:
			resp := decodeResp(t, e.payload)
			if id, ok := toFloat(resp.ID); ok && id == removeID {
				removeReplied = true
			}
		case abiKindRow:
			if removeReplied && rowRecordReqID(e.payload) == abandonedID {
				return true
			}
		}
	}
	return false
}

func rowRecordReqID(payload []byte) float64 {
	r := &recReader{buf: payload}
	return r.f64()
}
