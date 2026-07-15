package main

// E2E pull-mode tests over the full in-process stack: abi host →
// handleConnection → handleAddQueriesStream(pullMode) → demand gate → row
// plane → sink. Everything runs without the addon: grants/cancels call the
// same registry methods the goivm_stream_credit/goivm_stream_cancel exports
// are thin shims over (napi_lib.go), so the whole pull discipline is
// -race-testable with the ordinary toolchain.
//
// Pins:
//   - Lockstep at W=1: zero row-bearing deliveries before the first
//     grant; rows delivered == credits granted at every checkpoint
//     (produced ≤ consumed + 1 with the gate sitting before enqueue).
//   - Final/error/done frames ride free (never gated): the terminal
//     error frame of a cancelled stream arrives while credit is zero.
//   - Cancel → the RPC settles with a plain -32000 error frame, the gate
//     is unregistered, and the group remains fully usable.
//   - Idle sweep cancels a parked stream (same unwind as cancel).
//   - Teardown-cancels-gates: host Shutdown completes while a pull
//     producer is parked.
//   - pullMode without rowMode is rejected: the pull contract is
//     mandatory once requested, never silently downgraded to ungated frames.

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/kartikparsoya-eng/go-ivm/sqlite"
)

// startPullHost boots a table-mode host over a replica whose `users` table
// is seeded with nRows and returns the server, collector, host, and a send
// helper.
func startPullHost(t *testing.T, nRows int) (*Server, *sinkCollector, *abiHost, func(id float64, method string, params interface{})) {
	t.Helper()
	path := makeUsersReplica(t, nRows)
	srv := NewServer(path)
	srv.appID = "myapp"
	col := newSinkCollector()
	h := startABIHostWithServer(srv, col.sink, nil)
	t.Cleanup(h.Shutdown)

	send := func(id float64, method string, params interface{}) {
		t.Helper()
		if err := h.Send(encodeReq(t, method, id, params)); err != nil {
			t.Fatalf("send %s: %v", method, err)
		}
	}

	send(1, "init", initParams{
		ClientGroupID: "cg-pull",
		Tables: map[string]tableSchemaParams{
			"users": {
				Columns: map[string]sqlite.ColumnSchema{
					"id":         {Type: "string"},
					"name":       {Type: "string"},
					"_0_version": {Type: "string"},
				},
				PrimaryKey: []string{"id"},
				UniqueKeys: [][]string{{"id"}},
			},
		},
	})
	return srv, col, h, send
}

// makeUsersReplica builds a temp WAL replica with the _zero metadata tables
// + a users table seeded with nRows at version v1.
func makeUsersReplica(t *testing.T, nRows int) string {
	t.Helper()
	path, db := makeReplica(t)
	mustExec(t, db, `CREATE TABLE "users" ("id" TEXT PRIMARY KEY,"name" TEXT,"_0_version" TEXT)`)
	for i := 0; i < nRows; i++ {
		mustExec(t, db, `INSERT INTO "users" VALUES (?,?,'0000000001')`,
			fmt.Sprintf("u%04d", i), fmt.Sprintf("User-%d", i))
	}
	return path
}

func pullQueryParams(pull bool, rowMode bool) map[string]interface{} {
	return map[string]interface{}{
		"clientGroupID": "cg-pull",
		"initEpoch":     1,
		"rowMode":       rowMode,
		"pullMode":      pull,
		"queries": []map[string]interface{}{
			{"queryID": "q-pull", "ast": map[string]interface{}{
				"table":   "users",
				"orderBy": [][]string{{"id", "asc"}},
			}},
		},
	}
}

// countKind returns how many sink entries of `kind` have arrived.
func countKind(col *sinkCollector, kind int32) int {
	col.mu.Lock()
	defer col.mu.Unlock()
	n := 0
	for _, e := range col.entries {
		if e.kind == kind {
			n++
		}
	}
	return n
}

// waitRows polls until exactly `want` kind-3 records arrived; fails on
// timeout or overshoot (overshoot = the gate leaked deliveries).
func waitRows(t *testing.T, col *sinkCollector, want int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		got := countKind(col, abiKindRow)
		if got == want {
			return
		}
		if got > want {
			t.Fatalf("row records = %d, want exactly %d (gate leaked deliveries)", got, want)
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out: row records = %d, want %d", got, want)
		}
		time.Sleep(2 * time.Millisecond)
	}
}

// assertRowsStable asserts the row count stays at `want` for `window`.
func assertRowsStable(t *testing.T, col *sinkCollector, want int, window time.Duration) {
	t.Helper()
	time.Sleep(window)
	if got := countKind(col, abiKindRow); got != want {
		t.Fatalf("row records moved to %d during quiet window, want stable at %d", got, want)
	}
}

// waitGateRegistered polls until the pull gate for the RPC is registered.
func waitGateRegistered(t *testing.T, srv *Server, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for srv.streamGates.size() == 0 {
		if time.Now().After(deadline) {
			t.Fatal("pull gate never registered")
		}
		time.Sleep(2 * time.Millisecond)
	}
}

// frameFor scans kind-1 frames for the RPC id and returns (response, found)
// for the first frame matching pred.
func frameFor(col *sinkCollector, t *testing.T, id float64, pred func(RPCResponse) bool) (RPCResponse, bool) {
	col.mu.Lock()
	frames := make([][]byte, len(col.frames))
	copy(frames, col.frames)
	col.mu.Unlock()
	for _, f := range frames {
		resp := decodeResp(t, f)
		if got, ok := toFloat(resp.ID); !ok || got != id {
			continue
		}
		if pred(resp) {
			return resp, true
		}
	}
	return RPCResponse{}, false
}

// TestPullMode_OpeningWindowViaRequest verifies the race-free opening
// window: pullWindow rides the request, so exactly W rows flow with no
// grant call ever made — produced ≤ consumed + W — and top-ups extend the
// window from there. This is the production shape (the client always sends
// pullWindow ≥ 1; grant calls are top-ups only).
func TestPullMode_OpeningWindowViaRequest(t *testing.T) {
	const nRows = 30
	srv, col, _, send := startPullHost(t, nRows)

	params := pullQueryParams(true, true)
	params["pullWindow"] = 4
	send(3, "addQueriesStream", params)
	waitGateRegistered(t, srv, 5*time.Second)

	// Exactly the opening window flows; the producer parks at W.
	waitRows(t, col, 4, 5*time.Second)
	assertRowsStable(t, col, 4, 150*time.Millisecond)

	// A top-up extends it.
	srv.streamGates.grant(3, 4)
	waitRows(t, col, 8, 5*time.Second)
	assertRowsStable(t, col, 8, 100*time.Millisecond)

	// Drain the rest.
	srv.streamGates.grant(3, int64(nRows-8))
	waitRows(t, col, nRows, 5*time.Second)
}

// TestPullMode_LockstepDemandGate verifies the lockstep contract at W=1:
// no row-bearing delivery may cross the boundary until the client grants,
// and deliveries track grants exactly.
func TestPullMode_LockstepDemandGate(t *testing.T) {
	const nRows = 30
	srv, col, _, send := startPullHost(t, nRows)

	send(3, "addQueriesStream", pullQueryParams(true, true))
	waitGateRegistered(t, srv, 5*time.Second)

	// Zero credit ⇒ zero rows, no matter how long the producer has run.
	assertRowsStable(t, col, 0, 150*time.Millisecond)

	// One credit ⇒ exactly one row (one gated delivery == one row ==
	// literal TS next() at chunkSize=1).
	srv.streamGates.grant(3, 1)
	waitRows(t, col, 1, 5*time.Second)
	assertRowsStable(t, col, 1, 100*time.Millisecond)

	// Five more ⇒ exactly six.
	srv.streamGates.grant(3, 5)
	waitRows(t, col, 6, 5*time.Second)
	assertRowsStable(t, col, 6, 100*time.Millisecond)

	// Grant the remainder ⇒ full result + ungated final + done frames.
	srv.streamGates.grant(3, int64(nRows-6))
	waitRows(t, col, nRows, 5*time.Second)

	deadline := time.Now().Add(5 * time.Second)
	for {
		_, foundFinal := frameFor(col, t, 3, func(r RPCResponse) bool {
			m, ok := r.Result.(map[string]interface{})
			if !ok {
				return false
			}
			fin, _ := m["final"].(bool)
			return fin
		})
		_, foundDone := frameFor(col, t, 3, func(r RPCResponse) bool {
			s, ok := r.Result.(string)
			return ok && s == "done"
		})
		if foundFinal && foundDone {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("final/done frames missing after all credits (final=%v done=%v)",
				foundFinal, foundDone)
		}
		time.Sleep(5 * time.Millisecond)
	}

	// Gate is unregistered on handler return.
	deadline = time.Now().Add(2 * time.Second)
	for srv.streamGates.size() != 0 {
		if time.Now().After(deadline) {
			t.Fatalf("gate registry size = %d after completion, want 0", srv.streamGates.size())
		}
		time.Sleep(2 * time.Millisecond)
	}
}

// TestPullMode_CancelUnwindsAndRejects verifies that mid-stream cancel
// produces a clean "done" terminal frame (not an error frame), stops
// all row production, unregisters the gate, and leaves the group fully
// usable for the next hydrate. Returning "done" instead of a -32000
// error frame prevents false CG teardown on the TS side when a client
// cancels mid-stream (e.g. tab close, RPC timeout).
func TestPullMode_CancelUnwindsAndRejects(t *testing.T) {
	const nRows = 30
	srv, col, _, send := startPullHost(t, nRows)

	send(3, "addQueriesStream", pullQueryParams(true, true))
	waitGateRegistered(t, srv, 5*time.Second)

	srv.streamGates.grant(3, 3)
	waitRows(t, col, 3, 5*time.Second)

	// Cancel with zero credit outstanding — the producer is parked.
	srv.streamGates.cancel(3)

	// Terminal frame is a clean "done" (not an error frame) so the TS
	// client resolves the call promise without triggering CG teardown.
	deadline := time.Now().Add(5 * time.Second)
	for {
		if resp, found := frameFor(col, t, 3, func(r RPCResponse) bool {
			s, ok := r.Result.(string)
			return ok && s == "done"
		}); found {
			_ = resp
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("no terminal \"done\" frame after cancel")
		}
		time.Sleep(2 * time.Millisecond)
	}

	// No further rows; gate unregistered.
	rowsAtCancel := countKind(col, abiKindRow)
	assertRowsStable(t, col, rowsAtCancel, 150*time.Millisecond)
	if srv.streamGates.size() != 0 {
		t.Fatalf("gate registry size = %d after cancel settle, want 0", srv.streamGates.size())
	}

	// Group healthy: a fresh production row/pull hydrate completes.
	send(4, "addQueriesStream", map[string]interface{}{
		"clientGroupID": "cg-pull",
		"initEpoch":     1,
		"rowMode":       true,
		"pullMode":      true,
		"pullWindow":    nRows,
		"queries": []map[string]interface{}{
			{"queryID": "q-after", "ast": map[string]interface{}{
				"table":   "users",
				"orderBy": [][]string{{"id", "asc"}},
			}},
		},
	})
	deadline = time.Now().Add(10 * time.Second)
	for {
		if _, found := frameFor(col, t, 4, func(r RPCResponse) bool {
			s, ok := r.Result.(string)
			return ok && s == "done"
		}); found {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("post-cancel hydrate never completed — cancel wedged the group")
		}
		time.Sleep(5 * time.Millisecond)
	}
	if got := countKind(col, abiKindRow); got != rowsAtCancel+nRows {
		t.Fatalf("post-cancel hydrate rows = %d, want %d", got-rowsAtCancel, nRows)
	}
}

// TestPullMode_IdleSweepCancelsParked verifies that a stream parked past
// the idle window with no grants is auto-cancelled by the sweeper — a
// CLEAN CLOSE (Result: "done") instead of a terminal error frame. The
// idle timeout is a liveness mechanism, not an error: the client went
// idle (background tab, stopped consuming) and the server reclaims the
// WAL-frame pin. The JS iterator ends gracefully without throwing.
func TestPullMode_IdleSweepCancelsParked(t *testing.T) {
	srv, col, _, send := startPullHost(t, 10)

	send(3, "addQueriesStream", pullQueryParams(true, true))
	waitGateRegistered(t, srv, 5*time.Second)

	// Let the producer reach the gate and park (first acquire).
	time.Sleep(100 * time.Millisecond)

	// Sweep with a 50ms idle window — the gate has never been granted, so
	// lastGrant == creation (>100ms ago) and a producer is parked.
	if n := srv.streamGates.sweepIdle(time.Now(), 50*time.Millisecond); n != 1 {
		t.Fatalf("sweepIdle cancelled %d gates, want 1", n)
	}

	deadline := time.Now().Add(5 * time.Second)
	for {
		if r, found := frameFor(col, t, 3, func(r RPCResponse) bool { return r.Result != nil }); found {
			if r.Result != "done" {
				t.Fatalf("idle-swept stream result = %v, want \"done\" (clean close)", r.Result)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("no terminal \"done\" frame after idle sweep")
		}
		time.Sleep(2 * time.Millisecond)
	}
	if got := countKind(col, abiKindRow); got != 0 {
		t.Fatalf("idle-swept stream delivered %d rows, want 0", got)
	}
}

// TestPullMode_ShutdownUnparksProducer verifies the teardown broadcast:
// host Shutdown (→ closeAll → shutdownGroup) must cancel the group's gates
// before taking group.mu, or teardown deadlocks behind the parked
// producer's RPC handler.
func TestPullMode_ShutdownUnparksProducer(t *testing.T) {
	srv, _, h, send := startPullHost(t, 10)

	send(3, "addQueriesStream", pullQueryParams(true, true))
	waitGateRegistered(t, srv, 5*time.Second)
	time.Sleep(100 * time.Millisecond) // producer parked at zero credit

	done := make(chan struct{})
	go func() {
		h.Shutdown()
		close(done)
	}()
	select {
	case <-done:
		// teardown completed while a pull producer was parked
	case <-time.After(15 * time.Second):
		t.Fatal("host Shutdown deadlocked behind a parked pull producer (gates not cancelled before group.mu)")
	}
}

// TestPullMode_WithoutRowModeErrors verifies that pullMode without the
// row plane (rowMode=false) is a protocol error. The production path
// requires row records and pull credit; silently downgrading would
// reintroduce the eager buffered path this contract removes.
func TestPullMode_WithoutRowModeErrors(t *testing.T) {
	const nRows = 10
	srv, col, _, send := startPullHost(t, nRows)

	send(3, "addQueriesStream", pullQueryParams(true, false))

	deadline := time.Now().Add(10 * time.Second)
	var got RPCResponse
	for {
		if srv.streamGates.size() != 0 {
			t.Fatal("gate registered for a non-rowMode RPC — pull must require the row plane")
		}
		if _, found := frameFor(col, t, 3, func(r RPCResponse) bool {
			return r.Error != nil
		}); found {
			got, _ = frameFor(col, t, 3, func(r RPCResponse) bool {
				return r.Error != nil
			})
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("non-rowMode pull RPC never returned an error")
		}
		time.Sleep(5 * time.Millisecond)
	}
	if got.Error.Code != -32000 {
		t.Fatalf("error code = %d, want -32000 (%s)", got.Error.Code, got.Error.Message)
	}
	if !strings.Contains(got.Error.Message, "requires row-mode pull NAPI transport") {
		t.Fatalf("error = %q, want mandatory row-mode message", got.Error.Message)
	}
	if got := countKind(col, abiKindRow); got != 0 {
		t.Fatalf("non-rowMode RPC produced %d row records, want 0 (frames only)", got)
	}
}
