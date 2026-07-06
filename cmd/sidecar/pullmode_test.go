package main

// E2E pull-mode (ABI v3) tests over the full in-process stack: abi host →
// handleConnection → handleAddQueriesStream(pullMode) → demand gate → row
// plane → sink. Everything runs WITHOUT the addon: grants/cancels call the
// same registry methods the goivm_stream_credit/goivm_stream_cancel exports
// are paper-thin shims over (napi_lib.go), so the whole pull discipline is
// -race-testable with the ordinary toolchain.
//
// Pins (DESIGN-duplex-streaming §6 validation gates):
//   - I6 lockstep at W=1: ZERO row-bearing deliveries before the first
//     grant; rows delivered == credits granted at every checkpoint
//     (produced ≤ consumed + 1 with the gate sitting before enqueue).
//   - Final/error/done frames ride FREE (never gated): the terminal
//     error frame of a cancelled stream arrives while credit is zero.
//   - Cancel → the RPC settles with a -32000 error frame (I9: plain
//     -32000, never the -32102 data-error class), the gate is
//     unregistered, and the group remains fully usable.
//   - D7 idle sweep cancels a parked stream (same unwind as cancel).
//   - Teardown-cancels-gates: host Shutdown completes while a pull
//     producer is parked (pre-hook this deadlocks on group.mu).
//   - pullMode without rowMode degrades to the plain streaming path
//     (no gate registered, frames flow ungated).

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/kartikparsoya-eng/go-ivm/ivm"
	"github.com/kartikparsoya-eng/go-ivm/sqlite"
)

// startPullHost boots a memory-mode host with `users` seeded with nRows and
// returns the server, collector, host, and a send helper.
func startPullHost(t *testing.T, nRows int) (*Server, *sinkCollector, *abiHost, func(id float64, method string, params interface{})) {
	t.Helper()
	srv := NewServer(0, "")
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
		Storage:       t.TempDir() + "/storage.db",
		Tables: map[string]tableSchemaParams{
			"users": {
				Columns: map[string]sqlite.ColumnSchema{
					"id":   {Type: "string"},
					"name": {Type: "string"},
				},
				PrimaryKey: []string{"id"},
			},
		},
	})
	rows := make([]ivm.Row, nRows)
	for i := range rows {
		rows[i] = ivm.Row{"id": fmt.Sprintf("u%04d", i), "name": fmt.Sprintf("User-%d", i)}
	}
	send(2, "loadRows", loadRowsParams{
		ClientGroupID: "cg-pull",
		Table:         "users",
		InitEpoch:     1,
		Rows:          rows,
	})
	return srv, col, h, send
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

// TestPullMode_LockstepDemandGate is the I6 lockstep pin at W=1: no
// row-bearing delivery may cross the boundary until the client grants, and
// deliveries track grants exactly.
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

// TestPullMode_CancelUnwindsAndRejects: mid-stream cancel produces a plain
// -32000 terminal error frame WHILE credit is zero (error frames ride
// free), stops all row production, unregisters the gate, and leaves the
// group fully usable for the next hydrate.
func TestPullMode_CancelUnwindsAndRejects(t *testing.T) {
	const nRows = 30
	srv, col, _, send := startPullHost(t, nRows)

	send(3, "addQueriesStream", pullQueryParams(true, true))
	waitGateRegistered(t, srv, 5*time.Second)

	srv.streamGates.grant(3, 3)
	waitRows(t, col, 3, 5*time.Second)

	// Cancel with zero credit outstanding — the producer is parked.
	srv.streamGates.cancel(3)

	// Terminal error frame arrives ungated; I9: plain -32000, and the
	// message names the consumer-cancel (never the -32102 DataError
	// class, which would teach the TS client to tear the CG down).
	deadline := time.Now().Add(5 * time.Second)
	var errResp RPCResponse
	for {
		var found bool
		errResp, found = frameFor(col, t, 3, func(r RPCResponse) bool { return r.Error != nil })
		if found {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("no terminal error frame after cancel")
		}
		time.Sleep(2 * time.Millisecond)
	}
	if errResp.Error.Code != -32000 {
		t.Fatalf("cancel error code = %d, want -32000 (I9: never reset/data classes)", errResp.Error.Code)
	}
	if !strings.Contains(errResp.Error.Message, "cancelled by consumer") {
		t.Fatalf("cancel error message %q does not name the consumer cancel", errResp.Error.Message)
	}

	// No further rows; gate unregistered.
	rowsAtCancel := countKind(col, abiKindRow)
	assertRowsStable(t, col, rowsAtCancel, 150*time.Millisecond)
	if srv.streamGates.size() != 0 {
		t.Fatalf("gate registry size = %d after cancel settle, want 0", srv.streamGates.size())
	}

	// Group healthy: an ordinary (non-pull) row-mode hydrate completes.
	send(4, "addQueriesStream", map[string]interface{}{
		"clientGroupID": "cg-pull",
		"initEpoch":     1,
		"rowMode":       true,
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

// TestPullMode_IdleSweepCancelsParked pins D7: a stream parked past the
// idle window with no grants is auto-cancelled by the sweeper — the exact
// client-cancel unwind, terminal error frame included.
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
		if _, found := frameFor(col, t, 3, func(r RPCResponse) bool { return r.Error != nil }); found {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("no terminal error frame after idle sweep")
		}
		time.Sleep(2 * time.Millisecond)
	}
	if got := countKind(col, abiKindRow); got != 0 {
		t.Fatalf("idle-swept stream delivered %d rows, want 0", got)
	}
}

// TestPullMode_ShutdownUnparksProducer pins the teardown broadcast: host
// Shutdown (→ closeAll → shutdownGroup) must cancel the group's gates
// BEFORE taking group.mu, or teardown deadlocks behind the parked
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

// TestPullMode_WithoutRowModeDegrades: pullMode without the row plane
// (rowMode=false) is IGNORED — the RPC streams ordinary frames to
// completion with no gate registered. This is the rollout-safety property:
// pull engages only where credits can actually flow.
func TestPullMode_WithoutRowModeDegrades(t *testing.T) {
	const nRows = 10
	srv, col, _, send := startPullHost(t, nRows)

	send(3, "addQueriesStream", pullQueryParams(true, false))

	deadline := time.Now().Add(10 * time.Second)
	for {
		if srv.streamGates.size() != 0 {
			t.Fatal("gate registered for a non-rowMode RPC — pull must require the row plane")
		}
		if _, found := frameFor(col, t, 3, func(r RPCResponse) bool {
			s, ok := r.Result.(string)
			return ok && s == "done"
		}); found {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("non-rowMode pull RPC never completed")
		}
		time.Sleep(5 * time.Millisecond)
	}
	if got := countKind(col, abiKindRow); got != 0 {
		t.Fatalf("non-rowMode RPC produced %d row records, want 0 (frames only)", got)
	}
}
