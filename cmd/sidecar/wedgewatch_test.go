package main

// Pins for the wedge watchdog (wedgewatch.go) — the self-capture that the
// 7fd5a895/7fbeed43 ART incident had to be diagnosed WITHOUT:
//   - a handler running past the threshold produces [GO-IVM][WEDGE] lines
//     on every scan and exactly ONE [GO-IVM][WEDGE-STACKS] dump per
//     incident (the latch re-arms only when the handler completes);
//   - the worker emits [GO-IVM][WEDGE-CLEAR] with the total elapsed when a
//     past-threshold handler finally returns — the release-side timestamp
//     that discriminates "wedged forever" from "silently un-stuck";
//   - handlers under the threshold produce NOTHING (the watchdog must be
//     grep-silent on healthy runs — the ART gate hard-blocks on
//     [GO-IVM][WEDGE]).

import (
	"bytes"
	"strings"
	"sync"
	"testing"
	"time"
)

// syncBuf is a goroutine-safe capture sink for wedgeLogW (written by both
// the watchdog scanner and the CG worker goroutine).
type syncBuf struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuf) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuf) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// captureWedgeLog swaps wedgeLogW for a capture buffer for the test's
// duration. Tests using it must not run in parallel with each other.
func captureWedgeLog(t *testing.T) *syncBuf {
	t.Helper()
	b := &syncBuf{}
	saved := wedgeLogW
	wedgeLogW = b
	t.Cleanup(func() { wedgeLogW = saved })
	return b
}

// TestWedgeWatchdog_ScanReportsAndDumpsOnce drives the scanner directly
// against a synthetic in-flight request: past-threshold → WEDGE line each
// scan + exactly one STACKS dump; cleared stamp → silent.
func TestWedgeWatchdog_ScanReportsAndDumpsOnce(t *testing.T) {
	out := captureWedgeLog(t)
	srv := NewServer("unused")
	srv.wedgeThreshold = 50 * time.Millisecond
	t.Cleanup(srv.closeAll)
	g := srv.getGroup("cg-scan", true)

	// Under threshold: silent.
	g.curReq.Store(&activeReq{method: "addQueriesStream", cgID: "cg-scan", reqID: 7.0, start: time.Now()})
	if n := srv.scanWedgedGroups(time.Now()); n != 0 {
		t.Fatalf("under-threshold scan reported %d wedged groups, want 0", n)
	}
	if s := out.String(); s != "" {
		t.Fatalf("under-threshold scan produced output: %q", s)
	}

	// Past threshold: WEDGE on every scan, STACKS exactly once.
	g.curReq.Store(&activeReq{
		method: "addQueriesStream", cgID: "cg-scan", reqID: 7.0,
		start: time.Now().Add(-75 * time.Millisecond), queueWait: 123 * time.Microsecond,
	})
	for i := 0; i < 3; i++ {
		if n := srv.scanWedgedGroups(time.Now()); n != 1 {
			t.Fatalf("scan %d reported %d wedged groups, want 1", i, n)
		}
	}
	s := out.String()
	if got := strings.Count(s, "[GO-IVM][WEDGE] cg=cg-scan method=addQueriesStream"); got != 3 {
		t.Fatalf("WEDGE lines = %d, want 3 (one per scan)\n%s", got, s)
	}
	if got := strings.Count(s, "[GO-IVM][WEDGE-STACKS] BEGIN cg=cg-scan"); got != 1 {
		t.Fatalf("STACKS dumps = %d, want exactly 1 per incident\n%s", got, s)
	}
	if !strings.Contains(s, "[GO-IVM][WEDGE-STACKS] END cg=cg-scan") {
		t.Fatal("STACKS dump missing END sentinel (gate extraction needs both)")
	}
	// The dump must actually contain goroutine stacks — this test function
	// is on one of them.
	if !strings.Contains(s, "TestWedgeWatchdog_ScanReportsAndDumpsOnce") {
		t.Fatal("STACKS dump does not contain goroutine stacks")
	}
	if !strings.Contains(s, "queued=0") || !strings.Contains(s, "queueWait=123µs") {
		t.Fatalf("WEDGE line missing queued depth / queue-wait fields\n%s", s)
	}

	// Stamp cleared (handler returned): silent again.
	g.curReq.Store(nil)
	if n := srv.scanWedgedGroups(time.Now()); n != 0 {
		t.Fatal("cleared stamp still reported as wedged")
	}
}

// TestWedgeWatchdog_WorkerIntegration is the end-to-end red-proof: a REAL
// handler (destroy) blocked on group.mu — the wedge shape the incident's
// candidates all reduce to — must be stamped by the worker, detected by the
// scanner with a dump, and produce WEDGE-CLEAR (with the total elapsed) once
// the test releases the lock and the handler completes. The dump latch must
// re-arm for the NEXT incident.
func TestWedgeWatchdog_WorkerIntegration(t *testing.T) {
	out := captureWedgeLog(t)
	srv := NewServer("unused")
	srv.wedgeThreshold = 100 * time.Millisecond
	t.Cleanup(srv.closeAll)
	g := srv.getGroup("cg-int", true)

	// Wedge the handler: handleDestroy takes group.mu for the epoch check;
	// holding it in the test parks the worker mid-handler indefinitely.
	g.mu.Lock()
	respCh := make(chan RPCResponse, 1)
	ok := g.trySendReq(clientGroupReq{
		req: RPCRequest{
			JSONRPC: "2.0", ID: 42.0, Method: "destroy",
			Params: mustMarshal(t, destroyParams{ClientGroupID: "cg-int", InitEpoch: 1}),
		},
		respCh: respCh,
		group:  g,
		cgID:   "cg-int",
	})
	if !ok {
		g.mu.Unlock()
		t.Fatal("trySendReq refused")
	}

	// Wait for the worker to dequeue + stamp, then for the wedge to age
	// past the threshold, then scan.
	deadline := time.Now().Add(5 * time.Second)
	for g.curReq.Load() == nil {
		if time.Now().After(deadline) {
			g.mu.Unlock()
			t.Fatal("worker never stamped curReq")
		}
		time.Sleep(time.Millisecond)
	}
	time.Sleep(srv.wedgeThreshold + 50*time.Millisecond)
	if n := srv.scanWedgedGroups(time.Now()); n != 1 {
		g.mu.Unlock()
		t.Fatalf("scan saw %d wedged groups, want 1", n)
	}
	s := out.String()
	if !strings.Contains(s, "[GO-IVM][WEDGE] cg=cg-int method=destroy") {
		g.mu.Unlock()
		t.Fatalf("no WEDGE line for the blocked destroy\n%s", s)
	}
	if !strings.Contains(s, "[GO-IVM][WEDGE-STACKS] BEGIN cg=cg-int") {
		g.mu.Unlock()
		t.Fatalf("no stack dump for the incident\n%s", s)
	}
	// The dump must show WHERE the handler is blocked — the whole point.
	// The destroy is parked in a mutex Lock inside handleDestroy.
	if !strings.Contains(s, "handleDestroy") {
		g.mu.Unlock()
		t.Fatalf("stack dump does not name the blocking frame (handleDestroy)\n%s", s)
	}

	// Release: the handler completes and the worker must emit WEDGE-CLEAR.
	g.mu.Unlock()
	select {
	case <-respCh:
	case <-time.After(10 * time.Second):
		t.Fatal("destroy never completed after release")
	}
	// WEDGE-CLEAR is written after the respCh send — poll briefly.
	deadline = time.Now().Add(2 * time.Second)
	for !strings.Contains(out.String(), "[GO-IVM][WEDGE-CLEAR] cg=cg-int method=destroy") {
		if time.Now().After(deadline) {
			t.Fatalf("no WEDGE-CLEAR after the wedged handler returned\n%s", out.String())
		}
		time.Sleep(5 * time.Millisecond)
	}
	// Latch re-armed: the stamp is cleared and the dump flag reset, so a
	// FUTURE incident dumps again.
	if g.curReq.Load() != nil {
		t.Fatal("curReq not cleared after completion")
	}
	if g.wedgeDumped.Load() {
		t.Fatal("wedgeDumped latch not re-armed after completion")
	}
}

// TestWedgeWatchdog_ThresholdEnvDefault pins the default (90s — below the
// TS 120s RPC deadline so the dump lands while the client still waits) and
// that NewServer wires it.
func TestWedgeWatchdog_ThresholdEnvDefault(t *testing.T) {
	srv := NewServer("unused")
	if srv.wedgeThreshold != 90*time.Second {
		t.Fatalf("default wedgeThreshold = %v, want 90s", srv.wedgeThreshold)
	}
}
