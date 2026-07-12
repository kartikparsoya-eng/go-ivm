package main

// wedgewatch.go — the CG-worker wedge watchdog.
//
// Detects and diagnoses per-CG init/hydrate cycles that stall past the TS
// 120s RPC deadline. The watchdog scans every CG worker's in-flight
// request periodically and reports handlers running past a configurable
// threshold, emitting three log lines:
//
//   [GO-IVM][WEDGE]        — every scan tick while a handler runs past the
//                            threshold: cg, method, elapsed, queued depth,
//                            FIFO queue-wait, reqID. Correlates directly
//                            against the TS 120s deadline.
//   [GO-IVM][WEDGE-STACKS] — ONCE per wedge: a full all-goroutine dump
//                            (the exact `pprof/goroutine?debug=2` capture,
//                            minus the operator) between BEGIN/END sentinels
//                            so tooling can extract it mechanically.
//   [GO-IVM][WEDGE-CLEAR]  — emitted by the worker when a past-threshold
//                            handler finally returns: the release timestamp
//                            + total elapsed, which is what discriminates
//                            "wedged forever" from "silently un-stuck at
//                            ~120s".
//
// Threshold: GO_IVM_WEDGE_WATCHDOG_SEC (default 90s) — deliberately BELOW
// the TS 120s RPC deadline so the stack dump lands while the client is
// still waiting (the wedge is still live, not already unwound by the
// client's timeout teardown).
//
// Cost when nothing is wedged: one atomic load per group per tick
// (tick = threshold/4, clamped [250ms, 10s]) — nil curReq short-circuits.
// The all-goroutine dump stops the world briefly; it fires at most once per
// wedge on a worker that is, by definition, already failing its client.

import (
	"context"
	"fmt"
	"io"
	"os"
	"runtime"
	"strconv"
	"time"
)

// activeReq describes the request a CG worker is currently executing —
// stamped at dequeue, cleared after the respCh send (see worker()). The
// pointer lives in ClientGroup.curReq (atomic: written only by the worker
// goroutine, read lock-free by the watchdog scanner).
type activeReq struct {
	method string
	cgID   string
	reqID  interface{}
	// start is the DEQUEUE instant — elapsed here is pure handler time,
	// directly comparable to the TS-side RPC deadline (which also starts
	// ticking near dispatch; the FIFO wait is reported separately).
	start time.Time
	// queueWait is how long the request sat in reqC before the worker
	// picked it up (zero when the enqueue stamp is missing — direct
	// trySendReq callers in tests).
	queueWait time.Duration
}

// wedgeLogW is the watchdog's output sink. A package var (not a Server
// field) so tests can capture the WEDGE/WEDGE-CLEAR/WEDGE-STACKS stream
// without plumbing a writer through NewServer; production always writes
// os.Stderr, same as every other [GO-IVM] tag.
var wedgeLogW io.Writer = os.Stderr

// wedgeWatchdogThreshold reads GO_IVM_WEDGE_WATCHDOG_SEC (default 90s —
// below the TS 120s RPC deadline; see the file header). Values <= 0 are
// ignored: the watchdog is a tripwire, not a feature flag — it cannot be
// disabled, only tuned.
func wedgeWatchdogThreshold() time.Duration {
	if v := os.Getenv("GO_IVM_WEDGE_WATCHDOG_SEC"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return time.Duration(n) * time.Second
		}
	}
	return 90 * time.Second
}

// runWedgeWatchdog periodically scans every CG worker's in-flight request
// and reports the ones running past the threshold. Started by the ABI host
// next to the reaper and the pull idle sweeper (abi.go); blocking call, run
// in its own goroutine. Touches only s.mu (RLock, snapshot) and per-group
// atomics — it can never itself wedge behind a slow handler.
func (s *Server) runWedgeWatchdog(ctx context.Context) {
	tick := s.wedgeThreshold / 4
	if tick > 10*time.Second {
		tick = 10 * time.Second
	}
	if tick < 250*time.Millisecond {
		tick = 250 * time.Millisecond
	}
	ticker := time.NewTicker(tick)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			s.scanWedgedGroups(now)
		}
	}
}

// scanWedgedGroups is one watchdog pass: reports every group whose worker
// has been inside one handler for longer than s.wedgeThreshold, and dumps
// all goroutine stacks ONCE per wedge (g.wedgeDumped latches; the worker
// re-arms it when the handler completes). Returns the number of wedged
// groups found (tests).
func (s *Server) scanWedgedGroups(now time.Time) int {
	s.mu.RLock()
	groups := make([]*ClientGroup, 0, len(s.groups))
	for _, g := range s.groups {
		groups = append(groups, g)
	}
	s.mu.RUnlock()

	wedged := 0
	for _, g := range groups {
		info := g.curReq.Load()
		if info == nil {
			continue
		}
		elapsed := now.Sub(info.start)
		if elapsed < s.wedgeThreshold {
			continue
		}
		wedged++
		fmt.Fprintf(wedgeLogW,
			"[GO-IVM][WEDGE] cg=%s method=%s elapsed=%v queued=%d queueWait=%v reqID=%v\n",
			info.cgID, info.method, elapsed.Round(time.Millisecond),
			len(g.reqC), info.queueWait.Round(time.Microsecond), info.reqID)
		if g.wedgeDumped.CompareAndSwap(false, true) {
			dumpAllStacks(info, elapsed)
		}
	}
	return wedged
}

// dumpAllStacks prints every goroutine's stack between sentinel lines so
// the output can be extracted mechanically. Equivalent to hitting
// /debug/pprof/goroutine?debug=2 at the moment the wedge is detected.
func dumpAllStacks(info *activeReq, elapsed time.Duration) {
	// runtime.Stack(all=true) truncates when the buffer is too small —
	// grow geometrically until it fits (a saturated worker can carry a few
	// MB of stacks), capped so a pathological process can't OOM its own
	// forensics.
	const maxBuf = 16 << 20
	buf := make([]byte, 1<<20)
	var n int
	for {
		n = runtime.Stack(buf, true)
		if n < len(buf) || len(buf) >= maxBuf {
			break
		}
		buf = make([]byte, len(buf)*2)
	}
	fmt.Fprintf(wedgeLogW,
		"[GO-IVM][WEDGE-STACKS] BEGIN cg=%s method=%s elapsed=%v goroutines=%d\n%s\n[GO-IVM][WEDGE-STACKS] END cg=%s\n",
		info.cgID, info.method, elapsed.Round(time.Millisecond), runtime.NumGoroutine(), buf[:n], info.cgID)
}
