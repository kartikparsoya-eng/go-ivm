package main

// Reaper in-flight guard (scale-review A4).
//
// lastUsedNs is stamped when the worker DEQUEUES a request; nothing
// refreshed it while the handler ran. A handler outliving the idle window
// (a long hydrate stalled by transport backpressure) therefore made a LIVE
// group reap-eligible: the reaper deleted it from s.groups mid-handler and
// the next RPC for the same cgID created a SECOND group+engine over the
// same storage — split-brain. The fix: ClientGroup.inFlight is true from
// dequeue through respCh delivery; the reaper skips in-flight groups (scan
// AND double-check), and the worker stamps lastUsedNs fresh at completion
// so a just-finished group is never "idle since dequeue".

import (
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kartikparsoya-eng/go-ivm/sqlite"
)

// TestReaper_DoesNotReapGroupWithInFlightHandler drives a REAL parked
// handler through the ABI host: a gated sink stalls the delivery chain
// (sink → pump reader → pipe → flusher → flushCh), so an addQueriesStream
// with more partial frames than the chain absorbs parks the worker
// mid-handler — exactly the "long hydrate under backpressure" shape. The
// reaper must then refuse to reap the group even though lastUsedNs is
// ancient. Pre-fix this test fails: reapIdleGroups deletes the group while
// its handler is still streaming.
//
// Uses only pre-fix identifiers so it compiles (and demonstrably fails)
// against the pre-fix tree.
func TestReaper_DoesNotReapGroupWithInFlightHandler(t *testing.T) {
	var (
		gateMu sync.Mutex
		gateCh chan struct{} // nil = open; non-nil = deliveries park on it
		parked atomic.Int32

		entMu   sync.Mutex
		entries []sinkEntry
	)
	sink := func(kind int32, payload []byte) int32 {
		gateMu.Lock()
		ch := gateCh
		gateMu.Unlock()
		if ch != nil {
			parked.Add(1)
			<-ch
			parked.Add(-1)
		}
		buf := make([]byte, len(payload))
		copy(buf, payload)
		entMu.Lock()
		entries = append(entries, sinkEntry{kind: kind, payload: buf})
		entMu.Unlock()
		return deliverOK
	}

	path, db := makeReplica(t)
	mustExec(t, db, `CREATE TABLE "t" ("id" TEXT PRIMARY KEY, "_0_version" TEXT)`)

	h := startABIHostWithServer(NewServer(path), sink, nil)
	defer h.Shutdown()

	const cgID = "cg-reap-inflight"
	if err := h.Send(encodeReq(t, "init", 1, initParams{
		ClientGroupID: cgID,
		Tables: map[string]tableSchemaParams{
			"t": {
				Columns:    map[string]sqlite.ColumnSchema{"id": {Type: "string"}, "_0_version": {Type: "string"}},
				PrimaryKey: []string{"id"},
			},
		},
	})); err != nil {
		t.Fatalf("send init: %v", err)
	}
	waitFor(t, 5*time.Second, "init response", func() bool {
		entMu.Lock()
		defer entMu.Unlock()
		for _, e := range entries {
			if e.kind == abiKindFrame {
				if id, ok := toFloat(decodeResp(t, e.payload).ID); ok && id == 1 {
					return true
				}
			}
		}
		return false
	})

	// Close the gate, then send a hydrate whose per-query partial frames
	// exceed the chain's absorption (~1 parked + ~64KB bufio + flushCh 256):
	// 800 queries with 8KB queryIDs park the handler with 3x margin.
	gateMu.Lock()
	gateCh = make(chan struct{})
	gateMu.Unlock()

	queries := make([]map[string]interface{}, 800)
	for i := range queries {
		queries[i] = map[string]interface{}{
			"queryID": fmt.Sprintf("q-%04d-", i) + strings.Repeat("x", 8192),
			"ast": map[string]interface{}{
				"table":   "t",
				"orderBy": [][]string{{"id", "asc"}},
			},
		}
	}
	if err := h.Send(encodeReq(t, "addQueriesStream", 2, map[string]interface{}{
		"clientGroupID": cgID,
		"initEpoch":     1,
		"rowMode":       true,
		"pullMode":      true,
		"pullWindow":    1024,
		"queries":       queries,
	})); err != nil {
		t.Fatalf("send addQueriesStream: %v", err)
	}

	// A parked delivery proves the handler is running and the chain is
	// wedged behind the gate.
	waitFor(t, 10*time.Second, "first delivery parked at the gate", func() bool {
		return parked.Load() >= 1
	})
	// Let the flusher/flushCh saturate behind the parked delivery so the
	// handler is deterministically blocked inside its emission loop.
	time.Sleep(200 * time.Millisecond)

	h.server.mu.RLock()
	g := h.server.groups[cgID]
	h.server.mu.RUnlock()
	if g == nil {
		t.Fatal("group missing before reap attempt")
	}
	const ancient = int64(1) // ~epoch: older than any cutoff
	g.lastUsedNs.Store(ancient)

	// Reap with "everything older than now" — the ONLY thing that may save
	// this group is the in-flight guard. Pre-fix: reaps it mid-handler.
	if n := h.server.reapIdleGroups(time.Now()); n != 0 {
		t.Fatalf("reaper collected %d group(s) while a handler was in flight (A4 split-brain)", n)
	}
	if h.server.getGroup(cgID, false) == nil {
		t.Fatal("group vanished from s.groups while its handler was mid-stream (A4)")
	}

	// Open the gate; the stream must complete cleanly (no error frame).
	gateMu.Lock()
	close(gateCh)
	gateCh = nil
	gateMu.Unlock()

	waitFor(t, 30*time.Second, `terminal "done" for the hydrate`, func() bool {
		entMu.Lock()
		defer entMu.Unlock()
		for _, e := range entries {
			if e.kind != abiKindFrame {
				continue
			}
			resp := decodeResp(t, e.payload)
			if id, ok := toFloat(resp.ID); !ok || id != 2 {
				continue
			}
			if resp.Error != nil {
				t.Fatalf("hydrate errored: %+v", resp.Error)
			}
			if s, ok := resp.Result.(string); ok && s == "done" {
				return true
			}
		}
		return false
	})

	// Completion must have re-stamped lastUsedNs (the worker's completion
	// stamp): the group is NOT instantly reap-eligible after finishing.
	if got := g.lastUsedNs.Load(); got <= ancient {
		t.Fatalf("lastUsedNs not refreshed at handler completion: %d", got)
	}

	// And the guard must not pin the group forever: once idle again it reaps.
	g.lastUsedNs.Store(ancient)
	waitFor(t, 5*time.Second, "idle group reaped after completion", func() bool {
		return h.server.reapIdleGroups(time.Now()) == 1
	})
	if h.server.getGroup(cgID, false) != nil {
		t.Fatal("idle group survived the reaper after its handler completed")
	}
}

// TestReapIdleGroups_InFlightFlagContract pins the reaper's contract at the
// unit level: an in-flight group with an ancient lastUsedNs is skipped by
// BOTH the scan and the double-check; clearing the flag makes it reapable.
func TestReapIdleGroups_InFlightFlagContract(t *testing.T) {
	s := NewServer(makeReplicaPathOnly(t))
	g := s.getGroup("cg-contract", true)
	g.lastUsedNs.Store(1)

	g.inFlight.Store(true)
	if n := s.reapIdleGroups(time.Now()); n != 0 {
		t.Fatalf("reaped %d in-flight group(s); want 0", n)
	}
	if s.getGroup("cg-contract", false) == nil {
		t.Fatal("in-flight group removed from map")
	}

	g.inFlight.Store(false)
	if n := s.reapIdleGroups(time.Now()); n != 1 {
		t.Fatalf("reaped %d idle group(s); want 1", n)
	}
	if s.getGroup("cg-contract", false) != nil {
		t.Fatal("idle group still in map after reap")
	}
}

// waitFor polls cond until true or the deadline expires.
func waitFor(t *testing.T, timeout time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		if cond() {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(10 * time.Millisecond)
	}
}
