package main

// Tests for the ABI host (abi.go) — the in-process NAPI transport's frame
// pump. The pump wires the PRODUCTION handleConnection to a net.Pipe, so
// these tests assert transport-level behavior (delivery, ordering, teardown,
// backpressure survival), not RPC semantics — those are covered by the
// existing handleConnection suites, which run the same code path.

import (
	"sync"
	"testing"
	"time"

	"github.com/kartikparsoya-eng/go-ivm/sqlite"
)

// sinkCollector is a deliver callback that copies every payload (honoring
// the "valid only during the call" contract) and signals arrival.
type sinkCollector struct {
	mu      sync.Mutex
	frames  [][]byte // kind-1 payloads
	entries []sinkEntry
	notify  chan struct{}
}

type sinkEntry struct {
	kind    int32
	payload []byte
}

func newSinkCollector() *sinkCollector {
	return &sinkCollector{notify: make(chan struct{}, 4096)}
}

func (c *sinkCollector) sink(kind int32, payload []byte) int32 {
	buf := make([]byte, len(payload))
	copy(buf, payload)
	c.mu.Lock()
	c.entries = append(c.entries, sinkEntry{kind: kind, payload: buf})
	if kind == abiKindFrame {
		c.frames = append(c.frames, buf)
	}
	c.mu.Unlock()
	c.notify <- struct{}{}
	return deliverOK
}

func (c *sinkCollector) waitFrames(t *testing.T, n int, timeout time.Duration) [][]byte {
	t.Helper()
	deadline := time.After(timeout)
	for {
		c.mu.Lock()
		if len(c.frames) >= n {
			out := make([][]byte, len(c.frames))
			copy(out, c.frames)
			c.mu.Unlock()
			return out
		}
		c.mu.Unlock()
		select {
		case <-c.notify:
		case <-deadline:
			c.mu.Lock()
			got := len(c.frames)
			c.mu.Unlock()
			t.Fatalf("timed out waiting for %d frames (got %d)", n, got)
		}
	}
}

func decodeResp(t *testing.T, payload []byte) RPCResponse {
	t.Helper()
	var resp RPCResponse
	if err := mpUnmarshal(payload, &resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	return resp
}

// TestABIHost_InitAdvanceRoundTrip drives a real init + advanceToHeadStream
// through the pump and asserts the full response set arrives via the sink:
// init response, advance terminal partial (final=true), and its "done".
func TestABIHost_InitAdvanceRoundTrip(t *testing.T) {
	col := newSinkCollector()
	h := startABIHostWithServer(NewServer(makeReplicaPathOnly(t)), col.sink, nil)
	defer h.Shutdown()

	if err := h.Send(encodeReq(t, "init", 1, issueInitParams("cg-abi"))); err != nil {
		t.Fatalf("send init: %v", err)
	}
	frames := col.waitFrames(t, 1, 10*time.Second)
	initResp := decodeResp(t, frames[0])
	if initResp.Error != nil {
		t.Fatalf("init failed: %+v", initResp.Error)
	}

	if err := h.Send(encodeReq(t, "advanceToHeadStream", 2, advanceToHeadParams{
		ClientGroupID: "cg-abi",
		InitEpoch:     1,
	})); err != nil {
		t.Fatalf("send advanceToHeadStream: %v", err)
	}
	// The replica is already at head, so the advance emits exactly: 1
	// metadata header + 1 terminal partial (final=true, empty changes) +
	// 1 "done" response — all must arrive through the pump in order
	// (single flusher FIFO).
	frames = col.waitFrames(t, 4, 10*time.Second)
	partial := decodeResp(t, frames[1])
	if partial.Error != nil {
		t.Fatalf("header carried error: %+v", partial.Error)
	}
	done := decodeResp(t, frames[3])
	if done.Error != nil {
		t.Fatalf("done carried error: %+v", done.Error)
	}
	res, ok := done.Result.(string)
	if !ok || res != "done" {
		t.Fatalf("expected terminal \"done\" result, got %#v", done.Result)
	}
}

// TestABIHost_LosslessDeliveryUnderBurst fires a burst of pings and asserts
// every response arrives exactly once. NOTE: cross-RPC response ORDER is not
// a transport guarantee (on the socket either) — the dispatcher hands each
// response to flushCh from its own goroutine and the TS client routes by
// response ID. The transport contract is losslessness; within-stream partial
// ordering is covered by TestABIHost_InitAdvanceRoundTrip (partial precedes
// done via the single-flusher FIFO).
func TestABIHost_LosslessDeliveryUnderBurst(t *testing.T) {
	col := newSinkCollector()
	h := startABIHostWithServer(NewServer(makeReplicaPathOnly(t)), col.sink, nil)
	defer h.Shutdown()

	const n = 200
	for i := 0; i < n; i++ {
		if err := h.Send(encodeReq(t, "ping", float64(i), nil)); err != nil {
			t.Fatalf("send ping %d: %v", i, err)
		}
	}
	frames := col.waitFrames(t, n, 15*time.Second)
	seen := make(map[int]int, n)
	for i := 0; i < n; i++ {
		resp := decodeResp(t, frames[i])
		id, ok := toFloat(resp.ID)
		if !ok {
			t.Fatalf("frame %d: non-numeric id %#v", i, resp.ID)
		}
		seen[int(id)]++
	}
	for i := 0; i < n; i++ {
		if seen[i] != 1 {
			t.Fatalf("response id %d delivered %d times (want exactly 1)", i, seen[i])
		}
	}
}

func toFloat(v interface{}) (float64, bool) {
	switch x := v.(type) {
	case float64:
		return x, true
	case float32:
		return float64(x), true
	case int64:
		return float64(x), true
	case uint64:
		return float64(x), true
	case int8:
		return float64(x), true
	case uint8:
		return float64(x), true
	case int16:
		return float64(x), true
	case uint16:
		return float64(x), true
	case int32:
		return float64(x), true
	case uint32:
		return float64(x), true
	case int:
		return float64(x), true
	}
	return 0, false
}

// TestABIHost_SlowSinkBackpressureLossless: a sink that consumes slowly must
// not lose or duplicate frames — the pump blocks into handleConnection's
// flusher chain exactly like a slow socket, then drains completely.
func TestABIHost_SlowSinkBackpressureLossless(t *testing.T) {
	var mu sync.Mutex
	got := make(map[int]int)
	var count int
	release := make(chan struct{})
	slowSink := func(kind int32, payload []byte) int32 {
		<-release // hold every delivery until the test opens the gate
		var resp RPCResponse
		if err := mpUnmarshal(payload, &resp); err != nil {
			t.Errorf("decode: %v", err)
			return deliverOK
		}
		if f, ok := toFloat(resp.ID); ok {
			mu.Lock()
			got[int(f)]++
			count++
			mu.Unlock()
		}
		return deliverOK
	}
	h := startABIHostWithServer(NewServer(makeReplicaPathOnly(t)), slowSink, nil)
	defer h.Shutdown()

	const n = 300
	for i := 0; i < n; i++ {
		if err := h.Send(encodeReq(t, "ping", float64(i), nil)); err != nil {
			t.Fatalf("send %d: %v", i, err)
		}
	}
	// Everything upstream is now wedged behind the closed gate. Open it.
	close(release)

	deadline := time.Now().Add(20 * time.Second)
	for {
		mu.Lock()
		cnt := count
		mu.Unlock()
		if cnt >= n {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("drain incomplete: %d/%d responses after gate opened", cnt, n)
		}
		time.Sleep(20 * time.Millisecond)
	}
	mu.Lock()
	defer mu.Unlock()
	for i := 0; i < n; i++ {
		if got[i] != 1 {
			t.Fatalf("response id %d delivered %d times (want exactly 1)", i, got[i])
		}
	}
}

// TestABIHost_ShutdownSemantics: Send after Shutdown fails with
// errHostClosed; Shutdown is idempotent and does not hang.
func TestABIHost_ShutdownSemantics(t *testing.T) {
	col := newSinkCollector()
	h := startABIHostWithServer(NewServer(makeReplicaPathOnly(t)), col.sink, nil)

	if err := h.Send(encodeReq(t, "ping", 1, nil)); err != nil {
		t.Fatalf("send before shutdown: %v", err)
	}
	col.waitFrames(t, 1, 5*time.Second)

	doneCh := make(chan struct{})
	go func() {
		h.Shutdown()
		h.Shutdown() // idempotent
		close(doneCh)
	}()
	select {
	case <-doneCh:
	case <-time.After(10 * time.Second):
		t.Fatal("Shutdown hung")
	}

	if err := h.Send([]byte{0x01}); err != errHostClosed {
		t.Fatalf("Send after shutdown: got %v, want errHostClosed", err)
	}
}

// TestABIHost_DeathDeliversHostDeathRecord (scale-review A3): when the
// in-process connection dies UNEXPECTEDLY — handleConnection exit or pipe
// teardown, anything but a deliberate Shutdown — the host must deliver ONE
// kind-4 host-death record so the JS client can fail its pending RPCs
// immediately and fatal the worker (crash-don't-degrade). Pre-fix the death
// was silent: pending RPCs hung to their full timeout with no signal, and
// in-process there is no socket 'close' event to observe.
//
// Closing serverEnd reproduces exactly what an unexpected handleConnection
// exit does (its `defer conn.Close()` — main.go): the pump reader errors,
// both pumps exit, and the death watcher fires.
func TestABIHost_DeathDeliversHostDeathRecord(t *testing.T) {
	col := newSinkCollector()
	h := startABIHostWithServer(NewServer(makeReplicaPathOnly(t)), col.sink, nil)
	defer h.Shutdown()

	// Prove liveness first so the death is unambiguous.
	if err := h.Send(encodeReq(t, "ping", 1, nil)); err != nil {
		t.Fatalf("send ping: %v", err)
	}
	col.waitFrames(t, 1, 5*time.Second)

	// Kill the server side of the pipe (== handleConnection exiting).
	_ = h.serverEnd.Close()

	deadline := time.After(5 * time.Second)
	for {
		var death *sinkEntry
		col.mu.Lock()
		for i := range col.entries {
			if col.entries[i].kind == abiKindHostDeath {
				death = &col.entries[i]
			}
		}
		col.mu.Unlock()
		if death != nil {
			if len(death.payload) == 0 {
				t.Fatal("host-death record must carry a UTF-8 reason payload")
			}
			break
		}
		select {
		case <-col.notify:
		case <-deadline:
			t.Fatal("no kind-4 host-death record delivered after pipe death (A3)")
		}
	}

	// After death the host must fail sends fast, not queue them silently.
	if err := h.Send([]byte{0x01}); err != errHostClosed {
		t.Fatalf("Send after host death: got %v, want errHostClosed", err)
	}
}

// TestABIHost_ShutdownDoesNotDeliverDeathRecord: a DELIBERATE Shutdown must
// NOT deliver kind 4 — the embedder initiated the teardown; a death record
// would trigger a spurious worker fatal during graceful exit.
func TestABIHost_ShutdownDoesNotDeliverDeathRecord(t *testing.T) {
	col := newSinkCollector()
	h := startABIHostWithServer(NewServer(makeReplicaPathOnly(t)), col.sink, nil)

	if err := h.Send(encodeReq(t, "ping", 1, nil)); err != nil {
		t.Fatalf("send ping: %v", err)
	}
	col.waitFrames(t, 1, 5*time.Second)

	h.Shutdown() // joins the death watcher via <-h.done, so this is race-free

	col.mu.Lock()
	defer col.mu.Unlock()
	for _, e := range col.entries {
		if e.kind == abiKindHostDeath {
			t.Fatal("deliberate Shutdown must not deliver a host-death record")
		}
	}
}

// TestNewServerFromEnv_ParallelismKnob covers the production env contract
// shared by BOTH entry points — socket main() and the NAPI host.
func TestNewServerFromEnv_ParallelismKnob(t *testing.T) {
	replicaPath := makeReplicaPathOnly(t)
	clearEnv := func(t *testing.T) {
		t.Setenv("GO_IVM_REPLICA_DB_PATH", replicaPath)
		for _, k := range []string{
			"GO_IVM_HYDRATE_PARALLELISM", "GO_IVM_PARALLELISM",
			"GO_IVM_HYDRATE_READERS", "GO_IVM_HYDRATE_LANES",
			"GO_IVM_WARM_HYDRATE_POOL",
		} {
			t.Setenv(k, "")
		}
	}

	t.Run("production defaults: lanes=4 readers=8 warm-pool ON", func(t *testing.T) {
		clearEnv(t)
		srv, err := newServerFromEnv()
		if err != nil {
			t.Fatalf("newServerFromEnv: %v", err)
		}
		if srv.hydrateLanes != 4 || srv.hydrateReaders != 8 {
			t.Errorf("lanes=%d readers=%d, want 4/8", srv.hydrateLanes, srv.hydrateReaders)
		}
		if !srv.warmHydratePoolEnabled {
			t.Error("warm hydrate pool must default ON")
		}
	})

	t.Run("GO_IVM_HYDRATE_PARALLELISM scales hydrate facets", func(t *testing.T) {
		clearEnv(t)
		t.Setenv("GO_IVM_HYDRATE_PARALLELISM", "6")
		srv, err := newServerFromEnv()
		if err != nil {
			t.Fatalf("newServerFromEnv: %v", err)
		}
		if srv.hydrateLanes != 6 || srv.hydrateReaders != 12 {
			t.Errorf("lanes=%d readers=%d, want 6/12", srv.hydrateLanes, srv.hydrateReaders)
		}
	})

	t.Run("legacy GO_IVM_PARALLELISM remains hydrate fallback", func(t *testing.T) {
		clearEnv(t)
		t.Setenv("GO_IVM_PARALLELISM", "5")
		srv, err := newServerFromEnv()
		if err != nil {
			t.Fatalf("newServerFromEnv: %v", err)
		}
		if srv.hydrateLanes != 5 || srv.hydrateReaders != 10 {
			t.Errorf("lanes=%d readers=%d, want 5/10", srv.hydrateLanes, srv.hydrateReaders)
		}
	})

	t.Run("hydrate-specific knob wins over legacy knob", func(t *testing.T) {
		clearEnv(t)
		t.Setenv("GO_IVM_PARALLELISM", "5")
		t.Setenv("GO_IVM_HYDRATE_PARALLELISM", "6")
		srv, err := newServerFromEnv()
		if err != nil {
			t.Fatalf("newServerFromEnv: %v", err)
		}
		if srv.hydrateLanes != 6 || srv.hydrateReaders != 12 {
			t.Errorf("lanes=%d readers=%d, want 6/12", srv.hydrateLanes, srv.hydrateReaders)
		}
	})

	t.Run("legacy per-facet vars override the knob", func(t *testing.T) {
		clearEnv(t)
		t.Setenv("GO_IVM_HYDRATE_PARALLELISM", "6")
		t.Setenv("GO_IVM_HYDRATE_READERS", "3")
		t.Setenv("GO_IVM_HYDRATE_LANES", "2")
		srv, err := newServerFromEnv()
		if err != nil {
			t.Fatalf("newServerFromEnv: %v", err)
		}
		if srv.hydrateLanes != 2 || srv.hydrateReaders != 3 {
			t.Errorf("lanes=%d readers=%d, want 2/3 (facet overrides win)", srv.hydrateLanes, srv.hydrateReaders)
		}
	})

	t.Run("warm pool opt-out", func(t *testing.T) {
		clearEnv(t)
		t.Setenv("GO_IVM_WARM_HYDRATE_POOL", "false")
		srv, err := newServerFromEnv()
		if err != nil {
			t.Fatalf("newServerFromEnv: %v", err)
		}
		if srv.warmHydratePoolEnabled {
			t.Error("GO_IVM_WARM_HYDRATE_POOL=false must disable the warm pool")
		}
	})
}

// TestABIHostReapsIdleGroups is the regression guard for the napi-only
// half of the ART memory leak: the in-process host must run the same
// idle-group reaper main() does, or abandoned CGs (missed TS teardown)
// accumulate engines + prev-tx conns for the worker's whole life. Drives
// the host end-to-end with a fast reaper (env-tuned to sub-second) and
// asserts an idle memory-mode group is collected.
func TestABIHostReapsIdleGroups(t *testing.T) {
	t.Setenv("GO_IVM_REAPER_INTERVAL_SEC", "1")
	t.Setenv("GO_IVM_REAPER_IDLE_SEC", "1")

	col := newSinkCollector()
	h := startABIHostWithServer(NewServer(makeReplicaPathOnly(t)), col.sink, nil)
	defer h.Shutdown()

	// Create a group via a real init RPC so it has an engine + worker.
	if err := h.Send(encodeReq(t, "init", 1, initParams{
		ClientGroupID: "cg-idle",
		Storage:       t.TempDir() + "/s.db",
		Tables: map[string]tableSchemaParams{
			"t": {
				Columns:    map[string]sqlite.ColumnSchema{"id": {Type: "string"}},
				PrimaryKey: []string{"id"},
			},
		},
	})); err != nil {
		t.Fatalf("send init: %v", err)
	}
	col.waitFrames(t, 1, 5*time.Second)

	if g := h.server.getGroup("cg-idle", false); g == nil {
		t.Fatal("group not created by init")
	}

	// Backdate lastUsed so the 1s idle threshold trips on the next tick.
	h.server.mu.RLock()
	g := h.server.groups["cg-idle"]
	h.server.mu.RUnlock()
	if g == nil {
		t.Fatal("group missing before reap")
	}
	g.lastUsedNs.Store(time.Now().Add(-1 * time.Hour).UnixNano())

	deadline := time.Now().Add(6 * time.Second)
	for time.Now().Before(deadline) {
		if h.server.getGroup("cg-idle", false) == nil {
			return // reaped
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatal("idle group was NOT reaped by the ABI host — napi transport leaks CGs")
}
