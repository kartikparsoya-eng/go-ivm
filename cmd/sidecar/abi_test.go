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

func (c *sinkCollector) sink(kind int32, payload []byte) {
	buf := make([]byte, len(payload))
	copy(buf, payload)
	c.mu.Lock()
	c.entries = append(c.entries, sinkEntry{kind: kind, payload: buf})
	if kind == abiKindFrame {
		c.frames = append(c.frames, buf)
	}
	c.mu.Unlock()
	c.notify <- struct{}{}
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

// TestABIHost_InitAdvanceRoundTrip drives a real init + advanceStream through
// the pump and asserts the full response set arrives via the sink: init
// response, advanceStream terminal partial (final=true), and its "done".
func TestABIHost_InitAdvanceRoundTrip(t *testing.T) {
	col := newSinkCollector()
	h := startABIHostWithServer(NewServer(0, ""), col.sink, nil)
	defer h.Shutdown()

	if err := h.Send(encodeReq(t, "init", 1, initParams{
		ClientGroupID: "cg-abi",
		Storage:       t.TempDir() + "/storage.db",
		Tables:        map[string]tableSchemaParams{},
	})); err != nil {
		t.Fatalf("send init: %v", err)
	}
	frames := col.waitFrames(t, 1, 10*time.Second)
	initResp := decodeResp(t, frames[0])
	if initResp.Error != nil {
		t.Fatalf("init failed: %+v", initResp.Error)
	}

	if err := h.Send(encodeReq(t, "advanceStream", 2, advanceParams{
		ClientGroupID: "cg-abi",
		InitEpoch:     1,
		Changes:       nil,
	})); err != nil {
		t.Fatalf("send advanceStream: %v", err)
	}
	// Empty advance emits exactly: 1 terminal partial (final=true, empty
	// changes) + 1 "done" response — both must arrive through the pump in
	// order (single flusher FIFO).
	frames = col.waitFrames(t, 3, 10*time.Second)
	partial := decodeResp(t, frames[1])
	if partial.Error != nil {
		t.Fatalf("partial carried error: %+v", partial.Error)
	}
	done := decodeResp(t, frames[2])
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
	h := startABIHostWithServer(NewServer(0, ""), col.sink, nil)
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
	slowSink := func(kind int32, payload []byte) {
		<-release // hold every delivery until the test opens the gate
		var resp RPCResponse
		if err := mpUnmarshal(payload, &resp); err != nil {
			t.Errorf("decode: %v", err)
			return
		}
		if f, ok := toFloat(resp.ID); ok {
			mu.Lock()
			got[int(f)]++
			count++
			mu.Unlock()
		}
	}
	h := startABIHostWithServer(NewServer(0, ""), slowSink, nil)
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
	h := startABIHostWithServer(NewServer(0, ""), col.sink, nil)

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
