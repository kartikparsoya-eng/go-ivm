package main

// Review item #1: backpressure / head-of-line blocking on the shared socket.
//
// The chain (all statically confirmed in handleConnection + worker):
//   slow/stalled TS reader
//     → flusher goroutine blocks in writeFrame
//     → flushCh (cap 256) fills
//     → a STREAMING handler's streamW send blocks the CG WORKER mid-stream
//       (partials are enqueued from the worker goroutine; for advanceStream
//       that means engine.mu is held while blocked)
//     → that CG's reqC (cap 64) fills
//     → the connection READ LOOP blocks in trySendReq
//     → every other CG on the socket is starved (head-of-line).
//
// This is the known design gap the spill-on-backpressure plan addresses
// (DESIGN-streaming-hydrate.md). These tests do two jobs:
//   1. pin the enduring invariant that must survive any redesign:
//      backpressure is LOSSLESS — once the reader drains, every request
//      gets exactly one terminal response, nothing lost, nothing duplicated;
//   2. document the current wedge shape (send-count plateau while stalled)
//      so shipping the fix flips an explicit assertion instead of silently
//      changing behavior. See the CURRENT-BEHAVIOR markers.

import (
	"bufio"
	"fmt"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/vmihailenco/msgpack/v5"
)

func encodeReq(t *testing.T, method string, id float64, params interface{}) []byte {
	t.Helper()
	rawParams, err := mpMarshal(params)
	if err != nil {
		t.Fatalf("marshal params: %v", err)
	}
	data, err := mpMarshal(RPCRequest{
		JSONRPC: "2.0",
		Method:  method,
		Params:  msgpack.RawMessage(rawParams),
		ID:      id,
	})
	if err != nil {
		t.Fatalf("marshal request: %v", err)
	}
	return data
}

// TestBackpressure_StalledReader_HeadOfLineThenLosslessDrain drives the full
// chain end-to-end over a net.Pipe (zero kernel buffering — the harshest
// "slow reader"): init one CG, stop reading, fire a burst of
// advanceToHeadStream calls (each settles at head: 1 final partial + done). The burst must WEDGE (current behavior) and, once the client reads
// again, drain completely with exactly one "done" response per request.
func TestBackpressure_StalledReader_HeadOfLineThenLosslessDrain(t *testing.T) {
	server := NewServer(makeReplicaPathOnly(t))
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

	cli := bufio.NewReaderSize(cliConn, 64*1024)

	// --- Phase 0: init cg-A (reader active). ---
	if err := writeFrame(cliConn, encodeReq(t, "init", 1, issueInitParams("cg-A"))); err != nil {
		t.Fatalf("write init: %v", err)
	}
	frame, err := readFrame(cli)
	if err != nil {
		t.Fatalf("read init resp: %v", err)
	}
	var initResp RPCResponse
	if err := mpUnmarshal(frame, &initResp); err != nil {
		t.Fatalf("decode init resp: %v", err)
	}
	if initResp.Error != nil {
		t.Fatalf("init failed: %+v", initResp.Error)
	}

	// --- Phase 1: stall the reader, fire the burst. ---
	// Each at-head advanceToHeadStream emits 1 partial (final) via the
	// WORKER's streamW + 1 "done" via respCh — ~2 flushCh entries per call.
	// flushCh(256)+flusher(1) saturate around call ~130; the worker then
	// blocks mid-stream, reqC(64) fills, and the read loop wedges in
	// trySendReq around call ~200. N=400 guarantees the plateau.
	const burst = 400
	var sent atomic.Int32
	writerDone := make(chan error, 1)
	go func() {
		for i := 0; i < burst; i++ {
			data := encodeReq(t, "advanceToHeadStream", float64(100+i), advanceToHeadParams{
				ClientGroupID: "cg-A",
				InitEpoch:     1,
			})
			if err := writeFrame(cliConn, data); err != nil {
				writerDone <- fmt.Errorf("write %d: %w", i, err)
				return
			}
			sent.Add(1)
		}
		writerDone <- nil
	}()

	// Wait for the send count to plateau: no progress across 500ms.
	deadline := time.Now().Add(10 * time.Second)
	var plateau int32
	for {
		if time.Now().After(deadline) {
			t.Fatal("send count never plateaued — expected the socket to wedge")
		}
		before := sent.Load()
		time.Sleep(500 * time.Millisecond)
		if after := sent.Load(); after == before && after > 0 {
			plateau = after
			break
		}
	}

	// CURRENT-BEHAVIOR: the wedge strikes long before the burst completes.
	// When spill-on-backpressure ships, sends should complete without a
	// reader (plateau == burst) and this assertion must be flipped, not
	// deleted — that's the point of pinning it.
	if plateau >= burst {
		t.Fatalf("burst completed (%d/%d) with a stalled reader — "+
			"backpressure wedge is GONE; update this test to assert the new contract",
			plateau, burst)
	}
	t.Logf("CURRENT-BEHAVIOR: socket wedged after %d/%d sends (head-of-line engaged)", plateau, burst)

	// --- Phase 2: drain. The enduring invariant — LOSSLESS. ---
	// Reading again must unwedge everything: the writer finishes all 400
	// sends and every request yields exactly one terminal response.
	doneByID := map[float64]int{}
	partials := 0
	drainDeadline := time.Now().Add(30 * time.Second)
	for len(doneByID) < burst {
		if time.Now().After(drainDeadline) {
			t.Fatalf("drain incomplete: %d/%d terminal responses (lost under backpressure)",
				len(doneByID), burst)
		}
		_ = cliConn.SetReadDeadline(time.Now().Add(5 * time.Second))
		frame, err := readFrame(cli)
		if err != nil {
			t.Fatalf("drain read after %d dones, %d partials: %v", len(doneByID), partials, err)
		}
		var resp RPCResponse
		if err := mpUnmarshal(frame, &resp); err != nil {
			t.Fatalf("drain decode: %v", err)
		}
		id, ok := resp.ID.(float64)
		if !ok || id < 100 {
			continue // stray non-burst frame (none expected)
		}
		if resp.Error != nil {
			t.Fatalf("burst call %v failed: %+v", resp.ID, resp.Error)
		}
		// advanceStream emits partial frame(s) then a terminal "done" whose
		// Result is the string "done" (matching the TS client's contract).
		if s, isStr := resp.Result.(string); isStr && s == "done" {
			doneByID[id]++
			if doneByID[id] > 1 {
				t.Fatalf("duplicate terminal response for id %v", id)
			}
		} else {
			partials++
		}
	}
	if err := <-writerDone; err != nil {
		t.Fatalf("writer failed: %v", err)
	}
	if got := sent.Load(); got != burst {
		t.Fatalf("writer sent %d/%d after drain", got, burst)
	}
	if partials < burst {
		t.Fatalf("expected >= %d partial frames (1 final chunk per call), got %d", burst, partials)
	}
	t.Logf("lossless drain: %d dones, %d partials, 0 duplicates", len(doneByID), partials)
}

// TestBackpressure_WorkerStall_FIFOFillsAndTrySendReqBlocks pins the
// primitive underneath the socket test, deterministically: a worker stuck
// delivering one response (full respCh stands in for "blocked on flushCh")
// backs up reqC (cap 64), and the 65th trySendReq BLOCKS the caller — in
// production that caller is the connection read loop shared by every CG.
// Unblocking the worker drains everything in FIFO order with no loss.
func TestBackpressure_WorkerStall_FIFOFillsAndTrySendReqBlocks(t *testing.T) {
	s := NewServer(makeReplicaPathOnly(t))
	g := s.getGroup("cg-fifo", true)

	// Request 0: respCh is UNBUFFERED and unread → the worker blocks at its
	// `req.respCh <- resp` send, exactly like blocking on a full flushCh.
	stuckCh := make(chan RPCResponse)
	if !g.trySendReq(clientGroupReq{req: RPCRequest{ID: 0.0, Method: "ping"}, respCh: stuckCh}) {
		t.Fatal("enqueue 0 failed")
	}

	// Fill reqC to capacity behind it. The worker consumed request 0 and is
	// stuck, so none of these drain. Cap is 64 (main.go:924); a resize
	// invalidates this test's arithmetic — fail loudly if sends stop early.
	const fifoCap = 64
	chans := make([]chan RPCResponse, fifoCap)
	for i := 0; i < fifoCap; i++ {
		chans[i] = make(chan RPCResponse, 1)
		done := make(chan bool, 1)
		go func(i int) {
			done <- g.trySendReq(clientGroupReq{req: RPCRequest{ID: float64(i + 1), Method: "ping"}, respCh: chans[i]})
		}(i)
		select {
		case ok := <-done:
			if !ok {
				t.Fatalf("enqueue %d rejected", i+1)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("enqueue %d blocked — reqC cap changed below %d? update this test", i+1, fifoCap)
		}
	}

	// The 65th send must BLOCK (current design: blocking send with TCP-style
	// backpressure). This is the head-of-line primitive: in production this
	// goroutine is the connection read loop.
	overflowCh := make(chan RPCResponse, 1)
	overflowDone := make(chan bool, 1)
	go func() {
		overflowDone <- g.trySendReq(clientGroupReq{req: RPCRequest{ID: 999.0, Method: "ping"}, respCh: overflowCh})
	}()
	select {
	case <-overflowDone:
		t.Fatal("send into a full FIFO returned immediately — blocking-backpressure contract changed; " +
			"re-audit head-of-line handling and update this test")
	case <-time.After(300 * time.Millisecond):
		// blocked, as designed today (CURRENT-BEHAVIOR)
	}

	// Unstick the worker; everything must drain losslessly, in order.
	<-stuckCh
	for i := 0; i < fifoCap; i++ {
		select {
		case <-chans[i]:
		case <-time.After(2 * time.Second):
			t.Fatalf("queued request %d never got a response after unstick", i+1)
		}
	}
	select {
	case ok := <-overflowDone:
		if !ok {
			t.Fatal("overflow send rejected after drain")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("overflow send still blocked after FIFO drained")
	}
	select {
	case <-overflowCh:
	case <-time.After(2 * time.Second):
		t.Fatal("overflow request never processed")
	}
}
