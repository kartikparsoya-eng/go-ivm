package main

// ABI host for the in-process (NAPI) transport. This file is build-tag-free
// pure Go so the pump + env-construction logic is unit-testable with the
// ordinary test toolchain; the cgo //export shims live in napi_lib.go
// (build tag `napilib`) and are deliberately paper-thin over this.
//
// Design (frame pump): the socket transport's ONLY jobs are (a) carrying
// length-prefixed msgpack frames in each direction and (b) exerting
// backpressure. Everything else — request dispatch, per-group FIFO, streaming
// partials, the single-flusher ordering guarantee — lives in handleConnection
// and must not fork per transport. So the ABI host connects the EXISTING
// handleConnection to an in-memory net.Pipe:
//
//	goivm_send(bytes) → sendQ → writer goroutine → clientEnd ─pipe─ serverEnd → handleConnection
//	handleConnection → flushCh → writeFrame(serverEnd) ─pipe─ clientEnd → pump reader → frameSink → TSFN → JS
//
// handleConnection runs byte-identical to production; the pump reader strips
// the 4-byte length prefix (readFrame) and hands the payload to the sink.
// Backpressure is preserved end-to-end: a slow JS consumer blocks the sink
// call → blocks the pump reader → blocks handleConnection's flusher →
// fills flushCh/outC → blocks the request reader (same chain as a slow
// socket; see handleConnection's flusher comment).
//
// Send-side queue: Node's socket.write never blocks the JS thread (userspace
// buffering), so goivm_send must not either. sendQ is an unbounded
// mutex+cond queue drained by one writer goroutine; memory is bounded in
// practice by the TS client's own in-flight slot discipline (maxInFlight),
// exactly as with the socket.

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"strconv"
	"sync"

	"github.com/kartikparsoya-eng/go-ivm/internal/tablesource"
	"github.com/kartikparsoya-eng/go-ivm/sqlite"
)

// newServerFromEnv builds and configures a *Server from the same GO_IVM_*
// environment contract main() uses. Extracted so the NAPI host constructs an
// identical server in-process. Returns an error instead of os.Exit-ing —
// inside a host process, exiting would take the embedder down.
func newServerFromEnv() (*Server, error) {
	if err := sqlite.SelfCheckCoercion(); err != nil {
		return nil, fmt.Errorf("coercion self-check: %w", err)
	}

	sourceMode := tablesource.ParseMode()
	var replicaPath string
	if sourceMode == tablesource.ModeTable {
		replicaPath = os.Getenv("GO_IVM_REPLICA_DB_PATH")
		if replicaPath == "" {
			replicaPath = os.Getenv("ZERO_REPLICA_FILE")
		}
		if replicaPath == "" {
			return nil, errors.New(
				"GO_IVM_SOURCE_MODE=table but neither GO_IVM_REPLICA_DB_PATH nor ZERO_REPLICA_FILE is set")
		}
		fmt.Fprintf(os.Stderr,
			"[GO-IVM] table mode armed; replica %s will open lazily on first init\n",
			replicaPath)
	}

	server := NewServer(sourceMode, replicaPath)
	server.appID = os.Getenv("GO_IVM_APP_ID")
	server.advanceToHeadEnabled = os.Getenv("GO_IVM_ADVANCE_TO_HEAD") == "true"
	// GO_IVM_ADVANCE_DRIVE implies advanceToHead (P2 self-consistent advance).
	server.advanceDriveEnabled = os.Getenv("GO_IVM_ADVANCE_DRIVE") == "true"
	if server.advanceDriveEnabled {
		server.advanceToHeadEnabled = true
	}
	// ONE parallelism knob: GO_IVM_PARALLELISM (default 4) sets the hydrate
	// lane count (P — the engine package reads the SAME env for its lane
	// workers, so the two stay in lockstep) and the reader-pool floor
	// (K = 2×P; default 4 lanes / 8 readers is the prod-validated shape from
	// the Dockerfile rollout). GO_IVM_HYDRATE_LANES / GO_IVM_HYDRATE_READERS
	// override the facets individually for A/B work.
	parallelism := 4
	if v := os.Getenv("GO_IVM_PARALLELISM"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			parallelism = n
		}
	}
	server.hydrateLanes = parallelism
	server.hydrateReaders = 2 * parallelism
	if v := os.Getenv("GO_IVM_HYDRATE_READERS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 1 {
			server.hydrateReaders = n
		}
	}
	if v := os.Getenv("GO_IVM_HYDRATE_LANES"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			server.hydrateLanes = n
		}
	}
	// Warm-hydrate reader pool: production default ON — co-read-only (never
	// converges the pool to head), so it cannot desync live pipelines;
	// validated in the rust-test soak (pin-rate 100%, serial fallback 0).
	// GO_IVM_WARM_HYDRATE_POOL=false disables.
	server.warmHydratePoolEnabled = os.Getenv("GO_IVM_WARM_HYDRATE_POOL") != "false"
	fmt.Fprintf(os.Stderr,
		"[GO-IVM] hydrate config: streaming=%v (default-on under drive) readers=%d(floor) lanes=%d advanceDrive=%v\n",
		server.advanceDriveEnabled, server.hydrateReaders, server.hydrateLanes, server.advanceDriveEnabled)
	if server.advanceToHeadEnabled {
		if sourceMode != tablesource.ModeTable {
			fmt.Fprintln(os.Stderr,
				"[GO-IVM] GO_IVM_ADVANCE_TO_HEAD=true ignored: requires GO_IVM_SOURCE_MODE=table")
			server.advanceToHeadEnabled = false
			server.advanceDriveEnabled = false
		} else {
			mode := "derive-only (P1 shadow)"
			if server.advanceDriveEnabled {
				mode = "DRIVE (P2 frame-coordinated self-consistent advance)"
			}
			fmt.Fprintf(os.Stderr,
				"[GO-IVM] advanceToHead ARMED [%s] (appID=%q)\n", mode, server.appID)
		}
	}
	return server, nil
}

// abiHost owns one in-process "connection": the net.Pipe pair, the send
// queue + writer goroutine, and the pump reader that forwards response
// frames to the registered sink. One host per embedding (the addon creates
// exactly one), mirroring the one-socket-per-worker deployment shape.
type abiHost struct {
	server *Server

	clientEnd net.Conn // ABI side: requests written here, responses read here
	serverEnd net.Conn // handleConnection side

	// deliver receives every outbound entry: (kind, payload). Kind 1 =
	// msgpack RPC frame (length prefix stripped), kinds 2/3 = row-plane
	// records (see rowrecord.go). The bytes are valid ONLY for the
	// duration of the call — the receiver must copy before returning
	// (the cgo shim's C callback contract; the addon memcpy's into its
	// TSFN queue entry).
	deliver func(kind int32, payload []byte)

	mu     sync.Mutex
	cond   *sync.Cond
	sendQ  [][]byte
	closed bool
	done   chan struct{} // closed when both pump goroutines have exited
	wg     sync.WaitGroup

	// reaperCancel stops the idle-group reaper goroutine on Shutdown. The
	// socket transport runs this reaper from main(); the in-process host
	// must run its own or abandoned CGs never get collected (napi-only
	// leak — the whole reason abi.go reuses Server but not main()).
	reaperCancel context.CancelFunc
}

// errHostClosed is returned by Send after Shutdown (or pipe teardown).
var errHostClosed = errors.New("goivm abi host closed")

// startABIHost builds the server from env, wires the pipe to
// handleConnection, and starts the pump goroutines.
func startABIHost(deliver func(kind int32, payload []byte)) (*abiHost, error) {
	if deliver == nil {
		return nil, errors.New("deliver callback is required")
	}
	server, err := newServerFromEnv()
	if err != nil {
		return nil, err
	}
	return startABIHostWithServer(server, deliver, nil), nil
}

// startABIHostWithServer is the injectable core (tests pass their own
// *Server so env parsing isn't exercised in every pump test).
//
// deliver contract: the ABI host owns ONE delivery callback used by BOTH
// planes — the pump reader (kind 1, msgpack frames read back off the pipe)
// and the row plane (kinds 2/3 + row-mode kind-1 frames, called directly by
// handlers via server.abiDeliver). Every payload is valid only for the
// duration of the call; the receiver (the addon's C callback) copies into
// its TSFN queue entry before returning.
func startABIHostWithServer(server *Server, deliver func(kind int32, payload []byte), _ func([]byte)) *abiHost {
	clientEnd, serverEnd := net.Pipe()
	h := &abiHost{
		server:    server,
		clientEnd: clientEnd,
		serverEnd: serverEnd,
		deliver:   deliver,
		done:      make(chan struct{}),
	}
	h.cond = sync.NewCond(&h.mu)

	// Row plane: handlers deliver records/frames for rowMode RPCs directly
	// (bypassing the pipe — see rowplane.go's ordering invariant). Set
	// before handleConnection starts; never mutated after.
	server.abiDeliver = deliver

	// Idle-group reaper: the socket transport starts this from main(); the
	// in-process host must start its own (same Server, same leak otherwise).
	// Cancelled on Shutdown.
	reaperCtx, reaperCancel := context.WithCancel(context.Background())
	h.reaperCancel = reaperCancel
	go server.runReaper(reaperCtx)

	// The production connection handler, verbatim. When either pipe end
	// closes, its read loop errors out and it tears down exactly as it
	// would on a socket disconnect.
	go handleConnection(serverEnd, server)

	// Send-queue writer: drains sendQ → clientEnd. net.Pipe writes are
	// synchronous (block until handleConnection's reader consumes), which
	// is exactly the request-side backpressure the socket had via the
	// kernel buffer + outC chain — but it must block THIS goroutine, not
	// the JS thread, hence the queue.
	h.wg.Add(1)
	go func() {
		defer h.wg.Done()
		for {
			h.mu.Lock()
			for len(h.sendQ) == 0 && !h.closed {
				h.cond.Wait()
			}
			if h.closed && len(h.sendQ) == 0 {
				h.mu.Unlock()
				return
			}
			frame := h.sendQ[0]
			h.sendQ = h.sendQ[1:]
			h.mu.Unlock()
			if err := writeFrame(clientEnd, frame); err != nil {
				// Pipe torn down (shutdown or handler exit): drop the
				// remaining queue; pending RPCs fail via the sink close.
				h.markClosed()
				return
			}
		}
	}()

	// Pump reader: response frames → sink. readFrame strips the 4-byte
	// prefix, so the sink receives the raw msgpack payload. ONE bufio
	// reader for the connection's lifetime — a per-frame reader would
	// discard buffered bytes belonging to the next frame.
	h.wg.Add(1)
	go func() {
		defer h.wg.Done()
		defer h.markClosed()
		reader := bufio.NewReaderSize(clientEnd, 64*1024)
		for {
			payload, err := readFrame(reader)
			if err != nil {
				return
			}
			h.deliver(abiKindFrame, payload)
		}
	}()

	go func() {
		h.wg.Wait()
		close(h.done)
	}()
	return h
}

// Send enqueues one request frame (payload WITHOUT length prefix; the
// writer adds it). The buffer is copied before return, so the caller (the
// cgo shim pointing at C-owned memory) may reuse/free it immediately.
func (h *abiHost) Send(payload []byte) error {
	buf := make([]byte, len(payload))
	copy(buf, payload)
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return errHostClosed
	}
	h.sendQ = append(h.sendQ, buf)
	h.cond.Signal()
	return nil
}

func (h *abiHost) markClosed() {
	h.mu.Lock()
	h.closed = true
	h.cond.Broadcast()
	h.mu.Unlock()
	_ = h.clientEnd.Close()
	_ = h.serverEnd.Close()
}

// Shutdown tears down the pipe (handleConnection exits via read error, its
// deferred flusher drain runs) and waits for the pump goroutines, then
// closes all client groups. Idempotent.
func (h *abiHost) Shutdown() {
	if h.reaperCancel != nil {
		h.reaperCancel()
	}
	h.markClosed()
	<-h.done
	h.server.closeAll()
}
