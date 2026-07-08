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
	"net/http"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/kartikparsoya-eng/go-ivm/internal/tablesource"
	"github.com/kartikparsoya-eng/go-ivm/sqlite"
)

// newServerFromEnv builds and configures a *Server from the GO_IVM_*
// environment contract. Called by the NAPI host (goivm_start) — the only
// transport. Returns an error instead of os.Exit-ing — inside a host
// process, exiting would take the embedder down.
//
// Hard-wired prod path (removal sweep): the replica-backed table source is
// the ONLY leaf source and drive-mode advanceToHeadStream is the ONLY
// advance — the GO_IVM_SOURCE_MODE / GO_IVM_ADVANCE_TO_HEAD /
// GO_IVM_ADVANCE_DRIVE gates are gone, so a replica path is required.
func newServerFromEnv() (*Server, error) {
	if err := sqlite.SelfCheckCoercion(); err != nil {
		return nil, fmt.Errorf("coercion self-check: %w", err)
	}

	replicaPath := os.Getenv("GO_IVM_REPLICA_DB_PATH")
	if replicaPath == "" {
		replicaPath = os.Getenv("ZERO_REPLICA_FILE")
	}
	if replicaPath == "" {
		return nil, errors.New(
			"neither GO_IVM_REPLICA_DB_PATH nor ZERO_REPLICA_FILE is set (the replica-backed table source is the only leaf source)")
	}
	fmt.Fprintf(os.Stderr,
		"[GO-IVM] replica %s will open lazily on first init\n",
		replicaPath)

	server := NewServer(replicaPath)
	server.appID = os.Getenv("GO_IVM_APP_ID")
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
		"[GO-IVM] hydrate config: readers=%d(floor) lanes=%d (drive advance, streaming hydrate, appID=%q)\n",
		server.hydrateReaders, server.hydrateLanes, server.appID)
	// Startup-time non-default engine-knob markers (see PROD-PATH.md): a
	// default-path deployment prints NONE of these. Each gates an alternate
	// implementation kept as a rollback/experiment — code that is off the
	// TS-faithfulness review surface until deliberately engaged.
	if !tablesource.ParallelAdvance {
		nonDefault("GO_IVM_PARALLEL_ADVANCE=false (serial advance fanout fallback)")
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

	// deliver receives every outbound entry: (kind, payload) → status. Kind
	// 1 = msgpack RPC frame (length prefix stripped), kinds 2/3 = row-plane
	// records (see rowrecord.go), kind 4 = host death (see the death
	// watcher in startABIHostWithServer). The enqueue is NONBLOCKING (ABI
	// v4): deliverOK means the receiver copied the payload into its TSFN
	// queue entry synchronously (the bytes are valid ONLY for the duration
	// of the call); deliverFull means NOTHING was enqueued and the caller
	// owns the retry (rowplane.go retryDeliver for the row plane;
	// deliverPumpFrame for the pump); deliverClosed means the transport is
	// dead. Pre-v4 this callback BLOCKED on a full queue — an uncancellable
	// park inside cgo that composed with rp.mu + wg.Wait + the inFlight
	// worker into the G13 permanent CG wedge.
	deliver func(kind int32, payload []byte) int32

	mu     sync.Mutex
	cond   *sync.Cond
	sendQ  [][]byte
	closed bool
	// shuttingDown marks a DELIBERATE Shutdown() so the death watcher can
	// distinguish it from an unexpected pipe/handler death (A3): only the
	// latter delivers a kind-4 host-death record. Guarded by mu.
	shuttingDown bool
	// deathCause records the FIRST pump-exit error (read or write side) as
	// the reason payload of the host-death record. Guarded by mu.
	deathCause error
	done       chan struct{} // closed when the pumps have exited AND the death record (if any) was delivered
	wg         sync.WaitGroup

	// reaperCancel stops the idle-group reaper goroutine on Shutdown. The
	// socket transport runs this reaper from main(); the in-process host
	// must run its own or abandoned CGs never get collected (napi-only
	// leak — the whole reason abi.go reuses Server but not main()).
	reaperCancel context.CancelFunc

	// hcWg tracks the handleConnection goroutine so Shutdown can JOIN it.
	// Without this, Shutdown returned while handleConnection's deferred
	// cleanup (writerWg.Wait → close(flushCh) → flusher drain) was still
	// running — harmless in production (process exit reclaims; the pump
	// reader that touches the TSFN has already exited via <-h.done) but a
	// brief goroutine escape that tests observing "host fully torn down"
	// could race against (full-scale review 2026-07-03).
	hcWg sync.WaitGroup

	// pprofServer is the in-process pprof endpoint (nil unless
	// GO_IVM_PPROF_ADDR is set). Same O1 rationale as the reaper: pprof and
	// the PERF reporter lived only in main(), leaving napi mode blind.
	pprofServer *http.Server

	// otelShutdown flushes + tears down the OTLP trace exporter (otel.go).
	// nil when tracing is off or the host was built without env wiring
	// (startABIHostWithServer test path). Same O1 parity rationale as
	// pprof/PERF: otelInit used to live only in the socket main().
	otelShutdown func(context.Context) error
}

// errHostClosed is returned by Send after Shutdown (or pipe teardown).
var errHostClosed = errors.New("goivm abi host closed")

// startABIHost builds the server from env, wires the pipe to
// handleConnection, and starts the pump goroutines.
func startABIHost(deliver func(kind int32, payload []byte) int32) (*abiHost, error) {
	if deliver == nil {
		return nil, errors.New("deliver callback is required")
	}
	server, err := newServerFromEnv()
	if err != nil {
		return nil, err
	}
	h := startABIHostWithServer(server, deliver, nil)
	// OTLP trace exporter (env-gated noop without OTEL_EXPORTER_OTLP_*).
	// Telemetry must never take the host down — log and continue noop.
	if otelShutdown, oerr := otelInit(context.Background()); oerr != nil {
		fmt.Fprintf(os.Stderr, "[GO-IVM] OTel init failed (continuing without traces): %v\n", oerr)
	} else {
		h.otelShutdown = otelShutdown
	}
	return h, nil
}

// startABIHostWithServer is the injectable core (tests pass their own
// *Server so env parsing isn't exercised in every pump test).
//
// deliver contract: the ABI host owns ONE delivery callback used by BOTH
// planes — the pump reader (kind 1, msgpack frames read back off the pipe)
// and the row plane (kinds 2/3 + row-mode kind-1 frames, called directly by
// handlers via server.abiDeliver). The enqueue is NONBLOCKING and returns a
// status (deliverOK/deliverFull/deliverClosed — ABI v4); on deliverOK the
// receiver (the addon's C callback) copied the payload into its TSFN queue
// entry before returning, so payloads are valid only for the duration of
// the call. Retry policy is the CALLER's: the row plane parks cancellably
// (rowplane.go), the pump parks until the host closes (deliverPumpFrame).
func startABIHostWithServer(server *Server, deliver func(kind int32, payload []byte) int32, _ func([]byte)) *abiHost {
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
	// Pull idle sweeper (ABI v3, D7): auto-cancels pull gates parked past
	// GO_IVM_PULL_IDLE_TIMEOUT_SEC. Same lifecycle as the reaper.
	go server.runPullIdleSweeper(reaperCtx)
	// Wedge watchdog (wedgewatch.go): reports + stack-dumps any CG worker
	// stuck inside one handler past GO_IVM_WEDGE_WATCHDOG_SEC. Same
	// lifecycle as the reaper.
	go server.runWedgeWatchdog(reaperCtx)

	// Observability parity with the socket path (REVIEW-napi-transport O1):
	// the 10s [GO-IVM][PERF] reporter (what every soak greps) + the pprof
	// endpoint. Both were main()-only; the host never runs main(). pprof is
	// per-worker-port-derived (napi workers are separate processes).
	go server.runPerfReporter(reaperCtx)
	h.pprofServer = startPprofServer()

	// The production connection handler, verbatim. When either pipe end
	// closes, its read loop errors out and it tears down exactly as it
	// would on a socket disconnect. Tracked by hcWg so Shutdown can join
	// its deferred cleanup (which completes only after closeAll unblocks
	// the workers' respCh sends — hence the wait is AFTER closeAll).
	h.hcWg.Add(1)
	go func() {
		defer h.hcWg.Done()
		handleConnection(serverEnd, server)
	}()

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
				h.setDeathCause(err)
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
				h.setDeathCause(err)
				return
			}
			if !h.deliverPumpFrame(abiKindFrame, payload) {
				h.setDeathCause(errors.New("TSFN closed while delivering a response frame"))
				return
			}
		}
	}()

	go func() {
		h.wg.Wait()
		// Death watcher (A3, scale review): an UNEXPECTED pump death —
		// handleConnection exit (bad frame, internal error) or pipe
		// teardown, anything but a deliberate Shutdown — was previously
		// silent: every pending RPC hung to its full timeout and JS had no
		// way to notice (in-process there is no socket 'close' event to
		// observe). Deliver ONE kind-4 host-death record so the client
		// sweeps pending RPCs immediately and fatals the worker
		// (crash-don't-degrade — the host cannot be restarted in-process;
		// see napi_lib.go on Go runtime re-init). Delivered BEFORE
		// close(h.done) so Shutdown() cannot return — and the embedder
		// cannot release the TSFN — while this callback is still running.
		h.mu.Lock()
		deliberate := h.shuttingDown
		cause := h.deathCause
		h.mu.Unlock()
		if !deliberate {
			reason := "goivm host pump terminated"
			if cause != nil {
				reason += ": " + cause.Error()
			}
			// Best-effort bounded retry: the death record is the client's
			// ONLY signal that the host is gone, so give a starved loop a
			// few seconds to accept it — but the process is dying either
			// way, so never park forever (that would also hold close(h.done)
			// and with it Shutdown()).
			deadline := time.Now().Add(5 * time.Second)
			for {
				st := h.deliver(abiKindHostDeath, []byte(reason))
				if st != deliverFull || time.Now().After(deadline) {
					break
				}
				time.Sleep(5 * time.Millisecond)
			}
		}
		close(h.done)
	}()
	return h
}

// deliverPumpFrame delivers one control-plane frame off the pump, parking
// while the TSFN queue is full. Control frames (RPC responses, "done"
// sentinels) must never be DROPPED — a missing frame orphans its RPC into
// the TS timeout — so unlike the row plane there is no deadline here: the
// park IS the transport backpressure the pipe chain propagates (and it
// wedges nothing — the pump is its own goroutine; CG workers hand frames
// off via respCh and move on). The park stays escapable: host teardown
// (markClosed → h.closed) or a dying TSFN (deliverClosed) breaks it.
func (h *abiHost) deliverPumpFrame(kind int32, payload []byte) bool {
	switch h.deliver(kind, payload) {
	case deliverOK:
		return true
	case deliverClosed:
		return false
	}
	metrics.napiDeliverStalls.Add(1)
	sleep := 100 * time.Microsecond
	for {
		if h.isClosed() {
			return false
		}
		time.Sleep(sleep)
		if sleep < 5*time.Millisecond {
			sleep *= 2
		}
		switch h.deliver(kind, payload) {
		case deliverOK:
			return true
		case deliverClosed:
			return false
		}
	}
}

func (h *abiHost) isClosed() bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.closed
}

// Send enqueues one request frame (payload WITHOUT length prefix; the writer
// adds it). TAKES OWNERSHIP of payload — the caller must not reuse or mutate
// the slice after the call (REVIEW-napi-transport perf #2: goivm_send already
// hands us a fresh C.GoBytes copy, so an internal make+copy here was a second
// redundant allocation per request frame; every caller passes a freshly
// built, never-retained slice — verified).
func (h *abiHost) Send(payload []byte) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return errHostClosed
	}
	h.sendQ = append(h.sendQ, payload)
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

// setDeathCause records the first pump-exit error; later causes are noise
// (the teardown cascade after the first failure).
func (h *abiHost) setDeathCause(err error) {
	if err == nil {
		return
	}
	h.mu.Lock()
	if h.deathCause == nil {
		h.deathCause = err
	}
	h.mu.Unlock()
}

// Shutdown tears down the pipe (handleConnection exits via read error, its
// deferred flusher drain runs) and waits for the pump goroutines, then
// closes all client groups. Idempotent. Marks the teardown DELIBERATE
// first, so the death watcher does not deliver a host-death record (which
// would trigger a spurious worker fatal during graceful teardown).
func (h *abiHost) Shutdown() {
	h.mu.Lock()
	h.shuttingDown = true
	h.mu.Unlock()
	if h.reaperCancel != nil {
		h.reaperCancel()
	}
	if h.pprofServer != nil {
		h.pprofServer.Shutdown(context.Background())
	}
	h.markClosed()
	<-h.done
	h.server.closeAll()
	// Join handleConnection LAST: its deferred cleanup blocks on
	// writerWg.Wait(), whose writer goroutines unblock only after closeAll
	// drains the workers' respCh sends. Waiting before closeAll would
	// deadlock; waiting after guarantees no goroutine outlives Shutdown.
	h.hcWg.Wait()
	if h.otelShutdown != nil {
		if err := h.otelShutdown(context.Background()); err != nil {
			fmt.Fprintf(os.Stderr, "[GO-IVM] OTel shutdown error: %v\n", err)
		}
	}
}
