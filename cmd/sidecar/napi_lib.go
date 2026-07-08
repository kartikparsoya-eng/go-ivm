//go:build napilib

package main

// C ABI shims for the in-process (NAPI) transport. Paper-thin over abi.go:
// all logic lives there (build-tag-free, unit-tested); this file only
// translates between C and Go memory at the boundary.
//
// Build (produces libgoivm + header):
//
//	go build -tags "libsqlite3 napilib" -buildmode=c-shared \
//	  -o libgoivm.so ./cmd/sidecar
//
// ABI (see goivm_abi_version for compatibility):
//
//	typedef int32_t (*goivm_deliver_cb)(void* ctx, int32_t kind,
//	                                    const void* data, int32_t len);
//	int32_t goivm_start(goivm_deliver_cb cb, void* ctx);
//	int32_t goivm_send(const void* data, int32_t len);
//	void    goivm_shutdown(void);
//	int32_t goivm_abi_version(void);
//	void    goivm_stream_credit(double req_id, int32_t n);   // ABI v3
//	void    goivm_stream_cancel(double req_id);              // ABI v3
//
// Threading & memory contract:
//   - The deliver callback is invoked from Go-runtime goroutines (NOT the
//     JS thread). It must be NONBLOCKING (ABI v4): it attempts the TSFN
//     enqueue and returns 0 (queued — payload copied synchronously),
//     1 (queue full — nothing enqueued; the Go side owns the retry, which
//     is what makes a stalled JS consumer CANCELLABLE), or 2 (TSFN
//     closing — transport dead). It must NOT call back into goivm_send
//     (deadlock risk via the pipe backpressure chain). Pre-v4 the callback
//     was allowed to block — "backpressure like a slow socket" — which
//     parked Go goroutines in an uninterruptible cgo call for as long as
//     the JS event loop stayed starved (the G13 CG wedge).
//   - (data,len) passed to the callback are valid ONLY for the duration of
//     the call; the receiver must copy before returning 0. This satisfies
//     the cgo pointer rules: the Go-owned buffer is never retained by C.
//   - goivm_send copies (data,len) before returning; the caller may free
//     its buffer immediately. It never blocks the calling (JS) thread —
//     enqueue is O(1) into an unbounded queue drained by a Go goroutine.
//   - Exactly one host per process. goivm_start returns non-zero if already
//     started or if server construction failed (details on stderr).
//   - A Go panic that escapes a handler is recovered by the server's
//     existing recover machinery and surfaced as an error frame; Go runtime
//     FATAL errors (not panics) still kill the whole process — the embedder
//     accepts worker-restart as the crash domain (documented trade).

/*
#include <stdint.h>
#include <stdlib.h>
#include <unistd.h>

// Declare environ explicitly — it's in <unistd.h> on POSIX but not
// exported as a symbol cgo can link to on all platforms.
extern char **environ;

typedef int32_t (*goivm_deliver_cb)(void* ctx, int32_t kind, const void* data, int32_t len);

// cgo cannot call a C function pointer directly; this trampoline does.
static int32_t goivm_call_deliver(goivm_deliver_cb cb, void* ctx, int32_t kind, const void* data, int32_t len) {
	return cb(ctx, kind, data, len);
}

// goivm_env_count returns the number of entries in C environ.
static int goivm_env_count(void) {
	int n = 0;
	while (environ[n] != NULL) n++;
	return n;
}

// goivm_env_at returns the i-th environ entry (NULL-terminated "KEY=VALUE").
static const char *goivm_env_at(int i) {
	return environ[i];
}
*/
import "C"

import (
	"fmt"
	"os"
	"strings"
	"sync"
	"unsafe"
)

// goivmABIVersion increments on ANY breaking change to the exported
// functions, the delivery-kind tags, or the row-record layout
// (rowrecord.go). The addon refuses to start on a mismatch.
//
//	v2: added delivery kind 4 (host death — abi.go's death watcher; the
//	    client must fatal the worker on receipt, so a v1 addon that would
//	    silently warn-and-drop it must not pair with a v2 library).
//	v3: added goivm_stream_credit / goivm_stream_cancel (pull-hydration
//	    demand gate, DESIGN-duplex-streaming). Pull is a per-request
//	    opt-in (params.pullMode) so a v3 addon on a v3 library with pull
//	    disabled behaves exactly like v2; the version gates the SYMBOLS —
//	    a v3 addon dlsym-ing the credit exports must never pair with a
//	    library that silently lacks them (grants would vanish and every
//	    pull hydrate would park to idle-timeout).
//	v4: the deliver callback returns int32_t status (0=queued, 1=queue
//	    full, 2=closing) and the addon enqueues with napi_tsfn_nonblocking;
//	    the Go side owns the retry, which makes a delivery parked on a
//	    starved JS event loop CANCELLABLE (pull-gate cancel / group
//	    teardown / GO_IVM_DELIVER_TIMEOUT) — the G13 CG-wedge fix. The
//	    version gates the SIGNATURE: a v4 library reading a return value
//	    from a v3 addon's void callback would consume a garbage register
//	    (a phantom "queue full" retries an enqueue that SUCCEEDED —
//	    duplicate delivery → stream corruption), and a v3 library's
//	    blocking semantics on a v4 addon would silently reintroduce the
//	    wedge.
//	v5: added goivm_queue_drained (the addon signals when its TSFN queue
//	    drains below the low-water mark — event-driven producer wakeup
//	    replacing v4's 100µs→5ms sleep-poll, whose dead air was the
//	    latency tax the first v4 soak measured: 19,242 parks × up to 5ms)
//	    and delivery kind 5 (record batch — the row plane stages records
//	    under congestion and ships them as one queue item; rowplane.go).
//	    The version gates BOTH: a v4 addon never signals drain (v5
//	    producers would degrade to tick-polling) and, worse, would hand
//	    kind-5 batches to a JS side with no batch decoder — dropped
//	    deliveries → stream corruption.
const goivmABIVersion = 5

var (
	abiMu   sync.Mutex
	abiHst  *abiHost
	abiCB   C.goivm_deliver_cb
	abiCtx  unsafe.Pointer
	started bool
)

//export goivm_abi_version
func goivm_abi_version() C.int32_t {
	return goivmABIVersion
}

// syncCEnvToGo copies the live C environ into Go's os.Getenv cache. The
// Go runtime in c-shared mode snapshots environ at library-load (dlopen)
// time into an internal slice (runtime.environ); os.Getenv reads from that
// snapshot, NOT from the live C environ. When the embedder (Node.js)
// sets process.env.X = 'y' before dlopen, C setenv() updates environ and
// C getenv() sees it, but the Go snapshot was taken from a stale copy and
// os.Getenv returns "". This function bridges the gap by iterating the
// live C environ and calling os.Setenv for each entry, updating the Go
// runtime's cache so newServerFromEnv and tuneRuntime see the host's env.
func syncCEnvToGo() {
	n := int(C.goivm_env_count())
	for i := 0; i < n; i++ {
		s := C.GoString(C.goivm_env_at(C.int(i)))
		if idx := strings.IndexByte(s, '='); idx > 0 {
			os.Setenv(s[:idx], s[idx+1:])
		}
	}
}

//export goivm_start
func goivm_start(cb C.goivm_deliver_cb, ctx unsafe.Pointer) C.int32_t {
	abiMu.Lock()
	defer abiMu.Unlock()
	if started {
		fmt.Fprintln(os.Stderr, "[GO-IVM][napi] goivm_start: already started")
		return 1
	}
	if cb == nil {
		fmt.Fprintln(os.Stderr, "[GO-IVM][napi] goivm_start: nil deliver callback")
		return 2
	}
	abiCB = cb
	abiCtx = ctx

	// Sync the live C environ into Go's os.Getenv cache. The Go runtime
	// snapshots environ at dlopen (which the addon called before us), but
	// that snapshot misses env vars the host set via setenv() after the
	// process started (e.g. Node.js process.env assignments). Without this,
	// newServerFromEnv can't see GO_IVM_REPLICA_DB_PATH and returns rc=3.
	syncCEnvToGo()

	tuneRuntime()

	deliver := func(kind int32, payload []byte) int32 {
		// Pass the Go slice's base pointer into C for the DURATION OF THE
		// CALL only — legal under the cgo pointer rules; the addon copies
		// into its TSFN queue entry before returning 0. Empty payloads pass
		// a nil pointer with len 0. The returned status (0/1/2) is the
		// nonblocking enqueue outcome — see the ABI v4 notes above.
		var p unsafe.Pointer
		if len(payload) > 0 {
			p = unsafe.Pointer(&payload[0])
		}
		return int32(C.goivm_call_deliver(abiCB, abiCtx, C.int32_t(kind), p, C.int32_t(len(payload))))
	}

	h, err := startABIHost(deliver)
	if err != nil {
		fmt.Fprintf(os.Stderr, "[GO-IVM][napi] goivm_start: %v\n", err)
		return 3
	}
	abiHst = h
	started = true
	fmt.Fprintf(os.Stderr, "[GO-IVM][napi] in-process host started (abi v%d)\n",
		goivmABIVersion)
	return 0
}

//export goivm_send
func goivm_send(data unsafe.Pointer, length C.int32_t) C.int32_t {
	abiMu.Lock()
	h := abiHst
	abiMu.Unlock()
	if h == nil {
		return 1
	}
	if length < 0 {
		return 2
	}
	// C.GoBytes copies C→Go, so the caller's C buffer is free after return.
	// The resulting slice is fresh and unshared, so h.Send takes ownership of
	// it directly (no second copy — REVIEW-napi-transport perf #2).
	payload := C.GoBytes(data, C.int(length))
	if err := h.Send(payload); err != nil {
		return 3
	}
	return 0
}

//export goivm_shutdown
func goivm_shutdown() {
	abiMu.Lock()
	h := abiHst
	abiHst = nil
	// started stays true: the Go runtime cannot be re-initialized in a
	// loaded c-shared library, so a second start after shutdown is a
	// programming error we surface loudly rather than half-support.
	abiMu.Unlock()
	if h != nil {
		h.Shutdown()
	}
}

// goivm_stream_credit grants n credits to the pull gate of the in-flight
// pullMode RPC identified by reqID (ABI v3, DESIGN-duplex-streaming D8).
//
// Called DIRECTLY on the JS thread (dlsym'd, no TSFN round-trip): the JS
// iterator grants at its low-water mark as the app consumes rows. Safe
// because the whole path is a leaf-mutex registry lookup + cond broadcast —
// O(1), allocation-free, never blocks on engine or server state, never
// touches N-API. reqID rides the C `double` type because that is what a JS
// number is — bit-exact with the f64 reqID the row plane keys records by
// (rowrecord.go numericReqID). Unknown reqID is a silent no-op (the RPC
// already settled — same benign race as a late TSFN frame).
//
//export goivm_stream_credit
func goivm_stream_credit(reqID C.double, n C.int32_t) {
	abiMu.Lock()
	h := abiHst
	abiMu.Unlock()
	if h == nil {
		return
	}
	h.server.streamGates.grant(float64(reqID), int64(n))
}

// goivm_stream_cancel cancels the pull gate of the in-flight pullMode RPC
// identified by reqID — the JS iterator's .return()/.throw() crossing the
// boundary (ABI v3, D4). The parked producer unparks, the engine breaks
// its fetch range (operator chain unwinds, cursor closes, pool reader
// returns), and the RPC settles with a terminal error frame. Same direct-
// call constraints as goivm_stream_credit; idempotent; unknown reqID is a
// silent no-op.
//
//export goivm_stream_cancel
func goivm_stream_cancel(reqID C.double) {
	abiMu.Lock()
	h := abiHst
	abiMu.Unlock()
	if h == nil {
		return
	}
	h.server.streamGates.cancel(float64(reqID))
}

// goivm_queue_drained signals that the addon's TSFN queue drained below its
// low-water mark (ABI v5) — the event-driven wakeup for producers parked on
// a full queue (rowplane.go parkSlice / abi.go deliverPumpFrame). Replaces
// v4's sleep-poll, whose up-to-5ms dead air per park was the measured
// latency tax.
//
// Called DIRECTLY on the JS thread (dlsym'd, no TSFN round-trip), from
// call_js_deliver's drain accounting — same constraints as
// goivm_stream_credit: the whole path is a leaf-mutex channel close —
// O(1), allocation-light, never blocks, never touches N-API. Broadcasting
// with no parked producers is a harmless no-op.
//
//export goivm_queue_drained
func goivm_queue_drained() {
	tsfnDrain.broadcast()
}
