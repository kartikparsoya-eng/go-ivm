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
//	typedef void (*goivm_deliver_cb)(void* ctx, int32_t kind,
//	                                 const void* data, int32_t len);
//	int32_t goivm_start(goivm_deliver_cb cb, void* ctx);
//	int32_t goivm_send(const void* data, int32_t len);
//	void    goivm_shutdown(void);
//	int32_t goivm_abi_version(void);
//	void    goivm_stream_credit(double req_id, int32_t n);   // ABI v3
//	void    goivm_stream_cancel(double req_id);              // ABI v3
//
// Threading & memory contract:
//   - The deliver callback is invoked from Go-runtime goroutines (NOT the
//     JS thread). It may block — blocking propagates backpressure into the
//     engine exactly like a slow socket. It must NOT call back into
//     goivm_send (deadlock risk via the pipe backpressure chain).
//   - (data,len) passed to the callback are valid ONLY for the duration of
//     the call; the receiver must copy before returning. This satisfies the
//     cgo pointer rules: the Go-owned buffer is never retained by C.
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

typedef void (*goivm_deliver_cb)(void* ctx, int32_t kind, const void* data, int32_t len);

// cgo cannot call a C function pointer directly; this trampoline does.
static void goivm_call_deliver(goivm_deliver_cb cb, void* ctx, int32_t kind, const void* data, int32_t len) {
	cb(ctx, kind, data, len);
}
*/
import "C"

import (
	"fmt"
	"os"
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
const goivmABIVersion = 3

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

	tuneRuntime()

	deliver := func(kind int32, payload []byte) {
		// Pass the Go slice's base pointer into C for the DURATION OF THE
		// CALL only — legal under the cgo pointer rules; the addon copies
		// into its TSFN queue entry before returning. Empty payloads pass
		// a nil pointer with len 0.
		var p unsafe.Pointer
		if len(payload) > 0 {
			p = unsafe.Pointer(&payload[0])
		}
		C.goivm_call_deliver(abiCB, abiCtx, C.int32_t(kind), p, C.int32_t(len(payload)))
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
