package main

// streamgate.go — the pull-hydration demand gate (ABI v3, DESIGN-duplex-streaming §4.2/§4.3/§4.7).
//
// One streamGate per pull-mode addQueriesStream RPC. The rowPlane's onResult
// path acquires ONE credit before each row-BEARING delivery (kind-3 records
// and fallback kind-1 frames that carry rows); group defs, the terminal Final
// frame, and error frames ride free — gating any of those deadlocks, because
// the client is waiting for exactly that frame before deciding whether to
// grant more credit.
//
// Credits arrive from the JS thread via the goivm_stream_credit export as
// DIRECT calls (no TSFN round-trip) — grant/cancel must therefore be O(1),
// allocation-free, and must never block beyond a leaf mutex. That is also
// why the registry has its OWN mutex and never touches s.mu or group.mu:
// coupling a JS-thread direct call to server-wide locks would let a slow
// server operation stall the JS event loop.
//
// reqID keying: float64, NOT uint64. The row plane already keys everything
// by the f64 reqID (numericReqID, rowrecord.go:125; JS reads it back with
// readDoubleLE) — the credit call passes the same JS number through the C
// `double` type, giving bit-exact map lookups with zero conversion. NaN
// reqIDs are refused at gate creation (a NaN map key can never be looked
// up again, so grant/cancel could never reach it).
//
// Lifecycle: registered by handleAddQueriesStream when the request opts
// into pullMode (and the row plane engaged); unregistered on handler return
// (defer). grant/cancel on an unknown reqID are silent no-ops — the stream
// already settled; same benign race as late TSFN frames.

import (
	"math"
	"sync"
	"time"
)

// streamGate is the per-RPC credit gate. A producer (hydrate goroutine)
// parks in acquire() while credit==0; the JS consumer tops credit up via
// grant() as it drains rows, and cancel() (client .return()/.throw(),
// group teardown, or the idle sweeper) unparks every producer with false.
type streamGate struct {
	mu        sync.Mutex
	cond      *sync.Cond
	credit    int64
	cancelled bool
	// lastGrant is the creation/last-grant instant; the idle sweeper
	// auto-cancels a gate whose producers have been parked with no grant
	// for longer than the pull idle timeout (bounds the WAL-frame pin and
	// the group.mu hold — D7). Guarded by mu.
	lastGrant time.Time
	// waiters counts producers currently parked in acquire. The sweeper
	// only fires on gates that actually have someone parked: a stream
	// with zero credit but no one asking for it (e.g. mid SQLite scan)
	// is not idle-parked. Guarded by mu.
	waiters int
	// touch, when non-nil, is invoked on every successful grant to bump
	// the owning client group's liveness clock (lastUsedNs) so the idle-
	// group reaper never collects a CG whose client is actively pulling.
	// Called WITHOUT gate.mu held (it's an atomic store on the group).
	touch func()
}

func newStreamGate(touch func()) *streamGate {
	g := &streamGate{lastGrant: time.Now(), touch: touch}
	g.cond = sync.NewCond(&g.mu)
	return g
}

// acquire consumes one credit, parking until credit is available or the
// gate is cancelled. Returns false on cancellation — the producer must
// stop producing and unwind (break the fetch range).
func (g *streamGate) acquire() bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	for g.credit == 0 && !g.cancelled {
		g.waiters++
		g.cond.Wait()
		g.waiters--
	}
	if g.cancelled {
		return false
	}
	g.credit--
	return true
}

// tryAcquire consumes one credit WITHOUT parking. Returns true when a
// credit was consumed; false when the gate has no credit (the caller is
// about to park — see acquirePullCredit's flush-before-park rule) or is
// cancelled (the fallback acquire resolves that case with the proper
// unwind). Same single-mutex cost as a credit-available acquire, so the
// credit-in-hand fast path pays nothing new.
func (g *streamGate) tryAcquire() bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.cancelled || g.credit == 0 {
		return false
	}
	g.credit--
	return true
}

// grant adds n credits and wakes parked producers. n <= 0 is ignored.
// Granting a cancelled gate is a no-op (the producers already unwound).
func (g *streamGate) grant(n int64) {
	if n <= 0 {
		return
	}
	g.mu.Lock()
	if g.cancelled {
		g.mu.Unlock()
		return
	}
	g.credit += n
	g.lastGrant = time.Now()
	g.cond.Broadcast()
	touch := g.touch
	g.mu.Unlock()
	if touch != nil {
		touch()
	}
}

// cancel marks the gate cancelled and unparks every producer. Idempotent.
func (g *streamGate) cancel() {
	g.mu.Lock()
	g.cancelled = true
	g.cond.Broadcast()
	g.mu.Unlock()
}

// isCancelled reports the gate's cancelled flag. The row plane polls it
// between deliver-retry attempts (rowplane.go retryDeliver): a producer
// parked on a FULL TSFN queue is not a gate waiter, so cancel's cond
// broadcast cannot reach it — the poll is what carries the client's
// .return()/.throw()/timeout across that gap.
func (g *streamGate) isCancelled() bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.cancelled
}

// idleParked reports whether at least one producer has been parked with no
// grant for longer than idle. Used by the sweeper (D7): parked-past-timeout
// gates are auto-cancelled — same unwind as a client cancel; the client
// sees a terminal error frame and re-hydrates.
func (g *streamGate) idleParked(now time.Time, idle time.Duration) bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.waiters > 0 && !g.cancelled && now.Sub(g.lastGrant) > idle
}

// gateEntry pairs a gate with its owning client group so teardown can
// cancel a whole group's pulls (shutdownGroup broadcasts BEFORE taking
// group.mu — a parked producer holds group.mu via its RPC handler, so
// cancelling first is what lets teardown make progress). owner is compared
// by interface identity (the sidecar passes the *ClientGroup pointer):
// pointer identity — not the cgID string — so tearing down a DESTROYED
// generation of a cgID can never cancel pulls belonging to a freshly
// re-created group with the same id.
type gateEntry struct {
	gate  *streamGate
	owner any
}

// streamGateRegistry maps in-flight pull-RPC reqIDs to their gates.
// Leaf lock: never taken together with s.mu, group.mu, or gate.mu.
type streamGateRegistry struct {
	mu    sync.Mutex
	gates map[float64]*gateEntry
}

// register creates and registers a gate for reqID with `initial` opening
// credits. Returns nil (pull refused) for NaN reqIDs — see the file header.
// Re-registering a live reqID returns nil too: reqID collisions mean a
// client bug; refusing the gate degrades that RPC to ungated (correct, just
// not pull-bounded) instead of cross-wiring two RPCs' credits.
//
// The opening credit rides REGISTRATION (not a first grant call) because a
// grant that races ahead of the handler is a silent no-op — the client's
// opening window would vanish and the producer would park until the idle
// sweep. Carrying it in the request (params.pullWindow → initial) makes
// gate-exists ⇒ window-armed atomic; every later grant is a top-up
// triggered by a delivery, which itself proves the gate existed.
func (r *streamGateRegistry) register(reqID float64, owner any, initial int64, touch func()) *streamGate {
	if math.IsNaN(reqID) {
		return nil
	}
	g := newStreamGate(touch)
	if initial > 0 {
		g.credit = initial
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.gates == nil {
		r.gates = make(map[float64]*gateEntry)
	}
	if _, exists := r.gates[reqID]; exists {
		return nil
	}
	r.gates[reqID] = &gateEntry{gate: g, owner: owner}
	return g
}

// unregister removes reqID's gate (handler-return defer). The gate is
// cancelled as it leaves the registry so any producer STILL parked on it
// (e.g. the handler is unwinding on error while a lane is parked) is
// released rather than leaked.
func (r *streamGateRegistry) unregister(reqID float64) {
	r.mu.Lock()
	e := r.gates[reqID]
	delete(r.gates, reqID)
	r.mu.Unlock()
	if e != nil {
		e.gate.cancel()
	}
}

// grant tops up reqID's gate; unknown reqID is a silent no-op.
func (r *streamGateRegistry) grant(reqID float64, n int64) {
	r.mu.Lock()
	e := r.gates[reqID]
	r.mu.Unlock()
	if e != nil {
		e.gate.grant(n)
	}
}

// cancel cancels reqID's gate; unknown reqID is a silent no-op. The entry
// stays registered until the handler's unregister — cancel only flips the
// gate so producers unwind; ownership of the map entry is the handler's.
func (r *streamGateRegistry) cancel(reqID float64) {
	r.mu.Lock()
	e := r.gates[reqID]
	r.mu.Unlock()
	if e != nil {
		e.gate.cancel()
	}
}

// cancelOwner cancels every gate belonging to owner (group teardown /
// engine close; identity comparison — see gateEntry.owner). Entries stay
// registered — their handlers unwind and unregister themselves.
func (r *streamGateRegistry) cancelOwner(owner any) {
	r.mu.Lock()
	var toCancel []*streamGate
	for _, e := range r.gates {
		if e.owner == owner {
			toCancel = append(toCancel, e.gate)
		}
	}
	r.mu.Unlock()
	for _, g := range toCancel {
		g.cancel()
	}
}

// cancelAll cancels every registered gate (host shutdown).
func (r *streamGateRegistry) cancelAll() {
	r.mu.Lock()
	toCancel := make([]*streamGate, 0, len(r.gates))
	for _, e := range r.gates {
		toCancel = append(toCancel, e.gate)
	}
	r.mu.Unlock()
	for _, g := range toCancel {
		g.cancel()
	}
}

// sweepIdle auto-cancels every gate whose producers have been parked with
// no grant for longer than idle (D7). Returns the number cancelled.
func (r *streamGateRegistry) sweepIdle(now time.Time, idle time.Duration) int {
	r.mu.Lock()
	var toCancel []*streamGate
	for _, e := range r.gates {
		if e.gate.idleParked(now, idle) {
			toCancel = append(toCancel, e.gate)
		}
	}
	r.mu.Unlock()
	for _, g := range toCancel {
		g.cancel()
	}
	return len(toCancel)
}

// size reports the number of registered gates (tests + PERF reporter).
func (r *streamGateRegistry) size() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.gates)
}
