package main

// Phase-0 tripwires for the RPC-surface cleanup: each suspected-dead path
// emits ONE operator-facing stderr line on first use per process. Grepping
// deployment logs for "[GO-IVM][TRIPWIRE]" over a soak window converts
// "believed dead by code inspection" into deployment evidence before the
// handler is deleted — especially for topologies the repo cannot see
// (externally-managed socket sharing, out-of-repo tooling speaking the RPC
// protocol directly).
//
// Sites, and why each is suspect:
//
//	rpc loadRows (memory-mode seeding) — production is table mode: the
//	    replica is authoritative and TS ships no rows. Deleting memory
//	    mode is gated on this never firing outside tests.
//	rpc loadRows (table-mode no-op) — current TS skips loadRows entirely
//	    when a table ships zero rows (always, in table mode). A hit means
//	    an OLD zero-cache generation is still paired with this sidecar.
//	init memory-mode source — the handleInit else-branch that builds a
//	    loadRows-backed MemorySource. Production images bake
//	    GO_IVM_SOURCE_MODE=table; test fixtures are the only known users.
//	rpc advanceToHead (unary) — shadow-only caller set (drive-shadow +
//	    the P1 go-derived-diff audit). The serving path is
//	    advanceToHeadStream. Removal is bundled with shadow retirement.
//	socket transport accept — the config default is transport=napi
//	    (in-process). A hit means a socket deployment (or the
//	    externally-managed shared-sidecar topology) still exists — the
//	    one thing code inspection cannot rule out.
//
// Expected steady state in a production napi deployment: ZERO tripwire
// lines. Tests DO trip some sites (memory-mode fixtures) — one stderr line
// per `go test` process is accepted noise; the grep target is deployment
// logs, not CI output.

import (
	"fmt"
	"os"
	"sync"
)

// tripwireOnces holds one sync.Once per site. sync.Map keeps the hot path
// (post-first-hit) to a lock-free load; sites are a small fixed set.
var tripwireOnces sync.Map

// tripwire logs the first use of a suspected-dead path, once per site per
// process. Deliberately stderr (not the log framework): it must survive any
// log-level configuration and land in container logs unconditionally.
func tripwire(site string) {
	v, _ := tripwireOnces.LoadOrStore(site, &sync.Once{})
	v.(*sync.Once).Do(func() {
		fmt.Fprintf(os.Stderr,
			"[GO-IVM][TRIPWIRE] %s used — path is scheduled for removal; report this hit (RPC-surface cleanup plan)\n",
			site)
	})
}
