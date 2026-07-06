package main

// non-default path markers: each config-reachable non-default path emits
// ONE operator-facing stderr line on first use per process. These paths are
// NOT scheduled for removal — they are deliberate rollback/experiment knobs
// (eager advance, serial fanout, Phase-2 lazy hydrate) — but a production
// deployment on the default path must show ZERO of these lines. Anything
// outside the default path is outside the TS-faithfulness review surface;
// this marker makes that boundary greppable in deployment logs
// (see PROD-PATH.md).
//
// The Phase-0 tripwires (loadRows, advanceToHead unary, memory-mode init,
// socket accept) were removed in the removal sweep — all their call sites
// are gone. This file now hosts only nonDefault.

import (
	"fmt"
	"os"
	"sync"
)

// tripwireOnces holds one sync.Once per site. sync.Map keeps the hot path
// (post-first-hit) to a lock-free load; sites are a small fixed set.
var tripwireOnces sync.Map

// nonDefault logs the first use of a CONFIG-REACHABLE non-default path, once
// per site per process. Deliberately stderr (not the log framework): it must
// survive any log-level configuration and land in container logs unconditionally.
func nonDefault(site string) {
	v, _ := tripwireOnces.LoadOrStore("nd:"+site, &sync.Once{})
	v.(*sync.Once).Do(func() {
		fmt.Fprintf(os.Stderr,
			"[GO-IVM][NON-DEFAULT] %s engaged — this deployment is off the default (prod) path; see PROD-PATH.md\n",
			site)
	})
}
