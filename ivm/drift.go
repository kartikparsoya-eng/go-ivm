package ivm

import "fmt"

// DriftError signals that MemorySource's in-memory state diverged from the
// source-of-truth (SQLite via TS) — typically because the sidecar dropped
// an advance during a reconnect and a later Edit/Remove arrived for a row
// the MemorySource never received the Add for.
//
// Self-heal contract:
// MemorySource.genPush panics with *DriftError BEFORE mutating any state.
// engine.Advance / engine.AdvanceStream recover the panic, drop the
// in-flight advance entirely, and surface the drift up to the sidecar's
// RPC layer. The sidecar returns a structured error to TS; TS treats it
// like a sidecar restart — re-runs init + loadRows from the current
// SQLite snapshot + re-registers queries — and returns empty changes to
// its caller (clients miss exactly one delta from the IVM stream, then
// resume against the freshly-synced state).
//
// Recoverability invariants enforced by the call site:
//   - The panic is raised in genPush BEFORE pushEpoch++, overlay.Store,
//     or writeChange — so MemorySource state is unchanged at panic time.
//   - No connection.Output has fired yet — downstream operators have
//     observed nothing from this Push.
//   - Therefore the engine can drop the streamer's accumulated changes
//     without leaving dangling references to half-applied state.
type DriftError struct {
	Table    string           `json:"table"`    // source table whose row was missing
	Op       string           `json:"op"`       // "Add" (duplicate), "Edit", or "Remove"
	PK       map[string]Value `json:"pk"`       // primary-key columns of the offending row
	HasCount int              `json:"hasCount"` // current source row count (for log diagnostics)
}

func (e *DriftError) Error() string {
	return fmt.Sprintf(
		"source drift: table=%s op=%s pk=%v has_count=%d",
		e.Table, e.Op, e.PK, e.HasCount,
	)
}
