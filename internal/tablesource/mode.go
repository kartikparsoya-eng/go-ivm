// Package tablesource is the read-only TableSource port from TS.
//
// PHASE 0 (this commit): package scaffolding + mode flag only. The sidecar
// reads GO_IVM_SOURCE_MODE at startup; "table" is recognized but not yet
// implemented and falls back to memory with a clear log line. No behavior
// change for any caller.
//
// See DESIGN-tablesource-port.md for the phased plan.
package tablesource

import "os"

// Mode selects which leaf Source the engine uses for replicated tables.
type Mode int

const (
	// ModeMemory uses the in-memory MemorySource populated via loadRows RPC.
	// Default and current production behavior.
	ModeMemory Mode = iota

	// ModeTable will (Phase 3+) use a SQLite-backed TableSource reading the
	// replica directly. Recognized today but not yet wired.
	ModeTable
)

func (m Mode) String() string {
	switch m {
	case ModeTable:
		return "table"
	default:
		return "memory"
	}
}

// ParseMode reads GO_IVM_SOURCE_MODE from the environment. Unknown / empty
// values map to ModeMemory so misconfiguration cannot accidentally activate
// an unfinished code path.
func ParseMode() Mode {
	switch os.Getenv("GO_IVM_SOURCE_MODE") {
	case "table":
		return ModeTable
	default:
		return ModeMemory
	}
}
