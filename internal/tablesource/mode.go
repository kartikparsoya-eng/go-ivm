// Package tablesource is the read-only TableSource port from TS.
//
// The sidecar reads GO_IVM_SOURCE_MODE at startup. "table" selects the
// SQLite-backed Source (this package), which reads the replica directly;
// any other value falls back to the in-memory MemorySource. Both modes
// are fully wired in cmd/sidecar.
//
// See DESIGN-tablesource-port.md for the original phased plan.
package tablesource

import "os"

// Mode selects which leaf Source the engine uses for replicated tables.
type Mode int

const (
	// ModeMemory uses the in-memory MemorySource populated via loadRows RPC.
	// Default and current production behavior.
	ModeMemory Mode = iota

	// ModeTable uses a SQLite-backed tablesource.Source reading the replica
	// directly, constructed per (cg, table) in cmd/sidecar.
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
