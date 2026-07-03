package main

// tuneRuntime memory-limit regressions (scale review): a malformed
// GO_IVM_GOMEMLIMIT used to hit an unconditional `return` — silently
// skipping EVERY fallback (GOMEMLIMIT passthrough, cgroup-percent default,
// absolute default) — so one typo ran the engine with no memory ceiling at
// GOGC=200 (heap balloons to 3x live data; container OOM). It now warns
// and falls through; and byte sizes accept the Go runtime's own binary
// suffixes since operators habitually write "4GiB".

import (
	"math"
	"runtime/debug"
	"testing"
)

func TestParseByteSize(t *testing.T) {
	for _, tc := range []struct {
		in   string
		want int64
		ok   bool
	}{
		{"1073741824", 1 << 30, true},
		{"0", 0, true},
		{"512B", 512, true},
		{"4KiB", 4 << 10, true},
		{"16MiB", 16 << 20, true},
		{"4GiB", 4 << 30, true},
		{"1TiB", 1 << 40, true},
		{"", 0, false},
		{"-1", 0, false},
		{"4G", 0, false},   // Go runtime syntax requires the iB form
		{"4gib", 0, false}, // case-sensitive like the runtime
		{"abc", 0, false},
		{"12x34", 0, false},
		{"9999999999999999999TiB", 0, false}, // overflow
	} {
		got, ok := parseByteSize(tc.in)
		if ok != tc.ok || got != tc.want {
			t.Errorf("parseByteSize(%q) = (%d,%v), want (%d,%v)", tc.in, got, ok, tc.want, tc.ok)
		}
	}
}

func TestTuneRuntime_MalformedMemLimitFallsBack(t *testing.T) {
	prevLimit := debug.SetMemoryLimit(-1)
	prevGC := debug.SetGCPercent(100)
	defer func() {
		debug.SetMemoryLimit(prevLimit)
		debug.SetGCPercent(prevGC)
	}()
	debug.SetMemoryLimit(math.MaxInt64) // "no ceiling" baseline

	t.Setenv("GO_IVM_GOMEMLIMIT", "4G!garbage")
	t.Setenv("GOMEMLIMIT", "")
	t.Setenv("GO_IVM_GOGC", "200")
	t.Setenv("GO_IVM_GOMEMLIMIT_PERCENT", "")

	tuneRuntime()

	if got := debug.SetMemoryLimit(-1); got == math.MaxInt64 {
		t.Fatal("malformed GO_IVM_GOMEMLIMIT left the engine with NO memory ceiling — the fallback chain was skipped")
	}
}

func TestTuneRuntime_SuffixedMemLimitApplies(t *testing.T) {
	prevLimit := debug.SetMemoryLimit(-1)
	prevGC := debug.SetGCPercent(100)
	defer func() {
		debug.SetMemoryLimit(prevLimit)
		debug.SetGCPercent(prevGC)
	}()

	t.Setenv("GO_IVM_GOMEMLIMIT", "2GiB")
	t.Setenv("GOMEMLIMIT", "")
	t.Setenv("GO_IVM_GOGC", "200")

	tuneRuntime()

	if got := debug.SetMemoryLimit(-1); got != int64(2)<<30 {
		t.Fatalf("GO_IVM_GOMEMLIMIT=2GiB applied %d bytes, want %d", got, int64(2)<<30)
	}
}
