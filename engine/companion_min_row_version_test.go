package engine

import (
	"testing"

	"github.com/kartikparsoya-eng/go-ivm/ivm"
)

// F3 from the streaming/advance audit: TS's minRowVersion bump lives INSIDE
// #streamNodes (pipeline-driver.ts:2843-2850). Hydrate main-pipeline rows go
// through it (hydrateInternal → Streamer → #streamChanges → #streamNodes,
// pipeline-driver.ts:2981-2994) and are bumped; hydrate COMPANION rows are
// yielded directly, bypassing #streamNodes entirely
// (pipeline-driver.ts:1661-1670) — so TS ships them with their stored
// _0_version UNbumped. Go's old engine-level bumpRowVersions wrapper around
// the whole hydrateEntry result (main + companions) bumped companion rows
// too — a divergent `_0_version` on the wire in the post-RESET window.
// (Advance-path companion changes DO flow through the TS Streamer,
// pipeline-driver.ts:1729 → 2761-2814, so the advance-path bump is correct
// and unchanged.)

func newScalarMinRowVersionEngine(t *testing.T) *Engine {
	t.Helper()
	users := ivm.NewMemorySource("users",
		map[string]string{"id": "string", "name": "string", "_0_version": "string"},
		[]string{"id"})
	// Companion row: stored _0_version "0a", BELOW min "0e".
	users.BulkInsert([]ivm.Row{{"id": "u1", "name": "Alice", zeroVersionColumn: "0a"}})
	issues := ivm.NewMemorySource("issues",
		map[string]string{"id": "string", "ownerId": "string", "_0_version": "string"},
		[]string{"id"})
	// Main row: stored _0_version "0a", BELOW min "0e".
	issues.BulkInsert([]ivm.Row{{"id": "i1", "ownerId": "Alice", zeroVersionColumn: "0a"}})

	eng, err := NewEngine(EngineConfig{StoragePath: tempStoragePath(t)})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { eng.Close() })
	eng.RegisterMemorySource(users)
	eng.RegisterMemorySource(issues)
	eng.SetTableUniqueKeys("users", [][]string{{"id"}})
	eng.SetMinRowVersions(map[string]string{"users": "0e", "issues": "0e"})
	return eng
}

func assertCompanionSkipsBump(t *testing.T, changes []RowChange) {
	t.Helper()
	var sawIssue, sawUser bool
	for _, c := range changes {
		switch c.Table {
		case "issues":
			sawIssue = true
			// Main-pipeline row: bumped, exactly as TS #streamNodes does
			// (pipeline-driver.ts:2843-2850).
			if got := c.Row[zeroVersionColumn]; got != "0e" {
				t.Fatalf("main issues row must be bumped to 0e (TS #streamNodes); got %v", got)
			}
		case "users":
			sawUser = true
			// Companion hydrate row: TS yields it directly, BYPASSING the
			// #streamNodes bump (pipeline-driver.ts:1661-1670) — stored
			// version ships unbumped.
			if got := c.Row[zeroVersionColumn]; got != "0a" {
				t.Fatalf("companion users row must ship UNbumped 0a (TS bypasses #streamNodes for companions, pipeline-driver.ts:1661-1670); got %v", got)
			}
		}
	}
	if !sawIssue || !sawUser {
		t.Fatalf("expected both a main issues row and a companion users row; issues=%v users=%v", sawIssue, sawUser)
	}
}

// TestHydrate_CompanionRowsSkipMinRowVersionBump covers the AddQuery hydrate
// path (hydrateEntry).
func TestHydrate_CompanionRowsSkipMinRowVersionBump(t *testing.T) {
	eng := newScalarMinRowVersionEngine(t)
	changes, _, err := eng.AddQuery("q", scalarUsersIssuesAST())
	if err != nil {
		t.Fatal(err)
	}
	assertCompanionSkipsBump(t, changes)
}

// TestHydratePull_CompanionRowsSkipMinRowVersionBump covers the production
// pull-hydrate path (AddQueriesStreamPull), whose per-chunk flush used to
// re-apply the bump to companion rows.
func TestHydratePull_CompanionRowsSkipMinRowVersionBump(t *testing.T) {
	eng := newScalarMinRowVersionEngine(t)
	var all []RowChange
	err := eng.AddQueriesStreamPull(
		[]QuerySpec{{QueryID: "q", AST: scalarUsersIssuesAST()}},
		0,
		func(r QueryResult) bool {
			all = append(all, r.Changes...)
			return true
		})
	if err != nil {
		t.Fatal(err)
	}
	assertCompanionSkipsBump(t, all)
}
