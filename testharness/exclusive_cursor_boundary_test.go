package testharness

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

// TestExclusiveCursorBoundaryHydration asserts the ABSOLUTE-correct result
// (not just TS-vs-Go parity, since TS is also wrong here): an exclusive cursor
// at the newest row must yield an EMPTY page. Soak observed Go=2 / TS=1 / SQL=0.
func TestExclusiveCursorBoundaryHydration(t *testing.T) {
	projectRoot := findProjectRoot(t)
	f := filepath.Join(projectRoot, "go-ivm", "testharness", "testcase_exclusive_cursor_boundary.json")
	data, err := os.ReadFile(f)
	if err != nil {
		t.Fatal(err)
	}
	out, err := RunTestCaseJSON(data)
	if err != nil {
		t.Fatal(err)
	}
	var res HarnessResult
	if err := json.Unmarshal(out, &res); err != nil {
		t.Fatal(err)
	}
	if len(res.Hydration) != 0 {
		ids := make([]any, len(res.Hydration))
		for i, n := range res.Hydration {
			ids[i] = n.Row["conversationId"]
		}
		t.Errorf("exclusive-cursor hydration: got %d rows, want 0 (cursor is newest row); rows=%v",
			len(res.Hydration), ids)
	}
}

// TestExclusiveCursorBoundaryAdvance reconstructs Go's accumulated view state
// across a churn sequence and asserts ABSOLUTE correctness: after inserting a
// tie-at-boundary row (must be excluded by exclusive), a below-cursor row (must
// be excluded), and a strictly-after row (must appear), the only row in the
// page is c-400.
func TestExclusiveCursorBoundaryAdvance(t *testing.T) {
	projectRoot := findProjectRoot(t)
	f := filepath.Join(projectRoot, "go-ivm", "testharness", "testcase_exclusive_cursor_advance.json")
	data, err := os.ReadFile(f)
	if err != nil {
		t.Fatal(err)
	}
	out, err := RunTestCaseJSON(data)
	if err != nil {
		t.Fatal(err)
	}
	var res HarnessResult
	if err := json.Unmarshal(out, &res); err != nil {
		t.Fatal(err)
	}

	// Reconstruct the materialized view from hydration + per-step changes.
	view := map[string]bool{}
	for _, n := range res.Hydration {
		view[asStr(n.Row["conversationId"])] = true
	}
	for si, step := range res.Steps {
		for _, ch := range step {
			switch ch.Type {
			case "add":
				if ch.Node != nil {
					view[asStr(ch.Node.Row["conversationId"])] = true
				}
			case "remove":
				if ch.Node != nil {
					delete(view, asStr(ch.Node.Row["conversationId"]))
				}
			case "edit":
				// identity preserved (same pk); ignore for membership
			case "child":
				// scalar EXISTS child — no parent membership change here
			}
			_ = si
		}
	}

	got := make([]string, 0, len(view))
	for k := range view {
		got = append(got, k)
	}
	if len(got) != 1 || !view["c-400"] {
		t.Errorf("exclusive-cursor advance final view: got %v, want [c-400] only "+
			"(tie-at-boundary c-300b and below-cursor c-250 must be excluded)", got)
	}
}

func asStr(v any) string {
	if s, ok := v.(string); ok {
		return s
	}
	return ""
}
