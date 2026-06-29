package testharness

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

// TestDifferentialParity runs the same test case through both TS and Go IVM
// and compares their outputs.
func TestDifferentialParity(t *testing.T) {
	// Find project root (contains mono/ and go-ivm/)
	projectRoot := findProjectRoot(t)
	tsHarness := filepath.Join(projectRoot, "go-ivm", "testharness", "ts-harness.ts")

	// Load test case
	testCaseFile := filepath.Join(projectRoot, "go-ivm", "testharness", "testcase1.json")
	testCaseData, err := os.ReadFile(testCaseFile)
	if err != nil {
		t.Fatal(err)
	}

	// Run Go harness
	goOutput, err := RunTestCaseJSON(testCaseData)
	if err != nil {
		t.Fatalf("Go harness failed: %v", err)
	}

	// Run TS harness
	cmd := exec.Command("tsx", tsHarness)
	cmd.Dir = projectRoot
	cmd.Stdin = bytes.NewReader(testCaseData)
	var tsStdout, tsStderr bytes.Buffer
	cmd.Stdout = &tsStdout
	cmd.Stderr = &tsStderr
	if err := cmd.Run(); err != nil {
		t.Fatalf("TS harness failed: %v\nstderr: %s", err, tsStderr.String())
	}
	tsOutput := tsStdout.Bytes()

	// Normalize both outputs for comparison (re-marshal to canonical JSON)
	goNorm := normalizeJSON(t, goOutput)
	tsNorm := normalizeJSON(t, tsOutput)

	if goNorm != tsNorm {
		t.Errorf("PARITY MISMATCH!\n\nTS output:\n%s\n\nGo output:\n%s", tsOutput, goOutput)
	}
}

// TestDifferentialFilter tests with a WHERE clause.
func TestDifferentialFilter(t *testing.T) {
	testCase := `{
		"schema": {
			"items": {
				"columns": {
					"id": {"type": "string"},
					"name": {"type": "string"},
					"price": {"type": "number"}
				},
				"primaryKey": ["id"]
			}
		},
		"initialData": {
			"items": [
				{"id": "1", "name": "Apple", "price": 1.5},
				{"id": "2", "name": "Banana", "price": 0.5},
				{"id": "3", "name": "Cherry", "price": 3.0}
			]
		},
		"ast": {
			"table": "items",
			"orderBy": [["name", "asc"], ["id", "asc"]],
			"where": {
				"type": "simple",
				"left": {"type": "column", "name": "price"},
				"op": ">",
				"right": {"type": "literal", "value": 1.0}
			}
		},
		"mutations": [
			{"type": "add", "table": "items", "row": {"id": "4", "name": "Date", "price": 5.0}},
			{"type": "add", "table": "items", "row": {"id": "5", "name": "Elderberry", "price": 0.3}},
			{"type": "remove", "table": "items", "row": {"id": "3", "name": "Cherry", "price": 3.0}},
			{"type": "edit", "table": "items", "row": {"id": "2", "name": "Banana", "price": 2.0}, "oldRow": {"id": "2", "name": "Banana", "price": 0.5}}
		]
	}`

	runDifferential(t, []byte(testCase))
}

func runDifferential(t *testing.T, testCaseData []byte) {
	t.Helper()
	projectRoot := findProjectRoot(t)
	tsHarness := filepath.Join(projectRoot, "go-ivm", "testharness", "ts-harness.ts")

	// Go
	goOutput, err := RunTestCaseJSON(testCaseData)
	if err != nil {
		t.Fatalf("Go harness failed: %v", err)
	}

	// TS
	cmd := exec.Command("tsx", tsHarness)
	cmd.Dir = projectRoot
	cmd.Stdin = bytes.NewReader(testCaseData)
	var tsStdout, tsStderr bytes.Buffer
	cmd.Stdout = &tsStdout
	cmd.Stderr = &tsStderr
	if err := cmd.Run(); err != nil {
		t.Fatalf("TS harness failed: %v\nstderr: %s", err, tsStderr.String())
	}

	goNorm := normalizeJSON(t, goOutput)
	tsNorm := normalizeJSON(t, tsStdout.Bytes())

	if goNorm != tsNorm {
		t.Errorf("PARITY MISMATCH!\n\nTS output:\n%s\n\nGo output:\n%s", tsStdout.String(), string(goOutput))
	}
}

func findProjectRoot(t *testing.T) string {
	t.Helper()
	// Walk up from current working dir to find mono/ and go-ivm/
	dir, _ := os.Getwd()
	for {
		if _, err := os.Stat(filepath.Join(dir, "mono")); err == nil {
			if _, err := os.Stat(filepath.Join(dir, "go-ivm")); err == nil {
				return dir
			}
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatal("cannot find project root (directory with both mono/ and go-ivm/)")
		}
		dir = parent
	}
}

func normalizeJSON(t *testing.T, data []byte) string {
	t.Helper()
	var v interface{}
	if err := json.Unmarshal(data, &v); err != nil {
		t.Fatalf("invalid JSON: %v\ndata: %s", err, string(data))
	}
	out, _ := json.Marshal(v)
	return string(out)
}

// TestFuzzDifferential runs N random test cases through both TS and Go IVM.
func TestFuzzDifferential(t *testing.T) {
	numCases := 20
	seeds := []int64{42, 123, 456, 789, 1001, 2002, 3003, 4004, 5005, 6006,
		7007, 8008, 9009, 1010, 2020, 3030, 4040, 5050, 6060, 7070}

	for i := 0; i < numCases; i++ {
		seed := seeds[i%len(seeds)]
		t.Run(fmt.Sprintf("seed_%d", seed), func(t *testing.T) {
			fuzz := NewFuzzer(seed)
			opts := DefaultFuzzOpts()
			if i%2 == 0 {
				opts.WithFilter = true
			}
			testCaseData := fuzz.GenerateTestCase(opts)
			runDifferential(t, testCaseData)
		})
	}
}

// TestFuzzDifferentialLarge runs larger test cases.
func TestFuzzDifferentialLarge(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping large fuzz test in short mode")
	}

	for seed := int64(0); seed < 50; seed++ {
		t.Run(fmt.Sprintf("seed_%d", seed), func(t *testing.T) {
			fuzz := NewFuzzer(seed)
			opts := FuzzOpts{
				NumTables:    1,
				NumColumns:   4,
				NumRows:      10,
				NumMutations: 20,
				WithFilter:   seed%3 == 0,
			}
			testCaseData := fuzz.GenerateTestCase(opts)
			runDifferential(t, testCaseData)
		})
	}
}

// TestFuzzDifferentialCompound runs queries with AND/OR compound conditions.
func TestFuzzDifferentialCompound(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping compound fuzz test in short mode")
	}

	for seed := int64(100); seed < 130; seed++ {
		t.Run(fmt.Sprintf("seed_%d", seed), func(t *testing.T) {
			fuzz := NewFuzzer(seed)
			opts := FuzzOpts{
				NumTables:    1,
				NumColumns:   4,
				NumRows:      8,
				NumMutations: 15,
				WithCompound: true,
				WithLimit:    seed%2 == 0,
			}
			testCaseData := fuzz.GenerateTestCase(opts)
			runDifferential(t, testCaseData)
		})
	}
}

// TestFuzzDifferentialJoins runs queries with Related subqueries (joins).
func TestFuzzDifferentialJoins(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping join fuzz test in short mode")
	}

	for seed := int64(200); seed < 230; seed++ {
		t.Run(fmt.Sprintf("seed_%d", seed), func(t *testing.T) {
			fuzz := NewFuzzer(seed)
			opts := FuzzOpts{
				NumTables:    2,
				NumColumns:   3,
				NumRows:      5,
				NumMutations: 10,
				WithJoins:    true,
				WithFilter:   seed%3 == 0,
			}
			testCaseData := fuzz.GenerateTestCase(opts)
			runDifferential(t, testCaseData)
		})
	}
}

// TestFuzzDifferentialExists runs queries with EXISTS/NOT EXISTS conditions.
func TestFuzzDifferentialExists(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping exists fuzz test in short mode")
	}

	for seed := int64(300); seed < 330; seed++ {
		t.Run(fmt.Sprintf("seed_%d", seed), func(t *testing.T) {
			fuzz := NewFuzzer(seed)
			opts := FuzzOpts{
				NumTables:    2,
				NumColumns:   3,
				NumRows:      5,
				NumMutations: 10,
				WithExists:   true,
			}
			testCaseData := fuzz.GenerateTestCase(opts)
			runDifferential(t, testCaseData)
		})
	}
}

// TestFuzzDifferentialComplex runs queries combining joins + exists + filters + limits.
func TestFuzzDifferentialComplex(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping complex fuzz test in short mode")
	}

	for seed := int64(400); seed < 430; seed++ {
		t.Run(fmt.Sprintf("seed_%d", seed), func(t *testing.T) {
			fuzz := NewFuzzer(seed)
			opts := FuzzOpts{
				NumTables:    3,
				NumColumns:   4,
				NumRows:      6,
				NumMutations: 12,
				WithJoins:    true,
				WithExists:   seed%2 == 0,
				WithFilter:   seed%3 == 0,
				WithLimit:    seed%4 == 0,
				WithCompound: seed%5 == 0,
			}
			testCaseData := fuzz.GenerateTestCase(opts)
			runDifferential(t, testCaseData)
		})
	}
}

// TestFuzzDifferentialDescSort runs queries with DESC ordering.
func TestFuzzDifferentialDescSort(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping desc sort fuzz test in short mode")
	}

	for seed := int64(500); seed < 530; seed++ {
		t.Run(fmt.Sprintf("seed_%d", seed), func(t *testing.T) {
			fuzz := NewFuzzer(seed)
			opts := FuzzOpts{
				NumTables:    1,
				NumColumns:   4,
				NumRows:      8,
				NumMutations: 15,
				WithFilter:   seed%2 == 0,
				WithLimit:    seed%3 == 0,
				WithDescSort: true,
			}
			testCaseData := fuzz.GenerateTestCase(opts)
			runDifferential(t, testCaseData)
		})
	}
}

// TestFuzzDifferentialMultiSort runs queries with multi-column ordering.
func TestFuzzDifferentialMultiSort(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping multi-sort fuzz test in short mode")
	}

	for seed := int64(600); seed < 630; seed++ {
		t.Run(fmt.Sprintf("seed_%d", seed), func(t *testing.T) {
			fuzz := NewFuzzer(seed)
			opts := FuzzOpts{
				NumTables:     1,
				NumColumns:    5,
				NumRows:       10,
				NumMutations:  15,
				WithMultiSort: true,
				WithDescSort:  seed%2 == 0,
				WithFilter:    seed%3 == 0,
				WithLimit:     seed%4 == 0,
			}
			testCaseData := fuzz.GenerateTestCase(opts)
			runDifferential(t, testCaseData)
		})
	}
}

// TestFuzzDifferentialNested runs queries with deeply nested AND/OR conditions.
func TestFuzzDifferentialNested(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping nested condition fuzz test in short mode")
	}

	for seed := int64(700); seed < 730; seed++ {
		t.Run(fmt.Sprintf("seed_%d", seed), func(t *testing.T) {
			fuzz := NewFuzzer(seed)
			opts := FuzzOpts{
				NumTables:    1,
				NumColumns:   4,
				NumRows:      10,
				NumMutations: 15,
				WithNested:   true,
				WithLimit:    seed%2 == 0,
			}
			testCaseData := fuzz.GenerateTestCase(opts)
			runDifferential(t, testCaseData)
		})
	}
}

// TestFuzzDifferentialINList runs queries with IN-list style conditions (OR of = on same column).
func TestFuzzDifferentialINList(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping IN-list fuzz test in short mode")
	}

	for seed := int64(800); seed < 830; seed++ {
		t.Run(fmt.Sprintf("seed_%d", seed), func(t *testing.T) {
			fuzz := NewFuzzer(seed)
			opts := FuzzOpts{
				NumTables:    1,
				NumColumns:   4,
				NumRows:      8,
				NumMutations: 12,
				WithINList:   true,
				WithLimit:    seed%2 == 0,
			}
			testCaseData := fuzz.GenerateTestCase(opts)
			runDifferential(t, testCaseData)
		})
	}
}

// TestFuzzDifferentialPagination runs queries with cursor-based pagination (Start bound).
func TestFuzzDifferentialPagination(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping pagination fuzz test in short mode")
	}

	for seed := int64(900); seed < 930; seed++ {
		t.Run(fmt.Sprintf("seed_%d", seed), func(t *testing.T) {
			fuzz := NewFuzzer(seed)
			opts := FuzzOpts{
				NumTables:    1,
				NumColumns:   4,
				NumRows:      10,
				NumMutations: 15,
				WithFilter:   seed%3 == 0,
				WithLimit:    true,
				WithStart:    true,
			}
			testCaseData := fuzz.GenerateTestCase(opts)
			runDifferential(t, testCaseData)
		})
	}
}

// TestFuzzDifferentialKitchenSink combines all query features for maximum coverage.
func TestFuzzDifferentialKitchenSink(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping kitchen sink fuzz test in short mode")
	}

	for seed := int64(1000); seed < 1050; seed++ {
		t.Run(fmt.Sprintf("seed_%d", seed), func(t *testing.T) {
			fuzz := NewFuzzer(seed)
			opts := FuzzOpts{
				NumTables:     2,
				NumColumns:    5,
				NumRows:       12,
				NumMutations:  20,
				WithJoins:     seed%2 == 0,
				WithExists:    seed%3 == 0,
				WithCompound:  seed%4 == 0,
				WithNested:    seed%5 == 0,
				WithINList:    seed%6 == 0,
				WithFilter:    seed%7 == 0,
				WithLimit:     seed%2 == 0,
				WithDescSort:  seed%3 == 0,
				WithMultiSort: seed%4 == 0,
				WithStart:     seed%8 == 0,
			}
			testCaseData := fuzz.GenerateTestCase(opts)
			runDifferential(t, testCaseData)
		})
	}
}

// TestFuzzDifferentialHighMutations stress tests with many mutations on a small dataset.
func TestFuzzDifferentialHighMutations(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping high mutation fuzz test in short mode")
	}

	for seed := int64(1100); seed < 1120; seed++ {
		t.Run(fmt.Sprintf("seed_%d", seed), func(t *testing.T) {
			fuzz := NewFuzzer(seed)
			opts := FuzzOpts{
				NumTables:    1,
				NumColumns:   3,
				NumRows:      3,
				NumMutations: 50,
				WithFilter:   true,
				WithLimit:    true,
				WithCompound: seed%2 == 0,
			}
			testCaseData := fuzz.GenerateTestCase(opts)
			runDifferential(t, testCaseData)
		})
	}
}

// TestExistsMembershipFilter reproduces a shadow-mode mismatch observed
// in production (xyne-spaces-zero-external, 2026-05-25). The query
// fetches `conversations` filtered by EXISTS(channel_participants matching
// current user). Symptoms: Go returns conversations from channels the
// user is NOT a participant in; TS correctly filters them out.
//
// Test shape:
//   - 3 channels: ch1, ch2, ch3
//   - 2 participants: user-A in (ch1, ch2). NOT in ch3.
//   - 5 conversations: 2 in ch1, 2 in ch2, 1 in ch3
//   - Query: conversations WHERE EXISTS(channel_participants on channelId
//     AND userId = 'user-A')
//   - Expected (both TS and Go): 4 conversations (from ch1, ch2). The ch3
//     conversation must be filtered out.
func TestExistsMembershipFilter(t *testing.T) {
	projectRoot := findProjectRoot(t)
	testCaseFile := filepath.Join(projectRoot, "go-ivm", "testharness", "testcase_exists_membership.json")
	testCaseData, err := os.ReadFile(testCaseFile)
	if err != nil {
		t.Fatal(err)
	}
	runDifferential(t, testCaseData)
}

// TestOrWithExistsBranch is the second iteration of the production repro
// after pulling the actual AST from xyne-spaces-zero-external (queryID
// 1lbed0jdn1taa, 2026-05-25). The production query is shaped as:
//
//	WHERE updatedAt > X
//	  AND (visibility = 'PUBLIC' OR EXISTS(channel_participants matching user))
//
// Difference from TestExistsMembershipFilter: the EXISTS is now one
// branch of an OR (alongside a column-equality branch), not the entire
// WHERE clause. The simple-EXISTS test passes; this OR-with-EXISTS shape
// is what we see drifting in shadow mode.
//
// Test shape:
//   - 4 channels: ch1 (PUBLIC), ch2/ch3 (PRIVATE, user is member),
//     ch4 (PRIVATE, user NOT a member)
//   - 2 participants: user-A in ch2 and ch3
//   - Expected: 3 channels returned (ch1, ch2, ch3). ch4 must be excluded
//     (private + user not a participant).
//   - If Go returns 4 (including ch4), the OR-with-EXISTS evaluation is
//     broken in Go IVM.
func TestOrWithExistsBranch(t *testing.T) {
	projectRoot := findProjectRoot(t)
	testCaseFile := filepath.Join(projectRoot, "go-ivm", "testharness", "testcase_or_with_exists.json")
	testCaseData, err := os.ReadFile(testCaseFile)
	if err != nil {
		t.Fatal(err)
	}
	runDifferential(t, testCaseData)
}

// TestNestedRelated2Level reproduces the dominant prod drift signature
// (queryID 33dg3td8uhbjz, 2026-05-26): conversations with a 2-level nested
// related (conversations -> channel -> channel.participants) plus Take/
// LIMIT-driven displacement. Production logs show Go emitting extra
// channel_participants Add+Remove on advances where a new conversation
// pushes an older one out of the LIMIT window.
func TestNestedRelated2Level(t *testing.T) {
	projectRoot := findProjectRoot(t)
	testCaseFile := filepath.Join(projectRoot, "go-ivm", "testharness", "testcase_nested_related_2level.json")
	testCaseData, err := os.ReadFile(testCaseFile)
	if err != nil {
		t.Fatal(err)
	}
	runDifferential(t, testCaseData)
}

// TestNestedRelatedWithCSQ adds an inner EXISTS condition to the nested
// 2-level related shape — matching the truncated prod AST pattern. The
// log shows `WHERE and{channelId=X, {"typ...}}` which we guess is
// `AND EXISTS(channel_participants WHERE userId=user)` for visibility.
func TestNestedRelatedWithCSQ(t *testing.T) {
	projectRoot := findProjectRoot(t)
	testCaseFile := filepath.Join(projectRoot, "go-ivm", "testharness", "testcase_nested_related_with_csq.json")
	testCaseData, err := os.ReadFile(testCaseFile)
	if err != nil {
		t.Fatal(err)
	}
	runDifferential(t, testCaseData)
}
