package ivm

import (
	"iter"
	"slices"
	"sync/atomic"
	"testing"
)

// countingInput wraps an Input and counts how many nodes Fetch returns.
type countingInput struct {
	inner        Input
	fetchCount   int32
	rowsReturned int32
}

func (c *countingInput) GetSchema() *SourceSchema { return c.inner.GetSchema() }
func (c *countingInput) Destroy()                 { c.inner.Destroy() }
func (c *countingInput) SetOutput(o Output)       { c.inner.SetOutput(o) }
func (c *countingInput) Fetch(req FetchRequest) iter.Seq[Node] {
	atomic.AddInt32(&c.fetchCount, 1)
	nodes := slices.Collect(c.inner.Fetch(req))
	atomic.AddInt32(&c.rowsReturned, int32(len(nodes)))
	return slices.Values(nodes)
}

func makeTestSource(t *testing.T, name string, cols map[string]string, pk []string, sort Ordering, rows []Row) *MemorySource {
	t.Helper()
	src := NewMemorySource(name, cols, pk)
	src.BulkInsert(rows)
	return src
}

// TestLimitThroughFilter_Correctness verifies that Source→Filter→Take
// returns the correct rows when Take's limit propagates through Filter.
func TestLimitThroughFilter_Correctness(t *testing.T) {
	cols := map[string]string{"id": "string", "active": "boolean", "val": "number"}
	sort := Ordering{{"val", "asc"}, {"id", "asc"}}
	rows := []Row{
		{"id": "r1", "active": true, "val": float64(1)},
		{"id": "r2", "active": false, "val": float64(2)},
		{"id": "r3", "active": true, "val": float64(3)},
		{"id": "r4", "active": false, "val": float64(4)},
		{"id": "r5", "active": true, "val": float64(5)},
		{"id": "r6", "active": false, "val": float64(6)},
		{"id": "r7", "active": true, "val": float64(7)},
		{"id": "r8", "active": false, "val": float64(8)},
		{"id": "r9", "active": true, "val": float64(9)},
		{"id": "r10", "active": false, "val": float64(10)},
	}
	src := makeTestSource(t, "items", cols, []string{"id"}, sort, rows)
	conn := src.Connect(sort, nil, nil)

	fs := NewFilterStart(conn)
	f := NewFilter(fs, func(row Row) bool {
		return row["active"].(bool)
	})
	fe := NewFilterEnd(fs, f)

	take := NewTake(fe, NewMemoryTakeStorage(), 3, nil)
	out := &testOutput{}
	take.SetOutput(out)

	result := slices.Collect(take.Fetch(FetchRequest{}))
	if len(result) != 3 {
		t.Fatalf("expected 3 results, got %d", len(result))
	}
	for i, n := range result {
		if !n.Row["active"].(bool) {
			t.Errorf("result[%d] has active=false, should be true", i)
		}
	}
	expectedIDs := []string{"r1", "r3", "r5"}
	for i, id := range expectedIDs {
		if result[i].Row["id"] != id {
			t.Errorf("result[%d].id = %v, want %s", i, result[i].Row["id"], id)
		}
	}
}

// TestLimitThroughFilter_EarlyTermination verifies that Filter breaks its
// loop after collecting req.Limit post-filter rows, not iterating the entire
// upstream result. This is the core EXISTS-explosion fix.
func TestLimitThroughFilter_EarlyTermination(t *testing.T) {
	cols := map[string]string{"id": "string", "active": "boolean", "val": "number"}
	sort := Ordering{{"val", "asc"}, {"id", "asc"}}
	rows := make([]Row, 0, 20)
	for i := 1; i <= 20; i++ {
		rows = append(rows, Row{
			"id":     "r" + string(rune('a'+i-1)),
			"active": i%2 == 1,
			"val":    float64(i),
		})
	}
	src := makeTestSource(t, "items", cols, []string{"id"}, sort, rows)
	conn := src.Connect(sort, nil, nil)

	var filterCallCount int32
	fs := NewFilterStart(conn)
	f := NewFilter(fs, func(row Row) bool {
		atomic.AddInt32(&filterCallCount, 1)
		return row["active"].(bool)
	})
	fe := NewFilterEnd(fs, f)

	take := NewTake(fe, NewMemoryTakeStorage(), 3, nil)
	out := &testOutput{}
	take.SetOutput(out)

	result := slices.Collect(take.Fetch(FetchRequest{}))
	if len(result) != 3 {
		t.Fatalf("expected 3 results, got %d", len(result))
	}

	// With 20 rows, alternating active/inactive, limit=3:
	// r1(pass), r2(fail), r3(pass), r4(fail), r5(pass) → break at 3
	// Filter predicate should have been called ~5 times, NOT 20.
	filterCalls := atomic.LoadInt32(&filterCallCount)
	if filterCalls > 10 {
		t.Errorf("Filter was called %d times, expected <= 10 (early termination)", filterCalls)
	}
	if filterCalls < 3 {
		t.Errorf("Filter was called %d times, expected >= 3 (need at least limit passes)", filterCalls)
	}
	t.Logf("Filter called %d times for 20-row source with limit=3", filterCalls)
}

// TestLimitThroughFilter_AllPass verifies no under-fetch when Filter passes
// every row (filter rate = 100%).
func TestLimitThroughFilter_AllPass(t *testing.T) {
	cols := map[string]string{"id": "string", "val": "number"}
	sort := Ordering{{"val", "asc"}, {"id", "asc"}}
	rows := make([]Row, 0, 10)
	for i := 1; i <= 10; i++ {
		rows = append(rows, Row{"id": "r" + string(rune('a'+i-1)), "val": float64(i)})
	}
	src := makeTestSource(t, "items", cols, []string{"id"}, sort, rows)
	conn := src.Connect(sort, nil, nil)

	fs := NewFilterStart(conn)
	f := NewFilter(fs, func(row Row) bool { return true })
	fe := NewFilterEnd(fs, f)

	take := NewTake(fe, NewMemoryTakeStorage(), 5, nil)
	out := &testOutput{}
	take.SetOutput(out)

	result := slices.Collect(take.Fetch(FetchRequest{}))
	if len(result) != 5 {
		t.Fatalf("expected 5 results, got %d", len(result))
	}
}

// TestLimitThroughFilter_NonePass verifies Filter exhausts upstream when
// no rows pass the predicate (no early termination possible).
func TestLimitThroughFilter_NonePass(t *testing.T) {
	cols := map[string]string{"id": "string", "val": "number"}
	sort := Ordering{{"val", "asc"}, {"id", "asc"}}
	rows := make([]Row, 0, 5)
	for i := 1; i <= 5; i++ {
		rows = append(rows, Row{"id": "r" + string(rune('a'+i-1)), "val": float64(i)})
	}
	src := makeTestSource(t, "items", cols, []string{"id"}, sort, rows)
	conn := src.Connect(sort, nil, nil)

	fs := NewFilterStart(conn)
	f := NewFilter(fs, func(row Row) bool { return false })
	fe := NewFilterEnd(fs, f)

	take := NewTake(fe, NewMemoryTakeStorage(), 3, nil)
	out := &testOutput{}
	take.SetOutput(out)

	result := slices.Collect(take.Fetch(FetchRequest{}))
	if len(result) != 0 {
		t.Fatalf("expected 0 results (none pass filter), got %d", len(result))
	}
}

// TestLimitThroughSkip_Correctness verifies Source→Skip→Take returns
// the correct rows after the skip bound.
func TestLimitThroughSkip_Correctness(t *testing.T) {
	cols := map[string]string{"id": "string", "val": "number"}
	sort := Ordering{{"val", "asc"}, {"id", "asc"}}
	rows := make([]Row, 0, 10)
	for i := 1; i <= 10; i++ {
		id := "r" + string(rune('a'+i-1))
		if i > 9 {
			id = "r" + string(rune('a')) + string(rune('0'+i-10))
		}
		rows = append(rows, Row{"id": id, "val": float64(i)})
	}
	src := makeTestSource(t, "items", cols, []string{"id"}, sort, rows)
	conn := src.Connect(sort, nil, nil)

	skip := NewSkip(conn, Bound{
		Row:       Row{"val": float64(3), "id": "rc"},
		Exclusive: true,
	})

	take := NewTake(skip, NewMemoryTakeStorage(), 3, nil)
	out := &testOutput{}
	take.SetOutput(out)

	result := slices.Collect(take.Fetch(FetchRequest{}))
	if len(result) != 3 {
		t.Fatalf("expected 3 results, got %d", len(result))
	}
	for i, n := range result {
		expectedVal := float64(4 + i)
		if n.Row["val"] != expectedVal {
			t.Errorf("result[%d].val = %v, want %v", i, n.Row["val"], expectedVal)
		}
	}
}

// TestLimitThroughSkip_ForwardedToSource verifies that Skip forwards
// req.Limit to Source in the forward case, so Source truncates its output.
func TestLimitThroughSkip_ForwardedToSource(t *testing.T) {
	cols := map[string]string{"id": "string", "val": "number"}
	sort := Ordering{{"val", "asc"}, {"id", "asc"}}
	rows := make([]Row, 0, 20)
	for i := 1; i <= 20; i++ {
		id := "r" + string(rune('a'+(i-1)%26))
		if i > 26 {
			id = "r" + string(rune('a'+(i-1)%26)) + string(rune('a'+(i-1)/26-1))
		}
		rows = append(rows, Row{"id": id, "val": float64(i)})
	}
	src := makeTestSource(t, "items", cols, []string{"id"}, sort, rows)
	conn := src.Connect(sort, nil, nil)

	counter := &countingInput{inner: conn}

	skip := NewSkip(counter, Bound{
		Row:       Row{"val": float64(5), "id": "re"},
		Exclusive: true,
	})

	take := NewTake(skip, NewMemoryTakeStorage(), 3, nil)
	out := &testOutput{}
	take.SetOutput(out)

	result := slices.Collect(take.Fetch(FetchRequest{}))
	if len(result) != 3 {
		t.Fatalf("expected 3 results, got %d", len(result))
	}

	// Source should have received req.Limit=3, so it should return
	// at most 3 rows (after applying start at val=5).
	// Without Limit forwarding, Source would return all 15 rows after val=5.
	totalReturned := atomic.LoadInt32(&counter.rowsReturned)
	if totalReturned > 3 {
		t.Errorf("Source returned %d rows, expected <= 3 (Limit forwarded through Skip)", totalReturned)
	}
	t.Logf("Source returned %d rows with Limit forwarded through Skip", totalReturned)
}

// TestLimitThroughExists_EarlyTermination verifies that Source→Join→Filter(Exists)→Take
// stops calling Exists after collecting enough post-filter rows, instead of running
// EXISTS for every parent row in the source.
func TestLimitThroughExists_EarlyTermination(t *testing.T) {
	parentCols := map[string]string{"id": "string", "name": "string"}
	parentSort := Ordering{{"id", "asc"}}
	parentRows := make([]Row, 0, 10)
	for i := 1; i <= 10; i++ {
		parentRows = append(parentRows, Row{
			"id":   "p" + string(rune('0'+i)),
			"name": "parent" + string(rune('0'+i)),
		})
	}
	parentSrc := makeTestSource(t, "parents", parentCols, []string{"id"}, parentSort, parentRows)
	parentConn := parentSrc.Connect(parentSort, nil, nil)

	childCols := map[string]string{"id": "string", "parentId": "string"}
	childSort := Ordering{{"id", "asc"}}
	childRows := []Row{
		{"id": "c1", "parentId": "p1"},
		{"id": "c2", "parentId": "p3"},
		{"id": "c3", "parentId": "p5"},
		{"id": "c4", "parentId": "p7"},
		{"id": "c5", "parentId": "p9"},
	}
	childSrc := makeTestSource(t, "children", childCols, []string{"id"}, childSort, childRows)
	childConn := childSrc.Connect(childSort, nil, nil)

	childCounter := &countingInput{inner: childConn}

	join := NewJoin(JoinArgs{
		Parent:           parentConn,
		Child:            childCounter,
		ParentKey:        CompoundKey{"id"},
		ChildKey:         CompoundKey{"parentId"},
		RelationshipName: "children",
		Hidden:           false,
	})

	fs := NewFilterStart(join)
	exists := NewExists(fs, "children", CompoundKey{"id"}, ExistsTypeExists)
	fe := NewFilterEnd(fs, exists)

	take := NewTake(fe, NewMemoryTakeStorage(), 2, nil)
	out := &testOutput{}
	take.SetOutput(out)

	result := slices.Collect(take.Fetch(FetchRequest{}))
	if len(result) != 2 {
		t.Fatalf("expected 2 results, got %d", len(result))
	}

	// With 10 parents (sorted by id), children exist for p1, p3, p5, p7, p9.
	// Take limit=2: p1(pass), p2(fail), p3(pass) → break at 2.
	// Exists triggers a child Fetch per parent. With early termination,
	// child Fetch should be called ~3 times, NOT 10.
	childFetches := atomic.LoadInt32(&childCounter.fetchCount)
	if childFetches > 5 {
		t.Errorf("Exists triggered %d child fetches, expected <= 5 (early termination)", childFetches)
	}
	if childFetches < 2 {
		t.Errorf("Exists triggered %d child fetches, expected >= 2 (need at least limit passes)", childFetches)
	}
	t.Logf("Exists triggered %d child fetches for 10-parent source with limit=2", childFetches)
}

// TestLimitDirectSource_Regression verifies that Source→Take (no intermediate
// operators) still works correctly after removing the LeafSource gate.
func TestLimitDirectSource_Regression(t *testing.T) {
	cols := map[string]string{"id": "string", "val": "number"}
	sort := Ordering{{"val", "asc"}, {"id", "asc"}}
	rows := make([]Row, 0, 10)
	for i := 1; i <= 10; i++ {
		rows = append(rows, Row{"id": "r" + string(rune('a'+i-1)), "val": float64(i)})
	}
	src := makeTestSource(t, "items", cols, []string{"id"}, sort, rows)
	conn := src.Connect(sort, nil, nil)

	take := NewTake(conn, NewMemoryTakeStorage(), 3, nil)
	out := &testOutput{}
	take.SetOutput(out)

	result := slices.Collect(take.Fetch(FetchRequest{}))
	if len(result) != 3 {
		t.Fatalf("expected 3 results, got %d", len(result))
	}
	for i, n := range result {
		expectedVal := float64(1 + i)
		if n.Row["val"] != expectedVal {
			t.Errorf("result[%d].val = %v, want %v", i, n.Row["val"], expectedVal)
		}
	}
}

// TestLimitThroughFilterThenJoin verifies that Source→Filter→Take→Join(related)
// still works correctly — Take is between Filter and Join, so Join sees
// Take's truncated output (not Filter's early-terminated output).
func TestLimitThroughFilterThenJoin(t *testing.T) {
	parentCols := map[string]string{"id": "string", "active": "boolean", "val": "number"}
	parentSort := Ordering{{"val", "asc"}, {"id", "asc"}}
	parentRows := make([]Row, 0, 10)
	for i := 1; i <= 10; i++ {
		parentRows = append(parentRows, Row{
			"id":     "p" + string(rune('a'+i-1)),
			"active": i%2 == 1,
			"val":    float64(i),
		})
	}
	parentSrc := makeTestSource(t, "parents", parentCols, []string{"id"}, parentSort, parentRows)
	parentConn := parentSrc.Connect(parentSort, nil, nil)

	childCols := map[string]string{"id": "string", "parentId": "string"}
	childSort := Ordering{{"id", "asc"}}
	childRows := []Row{
		{"id": "c1", "parentId": "pa"},
		{"id": "c2", "parentId": "pc"},
		{"id": "c3", "parentId": "pe"},
	}
	childSrc := makeTestSource(t, "children", childCols, []string{"id"}, childSort, childRows)
	childConn := childSrc.Connect(childSort, nil, nil)

	fs := NewFilterStart(parentConn)
	f := NewFilter(fs, func(row Row) bool { return row["active"].(bool) })
	fe := NewFilterEnd(fs, f)

	take := NewTake(fe, NewMemoryTakeStorage(), 2, nil)

	join := NewJoin(JoinArgs{
		Parent:           take,
		Child:            childConn,
		ParentKey:        CompoundKey{"id"},
		ChildKey:         CompoundKey{"parentId"},
		RelationshipName: "children",
		Hidden:           false,
	})

	out := &testOutput{}
	join.SetOutput(out)
	result := slices.Collect(join.Fetch(FetchRequest{}))
	if len(result) != 2 {
		t.Fatalf("expected 2 results, got %d", len(result))
	}

	if result[0].Row["id"] != "pa" {
		t.Errorf("result[0].id = %v, want pa", result[0].Row["id"])
	}
	if result[1].Row["id"] != "pc" {
		t.Errorf("result[1].id = %v, want pc", result[1].Row["id"])
	}

	for _, n := range result {
		if n.Relationships == nil {
			t.Errorf("result %v has no relationships", n.Row["id"])
			continue
		}
		childFn, ok := n.Relationships["children"]
		if !ok {
			t.Errorf("result %v has no 'children' relationship", n.Row["id"])
			continue
		}
		children := slices.Collect(childFn())
		if len(children) != 1 {
			t.Errorf("result %v expected 1 child, got %d", n.Row["id"], len(children))
		}
	}
}

// TestLimitLargerThanSource verifies that a limit larger than the available
// rows doesn't cause issues (no under-fetch, correct takeState).
func TestLimitLargerThanSource(t *testing.T) {
	cols := map[string]string{"id": "string", "val": "number"}
	sort := Ordering{{"val", "asc"}, {"id", "asc"}}
	rows := []Row{
		{"id": "r1", "val": float64(1)},
		{"id": "r2", "val": float64(2)},
		{"id": "r3", "val": float64(3)},
	}
	src := makeTestSource(t, "items", cols, []string{"id"}, sort, rows)
	conn := src.Connect(sort, nil, nil)

	fs := NewFilterStart(conn)
	f := NewFilter(fs, func(row Row) bool { return true })
	fe := NewFilterEnd(fs, f)

	take := NewTake(fe, NewMemoryTakeStorage(), 10, nil)
	out := &testOutput{}
	take.SetOutput(out)

	result := slices.Collect(take.Fetch(FetchRequest{}))
	if len(result) != 3 {
		t.Fatalf("expected 3 results (all rows, limit > source), got %d", len(result))
	}
}

// TestLimitThroughSkipReverse verifies that Skip does NOT forward Limit
// in the reverse case (shouldBePresent post-filter can drop rows).
func TestLimitThroughSkipReverse(t *testing.T) {
	cols := map[string]string{"id": "string", "val": "number"}
	sort := Ordering{{"val", "asc"}, {"id", "asc"}}
	rows := make([]Row, 0, 10)
	for i := 1; i <= 10; i++ {
		rows = append(rows, Row{"id": "r" + string(rune('a'+i-1)), "val": float64(i)})
	}
	src := makeTestSource(t, "items", cols, []string{"id"}, sort, rows)
	conn := src.Connect(sort, nil, nil)

	skip := NewSkip(conn, Bound{
		Row:       Row{"val": float64(8), "id": "rh"},
		Exclusive: true,
	})

	take := NewTake(skip, NewMemoryTakeStorage(), 3, nil)
	out := &testOutput{}
	take.SetOutput(out)

	result := slices.Collect(take.Fetch(FetchRequest{Reverse: true}))
	if len(result) > 3 {
		t.Errorf("expected at most 3 results, got %d", len(result))
	}
	t.Logf("reverse skip returned %d results", len(result))
}
