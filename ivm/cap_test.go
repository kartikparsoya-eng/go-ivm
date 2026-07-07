package ivm

// Ported from mono/packages/zql/src/ivm/cap.push.test.ts at the operator
// level: MemorySource (unordered connect, the production Cap shape) → Cap →
// collecting output. The TS suite runs through the full Join pipeline via
// runPushTest; the Join/EXISTS wiring half is covered by the builder cap
// wiring tests — here we pin Cap's own push/fetch/storage transitions,
// including the exact storage snapshots the TS tests inline.

import (
	"reflect"
	"slices"
	"strings"
	"testing"
)

// newCapCommentFixture builds the cap.push.test.ts fixture: a comment
// source (PK id, partition column issueID), an unordered connection, a Cap
// with the given limit over partition ["issueID"], and a collecting output.
func newCapCommentFixture(t *testing.T, limit int, rows []Row) (*MemorySource, *Cap, *MemoryCapStorage, *testOutput) {
	t.Helper()
	cols := map[string]string{"id": "string", "issueID": "string", "text": "string"}
	source := NewMemorySource("comment", cols, []string{"id"})
	source.BulkInsert(rows)
	conn := source.Connect(nil, nil, nil)
	storage := NewMemoryCapStorage()
	cap := NewCap(conn, storage, limit, PartitionKey{"issueID"})
	out := &testOutput{}
	cap.SetOutput(out)
	return source, cap, storage, out
}

func capHydrate(t *testing.T, cap *Cap, constraint Constraint) []Node {
	t.Helper()
	return slices.Collect(cap.Fetch(FetchRequest{Constraint: &constraint}))
}

func capStateOf(t *testing.T, storage *MemoryCapStorage, key string) CapState {
	t.Helper()
	st, ok := storage.States()[key]
	if !ok {
		t.Fatalf("no cap state at key %q; have %v", key, storage.States())
	}
	return st
}

// TS: 'child add below cap limit is forwarded'
func TestCap_AddBelowLimitForwarded(t *testing.T) {
	source, cap, storage, out := newCapCommentFixture(t, 3, []Row{
		{"id": "c1", "issueID": "i1", "text": "c1"},
	})
	capHydrate(t, cap, Constraint{"issueID": "i1"})

	source.Push(MakeSourceChangeAdd(Row{"id": "c2", "issueID": "i1", "text": "c2"}))

	if len(out.changes) != 1 || out.changes[0].Type != ChangeTypeAdd {
		t.Fatalf("want 1 forwarded add, got %+v", out.changes)
	}
	// TS storage snapshot: {size: 2, pks: ['["c1"]', '["c2"]']}
	want := CapState{Size: 2, Pks: []string{`["c1"]`, `["c2"]`}}
	if got := capStateOf(t, storage, `["cap","i1"]`); !reflect.DeepEqual(got, want) {
		t.Fatalf("state = %+v, want %+v", got, want)
	}
}

// TS: 'child add at cap limit is dropped'
func TestCap_AddAtLimitDropped(t *testing.T) {
	source, cap, storage, out := newCapCommentFixture(t, 3, []Row{
		{"id": "c1", "issueID": "i1", "text": "c1"},
		{"id": "c2", "issueID": "i1", "text": "c2"},
		{"id": "c3", "issueID": "i1", "text": "c3"},
	})
	capHydrate(t, cap, Constraint{"issueID": "i1"})

	source.Push(MakeSourceChangeAdd(Row{"id": "c4", "issueID": "i1", "text": "c4"}))

	if len(out.changes) != 0 {
		t.Fatalf("add at limit must be dropped, got %+v", out.changes)
	}
	want := CapState{Size: 3, Pks: []string{`["c1"]`, `["c2"]`, `["c3"]`}}
	if got := capStateOf(t, storage, `["cap","i1"]`); !reflect.DeepEqual(got, want) {
		t.Fatalf("state = %+v, want %+v", got, want)
	}
}

// TS: 'child remove with refill'
func TestCap_RemoveWithRefill(t *testing.T) {
	source, cap, storage, out := newCapCommentFixture(t, 3, []Row{
		{"id": "c1", "issueID": "i1", "text": "c1"},
		{"id": "c2", "issueID": "i1", "text": "c2"},
		{"id": "c3", "issueID": "i1", "text": "c3"},
		{"id": "c4", "issueID": "i1", "text": "c4"},
	})
	capHydrate(t, cap, Constraint{"issueID": "i1"})

	source.Push(MakeSourceChangeRemove(Row{"id": "c2", "issueID": "i1", "text": "c2"}))

	// TS pushes: remove(c2) then add(c4) — the refill replacement.
	if len(out.changes) != 2 ||
		out.changes[0].Type != ChangeTypeRemove || out.changes[0].Node.Row["id"] != "c2" ||
		out.changes[1].Type != ChangeTypeAdd || out.changes[1].Node.Row["id"] != "c4" {
		t.Fatalf("want [remove c2, add c4], got %+v", out.changes)
	}
	want := CapState{Size: 3, Pks: []string{`["c1"]`, `["c3"]`, `["c4"]`}}
	if got := capStateOf(t, storage, `["cap","i1"]`); !reflect.DeepEqual(got, want) {
		t.Fatalf("state = %+v, want %+v", got, want)
	}
}

// TS: 'child remove without refill'
func TestCap_RemoveWithoutRefill(t *testing.T) {
	source, cap, storage, out := newCapCommentFixture(t, 3, []Row{
		{"id": "c1", "issueID": "i1", "text": "c1"},
		{"id": "c2", "issueID": "i1", "text": "c2"},
	})
	capHydrate(t, cap, Constraint{"issueID": "i1"})

	source.Push(MakeSourceChangeRemove(Row{"id": "c1", "issueID": "i1", "text": "c1"}))

	if len(out.changes) != 1 || out.changes[0].Type != ChangeTypeRemove {
		t.Fatalf("want single forwarded remove, got %+v", out.changes)
	}
	want := CapState{Size: 1, Pks: []string{`["c2"]`}}
	if got := capStateOf(t, storage, `["cap","i1"]`); !reflect.DeepEqual(got, want) {
		t.Fatalf("state = %+v, want %+v", got, want)
	}
}

// TS: 'child remove of last row causes parent retraction' — at operator
// level: state drops to size 0 with the remove forwarded.
func TestCap_RemoveLastRow(t *testing.T) {
	source, cap, storage, out := newCapCommentFixture(t, 3, []Row{
		{"id": "c1", "issueID": "i1", "text": "c1"},
	})
	capHydrate(t, cap, Constraint{"issueID": "i1"})

	source.Push(MakeSourceChangeRemove(Row{"id": "c1", "issueID": "i1", "text": "c1"}))

	if len(out.changes) != 1 || out.changes[0].Type != ChangeTypeRemove {
		t.Fatalf("want single forwarded remove, got %+v", out.changes)
	}
	got := capStateOf(t, storage, `["cap","i1"]`)
	if got.Size != 0 || len(got.Pks) != 0 {
		t.Fatalf("state = %+v, want size 0 / empty pks", got)
	}
	// A subsequent fetch on the size-0 partition returns nothing without
	// touching the input (cap.ts:108-110).
	if rows := capHydrate(t, cap, Constraint{"issueID": "i1"}); len(rows) != 0 {
		t.Fatalf("size-0 partition fetch returned rows: %v", rows)
	}
}

// TS: 'child remove of untracked PK is dropped'
func TestCap_RemoveUntrackedDropped(t *testing.T) {
	source, cap, storage, out := newCapCommentFixture(t, 3, []Row{
		{"id": "c1", "issueID": "i1", "text": "c1"},
		{"id": "c2", "issueID": "i1", "text": "c2"},
		{"id": "c3", "issueID": "i1", "text": "c3"},
		{"id": "c4", "issueID": "i1", "text": "c4"},
	})
	capHydrate(t, cap, Constraint{"issueID": "i1"})

	source.Push(MakeSourceChangeRemove(Row{"id": "c4", "issueID": "i1", "text": "c4"}))

	if len(out.changes) != 0 {
		t.Fatalf("remove of untracked pk must be dropped, got %+v", out.changes)
	}
	want := CapState{Size: 3, Pks: []string{`["c1"]`, `["c2"]`, `["c3"]`}}
	if got := capStateOf(t, storage, `["cap","i1"]`); !reflect.DeepEqual(got, want) {
		t.Fatalf("state = %+v, want %+v", got, want)
	}
}

// TS: 'child edit of tracked PK is forwarded'
func TestCap_EditTrackedForwarded(t *testing.T) {
	source, cap, storage, out := newCapCommentFixture(t, 3, []Row{
		{"id": "c1", "issueID": "i1", "text": "c1"},
		{"id": "c2", "issueID": "i1", "text": "c2"},
	})
	capHydrate(t, cap, Constraint{"issueID": "i1"})

	source.Push(MakeSourceChangeEdit(
		Row{"id": "c1", "issueID": "i1", "text": "c1 updated"},
		Row{"id": "c1", "issueID": "i1", "text": "c1"},
	))

	if len(out.changes) != 1 || out.changes[0].Type != ChangeTypeEdit {
		t.Fatalf("want forwarded edit, got %+v", out.changes)
	}
	want := CapState{Size: 2, Pks: []string{`["c1"]`, `["c2"]`}}
	if got := capStateOf(t, storage, `["cap","i1"]`); !reflect.DeepEqual(got, want) {
		t.Fatalf("state = %+v, want %+v", got, want)
	}
}

// Edit of an UNTRACKED pk is dropped (cap.ts:283-292 else-path).
func TestCap_EditUntrackedDropped(t *testing.T) {
	source, cap, _, out := newCapCommentFixture(t, 3, []Row{
		{"id": "c1", "issueID": "i1", "text": "c1"},
		{"id": "c2", "issueID": "i1", "text": "c2"},
		{"id": "c3", "issueID": "i1", "text": "c3"},
		{"id": "c4", "issueID": "i1", "text": "c4"},
	})
	capHydrate(t, cap, Constraint{"issueID": "i1"})

	source.Push(MakeSourceChangeEdit(
		Row{"id": "c4", "issueID": "i1", "text": "c4 updated"},
		Row{"id": "c4", "issueID": "i1", "text": "c4"},
	))
	if len(out.changes) != 0 {
		t.Fatalf("edit of untracked pk must be dropped, got %+v", out.changes)
	}
}

// TS: 'child edit that changes PK updates tracked set'
func TestCap_EditChangingPKUpdatesSet(t *testing.T) {
	source, cap, storage, out := newCapCommentFixture(t, 3, []Row{
		{"id": "c1", "issueID": "i1", "text": "c1"},
		{"id": "c2", "issueID": "i1", "text": "c2"},
	})
	capHydrate(t, cap, Constraint{"issueID": "i1"})

	source.Push(MakeSourceChangeEdit(
		Row{"id": "c1_renamed", "issueID": "i1", "text": "c1"},
		Row{"id": "c1", "issueID": "i1", "text": "c1"},
	))

	if len(out.changes) != 1 || out.changes[0].Type != ChangeTypeEdit {
		t.Fatalf("want forwarded edit, got %+v", out.changes)
	}
	// Position preserved: the renamed pk replaces the old one in place.
	want := CapState{Size: 2, Pks: []string{`["c1_renamed"]`, `["c2"]`}}
	if got := capStateOf(t, storage, `["cap","i1"]`); !reflect.DeepEqual(got, want) {
		t.Fatalf("state = %+v, want %+v", got, want)
	}
}

// cap.ts:261-268 — an edit that CHANGES the partition key violates the
// split-edit contract and must panic.
func TestCap_EditChangingPartitionKeyPanics(t *testing.T) {
	_, cap, _, _ := newCapCommentFixture(t, 3, []Row{
		{"id": "c1", "issueID": "i1", "text": "c1"},
	})
	capHydrate(t, cap, Constraint{"issueID": "i1"})

	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("expected panic on partition-key change")
		}
		if !strings.Contains(toStr(r), "Unexpected change of partition key") {
			t.Fatalf("wrong panic: %v", r)
		}
	}()
	cap.Push(MakeEditChange(
		Node{Row: Row{"id": "c1", "issueID": "i2", "text": "c1"}},
		Node{Row: Row{"id": "c1", "issueID": "i1", "text": "c1"}},
	), nil)
}

// CHILD changes are forwarded only for tracked pks (cap.ts:252-257).
func TestCap_ChildChangeGatedByTrackedSet(t *testing.T) {
	_, cap, _, out := newCapCommentFixture(t, 3, []Row{
		{"id": "c1", "issueID": "i1", "text": "c1"},
		{"id": "c2", "issueID": "i1", "text": "c2"},
		{"id": "c3", "issueID": "i1", "text": "c3"},
		{"id": "c4", "issueID": "i1", "text": "c4"},
	})
	capHydrate(t, cap, Constraint{"issueID": "i1"})

	tracked := MakeChildChange(
		Node{Row: Row{"id": "c1", "issueID": "i1", "text": "c1"}},
		ChildData{RelationshipName: "r", Change: MakeAddChange(Node{Row: Row{"id": "x"}})},
	)
	cap.Push(tracked, nil)
	if len(out.changes) != 1 || out.changes[0].Type != ChangeTypeChild {
		t.Fatalf("tracked child change must forward, got %+v", out.changes)
	}

	untracked := MakeChildChange(
		Node{Row: Row{"id": "c4", "issueID": "i1", "text": "c4"}},
		ChildData{RelationshipName: "r", Change: MakeAddChange(Node{Row: Row{"id": "x"}})},
	)
	cap.Push(untracked, nil)
	if len(out.changes) != 1 {
		t.Fatalf("untracked child change must drop, got %+v", out.changes)
	}
}

// TS 'Cap limit 0' — the limit-0 early return must run BEFORE the
// initialFetch constraint assert, for both partitioned and unpartitioned
// Caps, and no capState is ever persisted so subsequent pushes drop.
func TestCap_LimitZero(t *testing.T) {
	for _, tc := range []struct {
		name         string
		partitionKey PartitionKey
	}{
		{"no partition key", nil},
		{"with partition key", PartitionKey{"issueID"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cols := map[string]string{"id": "string", "issueID": "string", "text": "string"}
			source := NewMemorySource("comment", cols, []string{"id"})
			source.BulkInsert([]Row{{"id": "c1", "issueID": "i1", "text": "a"}})
			conn := source.Connect(Ordering{{"id", "asc"}}, nil, nil)
			storage := NewMemoryCapStorage()
			cap := NewCap(conn, storage, 0, tc.partitionKey)
			out := &testOutput{}
			cap.SetOutput(out)

			c := Constraint{"issueID": "i1"}
			if rows := slices.Collect(cap.Fetch(FetchRequest{Constraint: &c})); len(rows) != 0 {
				t.Fatalf("limit-0 fetch returned rows: %v", rows)
			}

			// ADD / REMOVE / EDIT must all be no-ops at the Cap level.
			source.Push(MakeSourceChangeAdd(Row{"id": "c2", "issueID": "i1", "text": "b"}))
			source.Push(MakeSourceChangeRemove(Row{"id": "c1", "issueID": "i1", "text": "a"}))
			source.Push(MakeSourceChangeEdit(
				Row{"id": "c2", "issueID": "i1", "text": "b updated"},
				Row{"id": "c2", "issueID": "i1", "text": "b"},
			))

			if len(out.changes) != 0 {
				t.Fatalf("limit-0 Cap forwarded pushes: %+v", out.changes)
			}
			if len(storage.States()) != 0 {
				t.Fatalf("limit-0 Cap persisted state: %v", storage.States())
			}
		})
	}
}

// cap.ts:88-100 — fetch asserts: no start, no reverse, and a partitioned
// Cap requires a matching constraint.
func TestCap_FetchAsserts(t *testing.T) {
	newCap := func(t *testing.T) *Cap {
		t.Helper()
		_, cap, _, _ := newCapCommentFixture(t, 3, []Row{
			{"id": "c1", "issueID": "i1", "text": "c1"},
		})
		return cap
	}
	expectPanic := func(t *testing.T, msg string, fn func()) {
		t.Helper()
		defer func() {
			r := recover()
			if r == nil {
				t.Fatalf("expected panic %q", msg)
			}
			if !strings.Contains(toStr(r), msg) {
				t.Fatalf("panic = %v, want contains %q", r, msg)
			}
		}()
		fn()
	}

	t.Run("start", func(t *testing.T) {
		cap := newCap(t)
		c := Constraint{"issueID": "i1"}
		expectPanic(t, "Cap does not support start", func() {
			_ = slices.Collect(cap.Fetch(FetchRequest{
				Constraint: &c,
				Start:      &Start{Row: Row{"id": "c1"}, Basis: "at"},
			}))
		})
	})
	t.Run("reverse", func(t *testing.T) {
		cap := newCap(t)
		c := Constraint{"issueID": "i1"}
		expectPanic(t, "Cap does not support reverse", func() {
			_ = slices.Collect(cap.Fetch(FetchRequest{Constraint: &c, Reverse: true}))
		})
	})
	t.Run("missing constraint on partitioned cap", func(t *testing.T) {
		cap := newCap(t)
		expectPanic(t, "constraint must match partition key", func() {
			_ = slices.Collect(cap.Fetch(FetchRequest{}))
		})
	})
}

// After hydration, a re-fetch serves the tracked rows via PK point lookups
// in tracked order (cap.ts:111-122) — NOT by re-scanning the partition.
func TestCap_RefetchUsesPKPointLookups(t *testing.T) {
	_, cap, storage, _ := newCapCommentFixture(t, 2, []Row{
		{"id": "c1", "issueID": "i1", "text": "c1"},
		{"id": "c2", "issueID": "i1", "text": "c2"},
		{"id": "c3", "issueID": "i1", "text": "c3"},
	})
	first := capHydrate(t, cap, Constraint{"issueID": "i1"})
	if len(first) != 2 {
		t.Fatalf("hydrate returned %d rows, want 2 (limit)", len(first))
	}
	want := CapState{Size: 2, Pks: []string{`["c1"]`, `["c2"]`}}
	if got := capStateOf(t, storage, `["cap","i1"]`); !reflect.DeepEqual(got, want) {
		t.Fatalf("state = %+v, want %+v", got, want)
	}

	second := capHydrate(t, cap, Constraint{"issueID": "i1"})
	ids := make([]string, len(second))
	for i, n := range second {
		ids[i] = n.Row["id"].(string)
	}
	if !reflect.DeepEqual(ids, []string{"c1", "c2"}) {
		t.Fatalf("re-fetch ids = %v, want [c1 c2] (tracked pks in order)", ids)
	}
}

// A partition hydrated with NO matching rows persists {size:0, pks:[]} —
// the TS compound-partition test pins this ("its partition is still
// hydrated with size=0 during the initial scan").
func TestCap_EmptyPartitionHydratesSizeZero(t *testing.T) {
	_, cap, storage, _ := newCapCommentFixture(t, 3, []Row{
		{"id": "c1", "issueID": "i1", "text": "c1"},
	})
	if rows := capHydrate(t, cap, Constraint{"issueID": "iEmpty"}); len(rows) != 0 {
		t.Fatalf("empty partition returned rows: %v", rows)
	}
	got := capStateOf(t, storage, `["cap","iEmpty"]`)
	if got.Size != 0 || len(got.Pks) != 0 {
		t.Fatalf("state = %+v, want size 0", got)
	}
}

// TS 'Cap push - compound partition key': storage keys carry every
// partition column and buckets stay independent.
func TestCap_CompoundPartitionKey(t *testing.T) {
	cols := map[string]string{"id": "string", "region": "string", "org": "string", "text": "string"}
	source := NewMemorySource("child", cols, []string{"id"})
	source.BulkInsert([]Row{
		{"id": "c1", "region": "us", "org": "acme", "text": "x"},
		{"id": "c2", "region": "us", "org": "acme", "text": "y"},
		{"id": "c3", "region": "us", "org": "acme", "text": "z"},
	})
	conn := source.Connect(nil, nil, nil)
	storage := NewMemoryCapStorage()
	cap := NewCap(conn, storage, 3, PartitionKey{"region", "org"})
	out := &testOutput{}
	cap.SetOutput(out)

	capHydrate(t, cap, Constraint{"region": "us", "org": "acme"})
	capHydrate(t, cap, Constraint{"region": "us", "org": "wayne"})

	// 4th child in (us,acme) — exceeds cap limit of 3 → dropped.
	source.Push(MakeSourceChangeAdd(Row{"id": "c4", "region": "us", "org": "acme", "text": "w"}))
	// 1st child in (us,wayne) — independent bucket, accepted.
	source.Push(MakeSourceChangeAdd(Row{"id": "c5", "region": "us", "org": "wayne", "text": "v"}))

	wantAcme := CapState{Size: 3, Pks: []string{`["c1"]`, `["c2"]`, `["c3"]`}}
	if got := capStateOf(t, storage, `["cap","us","acme"]`); !reflect.DeepEqual(got, wantAcme) {
		t.Fatalf("acme state = %+v, want %+v", got, wantAcme)
	}
	wantWayne := CapState{Size: 1, Pks: []string{`["c5"]`}}
	if got := capStateOf(t, storage, `["cap","us","wayne"]`); !reflect.DeepEqual(got, wantWayne) {
		t.Fatalf("wayne state = %+v, want %+v", got, wantWayne)
	}
	if len(out.changes) != 1 || out.changes[0].Node.Row["id"] != "c5" {
		t.Fatalf("want only add(c5) forwarded, got %+v", out.changes)
	}
}

// TS 'Cap push - compound primary key': serializePK/deserializePK round-trip
// multi-column PKs through hydration, refill point lookups, and PK edits.
func TestCap_CompoundPrimaryKey(t *testing.T) {
	cols := map[string]string{"groupId": "string", "seq": "string", "text": "string"}
	source := NewMemorySource("child", cols, []string{"groupId", "seq"})
	source.BulkInsert([]Row{
		{"groupId": "g1", "seq": "s1", "text": "a"},
		{"groupId": "g1", "seq": "s2", "text": "b"},
		{"groupId": "g1", "seq": "s3", "text": "c"},
		{"groupId": "g1", "seq": "s4", "text": "d"},
	})
	conn := source.Connect(nil, nil, nil)
	storage := NewMemoryCapStorage()
	cap := NewCap(conn, storage, 3, PartitionKey{"groupId"})
	out := &testOutput{}
	cap.SetOutput(out)

	capHydrate(t, cap, Constraint{"groupId": "g1"})
	// TS: pks are JSON arrays of [groupId, seq] in PK order.
	want := CapState{Size: 3, Pks: []string{`["g1","s1"]`, `["g1","s2"]`, `["g1","s3"]`}}
	if got := capStateOf(t, storage, `["cap","g1"]`); !reflect.DeepEqual(got, want) {
		t.Fatalf("state = %+v, want %+v", got, want)
	}

	// Remove + refill round-trips compound PKs through the point lookup.
	source.Push(MakeSourceChangeRemove(Row{"groupId": "g1", "seq": "s1", "text": "a"}))
	want = CapState{Size: 3, Pks: []string{`["g1","s2"]`, `["g1","s3"]`, `["g1","s4"]`}}
	if got := capStateOf(t, storage, `["cap","g1"]`); !reflect.DeepEqual(got, want) {
		t.Fatalf("post-refill state = %+v, want %+v", got, want)
	}

	// Edit that changes a non-leading PK column (groupId stable so the
	// partition assert passes) replaces the compound pk in place.
	source.Push(MakeSourceChangeEdit(
		Row{"groupId": "g1", "seq": "s2_new", "text": "b"},
		Row{"groupId": "g1", "seq": "s2", "text": "b"},
	))
	want = CapState{Size: 3, Pks: []string{`["g1","s2_new"]`, `["g1","s3"]`, `["g1","s4"]`}}
	if got := capStateOf(t, storage, `["cap","g1"]`); !reflect.DeepEqual(got, want) {
		t.Fatalf("post-edit state = %+v, want %+v", got, want)
	}

	// Re-fetch resolves each compound pk to the right row, in tracked order.
	rows := capHydrate(t, cap, Constraint{"groupId": "g1"})
	seqs := make([]string, len(rows))
	for i, n := range rows {
		seqs[i] = n.Row["seq"].(string)
	}
	if !reflect.DeepEqual(seqs, []string{"s2_new", "s3", "s4"}) {
		t.Fatalf("re-fetch seqs = %v, want [s2_new s3 s4]", seqs)
	}
}

// NewCap panics on a negative limit (cap.ts:68).
func TestCap_NegativeLimitPanics(t *testing.T) {
	cols := map[string]string{"id": "string"}
	source := NewMemorySource("t", cols, []string{"id"})
	conn := source.Connect(nil, nil, nil)
	defer func() {
		if recover() == nil {
			t.Fatal("expected panic on negative limit")
		}
	}()
	NewCap(conn, NewMemoryCapStorage(), -1, nil)
}

// toStr renders a recovered panic value for message matching.
func toStr(r interface{}) string {
	if s, ok := r.(string); ok {
		return s
	}
	if e, ok := r.(error); ok {
		return e.Error()
	}
	return ""
}
