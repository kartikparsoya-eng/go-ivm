package ivm

// Regression tests for the invariant asserts ported from TS (faithfulness
// item #2):
//   - AssertOrderingIncludesPK        (complete-ordering.ts:30-44)
//   - Take constructor assert         (take.ts:75)
//   - Take.initialFetch asserts       (take.ts:159-175)
//   - MemorySource.Connect assert     (memory-source.ts:198-200)
//   - MemorySource.Disconnect assert  (memory-source.ts:205-208)
//
// Pre-fix, none of these panicked: a PK-less sort silently produced a
// non-total comparator, and a double-disconnect silently no-oped.

import (
	"iter"
	"strings"
	"testing"
)

// mustPanic runs fn and asserts it panics with a message containing want.
func mustPanic(t *testing.T, want string, fn func()) {
	t.Helper()
	defer func() {
		r := recover()
		if r == nil {
			t.Fatalf("expected panic containing %q, got none", want)
		}
		msg := ""
		switch v := r.(type) {
		case string:
			msg = v
		case error:
			msg = v.Error()
		default:
			t.Fatalf("unexpected panic type %T: %v", r, r)
		}
		if !strings.Contains(msg, want) {
			t.Fatalf("panic %q does not contain %q", msg, want)
		}
	}()
	fn()
}

func TestAssertOrderingIncludesPK(t *testing.T) {
	// All PK columns present — no panic.
	AssertOrderingIncludesPK(Ordering{{"name", "asc"}, {"id", "asc"}}, []string{"id"})
	AssertOrderingIncludesPK(Ordering{{"a", "desc"}, {"b", "asc"}}, []string{"a", "b"})
	// Empty PK — vacuously fine.
	AssertOrderingIncludesPK(Ordering{{"x", "asc"}}, nil)

	// Missing columns panic with the TS message, listing them in PK order.
	mustPanic(t, "Ordering must include all primary key fields. Missing: id.", func() {
		AssertOrderingIncludesPK(Ordering{{"name", "asc"}}, []string{"id"})
	})
	mustPanic(t, "Missing: a, b.", func() {
		AssertOrderingIncludesPK(Ordering{{"x", "asc"}}, []string{"a", "b"})
	})
}

func TestMemorySourceConnectPanicsWhenSortMissingPK(t *testing.T) {
	ms := NewMemorySource("t", map[string]string{"id": "string", "name": "string"}, []string{"id"})
	mustPanic(t, "Ordering must include all primary key fields", func() {
		ms.Connect(Ordering{{"name", "asc"}}, nil, nil)
	})
	// nil sort defaults to the primary index sort — never asserts
	// (memory-source.ts "unordered" branch).
	si := ms.Connect(nil, nil, nil)
	si.Destroy()
}

func TestMemorySourceDisconnectTwicePanics(t *testing.T) {
	ms := NewMemorySource("t", map[string]string{"id": "string"}, []string{"id"})
	si := ms.Connect(Ordering{{"id", "asc"}}, nil, nil)
	si.Destroy()
	mustPanic(t, "Connection not found", func() {
		si.Destroy()
	})
}

// pkAssertStubInput hands Take a schema whose Sort omits the PK — impossible
// to obtain through a source Connect (which now asserts) so the ctor assert
// needs its own synthetic input.
type pkAssertStubInput struct{ schema *SourceSchema }

func (s *pkAssertStubInput) GetSchema() *SourceSchema          { return s.schema }
func (s *pkAssertStubInput) Destroy()                          {}
func (s *pkAssertStubInput) SetOutput(Output)                  {}
func (s *pkAssertStubInput) Fetch(FetchRequest) iter.Seq[Node] { return emptyNodeSeq }

func TestNewTakePanicsWhenSortMissingPK(t *testing.T) {
	sort := Ordering{{"name", "asc"}}
	in := &pkAssertStubInput{schema: &SourceSchema{
		TableName:   "t",
		PrimaryKey:  []string{"id"},
		Sort:        sort,
		CompareRows: MakeComparator(sort, false),
	}}
	mustPanic(t, "Ordering must include all primary key fields. Missing: id.", func() {
		NewTake(in, NewMemoryTakeStorage(), 5, nil)
	})
}

// drain consumes a Node seq (panics propagate to the caller).
func drain(seq iter.Seq[Node]) {
	for range seq {
	}
}

func TestTakeInitialFetchStartAssert(t *testing.T) {
	ms := NewMemorySource("t", map[string]string{"id": "string"}, []string{"id"})
	si := ms.Connect(Ordering{{"id", "asc"}}, nil, nil)
	take := NewTake(si, NewMemoryTakeStorage(), 3, nil)
	mustPanic(t, "Start should be undefined", func() {
		drain(take.Fetch(FetchRequest{Start: &Start{Row: Row{"id": "a"}, Basis: "at"}}))
	})
}

func TestTakeInitialFetchReverseAssert(t *testing.T) {
	ms := NewMemorySource("t", map[string]string{"id": "string"}, []string{"id"})
	si := ms.Connect(Ordering{{"id", "asc"}}, nil, nil)
	take := NewTake(si, NewMemoryTakeStorage(), 3, nil)
	mustPanic(t, "Reverse should be false", func() {
		drain(take.Fetch(FetchRequest{Reverse: true}))
	})
}

// take.ts:162-163 — the limit-0 return sits AFTER the start/reverse asserts
// but BEFORE the constraint/state asserts and the try/finally, so a limit-0
// Take yields nothing, persists no takeState, and still trips the
// start/reverse asserts.
func TestTakeInitialFetchLimitZeroAfterStartAssert(t *testing.T) {
	ms := NewMemorySource("t", map[string]string{"id": "string"}, []string{"id"})
	ms.Push(SourceChange{Type: ChangeTypeAdd, Row: Row{"id": "a"}})

	si := ms.Connect(Ordering{{"id", "asc"}}, nil, nil)
	storage := NewMemoryTakeStorage()
	take := NewTake(si, storage, 0, nil)

	// Well-formed request: yields nothing, no state persisted (TS returns
	// before the try/finally that calls setTakeState).
	count := 0
	for range take.Fetch(FetchRequest{}) {
		count++
	}
	if count != 0 {
		t.Fatalf("limit-0 Take yielded %d nodes, want 0", count)
	}
	if st := storage.GetTakeState(`["take"]`); st != nil {
		t.Fatalf("limit-0 initialFetch persisted takeState %+v, want none", st)
	}

	// Malformed request still asserts, even at limit 0 (lazy, in-generator).
	take2 := NewTake(ms.Connect(Ordering{{"id", "asc"}}, nil, nil), NewMemoryTakeStorage(), 0, nil)
	mustPanic(t, "Start should be undefined", func() {
		drain(take2.Fetch(FetchRequest{Start: &Start{Row: Row{"id": "a"}, Basis: "at"}}))
	})
}
