package engine

import (
	"errors"
	"strings"
	"testing"

	"github.com/kartikparsoya-eng/go-ivm/builder"
	"github.com/kartikparsoya-eng/go-ivm/ivm"
)

// C1 regression: a panic inside a hydrate goroutine (here a nil-PK row, which
// trips streamer.pkValue) must be converted to a returned error, NOT propagate
// out of the goroutine and abort the whole sidecar process. The test reaching
// its assertions at all proves the process did not crash.

func newPanicSeedEngine(t *testing.T) *Engine {
	t.Helper()
	eng, err := NewEngine(EngineConfig{StoragePath: tempStoragePath(t)})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { eng.Close() })

	ms := ivm.NewMemorySource(
		"users",
		map[string]string{"id": "string", "name": "string"},
		[]string{"id"},
	)
	// A row missing its primary key "id" — corrupt-replica / join-misconstruction
	// shape that pkValue panics on during hydrate output.
	ms.BulkInsert([]ivm.Row{{"name": "NoPK"}})
	eng.RegisterMemorySource(ms)
	return eng
}

func panicQuery(id string) QuerySpec {
	return QuerySpec{
		QueryID: id,
		AST: builder.AST{
			Table:   "users",
			OrderBy: ivm.Ordering{{"id", "asc"}},
		},
	}
}

func TestAddQueries_HydratePanic_ReturnsError(t *testing.T) {
	eng := newPanicSeedEngine(t)
	_, err := eng.AddQueries([]QuerySpec{panicQuery("q1")})
	if err == nil {
		t.Fatal("expected an error from a nil-PK hydrate panic, got nil")
	}
	if !strings.Contains(err.Error(), "hydrate panic") || !strings.Contains(err.Error(), "primary-key") {
		t.Fatalf("error should name the hydrate panic + nil PK; got: %v", err)
	}
}

func TestAddQueriesStream_HydratePanic_ReturnsError(t *testing.T) {
	eng := newPanicSeedEngine(t)
	err := eng.AddQueriesStream([]QuerySpec{panicQuery("q1")}, func(QueryResult) {})
	if err == nil {
		t.Fatal("expected an error from a nil-PK hydrate panic, got nil")
	}
	if !strings.Contains(err.Error(), "hydrate panic") {
		t.Fatalf("error should name the hydrate panic; got: %v", err)
	}
}

func TestFirstHydratePanic(t *testing.T) {
	built := []*pipelineEntry{{queryID: "qA"}, {queryID: "qB"}}

	if err := firstHydratePanic(built, []any{nil, nil}); err != nil {
		t.Fatalf("no panic => nil error, got %v", err)
	}

	err := firstHydratePanic(built, []any{nil, "boom"})
	if err == nil || !strings.Contains(err.Error(), "qB") {
		t.Fatalf("want error naming query qB, got %v", err)
	}

	// A *DriftError is preserved as-is so the caller's drift path can detect it.
	d := &ivm.DriftError{}
	got := firstHydratePanic(built, []any{d, nil})
	var de *ivm.DriftError
	if !errors.As(got, &de) {
		t.Fatalf("DriftError panic should be returned unwrapped, got %T (%v)", got, got)
	}
}
