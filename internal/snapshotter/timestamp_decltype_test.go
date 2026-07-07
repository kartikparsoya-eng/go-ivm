package snapshotter

// Regression for the advance leg of ART G15 (2026-07-07): the snapshotter's
// GetRow/GetRows SELECTs read changed-row contents from the replica, and a
// NULLABLE temporal column (declared exactly "timestamp"/"datetime"/"date")
// went through mattn/go-sqlite3's decltype conversion — INTEGER |v| <= 1e12
// ⇒ time.Time via the SECONDS heuristic — which coerceRow's old UnixMilli
// reversal turned into v*1000. The advance NextValue/PrevValues for any
// pre-2001 timestamp shipped ×1000, same class as the hydrate leg
// (internal/tablesource/timestamp_decltype_test.go, where the full chain is
// documented).
//
// Fix: selectColList wraps every column in the unary-+ no-op, stripping the
// declared type so the driver ships the raw cell — byte-identical to TS's
// better-sqlite3 snapshotter read. This test drives the REAL advance path
// (changeLog → Advance → Diff.Collect → GetRow → coerceRow) and fails
// pre-fix with NextValue["ts"] = 1000.

import (
	"testing"

	"github.com/kartikparsoya-eng/go-ivm/sqlite"
)

func (f *fixture) createEventTable() {
	// Nullable temporal column declared with the BARE type (the shape that
	// hits mattn's decltype match); NOT-null one with the "|NOT_NULL"
	// suffix zero-cache's lite schema uses.
	f.exec(`CREATE TABLE "event" (
		"id" TEXT PRIMARY KEY,
		"ts" timestamp,
		"tsnn" "timestamp|NOT_NULL",
		"_0_version" TEXT)`)
}

func eventSpec() *TableSpec {
	return &TableSpec{
		Name: "event",
		Columns: map[string]sqlite.ColumnSchema{
			"id":         {Type: "string"},
			"ts":         {Type: "number", Optional: true},
			"tsnn":       {Type: "number"},
			"_0_version": {Type: "string"},
		},
		UniqueKeys: [][]string{{"id"}},
	}
}

func TestDiff_NullableTemporalColumnShipsRawEpochMs(t *testing.T) {
	f := newFixture(t)
	f.createEventTable()
	f.setStateVersion(ver(1))
	f.initSnapshotter()

	// V2: insert a row whose temporal columns hold epoch-ms 1
	// (1970-01-01T00:00:00.001Z — inside mattn's |v| <= 1e12 seconds
	// window). Pre-fix the diff emitted NextValue["ts"] = 1000.
	f.exec(`INSERT INTO "event" VALUES (?,?,?,?)`, "e1", int64(1), int64(1), ver(2))
	f.logSet(ver(2), 0, "event", `{"id":"e1"}`)
	f.setStateVersion(ver(2))

	diff, err := f.snap.Advance(syncable(eventSpec()), allNames("event"))
	if err != nil {
		t.Fatalf("Advance: %v", err)
	}
	changes, err := diff.Collect()
	if err != nil {
		t.Fatalf("Collect: %v", err)
	}
	c := findChange(t, changes, "e1")
	if c.NextValue == nil {
		t.Fatalf("no NextValue for e1: %+v", c)
	}
	if got := c.NextValue["ts"]; got != float64(1) {
		t.Errorf("NextValue[ts] = %#v (%T), want raw 1 — nullable temporal column ×1000 on the advance path", got, got)
	}
	if got := c.NextValue["tsnn"]; got != float64(1) {
		t.Errorf("NextValue[tsnn] = %#v, want 1 — NOT_NULL temporal column regressed", got)
	}
}
