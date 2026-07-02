package engine

// End-to-end parity test for the LazyAdvance streaming leaf fetch
// (tablesource.LazyAdvance / GO_IVM_LAZY_ADVANCE): the SAME advance batches,
// run through the full engine + operator stack (EXISTS Join + Exists + Take +
// streamer), must produce identical RowChanges with the flag off and on.
//
// The scenario is the compound-EXISTS shape from
// exists_compound_repro_test.go because it exercises every lazy-path
// ingredient during push processing:
//   - Join.pushChildChange → parent.Fetch (a lazy cursor on channels held
//     open WHILE child changes are pushed through it),
//   - Exists re-check → child fetch (nested lazy cursor on
//     channel_participants, same prev-tx conn),
//   - the in-flight overlay row spliced into those fetches via the epoch
//     gate (the pushed participant isn't in the prev tx during fanout),
//   - Edit and Remove splice plans flipping EXISTS back off,
//   - a two-change batch where the second push's fetches must see the first
//     push's writeChange in the prev tx (intra-batch read-your-writes
//     through the live cursor).
//
// Each advance shape gets a FRESH engine + seeded replica because the mattn
// test build has no BEGIN CONCURRENT: a Source prev tx (plain BEGIN) cannot
// writeChange after an external writer commits past its snapshot, and batch
// rollback discards writeChanges between advances — so multi-advance chains
// on one engine can't be made self-consistent here. One seeded fixture per
// advance sidesteps both (matching how exists_compound_repro_test.go's
// advance test is built).

import (
	"database/sql"
	"fmt"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/kartikparsoya-eng/go-ivm/builder"
	"github.com/kartikparsoya-eng/go-ivm/internal/tablesource"
	"github.com/kartikparsoya-eng/go-ivm/ivm"
	"github.com/kartikparsoya-eng/go-ivm/sqlite"

	_ "github.com/mattn/go-sqlite3"
)

func lazyParityCPRow(user string) ivm.Row {
	return ivm.Row{
		"id": "cp-1", "channelId": "chan-A", "userId": user,
		"joinedAt": int64(0), "role": "MEMBER",
	}
}

// runLazyParityAdvance builds a fresh replica (optionally pre-seeding cp-1
// for user-X), hydrates the compound-EXISTS query, runs ONE advance batch,
// and returns its RowChanges. Drift is fatal — every batch here is
// constructed to be valid against the seeded prev tx.
func runLazyParityAdvance(t *testing.T, seedCP bool, batch []SnapshotChange) []RowChange {
	t.Helper()

	path := filepath.Join(t.TempDir(), "replica.sqlite")
	w, err := sql.Open("sqlite3", path)
	if err != nil {
		t.Fatalf("seed open: %v", err)
	}
	seed := []string{
		"PRAGMA journal_mode=WAL",
		`CREATE TABLE channels (id TEXT PRIMARY KEY, name TEXT, visibility TEXT)`,
		`CREATE TABLE channel_participants (id TEXT PRIMARY KEY, channelId TEXT, userId TEXT, joinedAt INTEGER, role TEXT)`,
		`INSERT INTO channels VALUES ('chan-A', 'sandbox-channel', 'PRIVATE'), ('chan-B', 'other', 'PUBLIC')`,
	}
	if seedCP {
		seed = append(seed, `INSERT INTO channel_participants VALUES ('cp-1', 'chan-A', 'user-X', 0, 'MEMBER')`)
	}
	for _, stmt := range seed {
		if _, err := w.Exec(stmt); err != nil {
			t.Fatalf("seed exec %q: %v", stmt, err)
		}
	}
	w.Close()

	db, err := tablesource.Open(path, tablesource.OpenOptions{})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	wdb, err := tablesource.OpenWritable(path, tablesource.OpenOptions{})
	if err != nil {
		t.Fatalf("OpenWritable: %v", err)
	}
	t.Cleanup(func() { wdb.Close() })

	chanSrc, err := tablesource.New(db, wdb, "channels",
		map[string]sqlite.ColumnSchema{
			"id": {Type: "string"}, "name": {Type: "string"}, "visibility": {Type: "string"},
		},
		[]string{"id"},
	)
	if err != nil {
		t.Fatalf("channels New: %v", err)
	}
	cpSrc, err := tablesource.New(db, wdb, "channel_participants",
		map[string]sqlite.ColumnSchema{
			"id": {Type: "string"}, "channelId": {Type: "string"}, "userId": {Type: "string"},
			"joinedAt": {Type: "number"}, "role": {Type: "string"},
		},
		[]string{"id"},
	)
	if err != nil {
		t.Fatalf("channel_participants New: %v", err)
	}

	eng, err := NewEngine(EngineConfig{StoragePath: tempStoragePath(t)})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { eng.Close() })
	eng.RegisterSource(chanSrc)
	eng.RegisterSource(cpSrc)
	eng.SetTableUniqueKeys("channels", [][]string{{"id"}})
	eng.SetTableUniqueKeys("channel_participants", [][]string{{"id"}})

	ast := builder.AST{
		Table: "channels",
		Where: &builder.Condition{
			Type: "correlatedSubquery", Op: "EXISTS",
			Related: &builder.CorrelatedSubquery{
				System: "client",
				Correlation: builder.Correlation{
					ParentField: []string{"id", "id"},
					ChildField:  []string{"channelId", "channelId"},
				},
				Subquery: builder.AST{
					Table: "channel_participants",
					Alias: "zsubq_cp_lazyparity",
					Where: &builder.Condition{
						Type: "and",
						Conditions: []builder.Condition{{
							Type:  "simple",
							Left:  &builder.ValuePos{Type: "column", Name: "userId"},
							Op:    "=",
							Right: &builder.ValuePos{Type: "literal", Value: "user-X"},
						}},
					},
				},
			},
		},
		OrderBy: ivm.Ordering{{"id", "asc"}},
	}

	if _, _, err := eng.AddQuery("q-lazy-parity", ast); err != nil {
		t.Fatalf("AddQuery: %v", err)
	}

	r := eng.Advance(batch)
	if r.Drift != nil {
		t.Fatalf("advance drifted (batch %+v): %+v", batch, r.Drift)
	}
	return r.Changes
}

// lazyParityBatches returns the advance shapes under test, each paired with
// whether its fixture pre-seeds cp-1 in the replica (so the prev tx contains
// it and Edit/Remove validate).
func lazyParityBatches() []struct {
	name   string
	seedCP bool
	batch  []SnapshotChange
} {
	return []struct {
		name   string
		seedCP bool
		batch  []SnapshotChange
	}{
		{
			// EXISTS flips true mid-fanout: the pushed row reaches the
			// re-check only via the overlay splice.
			name:   "addFlipsExistsOn",
			seedCP: false,
			batch: []SnapshotChange{{
				Table:     "channel_participants",
				NextValue: lazyParityCPRow("user-X"),
			}},
		},
		{
			// Edit splice plan (remove old + add new): userId moves off the
			// subquery filter → EXISTS flips false → channel Remove.
			name:   "editFlipsExistsOff",
			seedCP: true,
			batch: []SnapshotChange{{
				Table:      "channel_participants",
				PrevValues: []ivm.Row{lazyParityCPRow("user-X")},
				NextValue:  lazyParityCPRow("user-OTHER"),
			}},
		},
		{
			// Remove splice: participant deleted → EXISTS flips false.
			name:   "removeFlipsExistsOff",
			seedCP: true,
			batch: []SnapshotChange{{
				Table:      "channel_participants",
				PrevValues: []ivm.Row{lazyParityCPRow("user-X")},
			}},
		},
		{
			// Two changes in ONE batch: the second push's fetches must see
			// the first push's writeChange through the prev tx (intra-batch
			// read-your-writes on the live cursor), and chan-B flips on
			// while chan-A flips off.
			name:   "twoChangeBatch",
			seedCP: true,
			batch: []SnapshotChange{
				{
					Table: "channel_participants",
					NextValue: ivm.Row{
						"id": "cp-2", "channelId": "chan-B", "userId": "user-X",
						"joinedAt": int64(1), "role": "MEMBER",
					},
				},
				{
					Table:      "channel_participants",
					PrevValues: []ivm.Row{lazyParityCPRow("user-X")},
				},
			},
		},
	}
}

func TestAdvanceLazyLeafParity_CompoundExists(t *testing.T) {
	prev := tablesource.LazyAdvance
	defer func() { tablesource.LazyAdvance = prev }()

	for _, sc := range lazyParityBatches() {
		t.Run(sc.name, func(t *testing.T) {
			tablesource.LazyAdvance = false
			base := runLazyParityAdvance(t, sc.seedCP, sc.batch)

			tablesource.LazyAdvance = true
			lazy := runLazyParityAdvance(t, sc.seedCP, sc.batch)

			if !reflect.DeepEqual(base, lazy) {
				t.Fatalf("lazy advance diverged from eager:\n eager (%d): %+v\n lazy  (%d): %+v",
					len(base), base, len(lazy), lazy)
			}

			// Non-degeneracy: every shape here must emit at least one
			// channels row change, otherwise parity compared empties.
			saw := false
			for _, c := range base {
				if c.Table == "channels" {
					saw = true
				}
			}
			if !saw {
				t.Fatalf("scenario degenerate (no channels change): %s", describeChanges(base))
			}
		})
	}
}

func describeChanges(cs []RowChange) string {
	if len(cs) == 0 {
		return "0 changes"
	}
	out := ""
	for _, c := range cs {
		out += fmt.Sprintf("{type=%d table=%s key=%v} ", c.Type, c.Table, c.RowKey)
	}
	return out
}
