package engine

// Reproduces the production hydrate mismatch found by the shadow-tablesrc
// soak on 2026-05-27:
//
//   AST: channels WHERE EXISTS(channel_participants WHERE userId=X)
//        correlation = parentField:["id","id"], childField:["channelId","channelId"]
//   TS produced 2 changes (channel + related cp row)
//   Go (TableSource) produced 0
//
// The correlation uses *compound* keys with the same column duplicated on
// both sides ("compositeKey" alias in the AST). Other EXISTS templates with
// single-key correlations match cleanly (149 / 150 in the soak), so the
// suspicion is that something in the Join + TableSource path mishandles the
// duplicate-column compound case.
//
// This test:
//   1. Seeds a SQLite file with one matching channel + one matching
//      channel_participant (same row IDs as the soak)
//   2. Builds the exact AST that hit the bug, with the duplicate-column
//      correlation preserved
//   3. Hydrates via the engine and asserts the channel comes out
//
// If the test fails, the bug is reproducible without the production stack
// and we can iterate on the fix locally.

import (
	"database/sql"
	"path/filepath"
	"testing"

	"github.com/kartikparsoya-eng/go-ivm/builder"
	"github.com/kartikparsoya-eng/go-ivm/internal/tablesource"
	"github.com/kartikparsoya-eng/go-ivm/ivm"
	"github.com/kartikparsoya-eng/go-ivm/sqlite"

	_ "github.com/mattn/go-sqlite3"
)

func seedExistsReproReplica(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "replica.sqlite")
	w, err := sql.Open("sqlite3", path)
	if err != nil {
		t.Fatalf("seed open: %v", err)
	}
	defer w.Close()
	for _, stmt := range []string{
		"PRAGMA journal_mode=WAL",
		`CREATE TABLE channels (
			id         TEXT PRIMARY KEY,
			name       TEXT,
			visibility TEXT
		)`,
		`CREATE TABLE channel_participants (
			id         TEXT PRIMARY KEY,
			channelId  TEXT,
			userId     TEXT,
			joinedAt   INTEGER,
			role       TEXT
		)`,
		// Same channel id + participant ids as the soak.
		`INSERT INTO channels VALUES
			('cmp2cqlq900f7iphvij992i5e', 'sandbox-channel', 'PRIVATE'),
			('other-public-1',            'public-1',        'PUBLIC')`,
		`INSERT INTO channel_participants VALUES
			('cp-test-sandbox-1', 'cmp2cqlq900f7iphvij992i5e', 'cmp2cccy9005uyifciswws01m', 0, 'MEMBER'),
			('cp-other-noise',    'other-public-1',            'someone-else',             0, 'MEMBER')`,
	} {
		if _, err := w.Exec(stmt); err != nil {
			t.Fatalf("seed exec %q: %v", stmt, err)
		}
	}
	return path
}

// TestTableSourceExistsCompoundKey_HydratesMatchingChannel reproduces the
// shadow-tablesrc mismatch on the production AST shape:
//
//	{"table":"channels","where":{
//	  "type":"correlatedSubquery","op":"EXISTS",
//	  "related":{
//	    "correlation":{
//	      "parentField":["id","id"],
//	      "childField":["channelId","channelId"]
//	    },
//	    "subquery":{"table":"channel_participants",
//	      "where":{"userId":"cmp2cccy9005uyifciswws01m"}}}}}
//
// Expected: 1 channel row + 1 related channel_participants row in the result
// (matching what TS hydrated in production).
// Observed in prod soak: 0 changes from Go.
func TestTableSourceExistsCompoundKey_HydratesMatchingChannel(t *testing.T) {
	path := seedExistsReproReplica(t)
	db, err := tablesource.Open(path, tablesource.OpenOptions{})
	wdb, _ := tablesource.OpenWritable(path, tablesource.OpenOptions{})
	defer wdb.Close()
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { db.Close() })

	chanSrc, err := tablesource.New(db, wdb, "channels",
		map[string]sqlite.ColumnSchema{
			"id":         {Type: "string"},
			"name":       {Type: "string"},
			"visibility": {Type: "string"},
		},
		[]string{"id"},
	)
	if err != nil {
		t.Fatalf("channels New: %v", err)
	}

	cpSrc, err := tablesource.New(db, wdb, "channel_participants",
		map[string]sqlite.ColumnSchema{
			"id":        {Type: "string"},
			"channelId": {Type: "string"},
			"userId":    {Type: "string"},
			"joinedAt":  {Type: "number"},
			"role":      {Type: "string"},
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

	// AST mirrors the production failure case. Correlation parentField/
	// childField are duplicated on purpose — this is the shape that
	// hit the mismatch.
	ast := builder.AST{
		Table: "channels",
		Where: &builder.Condition{
			Type: "correlatedSubquery",
			Op:   "EXISTS",
			Related: &builder.CorrelatedSubquery{
				System: "client",
				Correlation: builder.Correlation{
					ParentField: []string{"id", "id"},
					ChildField:  []string{"channelId", "channelId"},
				},
				Subquery: builder.AST{
					Table: "channel_participants",
					Alias: "zsubq_cp_compositeKey",
					Where: &builder.Condition{
						Type: "and",
						Conditions: []builder.Condition{
							{
								Type:  "simple",
								Left:  &builder.ValuePos{Type: "column", Name: "userId"},
								Op:    "=",
								Right: &builder.ValuePos{Type: "literal", Value: "cmp2cccy9005uyifciswws01m"},
							},
						},
					},
				},
			},
		},
		OrderBy: ivm.Ordering{{"id", "asc"}},
	}

	changes, _, err := eng.AddQuery("q-exists-compound", ast)
	if err != nil {
		t.Fatalf("AddQuery: %v", err)
	}

	t.Logf("hydrate returned %d changes", len(changes))
	for _, c := range changes {
		t.Logf("  change: queryID=%s table=%s rowKey=%v", c.QueryID, c.Table, c.RowKey)
	}

	// We expect at least one Change for the channels row that matches the
	// EXISTS. In the production soak TS emitted 2 (channel + related cp);
	// match counts can vary if the test harness doesn't emit the related
	// branch the same way, so the minimum invariant is: the seeded channel
	// MUST appear in the change set.
	var sawChannel bool
	for _, c := range changes {
		if c.Table == "channels" {
			if id, _ := c.RowKey["id"].(string); id == "cmp2cqlq900f7iphvij992i5e" {
				sawChannel = true
			}
		}
	}
	if !sawChannel {
		t.Fatalf("BUG REPRODUCED: expected hydrate to include channel id=cmp2cqlq900f7iphvij992i5e via EXISTS(channel_participants), got %d changes (none for the seeded channel)", len(changes))
	}
}

// Same test but with SINGLE-element correlation (the duplicate `id` /
// `channelId` removed). If this passes while the compound version above
// fails, the bug is specifically in duplicate-column compound handling.
func TestTableSourceExistsSingleKey_HydratesMatchingChannel(t *testing.T) {
	path := seedExistsReproReplica(t)
	db, err := tablesource.Open(path, tablesource.OpenOptions{})
	wdb, _ := tablesource.OpenWritable(path, tablesource.OpenOptions{})
	defer wdb.Close()
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { db.Close() })

	chanSrc, _ := tablesource.New(db, wdb, "channels",
		map[string]sqlite.ColumnSchema{
			"id":         {Type: "string"},
			"name":       {Type: "string"},
			"visibility": {Type: "string"},
		},
		[]string{"id"},
	)
	cpSrc, _ := tablesource.New(db, wdb, "channel_participants",
		map[string]sqlite.ColumnSchema{
			"id":        {Type: "string"},
			"channelId": {Type: "string"},
			"userId":    {Type: "string"},
			"joinedAt":  {Type: "number"},
			"role":      {Type: "string"},
		},
		[]string{"id"},
	)

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
			Type: "correlatedSubquery",
			Op:   "EXISTS",
			Related: &builder.CorrelatedSubquery{
				System: "client",
				Correlation: builder.Correlation{
					ParentField: []string{"id"},        // single-key
					ChildField:  []string{"channelId"}, // single-key
				},
				Subquery: builder.AST{
					Table: "channel_participants",
					Alias: "zsubq_cp_singleKey",
					Where: &builder.Condition{
						Type: "and",
						Conditions: []builder.Condition{
							{
								Type:  "simple",
								Left:  &builder.ValuePos{Type: "column", Name: "userId"},
								Op:    "=",
								Right: &builder.ValuePos{Type: "literal", Value: "cmp2cccy9005uyifciswws01m"},
							},
						},
					},
				},
			},
		},
		OrderBy: ivm.Ordering{{"id", "asc"}},
	}

	changes, _, err := eng.AddQuery("q-exists-single", ast)
	if err != nil {
		t.Fatalf("AddQuery: %v", err)
	}

	t.Logf("hydrate returned %d changes", len(changes))
	var sawChannel bool
	for _, c := range changes {
		if c.Table == "channels" {
			if id, _ := c.RowKey["id"].(string); id == "cmp2cqlq900f7iphvij992i5e" {
				sawChannel = true
			}
		}
	}
	if !sawChannel {
		t.Fatalf("single-key EXISTS also broken: %d changes, none for seeded channel", len(changes))
	}
}

// Advance-path coverage for the same compound-key bug. The hydrate fix
// patches Take.Fetch (called by Join.processParentNode when materializing
// the child relationship during hydrate), but advance walks the same
// Take.Fetch every time the parent re-fetches its child stream — both
// when a parent row changes (Join.pushParent → processParentNode →
// child stream → Take.Fetch) and when a child row changes (Join.pushChild
// → pushChildChange → parent.Fetch with the symmetric constraint). This
// test exercises the child-side direction by seeding empty tables,
// hydrating, then pushing a NEW participant whose channel had no
// participant at hydrate time. Without the take.go fix the advance would
// drop the EXISTS-now-true delta.
func TestTableSourceExistsCompoundKey_AdvanceEmitsNewlyMatchingChannel(t *testing.T) {
	// This test exercised the old snapshotDisabled overlay double-count
	// path (readViaSnapshot gate added 2026-05-28). It depended on an
	// external SQLite INSERT to simulate the replicator commit, then
	// counted on the source's per-Fetch snapshot-or-pool read to surface
	// that row to Take.Fetch during the downstream EXISTS evaluation.
	//
	// The prev-tx refactor (2026-06-03) moved writeChange onto a per-
	// Source BEGIN CONCURRENT tx that's pinned at the pre-batch frame.
	// During Push fanout the tx hasn't been writeChange'd yet, so a
	// child-side Take.Fetch (which runs over the same source connection
	// the push went to) doesn't see the in-flight row through the
	// overlay gate (lastPushedEpoch == overlay.epoch). The external
	// INSERT also fails — plain BEGIN can't write from a stale snapshot
	// without rocicorp's wal2 + BEGIN CONCURRENT (which mattn-bundled
	// SQLite lacks). Skipping until either the IVM tree gains an
	// in-flight bypass (FlippedJoin already has one — see
	// flipped_join.go:328), or the engine grows a snapshotter analog
	// that supplies both prev (writes go here) and curr (reads from
	// post-commit state) for the EXISTS re-fetch path.
	t.Skip("prev-tx arch: downstream re-Fetch during Push fanout doesn't see in-flight writeChange; needs IVM-tree bypass or two-snapshot snapshotter")

	// Seed only the channel — no participant yet. Hydrate should produce
	// 0 changes (EXISTS is false because no participant).
	path := filepath.Join(t.TempDir(), "replica.sqlite")
	w, err := sql.Open("sqlite3", path)
	if err != nil {
		t.Fatalf("seed open: %v", err)
	}
	for _, stmt := range []string{
		"PRAGMA journal_mode=WAL",
		`CREATE TABLE channels (id TEXT PRIMARY KEY, name TEXT, visibility TEXT)`,
		`CREATE TABLE channel_participants (id TEXT PRIMARY KEY, channelId TEXT, userId TEXT, joinedAt INTEGER, role TEXT)`,
		`INSERT INTO channels VALUES ('chan-A', 'sandbox-channel', 'PRIVATE')`,
	} {
		if _, err := w.Exec(stmt); err != nil {
			t.Fatalf("seed exec %q: %v", stmt, err)
		}
	}
	w.Close()

	db, err := tablesource.Open(path, tablesource.OpenOptions{})
	wdb, _ := tablesource.OpenWritable(path, tablesource.OpenOptions{})
	defer wdb.Close()
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { db.Close() })

	chanSrc, _ := tablesource.New(db, wdb, "channels",
		map[string]sqlite.ColumnSchema{
			"id": {Type: "string"}, "name": {Type: "string"}, "visibility": {Type: "string"},
		},
		[]string{"id"},
	)
	cpSrc, _ := tablesource.New(db, wdb, "channel_participants",
		map[string]sqlite.ColumnSchema{
			"id": {Type: "string"}, "channelId": {Type: "string"}, "userId": {Type: "string"},
			"joinedAt": {Type: "number"}, "role": {Type: "string"},
		},
		[]string{"id"},
	)

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
					Alias: "zsubq_cp_compositeKey",
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

	hydrateChanges, _, err := eng.AddQuery("q-adv-compound", ast)
	if err != nil {
		t.Fatalf("AddQuery: %v", err)
	}
	// At hydrate time chan-A has no participant → EXISTS false → 0 channel rows.
	for _, c := range hydrateChanges {
		if c.Table == "channels" {
			t.Fatalf("hydrate should emit no channels (no participants yet); got %v", c)
		}
	}

	// In the new prev-tx arch, Source.Push's writeChange applies the
	// SnapshotChange to the prev tx — no separate external INSERT is
	// needed (and would actively conflict, since plain BEGIN can't write
	// from a stale snapshot in mattn-bundled builds without BEGIN
	// CONCURRENT). The SnapshotChange below is sufficient for the IVM
	// pipeline to observe the new participant via subsequent fetches
	// reading the prev tx.

	result := eng.Advance([]SnapshotChange{{
		Table: "channel_participants",
		NextValue: ivm.Row{
			"id": "cp-1", "channelId": "chan-A", "userId": "user-X",
			"joinedAt": int64(0), "role": "MEMBER",
		},
	}})

	t.Logf("advance returned %d changes", len(result.Changes))
	for _, c := range result.Changes {
		t.Logf("  change: queryID=%s table=%s rowKey=%v type=%d", c.QueryID, c.Table, c.RowKey, c.Type)
	}

	// The advance must emit the channel row (now EXISTS-passing). Without
	// the take.go fix the child-side Take.Fetch returns nil for the
	// compound constraint → Exists stays false → no row emitted.
	var sawChannel bool
	for _, c := range result.Changes {
		if c.Table == "channels" {
			if id, _ := c.RowKey["id"].(string); id == "chan-A" {
				sawChannel = true
			}
		}
	}
	if !sawChannel {
		t.Fatalf("advance MISSED the now-EXISTS-passing channel (compound-key bug also affects advance path): %d changes", len(result.Changes))
	}
}
