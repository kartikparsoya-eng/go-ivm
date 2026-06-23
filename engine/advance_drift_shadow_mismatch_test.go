package engine

// Reproduction tests for the two shadow mismatches observed after
// enabling GO_IVM_ADVANCE_DRIVE=true on the sandbox view-syncer.
//
// ─── Analysis ───
//
// Go does NOT emit the same change-event stream as TS. It emits a
// different stream that converges to the exact same final client state.
// Two causes, only one of which is timing:
//
// Cause 1 — Frame-batching skew (the 588 channel_participants gap).
// The production userConversationsPaginated query uses a NON-scalar
// whereExists('participants', p => p.where('userId', userId)). When a
// new channel_participants row is added, both Go and TS fan it out to
// every matching parent conversation (N CHILD→ADD emissions, one per
// parent). The 588 TS-only emissions arose because Go's snapshotter
// (advanceToHead) placed the channel_participants ADD in a different
// advance batch than TS's snapshotter. Go's advance 81b34fyk7k
// contained 41 conversation EDITs (no child ADD → 0 channel_participants
// emissions). TS's same advance contained the child ADD (→ 588 fan-out).
// Both engines process the change correctly when they receive it; the
// mismatch is in WHEN, not WHETHER. The final state converges: after
// both advances complete, both clients have the channel_participants
// row present.
//
// Cause 2 — Frame-batching skew for EDIT (the 6 channel_user_status gap).
// The same channel_user_status EDIT was emitted by both engines but in
// different advance frames. Go processed it in advance 81b34fyk7k
// (among the 42 Go-only). TS processed it in advance 81b34glm9s (the
// 6 TS-only). Sum across all advances: both have the EDIT exactly once.
// Harmless — converges.
//
// The thing that would make it a BUG — same row key, different value
// between Go and TS (or Go missing a value entirely at the end) — does
// NOT happen. Every difference is presence/timing of identical-content
// rows. The SQL ground-truth oracle stayed clean.
//
// ─── Tests ───
//
// Mismatch 1 tests:
//   1. ChildAddFanOut — Go emits N child ADDs for N matching parents
//      (same as TS). Documents the fan-out behavior.
//   2. ParentEditNoChildReEmission — parent EDIT produces only the
//      parent EDIT, 0 child ADDs. This is the production scenario:
//      Go's advance had 41 conversation EDITs → 0 channel_participants.
//   3. ScalarExistsSuppressesChild — with Scalar=true AND a unique-key
//      filter, the resolver pre-resolves → IsScalar=true → 0 child
//      ADDs. Documents the IsScalar suppression path.
//
// Mismatch 2 tests:
//   1. EditEmittedInAdvance — Go correctly emits EDIT for a received
//      channel_user_status change.
//   2. EditEmittedAcrossMultipleQueries — EDIT fans out to all
//      registered queries (6 queries → 6 EDITs).
//   3. SequentialEdits — two EDITs in two advances both emit correctly.

import (
	"database/sql"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/kartikparsoya-eng/go-ivm/builder"
	"github.com/kartikparsoya-eng/go-ivm/internal/tablesource"
	"github.com/kartikparsoya-eng/go-ivm/ivm"
	"github.com/kartikparsoya-eng/go-ivm/sqlite"

	_ "github.com/mattn/go-sqlite3"
)

// ─── Mismatch 1: channel_participants fan-out ───

// setupConversationsAndParticipants creates two tables (conversations +
// channel_participants), seeds them, and registers both as TableSources.
// partUniqueKeys controls the unique keys set on channel_participants,
// which determines whether the scalar resolver can resolve a scalar EXISTS.
func setupConversationsAndParticipants(t *testing.T, convSeed, partSeed []ivm.Row, partUniqueKeys [][]string) (*Engine, *tablesource.Source, *tablesource.Source) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "replica.sqlite")
	w, _ := sql.Open("sqlite3", path)
	if _, err := w.Exec("PRAGMA journal_mode=WAL"); err != nil {
		t.Fatalf("pragma: %v", err)
	}
	if _, err := w.Exec(`CREATE TABLE conversations (
		conversationId TEXT PRIMARY KEY,
		channelId TEXT,
		createdAt INTEGER,
		lastActivityAt INTEGER,
		replyCount INTEGER
	)`); err != nil {
		t.Fatalf("create conversations: %v", err)
	}
	if _, err := w.Exec(`CREATE TABLE channel_participants (
		id TEXT PRIMARY KEY,
		channelId TEXT,
		userId TEXT,
		role TEXT
	)`); err != nil {
		t.Fatalf("create channel_participants: %v", err)
	}
	for _, row := range convSeed {
		if _, err := w.Exec(
			`INSERT INTO conversations VALUES (?, ?, ?, ?, ?)`,
			row["conversationId"], row["channelId"], row["createdAt"],
			row["lastActivityAt"], row["replyCount"],
		); err != nil {
			t.Fatalf("seed conversations: %v", err)
		}
	}
	for _, row := range partSeed {
		if _, err := w.Exec(
			`INSERT INTO channel_participants VALUES (?, ?, ?, ?)`,
			row["id"], row["channelId"], row["userId"], row["role"],
		); err != nil {
			t.Fatalf("seed channel_participants: %v", err)
		}
	}
	w.Close()

	db, _ := tablesource.Open(path, tablesource.OpenOptions{})
	wdb, _ := tablesource.OpenWritable(path, tablesource.OpenOptions{})
	t.Cleanup(func() { wdb.Close() })
	t.Cleanup(func() { db.Close() })

	convSrc, err := tablesource.New(db, wdb, "conversations",
		map[string]sqlite.ColumnSchema{
			"conversationId":  {Type: "string"},
			"channelId":      {Type: "string"},
			"createdAt":      {Type: "number"},
			"lastActivityAt": {Type: "number"},
			"replyCount":     {Type: "number"},
		},
		[]string{"conversationId"},
	)
	if err != nil {
		t.Fatalf("tablesource.New conversations: %v", err)
	}

	partSrc, err := tablesource.New(db, wdb, "channel_participants",
		map[string]sqlite.ColumnSchema{
			"id":        {Type: "string"},
			"channelId": {Type: "string"},
			"userId":    {Type: "string"},
			"role":      {Type: "string"},
		},
		[]string{"id"},
	)
	if err != nil {
		t.Fatalf("tablesource.New channel_participants: %v", err)
	}

	eng, err := NewEngine(EngineConfig{StoragePath: tempStoragePath(t)})
	if err != nil {
		t.Fatalf("NewEngine: %v", err)
	}
	t.Cleanup(func() { eng.Close() })
	eng.RegisterSource(convSrc)
	eng.RegisterSource(partSrc)

	eng.SetTableUniqueKeys("conversations", [][]string{{"conversationId"}})
	eng.SetTableUniqueKeys("channel_participants", partUniqueKeys)

	return eng, convSrc, partSrc
}

// userConversationsPaginatedAST builds the AST for a query mirroring
// the production userConversationsPaginated:
//   conversations
//     .where('replyCount', '>', 0)
//     .whereExists('participants', p => p.where('userId', userId))
//     .orderBy('lastActivityAt', 'desc')
//     .limit(limit)
//
// scalar controls whether the correlatedSubquery is marked Scalar=true.
// The production query does NOT use {scalar: true}, so scalar=false
// matches production behavior.
func userConversationsPaginatedAST(userID string, limit int, scalar bool) builder.AST {
	return builder.AST{
		Table: "conversations",
		Where: &builder.Condition{
			Type: "and",
			Conditions: []builder.Condition{
				{
					Type: "simple",
					Op:   ">",
					Left:  &builder.ValuePos{Type: "column", Name: "replyCount"},
					Right: &builder.ValuePos{Type: "literal", Value: float64(0)},
				},
				{
					Type: "correlatedSubquery",
					Op:   "EXISTS",
					Scalar: scalar,
					Related: &builder.CorrelatedSubquery{
						System: "client",
						Correlation: builder.Correlation{
							ParentField: []string{"channelId"},
							ChildField:  []string{"channelId"},
						},
						Subquery: builder.AST{
							Table: "channel_participants",
							Alias: "zsubq_participants",
							Where: &builder.Condition{
								Type: "simple",
								Op:   "=",
								Left:  &builder.ValuePos{Type: "column", Name: "userId"},
								Right: &builder.ValuePos{Type: "literal", Value: userID},
							},
						},
					},
				},
			},
		},
		OrderBy: ivm.Ordering{
			{"lastActivityAt", "desc"},
			{"conversationId", "desc"},
		},
		Limit: &limit,
	}
}

// seedConversations returns numConversations conversation rows in channel ch1,
// all with replyCount=1 (matching the WHERE replyCount > 0 filter).
func seedConversations(numConversations int) []ivm.Row {
	convSeed := make([]ivm.Row, numConversations)
	for i := 0; i < numConversations; i++ {
		convSeed[i] = ivm.Row{
			"conversationId":  fmt.Sprintf("conv-%d", i),
			"channelId":      "ch1",
			"createdAt":      float64(1000 + i),
			"lastActivityAt": float64(2000 + i),
			"replyCount":     float64(1),
		}
	}
	return convSeed
}

// TestMismatch1_ChildAddFanOut verifies that when a new channel_participants
// row is added and N parent conversations match, Go emits N CHILD→ADD
// changes (one per matching parent) — the same fan-out TS would produce.
//
// The production whereExists is NON-scalar (no {scalar: true} option),
// so the scalar resolver does not touch it. The builder creates a Join
// with Scalar=false, IsScalar=false, and the streamer does not suppress
// child emissions.
//
// In production, the 588 TS-only channel_participants ADDs arose because
// Go's snapshotter placed the child ADD in a different advance batch.
// When Go DOES receive the child ADD, it fans out identically to TS.
// Both engines produce the same N emissions for the same child ADD.
func TestMismatch1_ChildAddFanOut(t *testing.T) {
	const numConversations = 10
	const userID = "user1"

	convSeed := seedConversations(numConversations)
	partSeed := []ivm.Row{
		{"id": "part1", "channelId": "ch1", "userId": userID, "role": "ADMIN"},
	}

	// Non-scalar whereExists, unique key on {id} only — resolver cannot
	// resolve (userId is not a unique key), so IsScalar=false.
	eng, _, _ := setupConversationsAndParticipants(t, convSeed, partSeed, [][]string{{"id"}})

	ast := userConversationsPaginatedAST(userID, 50, false)
	hydrate, _, err := eng.AddQuery("q-fanout", ast)
	if err != nil {
		t.Fatalf("AddQuery: %v", err)
	}
	logChanges(t, "hydrate", hydrate)

	convCount := 0
	for _, c := range hydrate {
		if c.Table == "conversations" {
			convCount++
		}
	}
	if convCount != numConversations {
		t.Fatalf("hydrate: expected %d conversations, got %d", numConversations, convCount)
	}

	// Advance: ADD a new channel_participants row for user1 in ch1.
	r := eng.Advance([]SnapshotChange{
		{
			Table: "channel_participants",
			NextValue: ivm.Row{
				"id":        "part2",
				"channelId": "ch1",
				"userId":    userID,
				"role":      "MEMBER",
			},
		},
	})
	if r.Drift != nil {
		t.Fatalf("advance drift: %v", r.Drift)
	}
	logChanges(t, "advance (ADD channel_participants part2)", r.Changes)

	// Go should emit N channel_participants CHILD→ADD (one per matching
	// parent conversation). This is the same fan-out TS produces.
	partCount := 0
	for _, c := range r.Changes {
		if c.Table == "channel_participants" {
			partCount++
		}
	}
	if partCount != numConversations {
		t.Fatalf("Expected %d channel_participants CHILD→ADD (one per matching parent), got %d. "+
			"Both Go and TS should fan out identically for the same child ADD.",
			numConversations, partCount)
	}

	// All should be ADDs with the correct row key.
	for i, c := range r.Changes {
		if c.Table != "channel_participants" {
			continue
		}
		if c.Type != RowChangeAdd {
			t.Errorf("change[%d]: expected ADD (type=0), got type=%d", i, c.Type)
		}
		if c.RowKey["id"] != "part2" {
			t.Errorf("change[%d]: expected rowKey id=part2, got %v", i, c.RowKey)
		}
	}
}

// TestMismatch1_ParentEditNoChildReEmission verifies that when a parent
// conversation is EDITed (e.g., lastActivityAt updated), Go emits ONLY
// the conversation EDIT — 0 channel_participants child ADDs.
//
// This is the production scenario for advance 81b34fyk7k: Go's diff
// contained 41 conversation EDITs but NOT the channel_participants ADD
// (it was in a different frame). Go emitted 41 conversation EDITs
// (Go-only) and 0 channel_participants (absent from Go's diff). TS's
// diff contained the channel_participants ADD → 588 fan-out (TS-only).
//
// The Join's pushParent handler for EDIT creates an EditChange, and the
// streamer emits only the parent row (no child recursion for EDIT).
// Both Go and TS behave this way — neither re-emits child rows on
// parent EDIT.
func TestMismatch1_ParentEditNoChildReEmission(t *testing.T) {
	const numConversations = 10
	const userID = "user1"

	convSeed := seedConversations(numConversations)
	partSeed := []ivm.Row{
		{"id": "part1", "channelId": "ch1", "userId": userID, "role": "ADMIN"},
	}

	eng, _, _ := setupConversationsAndParticipants(t, convSeed, partSeed, [][]string{{"id"}})

	ast := userConversationsPaginatedAST(userID, 50, false)
	hydrate, _, err := eng.AddQuery("q-parent-edit", ast)
	if err != nil {
		t.Fatalf("AddQuery: %v", err)
	}
	logChanges(t, "hydrate", hydrate)

	// Advance: EDIT a conversation (update lastActivityAt).
	// This mirrors the production scenario where 41 conversations were
	// EDITed (lastActivityAt bumped by new messages).
	r := eng.Advance([]SnapshotChange{
		{
			Table: "conversations",
			PrevValues: []ivm.Row{
				{
					"conversationId":  "conv-0",
					"channelId":      "ch1",
					"createdAt":      float64(1000),
					"lastActivityAt": float64(2000),
					"replyCount":     float64(1),
				},
			},
			NextValue: ivm.Row{
				"conversationId":  "conv-0",
				"channelId":      "ch1",
				"createdAt":      float64(1000),
				"lastActivityAt": float64(3000),
				"replyCount":     float64(2),
			},
		},
	})
	if r.Drift != nil {
		t.Fatalf("advance drift: %v", r.Drift)
	}
	logChanges(t, "advance (EDIT conversation conv-0)", r.Changes)

	// Go should emit exactly 1 conversation EDIT and 0 channel_participants.
	convEditCount := 0
	partCount := 0
	for _, c := range r.Changes {
		if c.Table == "conversations" && c.Type == RowChangeEdit {
			convEditCount++
		}
		if c.Table == "channel_participants" {
			partCount++
		}
	}
	if convEditCount != 1 {
		t.Fatalf("Expected 1 conversation EDIT, got %d", convEditCount)
	}
	if partCount != 0 {
		t.Errorf("Expected 0 channel_participants emissions on parent EDIT (no child re-emission), got %d. "+
			"Parent EDITs do not trigger child fan-out — this is why Go's advance with 41 conversation "+
			"EDITs produced 0 channel_participants in production.", partCount)
	}
}

// TestMismatch1_ScalarExistsSuppressesChild verifies that when a scalar
// EXISTS (Scalar=true) CAN be resolved by the scalar resolver (the
// subquery filters on a column that IS a unique key), Go pre-resolves
// the EXISTS into a literal comparison, marks the child schema IsScalar=true,
// and the streamer suppresses child fan-out from the Join.
//
// This test uses a unique key on {userId} so the subquery
// channel_participants.where('userId', literal) is "simple" — all unique-
// key columns are equality-constrained by literals. The resolver resolves
// it, IsScalar=true, and the Join's child relationship emissions are
// suppressed.
//
// However, the companion pipeline still emits the new channel_participants
// row as a TOP-LEVEL ADD (not as a child of conversations). This is
// correct: the companion's job is to emit subquery rows so the client
// can re-evaluate its own EXISTS condition. The distinction from the
// non-scalar case is:
//   - Non-scalar: N CHILD→ADD from Join (one per matching parent)
//   - Scalar: 1 top-level ADD from companion (no child fan-out)
//
// This documents the IsScalar suppression path. The production query
// does NOT trigger this path (it's non-scalar and userId is not a unique
// key), but the path exists for queries that do use {scalar: true} with
// a unique-key filter.
func TestMismatch1_ScalarExistsSuppressesChild(t *testing.T) {
	const numConversations = 10
	const userID = "user1"

	convSeed := seedConversations(numConversations)
	partSeed := []ivm.Row{
		{"id": "part1", "channelId": "ch1", "userId": userID, "role": "ADMIN"},
	}

	// Scalar=true AND unique key on {userId} — the resolver CAN resolve
	// this subquery (userId is a unique key and is equality-constrained).
	eng, _, _ := setupConversationsAndParticipants(t, convSeed, partSeed, [][]string{{"id"}, {"userId"}})

	ast := userConversationsPaginatedAST(userID, 50, true)
	hydrate, _, err := eng.AddQuery("q-scalar", ast)
	if err != nil {
		t.Fatalf("AddQuery: %v", err)
	}
	logChanges(t, "hydrate", hydrate)

	// Hydrate: 10 conversations (no child relationship rows — IsScalar
	// suppresses them) + 1 companion channel_participants row (part1).
	convCount := 0
	for _, c := range hydrate {
		if c.Table == "conversations" {
			convCount++
		}
	}
	if convCount != numConversations {
		t.Fatalf("hydrate: expected %d conversations, got %d", numConversations, convCount)
	}

	// Advance: ADD a new channel_participants row.
	r := eng.Advance([]SnapshotChange{
		{
			Table: "channel_participants",
			NextValue: ivm.Row{
				"id":        "part2",
				"channelId": "ch1",
				"userId":    userID,
				"role":      "MEMBER",
			},
		},
	})
	if r.Drift != nil {
		t.Fatalf("advance drift: %v", r.Drift)
	}
	logChanges(t, "advance (ADD channel_participants part2, scalar)", r.Changes)

	// With IsScalar=true, the Join's child relationship emissions are
	// suppressed — no N-way fan-out. But the companion pipeline emits
	// the new row as 1 top-level ADD (so the client can re-evaluate
	// its EXISTS). The scalar value (channelId=ch1) didn't change, so
	// no drift/reset is triggered.
	//
	// Key distinction from non-scalar: 1 companion ADD vs N child fan-out.
	partCount := 0
	for _, c := range r.Changes {
		if c.Table == "channel_participants" {
			partCount++
		}
	}
	if partCount != 1 {
		t.Errorf("Expected 1 channel_participants ADD (companion emission, no child fan-out), got %d. "+
			"With IsScalar=true, the Join suppresses child fan-out but the companion pipeline "+
			"still emits the new row as a top-level ADD.", partCount)
	}
}

// ─── Mismatch 2: channel_user_status EDIT frame skew ───

// setupChannelUserStatus creates a channel_user_status table and registers
// it as a TableSource. Mirrors the production getChannelUserStatus /
// getAllChannelsUserStatus queries.
func setupChannelUserStatus(t *testing.T, seed []ivm.Row) (*Engine, *tablesource.Source) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "replica.sqlite")
	w, _ := sql.Open("sqlite3", path)
	if _, err := w.Exec("PRAGMA journal_mode=WAL"); err != nil {
		t.Fatalf("pragma: %v", err)
	}
	if _, err := w.Exec(`CREATE TABLE channel_user_status (
		id TEXT PRIMARY KEY,
		channelId TEXT,
		userId TEXT,
		isDeleted INTEGER,
		unreadCount INTEGER,
		lastViewedAt INTEGER,
		updatedAt INTEGER
	)`); err != nil {
		t.Fatalf("create: %v", err)
	}
	for _, row := range seed {
		if _, err := w.Exec(
			`INSERT INTO channel_user_status VALUES (?, ?, ?, ?, ?, ?, ?)`,
			row["id"], row["channelId"], row["userId"], row["isDeleted"],
			row["unreadCount"], row["lastViewedAt"], row["updatedAt"],
		); err != nil {
			t.Fatalf("seed: %v", err)
		}
	}
	w.Close()

	db, _ := tablesource.Open(path, tablesource.OpenOptions{})
	wdb, _ := tablesource.OpenWritable(path, tablesource.OpenOptions{})
	t.Cleanup(func() { wdb.Close() })
	t.Cleanup(func() { db.Close() })

	src, err := tablesource.New(db, wdb, "channel_user_status",
		map[string]sqlite.ColumnSchema{
			"id":           {Type: "string"},
			"channelId":   {Type: "string"},
			"userId":       {Type: "string"},
			"isDeleted":    {Type: "number"},
			"unreadCount":  {Type: "number"},
			"lastViewedAt": {Type: "number"},
			"updatedAt":    {Type: "number"},
		},
		[]string{"id"},
	)
	if err != nil {
		t.Fatalf("tablesource.New: %v", err)
	}

	eng, err := NewEngine(EngineConfig{StoragePath: tempStoragePath(t)})
	if err != nil {
		t.Fatalf("NewEngine: %v", err)
	}
	t.Cleanup(func() { eng.Close() })
	eng.RegisterSource(src)
	eng.SetTableUniqueKeys("channel_user_status", [][]string{{"id"}})

	return eng, src
}

// channelUserStatusAST builds the AST for getChannelUserStatus:
//   channel_user_status
//     .where('channelId', channelId)
//     .where('userId', userId)
//     .where('isDeleted', false)
//     .one()
func channelUserStatusAST(channelID, userID string) builder.AST {
	return builder.AST{
		Table: "channel_user_status",
		Where: &builder.Condition{
			Type: "and",
			Conditions: []builder.Condition{
				{
					Type: "simple", Op: "=",
					Left:  &builder.ValuePos{Type: "column", Name: "channelId"},
					Right: &builder.ValuePos{Type: "literal", Value: channelID},
				},
				{
					Type: "simple", Op: "=",
					Left:  &builder.ValuePos{Type: "column", Name: "userId"},
					Right: &builder.ValuePos{Type: "literal", Value: userID},
				},
				{
					Type: "simple", Op: "=",
					Left:  &builder.ValuePos{Type: "column", Name: "isDeleted"},
					Right: &builder.ValuePos{Type: "literal", Value: float64(0)},
				},
			},
		},
	}
}

// TestMismatch2_EditEmittedInAdvance verifies that Go correctly emits
// an EDIT for channel_user_status when it receives the change as a
// snapshot change. This confirms Go's IVM handles the EDIT path
// correctly — the production mismatch (6 TS-only EDITs) is due to
// DRIVE mode diff version skew (Go processed the same change in a
// different advance batch), NOT an IVM bug.
func TestMismatch2_EditEmittedInAdvance(t *testing.T) {
	seed := []ivm.Row{
		{
			"id":           "ae0f0bc7-6a1a-4d64-8124-48e7c472eb5f",
			"channelId":   "cmi39e2jj00fex66cheedyjc8",
			"userId":       "h32jgo10qc6r8e1mh0crc3qs",
			"isDeleted":    float64(0),
			"unreadCount":  float64(5),
			"lastViewedAt": float64(1782133000000),
			"updatedAt":    float64(1782133000000),
		},
	}

	eng, _ := setupChannelUserStatus(t, seed)

	ast := channelUserStatusAST("cmi39e2jj00fex66cheedyjc8", "h32jgo10qc6r8e1mh0crc3qs")
	hydrate, _, err := eng.AddQuery("q-cus1", ast)
	if err != nil {
		t.Fatalf("AddQuery: %v", err)
	}
	logChanges(t, "hydrate", hydrate)
	if len(hydrate) != 1 || hydrate[0].RowKey["id"] != "ae0f0bc7-6a1a-4d64-8124-48e7c472eb5f" {
		t.Fatalf("hydrate should produce 1 ADD, got %v", hydrate)
	}

	// Advance 1: EDIT the row (mark as read — unreadCount=0, update timestamps).
	// This mirrors the production change that was seen in advance 81b34fyk7k
	// (Go-only) and advance 81b34glm9s (TS-only).
	r1 := eng.Advance([]SnapshotChange{
		{
			Table: "channel_user_status",
			PrevValues: []ivm.Row{
				{
					"id":           "ae0f0bc7-6a1a-4d64-8124-48e7c472eb5f",
					"channelId":   "cmi39e2jj00fex66cheedyjc8",
					"userId":       "h32jgo10qc6r8e1mh0crc3qs",
					"isDeleted":    float64(0),
					"unreadCount":  float64(5),
					"lastViewedAt": float64(1782133000000),
					"updatedAt":    float64(1782133000000),
				},
			},
			NextValue: ivm.Row{
				"id":           "ae0f0bc7-6a1a-4d64-8124-48e7c472eb5f",
				"channelId":   "cmi39e2jj00fex66cheedyjc8",
				"userId":       "h32jgo10qc6r8e1mh0crc3qs",
				"isDeleted":    float64(0),
				"unreadCount":  float64(0),
				"lastViewedAt": float64(1782134027123),
				"updatedAt":    float64(1782134027123),
			},
		},
	})
	if r1.Drift != nil {
		t.Fatalf("advance 1 drift: %v", r1.Drift)
	}
	logChanges(t, "advance 1 (EDIT: mark as read)", r1.Changes)

	// Go should emit exactly 1 EDIT for the channel_user_status row.
	if len(r1.Changes) != 1 {
		t.Fatalf("advance 1: expected 1 EDIT, got %d changes: %v", len(r1.Changes), r1.Changes)
	}
	c := r1.Changes[0]
	if c.Type != RowChangeEdit {
		t.Fatalf("advance 1: expected EDIT (type=2), got type=%d", c.Type)
	}
	if c.Table != "channel_user_status" {
		t.Fatalf("advance 1: expected table=channel_user_status, got %s", c.Table)
	}
	if c.RowKey["id"] != "ae0f0bc7-6a1a-4d64-8124-48e7c472eb5f" {
		t.Fatalf("advance 1: expected rowKey id=ae0f0bc7..., got %v", c.RowKey)
	}
	// Verify the new values are correct.
	if c.Row["unreadCount"] != float64(0) {
		t.Errorf("advance 1: expected unreadCount=0, got %v", c.Row["unreadCount"])
	}
	if c.Row["lastViewedAt"] != float64(1782134027123) {
		t.Errorf("advance 1: expected lastViewedAt=1782134027123, got %v", c.Row["lastViewedAt"])
	}
}

// TestMismatch2_EditEmittedAcrossMultipleQueries verifies that a single
// channel_user_status EDIT is fanned out to ALL registered queries that
// include that row. In the production mismatch, 6 queries had the same
// row as a result-table or related-table. Go should emit 6 EDITs (one
// per query) when it receives the change.
func TestMismatch2_EditEmittedAcrossMultipleQueries(t *testing.T) {
	seed := []ivm.Row{
		{
			"id":           "ae0f0bc7-6a1a-4d64-8124-48e7c472eb5f",
			"channelId":   "cmi39e2jj00fex66cheedyjc8",
			"userId":       "h32jgo10qc6r8e1mh0crc3qs",
			"isDeleted":    float64(0),
			"unreadCount":  float64(5),
			"lastViewedAt": float64(1782133000000),
			"updatedAt":    float64(1782133000000),
		},
	}

	eng, _ := setupChannelUserStatus(t, seed)

	// Register 6 queries (simulating 6 clients in the same CG with
	// different query IDs but the same query shape).
	ast := channelUserStatusAST("cmi39e2jj00fex66cheedyjc8", "h32jgo10qc6r8e1mh0crc3qs")
	for i := 0; i < 6; i++ {
		qid := fmt.Sprintf("q-cus-%d", i)
		h, _, err := eng.AddQuery(qid, ast)
		if err != nil {
			t.Fatalf("AddQuery %s: %v", qid, err)
		}
		logChanges(t, fmt.Sprintf("hydrate %s", qid), h)
	}

	// Advance: EDIT the row.
	r := eng.Advance([]SnapshotChange{
		{
			Table: "channel_user_status",
			PrevValues: []ivm.Row{
				{
					"id":           "ae0f0bc7-6a1a-4d64-8124-48e7c472eb5f",
					"channelId":   "cmi39e2jj00fex66cheedyjc8",
					"userId":       "h32jgo10qc6r8e1mh0crc3qs",
					"isDeleted":    float64(0),
					"unreadCount":  float64(5),
					"lastViewedAt": float64(1782133000000),
					"updatedAt":    float64(1782133000000),
				},
			},
			NextValue: ivm.Row{
				"id":           "ae0f0bc7-6a1a-4d64-8124-48e7c472eb5f",
				"channelId":   "cmi39e2jj00fex66cheedyjc8",
				"userId":       "h32jgo10qc6r8e1mh0crc3qs",
				"isDeleted":    float64(0),
				"unreadCount":  float64(0),
				"lastViewedAt": float64(1782134027123),
				"updatedAt":    float64(1782134027123),
			},
		},
	})
	if r.Drift != nil {
		t.Fatalf("advance drift: %v", r.Drift)
	}
	logChanges(t, "advance (EDIT: mark as read, 6 queries)", r.Changes)

	// Should emit 6 EDITs (one per query).
	editCount := 0
	for _, c := range r.Changes {
		if c.Type == RowChangeEdit && c.Table == "channel_user_status" {
			editCount++
		}
	}
	if editCount != 6 {
		t.Errorf("Expected 6 EDITs (one per query), got %d — "+
			"this would explain the production mismatch where Go emitted 0 "+
			"for 6 queries in advance 81b34glm9s. If Go receives the change, "+
			"it should fan out to all queries.", editCount)
	}
}

// TestMismatch2_SequentialEdits verifies that Go correctly emits EDITs
// in both advance 1 and advance 2 when the same row is edited twice.
// This rules out a state corruption where advance 1's edit leaves the
// Take/Join state inconsistent for advance 2.
func TestMismatch2_SequentialEdits(t *testing.T) {
	seed := []ivm.Row{
		{
			"id":           "ae0f0bc7-6a1a-4d64-8124-48e7c472eb5f",
			"channelId":   "cmi39e2jj00fex66cheedyjc8",
			"userId":       "h32jgo10qc6r8e1mh0crc3qs",
			"isDeleted":    float64(0),
			"unreadCount":  float64(5),
			"lastViewedAt": float64(1782133000000),
			"updatedAt":    float64(1782133000000),
		},
	}

	eng, _ := setupChannelUserStatus(t, seed)

	ast := channelUserStatusAST("cmi39e2jj00fex66cheedyjc8", "h32jgo10qc6r8e1mh0crc3qs")
	hydrate, _, err := eng.AddQuery("q-seq-edit", ast)
	if err != nil {
		t.Fatalf("AddQuery: %v", err)
	}
	logChanges(t, "hydrate", hydrate)
	if len(hydrate) != 1 {
		t.Fatalf("hydrate should produce 1 ADD, got %d", len(hydrate))
	}

	// Advance 1: EDIT (mark as read — unreadCount 5→0)
	r1 := eng.Advance([]SnapshotChange{
		{
			Table: "channel_user_status",
			PrevValues: []ivm.Row{{
				"id": "ae0f0bc7-6a1a-4d64-8124-48e7c472eb5f", "channelId": "cmi39e2jj00fex66cheedyjc8",
				"userId": "h32jgo10qc6r8e1mh0crc3qs", "isDeleted": float64(0),
				"unreadCount": float64(5), "lastViewedAt": float64(1782133000000), "updatedAt": float64(1782133000000),
			}},
			NextValue: ivm.Row{
				"id": "ae0f0bc7-6a1a-4d64-8124-48e7c472eb5f", "channelId": "cmi39e2jj00fex66cheedyjc8",
				"userId": "h32jgo10qc6r8e1mh0crc3qs", "isDeleted": float64(0),
				"unreadCount": float64(0), "lastViewedAt": float64(1782134027123), "updatedAt": float64(1782134027123),
			},
		},
	})
	if r1.Drift != nil {
		t.Fatalf("advance 1 drift: %v", r1.Drift)
	}
	logChanges(t, "advance 1 (EDIT: unreadCount 5→0)", r1.Changes)
	if len(r1.Changes) != 1 || r1.Changes[0].Type != RowChangeEdit {
		t.Fatalf("advance 1: expected 1 EDIT, got %v", r1.Changes)
	}

	// Advance 2: EDIT again (new message — unreadCount 0→3)
	r2 := eng.Advance([]SnapshotChange{
		{
			Table: "channel_user_status",
			PrevValues: []ivm.Row{{
				"id": "ae0f0bc7-6a1a-4d64-8124-48e7c472eb5f", "channelId": "cmi39e2jj00fex66cheedyjc8",
				"userId": "h32jgo10qc6r8e1mh0crc3qs", "isDeleted": float64(0),
				"unreadCount": float64(0), "lastViewedAt": float64(1782134027123), "updatedAt": float64(1782134027123),
			}},
			NextValue: ivm.Row{
				"id": "ae0f0bc7-6a1a-4d64-8124-48e7c472eb5f", "channelId": "cmi39e2jj00fex66cheedyjc8",
				"userId": "h32jgo10qc6r8e1mh0crc3qs", "isDeleted": float64(0),
				"unreadCount": float64(3), "lastViewedAt": float64(1782134027123), "updatedAt": float64(1782134500000),
			},
		},
	})
	if r2.Drift != nil {
		t.Fatalf("advance 2 drift: %v", r2.Drift)
	}
	logChanges(t, "advance 2 (EDIT: unreadCount 0→3)", r2.Changes)
	if len(r2.Changes) != 1 || r2.Changes[0].Type != RowChangeEdit {
		t.Fatalf("advance 2: expected 1 EDIT, got %v", r2.Changes)
	}
	if r2.Changes[0].Row["unreadCount"] != float64(3) {
		t.Errorf("advance 2: expected unreadCount=3, got %v", r2.Changes[0].Row["unreadCount"])
	}
}
