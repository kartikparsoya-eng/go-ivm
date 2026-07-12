package engine

// Tests that a nested flipped-EXISTS query does not over-emit rows from
// the inner correlated subquery.
//
// Query shape: channelStats(channelId=X) with WHERE containing
// flipped-EXISTS(channels WHERE id=channelId AND
//   (visibility='PUBLIC' OR
//    flipped-EXISTS(channel_participants WHERE channelId=channels.id AND userId=Y)))
//
// When both EXISTS clauses are flipped, the OR branches are merged via
// UnionFanOut/UnionFanIn with branch-order-aware dedup. The PUBLIC branch's
// channels node (no inner relationship) shadows the FlippedJoin branch's
// channels node (with the channel_participants relationship attached). The
// merged node carries no inner relationship, so the streamer should not
// recurse into participants. Expected output: 2 RowChanges
// (channel_stats + channels), not 3.

import (
	"testing"

	"github.com/kartikparsoya-eng/go-ivm/builder"
	"github.com/kartikparsoya-eng/go-ivm/ivm"
)

func TestChannelStats_DoesNotOverEmitNestedExistsRow(t *testing.T) {
	const channelID = "ch-public-1"
	const userID = "user-soak-1"

	// Three MemorySource leaves matching the production schema.
	channelStats := ivm.NewMemorySource("channel_stats",
		map[string]string{
			"channelId": "string",
			"messages":  "number",
			"unread":    "number",
		},
		[]string{"channelId"},
	)
	channels := ivm.NewMemorySource("channels",
		map[string]string{
			"id":         "string",
			"visibility": "string",
		},
		[]string{"id"},
	)
	channelParticipants := ivm.NewMemorySource("channel_participants",
		map[string]string{
			"id":        "string",
			"channelId": "string",
			"userId":    "string",
		},
		[]string{"id"},
	)

	// Seed: one public channel with a stats row + an inner participant row.
	// Channel is PUBLIC so the OR's first branch matches; the participant row
	// exists so the second branch ALSO matches. Both branches fire — and that's
	// when Go over-emits.
	channelStats.Push(ivm.MakeSourceChangeAdd(ivm.Row{
		"channelId": channelID,
		"messages":  float64(5),
		"unread":    float64(2),
	}))
	channels.Push(ivm.MakeSourceChangeAdd(ivm.Row{
		"id":         channelID,
		"visibility": "PUBLIC",
	}))
	channelParticipants.Push(ivm.MakeSourceChangeAdd(ivm.Row{
		"id":        "cp-soak-" + channelID,
		"channelId": channelID,
		"userId":    userID,
	}))

	eng, err := NewEngine(EngineConfig{StoragePath: tempStoragePath(t)})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { eng.Close() })
	eng.RegisterMemorySource(channelStats)
	eng.RegisterMemorySource(channels)
	eng.RegisterMemorySource(channelParticipants)
	eng.SetTableUniqueKeys("channel_stats", [][]string{{"channelId"}})
	eng.SetTableUniqueKeys("channels", [][]string{{"id"}})
	eng.SetTableUniqueKeys("channel_participants", [][]string{{"id"}})

	// AST mirrors the production failure shape: outer channelStats with
	// nested EXISTS chains.
	ast := builder.AST{
		Table: "channel_stats",
		Where: &builder.Condition{
			Type: "and",
			Conditions: []builder.Condition{
				{
					Type:  "simple",
					Op:    "=",
					Left:  &builder.ValuePos{Type: "column", Name: "channelId"},
					Right: &builder.ValuePos{Type: "literal", Value: channelID},
				},
				{
					Type: "correlatedSubquery",
					Op:   "EXISTS",
					Flip: true,
					Related: &builder.CorrelatedSubquery{
						System: "client",
						Correlation: builder.Correlation{
							ParentField: []string{"channelId"},
							ChildField:  []string{"id"},
						},
						Subquery: builder.AST{
							Table: "channels",
							Alias: "zsubq_channel",
							Where: &builder.Condition{
								Type: "or",
								Conditions: []builder.Condition{
									{
										Type:  "simple",
										Op:    "=",
										Left:  &builder.ValuePos{Type: "column", Name: "visibility"},
										Right: &builder.ValuePos{Type: "literal", Value: "PUBLIC"},
									},
									{
										Type: "correlatedSubquery",
										Op:   "EXISTS",
										Flip: true,
										Related: &builder.CorrelatedSubquery{
											System: "client",
											Correlation: builder.Correlation{
												ParentField: []string{"id"},
												ChildField:  []string{"channelId"},
											},
											Subquery: builder.AST{
												Table: "channel_participants",
												Alias: "zsubq_participants",
												Where: &builder.Condition{
													Type:  "simple",
													Op:    "=",
													Left:  &builder.ValuePos{Type: "column", Name: "userId"},
													Right: &builder.ValuePos{Type: "literal", Value: userID},
												},
											},
										},
									},
								},
							},
						},
					},
				},
			},
		},
	}

	limit := 1
	ast.Limit = &limit

	changes, _, err := eng.AddQuery("q-channelstats", ast)
	if err != nil {
		t.Fatalf("AddQuery: %v", err)
	}

	t.Logf("hydrate returned %d changes", len(changes))
	for _, c := range changes {
		t.Logf("  change: queryID=%s table=%s rowKey=%v", c.QueryID, c.Table, c.RowKey)
	}

	// Count per table.
	counts := map[string]int{}
	for _, c := range changes {
		counts[c.Table]++
	}

	// Expected: 2 changes (channel_stats + channels), not 3.
	if counts["channel_stats"] != 1 {
		t.Errorf("channel_stats: want 1 change, got %d (test bug?)", counts["channel_stats"])
	}
	if counts["channels"] != 1 {
		t.Errorf("channels: want 1 change, got %d", counts["channels"])
	}
	if counts["channel_participants"] != 0 {
		t.Errorf("channel_participants: want 0 changes (filter-only inner CSQ should be suppressed), got %d", counts["channel_participants"])
	}
}
