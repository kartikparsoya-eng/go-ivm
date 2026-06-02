package engine

// H18-cont regression test: TS's cost-model planner annotates the inner
// EXISTS(participants) CSQ with flip:true at materialization time, so the
// AST Go actually receives in production has Flip:true on that CSQ (after
// the zero-cache pipeline-driver patch that plans the AST before
// dispatching to Go). With the FlippedJoin path wired up, Go's pipeline
// builds UnionFanOut → [Filter(visibility=PUBLIC), FlippedJoin(participants)]
// → UnionFanIn. The merge-with-dedup picks the simpler filter branch's
// node, so the channels node carries no inner-CSQ relationship and the
// streamer doesn't emit channel_participants. Expected: 2 changes.

import (
	"testing"

	"github.com/kartikparsoya-eng/go-ivm/builder"
	"github.com/kartikparsoya-eng/go-ivm/ivm"
)

func TestChannelStats_H18cont_NoFlipProductionShape(t *testing.T) {
	const targetChannelID = "ch-target-public"
	const otherPublicChannelID = "ch-other-public"
	const privateChannelID = "ch-private"
	const userID = "user-soak-1"
	const otherUserID = "user-other"

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

	// Seed multiple channel_stats rows.
	for _, chID := range []string{targetChannelID, otherPublicChannelID, privateChannelID} {
		channelStats.Push(ivm.MakeSourceChangeAdd(ivm.Row{
			"channelId": chID,
			"messages":  float64(5),
			"unread":    float64(2),
		}))
	}

	// Three channels, two public and one private.
	channels.Push(ivm.MakeSourceChangeAdd(ivm.Row{"id": targetChannelID, "visibility": "PUBLIC"}))
	channels.Push(ivm.MakeSourceChangeAdd(ivm.Row{"id": otherPublicChannelID, "visibility": "PUBLIC"}))
	channels.Push(ivm.MakeSourceChangeAdd(ivm.Row{"id": privateChannelID, "visibility": "PRIVATE"}))

	// Participants for user-soak-1 across all three channels (the soak pattern).
	// Plus a participant for a different user (to make the participants source non-trivial).
	channelParticipants.Push(ivm.MakeSourceChangeAdd(ivm.Row{
		"id":        "cp-soak-" + targetChannelID,
		"channelId": targetChannelID,
		"userId":    userID,
	}))
	channelParticipants.Push(ivm.MakeSourceChangeAdd(ivm.Row{
		"id":        "cp-soak-" + otherPublicChannelID,
		"channelId": otherPublicChannelID,
		"userId":    userID,
	}))
	channelParticipants.Push(ivm.MakeSourceChangeAdd(ivm.Row{
		"id":        "cp-soak-" + privateChannelID,
		"channelId": privateChannelID,
		"userId":    userID,
	}))
	channelParticipants.Push(ivm.MakeSourceChangeAdd(ivm.Row{
		"id":        "cp-other-" + targetChannelID,
		"channelId": targetChannelID,
		"userId":    otherUserID,
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

	// AST mirrors production EXACTLY: no Flip, no Scalar.
	ast := builder.AST{
		Table: "channel_stats",
		Where: &builder.Condition{
			Type: "and",
			Conditions: []builder.Condition{
				{
					Type:  "simple",
					Op:    "=",
					Left:  &builder.ValuePos{Type: "column", Name: "channelId"},
					Right: &builder.ValuePos{Type: "literal", Value: targetChannelID},
				},
				{
					Type: "correlatedSubquery",
					Op:   "EXISTS",
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

	changes, _, err := eng.AddQuery("q-channelstats-h18cont", ast)
	if err != nil {
		t.Fatalf("AddQuery: %v", err)
	}

	t.Logf("hydrate returned %d changes for target channel %s", len(changes), targetChannelID)
	for _, c := range changes {
		t.Logf("  change: table=%s rowKey=%v", c.Table, c.RowKey)
	}

	counts := map[string]int{}
	for _, c := range changes {
		counts[c.Table]++
	}

	// TS produces 2 for this case (channel_stats + channels).
	// Production: Go also produces 3 (extra channel_participants).
	// Goal: this test reproduces the over-emit so we can iterate on the fix.
	if counts["channel_stats"] != 1 {
		t.Errorf("channel_stats: want 1, got %d", counts["channel_stats"])
	}
	if counts["channels"] != 1 {
		t.Errorf("channels: want 1, got %d", counts["channels"])
	}
	if counts["channel_participants"] != 0 {
		t.Errorf("channel_participants: want 0 (TS suppresses inner CSQ row when OR's PUBLIC branch matches), got %d — over-emit reproduced", counts["channel_participants"])
	}
}
