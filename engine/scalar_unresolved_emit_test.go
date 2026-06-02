package engine

// Regression for the 20-user soak-traffic-generator finding (template
// #5 channels-EXISTS-scalar-conv): when a scalar:true EXISTS CSQ has a
// subquery the scalar resolver CANNOT rewrite (no unique-key constraint
// in the subquery WHERE), TS leaves it as a normal Join + Exists and
// emits the subquery rows. Go used to propagate csqCond.Scalar onto the
// Join, which marked the child schema IsScalar so streamNodes dropped
// the entire subtree. Resulted in TS=16, Go=4 mismatches in the
// rust-test shadow soak.
//
// Fix (commit 971dcd1): drop Scalar from the JoinArgs at the
// csqConditions-loop call site — any CSQ reaching that loop is one the
// resolver could not simplify, so emission should match TS.

import (
	"testing"

	"github.com/kartikparsoya-eng/go-ivm/builder"
	"github.com/kartikparsoya-eng/go-ivm/ivm"
)

func TestAddQuery_UnresolvedScalarCSQ_EmitsBothSides(t *testing.T) {
	channels := ivm.NewMemorySource("channels",
		map[string]string{"id": "string", "visibility": "string"},
		[]string{"id"})
	channels.BulkInsert([]ivm.Row{
		{"id": "c1", "visibility": "PUBLIC"},
		{"id": "c2", "visibility": "PUBLIC"},
		{"id": "c3", "visibility": "PRIVATE"},
	})

	// conversations.channelId is NOT a unique key on conversations, so the
	// scalar resolver's isSimpleSubquery returns false for the subquery
	// below and the CSQ survives into the builder's csqConditions loop.
	conversations := ivm.NewMemorySource("conversations",
		map[string]string{"conversationId": "string", "channelId": "string"},
		[]string{"conversationId"})
	conversations.BulkInsert([]ivm.Row{
		{"conversationId": "v1", "channelId": "c1"},
		{"conversationId": "v2", "channelId": "c1"},
		{"conversationId": "v3", "channelId": "c2"},
		// c3 intentionally has no conversation so it should be filtered out.
	})

	eng, err := NewEngine(EngineConfig{StoragePath: tempStoragePath(t)})
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()
	eng.RegisterMemorySource(channels)
	eng.RegisterMemorySource(conversations)
	eng.SetTableUniqueKeys("channels", [][]string{{"id"}})
	eng.SetTableUniqueKeys("conversations", [][]string{{"conversationId"}})

	// AST mirrors the production failure: WHERE = AND([scalar EXISTS on
	// conversations]). The inner subquery has an empty AND so no literal
	// equality constraints exist — isSimpleSubquery returns false.
	ast := builder.AST{
		Table: "channels",
		Where: &builder.Condition{
			Type: "and",
			Conditions: []builder.Condition{
				{
					Type:   "correlatedSubquery",
					Op:     "EXISTS",
					Scalar: true,
					Related: &builder.CorrelatedSubquery{
						System: "client",
						Correlation: builder.Correlation{
							ParentField: []string{"id"},
							ChildField:  []string{"channelId"},
						},
						Subquery: builder.AST{
							Table: "conversations",
							Alias: "zsubq_conv_scalar",
							Where: &builder.Condition{
								Type:       "and",
								Conditions: []builder.Condition{},
							},
						},
					},
				},
			},
		},
	}

	changes, _, err := eng.AddQuery("q-scalar-unresolved", ast)
	if err != nil {
		t.Fatal(err)
	}

	counts := map[string]int{}
	for _, c := range changes {
		counts[c.Table]++
	}

	// Expected: 2 channels pass the EXISTS check (c1 and c2; c3 has no
	// conversation), and the 3 matching conversation rows ship as part of
	// the channels nodes' zsubq_conv_scalar relationship.
	if counts["channels"] != 2 {
		t.Errorf("channels: want 2 (c1, c2), got %d", counts["channels"])
	}
	if counts["conversations"] != 3 {
		t.Errorf("conversations: want 3 (v1, v2, v3), got %d — IsScalar streamer-drop regressed",
			counts["conversations"])
	}
}
