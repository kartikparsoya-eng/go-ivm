package main

import (
	"reflect"
	"testing"

	"github.com/kartikparsoya-eng/go-ivm/engine"
	"github.com/kartikparsoya-eng/go-ivm/ivm"
)

// TestPositionalRoundTrip locks in that toPositional → fromPositional is the
// identity for well-formed RowChange chunks: add/edit rows preserve their full
// Row + derived rowKey, removes preserve their PK rowKey with a nil Row, and a
// heterogeneous frame (multiple queryID/table groups, null column values)
// reassembles exactly. This is the deterministic guard behind the rev-9 wire
// format; the live shadow soak is the end-to-end check.
func TestPositionalRoundTrip(t *testing.T) {
	in := []engine.RowChange{
		// conversations: add with a null column + the synthetic _0_version
		{
			Type: engine.RowChangeAdd, QueryID: "q1", Table: "conversations",
			RowKey: map[string]interface{}{"conversationId": "c1"},
			Row: ivm.Row{
				"conversationId": "c1", "channelId": "ch1",
				"createdAt": float64(1779813865070), "parentMessageId": nil,
				"_0_version": "0000000abc",
			},
		},
		// conversations: edit (same group → shares the dict entry)
		{
			Type: engine.RowChangeEdit, QueryID: "q1", Table: "conversations",
			RowKey: map[string]interface{}{"conversationId": "c2"},
			Row: ivm.Row{
				"conversationId": "c2", "channelId": "ch1",
				"createdAt": float64(1779813999999), "parentMessageId": "m9",
				"_0_version": "0000000abd",
			},
		},
		// related table in the SAME query frame → second dict group
		{
			Type: engine.RowChangeAdd, QueryID: "q1", Table: "channel_user_status",
			RowKey: map[string]interface{}{"channelId": "ch1", "userId": "u1"},
			Row: ivm.Row{
				"channelId": "ch1", "userId": "u1",
				"conversationSeenCutoffAt": nil, "_0_version": "0000000abe",
			},
		},
		// remove: carries only the PK rowKey, Row is nil
		{
			Type: engine.RowChangeRemove, QueryID: "q1", Table: "conversations",
			RowKey: map[string]interface{}{"conversationId": "c3"},
		},
	}

	got := fromPositional(toPositional(in))

	if len(got) != len(in) {
		t.Fatalf("round-trip changed length: got %d, want %d", len(got), len(in))
	}
	for i := range in {
		w, g := in[i], got[i]
		if g.Type != w.Type || g.QueryID != w.QueryID || g.Table != w.Table {
			t.Errorf("[%d] header mismatch: got %+v, want %+v", i, g, w)
		}
		if !reflect.DeepEqual(g.RowKey, w.RowKey) {
			t.Errorf("[%d] rowKey mismatch: got %v, want %v", i, g.RowKey, w.RowKey)
		}
		// Row equality: add/edit must preserve the full row (incl. nil cols and
		// _0_version); remove must have a nil Row.
		if w.Type == engine.RowChangeRemove {
			if g.Row != nil {
				t.Errorf("[%d] remove must have nil Row, got %v", i, g.Row)
			}
		} else if !reflect.DeepEqual(g.Row, w.Row) {
			t.Errorf("[%d] row mismatch:\n got  %v\n want %v", i, g.Row, w.Row)
		}
	}

	if got := toPositional(nil); got.Dict != nil || got.Rows != nil {
		t.Errorf("empty input must yield an empty frame, got %+v", got)
	}
}
