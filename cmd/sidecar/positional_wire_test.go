package main

import (
	"encoding/base64"
	"reflect"
	"testing"

	"github.com/kartikparsoya-eng/go-ivm/engine"
	"github.com/kartikparsoya-eng/go-ivm/ivm"
)

// goldenHydrateFrameB64 is the exact byte sequence the sidecar emits for the
// canonical positional hydrate frame built in positionalWireFixture, encoded
// with the PRODUCTION codec (mpMarshal: vmihailenco/msgpack, UseCompactInts).
//
// The SAME base64 is embedded in the TS contract test
// (go-ivm-client.test.ts → "decodes REAL Go-encoded wire bytes"), where the
// production msgpackr Unpackr decodes it. Together the two tests pin the
// vmihailenco-bytes → msgpackr-decode boundary that the unit-level frame tests
// (which start from already-decoded JS/Go objects) never cross — only the live
// shadow soak did, and that is ephemeral + not in CI.
//
// To regenerate after an intentional wire change: run
//
//	go test ./cmd/sidecar -run TestPositionalWireFixture -v
//
// copy the FIXTURE_BASE64 line into this constant AND the TS test constant.
const goldenHydrateFrameB64 = "hqdxdWVyeUlEonExoWSShKFxonExoXStY29udmVyc2F0aW9uc6FjlKpfMF92ZXJzaW9uqWNoYW5uZWxJZK5jb252ZXJzYXRpb25JZKljcmVhdGVkQXSha5GuY29udmVyc2F0aW9uSWSEoXGicTGhdLNjaGFubmVsX3VzZXJfc3RhdHVzoWOTql8wX3ZlcnNpb26pY2hhbm5lbElkpnVzZXJJZKFrkqljaGFubmVsSWSmdXNlcklkoXKTlgAApDBhYmOjY2gxomMxy0J55lLFZuAAlQEApDBhYmWjY2gxonUxkwABomMzqmNodW5rSW5kZXgApWZpbmFsw6h0aW1pbmdNc8s/+AAAAAAAAA=="

// positionalWireFixture is the canonical RowChange chunk shared by the Go and
// TS wire-contract tests. It mirrors the hand-built frame in the TS test's
// "positional (rev 9) frame decoding" block exactly, so the real-bytes test and
// the decoded-object test corroborate each other:
//   - two (queryID,table) groups in one frame (conversations + channel_user_status)
//   - an add with a float column (createdAt) + the synthetic _0_version
//   - a second-group add with a composite PK
//   - a remove carrying PK only (nil Row)
func positionalWireFixture() []engine.RowChange {
	return []engine.RowChange{
		{
			Type: engine.RowChangeAdd, QueryID: "q1", Table: "conversations",
			RowKey: map[string]interface{}{"conversationId": "c1"},
			Row: ivm.Row{
				"_0_version": "0abc", "channelId": "ch1",
				"conversationId": "c1", "createdAt": float64(1779813865070),
			},
		},
		{
			Type: engine.RowChangeAdd, QueryID: "q1", Table: "channel_user_status",
			RowKey: map[string]interface{}{"channelId": "ch1", "userId": "u1"},
			Row: ivm.Row{
				"_0_version": "0abe", "channelId": "ch1", "userId": "u1",
			},
		},
		{
			Type: engine.RowChangeRemove, QueryID: "q1", Table: "conversations",
			RowKey: map[string]interface{}{"conversationId": "c3"},
		},
	}
}

// TestPositionalWireFixture encodes the canonical frame through the production
// mpMarshal codec and (a) round-trips it back through mpUnmarshal + fromPositional
// to prove the real msgpack codec — not just the in-memory toPositional/
// fromPositional pair that positional_test.go covers — preserves every change,
// and (b) pins the exact bytes against goldenHydrateFrameB64 so any codec/format
// drift fails loudly here and signals that the TS fixture must be regenerated.
func TestPositionalWireFixture(t *testing.T) {
	in := positionalWireFixture()
	pc := toPositional(in)

	// The real wire DTO the sidecar streams (handleAddQueriesStream), not a
	// bare positionalChanges — so the fixture bytes are byte-identical to a
	// production hydrate frame.
	part := addQueriesStreamPartial{
		QueryID:    "q1",
		Dict:       pc.Dict,
		Rows:       pc.Rows,
		ChunkIndex: 0,
		Final:      true,
		TimingMs:   1.5,
	}

	wire, err := mpMarshal(part)
	if err != nil {
		t.Fatalf("mpMarshal: %v", err)
	}

	b64 := base64.StdEncoding.EncodeToString(wire)
	// Logged so the TS fixture can be (re)generated from a single source of truth.
	t.Logf("FIXTURE_BASE64=%s", b64)

	// (a) Real-codec round-trip: decode the bytes the way the Go side would and
	// rebuild RowChanges. mpUnmarshal normalizes numeric interfaces to float64,
	// which fromPositional's toInt tolerates for the dictId/type positions.
	var decoded addQueriesStreamPartial
	if err := mpUnmarshal(wire, &decoded); err != nil {
		t.Fatalf("mpUnmarshal: %v", err)
	}
	if decoded.QueryID != "q1" || !decoded.Final || decoded.ChunkIndex != 0 {
		t.Fatalf("envelope round-trip mismatch: %+v", decoded)
	}
	got := fromPositional(positionalChanges{Dict: decoded.Dict, Rows: decoded.Rows})
	if len(got) != len(in) {
		t.Fatalf("round-trip length: got %d want %d", len(got), len(in))
	}
	for i := range in {
		w, g := in[i], got[i]
		if g.Type != w.Type || g.QueryID != w.QueryID || g.Table != w.Table {
			t.Errorf("[%d] header mismatch: got %+v want %+v", i, g, w)
		}
		if !reflect.DeepEqual(g.RowKey, w.RowKey) {
			t.Errorf("[%d] rowKey mismatch: got %v want %v", i, g.RowKey, w.RowKey)
		}
		if w.Type == engine.RowChangeRemove {
			if g.Row != nil {
				t.Errorf("[%d] remove must have nil Row, got %v", i, g.Row)
			}
		} else if !reflect.DeepEqual(g.Row, w.Row) {
			t.Errorf("[%d] row mismatch:\n got  %v\n want %v", i, g.Row, w.Row)
		}
	}

	// (b) Golden byte pin — locks the cross-language fixture. Skipped until the
	// constant is filled in (first run prints FIXTURE_BASE64 above).
	if goldenHydrateFrameB64 != "PLACEHOLDER" && b64 != goldenHydrateFrameB64 {
		t.Errorf("wire bytes drifted from golden fixture.\n got  %s\n want %s\n"+
			"If this change is intentional, update goldenHydrateFrameB64 AND the "+
			"TS test's GO_WIRE_FIXTURE_B64 with the new value.", b64, goldenHydrateFrameB64)
	}
}

// TestPositionalWire_JSONScalarRoundTrip pins that a json-column SCALAR STRING
// value — the bug class ("Payment Failures") — survives the REAL wire codec
// (mpMarshal → mpUnmarshal → fromPositional) unchanged, alongside the other
// top-level scalar shapes a row carries. The encode side (toPositional) just
// copies row values, so json CONTAINER fidelity is pinned at the SQLite
// write/read boundary (sqlite.TestJSONWriteReadRoundTrip_AllShapes); here we
// guard that the framing/codec doesn't mangle the bytes carrying a parsed-json
// scalar. (Production decodes the wire in TS via msgpackr; the Go decode here is
// test-only, which is why this stays to unambiguous top-level scalars.)
func TestPositionalWire_JSONScalarRoundTrip(t *testing.T) {
	in := []engine.RowChange{{
		Type: engine.RowChangeAdd, QueryID: "q1", Table: "docs",
		RowKey: map[string]interface{}{"id": "1"},
		Row: ivm.Row{
			"id":      "1",
			"payload": "Payment Failures", // parsed json scalar string (bug class)
			"count":   float64(42),
			"ok":      true,
			"missing": nil,
		},
	}}

	pc := toPositional(in)
	wire, err := mpMarshal(addQueriesStreamPartial{
		QueryID: "q1",
		Dict:    pc.Dict,
		Rows:    pc.Rows,
		Final:   true,
	})
	if err != nil {
		t.Fatalf("mpMarshal: %v", err)
	}

	var decoded addQueriesStreamPartial
	if err := mpUnmarshal(wire, &decoded); err != nil {
		t.Fatalf("mpUnmarshal: %v", err)
	}
	got := fromPositional(positionalChanges{Dict: decoded.Dict, Rows: decoded.Rows})
	if len(got) != 1 {
		t.Fatalf("round-trip length: got %d want 1", len(got))
	}
	if !reflect.DeepEqual(got[0].Row, in[0].Row) {
		t.Fatalf("row mangled through wire:\n got  %#v\n want %#v", got[0].Row, in[0].Row)
	}
}
