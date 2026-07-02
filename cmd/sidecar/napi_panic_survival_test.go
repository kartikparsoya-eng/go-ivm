package main

// NAPI-transport panic-survival parity (the production transport).
//
// The panic→RPC-error CLASSIFICATION is unit-tested in
// panic_classification_test.go. What THAT can't show is the thing that
// matters most for the in-process transport: on the socket, a handler panic
// that somehow escapes the recover only kills an isolated sidecar SUBPROCESS
// (SidecarManager restarts it); on NAPI the same escape would take down the
// whole syncer WORKER (Go runtime shares the Node process). So this drives a
// real handler panic END-TO-END through the ABI host — the exact
// handleConnection → worker → handleStreamWithRecover path abi.go:224 reuses
// verbatim — and asserts:
//
//  1. the recovered error is DELIVERED as a kind-1 frame (through the pump,
//     onto the same ordered TSFN queue the rows use), with the DataError
//     code (-32102 → TS tears the CG down) attributed to the right request;
//  2. NO row records (kind 2/3) leak for the failed request; and
//  3. the host SURVIVES — a follow-up ping returns "pong", proving the panic
//     did not abort the process (which on NAPI is the syncer worker itself).
//
// This is the Go-side mirror of TS catching the equivalent throw in
// #syncQueryPipelineSet and rejecting the RPC without crashing the worker.

import (
	"testing"
	"time"

	"github.com/kartikparsoya-eng/go-ivm/sqlite"
)

func TestABIHost_RowModeHandlerPanicSurvivesAndClassifies(t *testing.T) {
	col := newSinkCollector()
	h := startABIHostWithServer(NewServer(0, ""), col.sink, nil)
	defer h.Shutdown()

	send := func(id float64, method string, params interface{}) {
		t.Helper()
		if err := h.Send(encodeReq(t, method, id, params)); err != nil {
			t.Fatalf("send %s: %v", method, err)
		}
	}

	send(1, "init", initParams{
		ClientGroupID: "cg-panic",
		Storage:       t.TempDir() + "/storage.db",
		Tables: map[string]tableSchemaParams{
			"users": {
				Columns:    map[string]sqlite.ColumnSchema{"id": {Type: "string"}},
				PrimaryKey: []string{"id"},
			},
		},
	})
	// rowMode addQueriesStream against an UNKNOWN table → builder.BuildPipeline
	// panics *ivm.DataError during the (pre-hydrate) build, before any record
	// is emitted. handleStreamWithRecover must convert it to a -32102 frame.
	send(3, "addQueriesStream", map[string]interface{}{
		"clientGroupID": "cg-panic",
		"initEpoch":     1,
		"rowMode":       true,
		"queries": []map[string]interface{}{
			{"queryID": "q-bad", "ast": map[string]interface{}{
				"table":   "no_such_table",
				"orderBy": [][]string{{"id", "asc"}},
			}},
		},
	})
	// Follow-up ping on the SAME host — its "pong" is the survival proof.
	send(4, "ping", nil)

	deadline := time.Now().Add(15 * time.Second)
	var (
		errFrame   *RPCResponse
		sawPong    bool
		recForReq3 int
	)
	for time.Now().Before(deadline) {
		col.mu.Lock()
		entries := append([]sinkEntry(nil), col.entries...)
		col.mu.Unlock()

		errFrame, sawPong, recForReq3 = nil, false, 0
		for i := range entries {
			e := entries[i]
			switch e.kind {
			case abiKindRow, abiKindGroupDef:
				// First 8 bytes of every record are the f64 reqID.
				if len(e.payload) >= 8 && readReqID(e.payload) == 3 {
					recForReq3++
				}
			case abiKindFrame:
				resp := decodeResp(t, e.payload)
				if id, ok := toFloat(resp.ID); ok {
					if id == 3 && resp.Error != nil {
						r := resp
						errFrame = &r
					}
					if id == 4 && resp.Result == "pong" {
						sawPong = true
					}
				}
			}
		}
		if errFrame != nil && sawPong {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	if errFrame == nil {
		t.Fatal("no error frame for the panicking request (id=3) — a handler panic must surface as a delivered error frame, not a lost RPC")
	}
	if errFrame.Error.Code != rpcCodeDataError {
		t.Fatalf("panic classified as %d, want %d (unknown-table DataError → TS teardown, not reset)",
			errFrame.Error.Code, rpcCodeDataError)
	}
	if recForReq3 != 0 {
		t.Fatalf("build panic leaked %d row-plane record(s) for the failed request — none may be delivered", recForReq3)
	}
	if !sawPong {
		t.Fatal("no pong after the panic — the ABI host did not survive (on NAPI this is a syncer-worker crash)")
	}
}

// readReqID reads the little-endian f64 reqID prefix of a row-plane record.
func readReqID(payload []byte) float64 {
	bits := uint64(payload[0]) | uint64(payload[1])<<8 | uint64(payload[2])<<16 |
		uint64(payload[3])<<24 | uint64(payload[4])<<32 | uint64(payload[5])<<40 |
		uint64(payload[6])<<48 | uint64(payload[7])<<56
	return mathFloat64frombits(bits)
}
