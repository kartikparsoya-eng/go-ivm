package main

// Pins for the RPC-surface cleanup. Two waves:
//
//  1. Phase 2a: unary hydrate methods (addQuery, addQueries) — TS routes
//     all hydrate traffic through addQueriesStream; wrappers deleted first
//     (callers-before-handlers, mono 1d49a5862).
//  2. Removal sweep: loadRows, advanceStream, advanceToHead (unary),
//     refreshSnapshot, pipelineCount — all deleted; TS callers removed
//     in the mono half (cd94ed9c8). advanceToHeadStream (stream) STAYS.
//
// A caller gets JSON-RPC -32601 (method not found), never a silent no-op.

import "testing"

func TestRemovedUnaryHydrateRPCs_MethodNotFound(t *testing.T) {
	srv := NewServer(makeReplicaPathOnly(t))
	t.Cleanup(srv.closeAll)
	for _, method := range []string{"addQuery", "addQueries"} {
		resp := srv.handleRequest(RPCRequest{Method: method, ID: 1})
		if resp.Error == nil || resp.Error.Code != -32601 {
			t.Fatalf("%s: got %+v, want -32601 method-not-found", method, resp.Error)
		}
	}
}

func TestRemovedSweepRPCs_MethodNotFound(t *testing.T) {
	srv := NewServer(makeReplicaPathOnly(t))
	t.Cleanup(srv.closeAll)
	for _, method := range []string{
		"loadRows",
		"advanceStream",
		"advanceToHead",
		"refreshSnapshot",
		"pipelineCount",
	} {
		resp := srv.handleRequest(RPCRequest{Method: method, ID: 1})
		if resp.Error == nil || resp.Error.Code != -32601 {
			t.Fatalf("%s: got %+v, want -32601 method-not-found", method, resp.Error)
		}
	}
}
