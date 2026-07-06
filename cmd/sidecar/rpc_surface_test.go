package main

// Pins for the Phase-2 RPC-surface cleanup: the unary hydrate methods are
// GONE from the dispatch surface — a caller gets JSON-RPC -32601 (method
// not found), never a silent no-op. TS routes all hydrate traffic through
// addQueriesStream (go-ivm-client.ts addQueryStream → addQueriesStream —
// the fat-frame fix); the TS wrappers were deleted first
// (callers-before-handlers, mono 1d49a5862), then these handlers.

import "testing"

func TestRemovedUnaryHydrateRPCs_MethodNotFound(t *testing.T) {
	srv := NewServer(0, "")
	t.Cleanup(srv.closeAll)
	for _, method := range []string{"addQuery", "addQueries"} {
		resp := srv.handleRequest(RPCRequest{Method: method, ID: 1})
		if resp.Error == nil || resp.Error.Code != -32601 {
			t.Fatalf("%s: got %+v, want -32601 method-not-found", method, resp.Error)
		}
	}
}
