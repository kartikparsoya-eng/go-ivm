package main

import "testing"

// TestCapFrameBytes_UnderCap verifies a normal-size frame passes through
// untouched (no substitution, original bytes returned).
func TestCapFrameBytes_UnderCap(t *testing.T) {
	const cap = 1024
	data, err := mpMarshal(RPCResponse{JSONRPC: "2.0", Result: "ok", ID: 1})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	got, over := capFrameBytes(1, data, cap)
	if over {
		t.Fatalf("small frame (%d bytes) wrongly flagged oversize", len(data))
	}
	if len(got) != len(data) {
		t.Fatalf("passthrough changed length: got %d want %d", len(got), len(data))
	}
}

// TestCapFrameBytes_OverCap verifies an oversize frame is replaced by a small,
// valid RPC error for the SAME id — instead of an oversized frame the TS reader
// would silently skip and orphan into a 60s timeout. This is the defense-in-
// depth net guaranteeing no non-streaming path can ever silently freeze a CG.
func TestCapFrameBytes_OverCap(t *testing.T) {
	const cap = 1024
	big := make([]byte, cap+1) // already-marshaled payload that blows the cap
	got, over := capFrameBytes(7, big, cap)
	if !over {
		t.Fatalf("oversize frame (%d > %d) not flagged", len(big), cap)
	}
	if len(got) > cap {
		t.Fatalf("substitute error frame still over cap: %d > %d", len(got), cap)
	}
	var resp RPCResponse
	if err := mpUnmarshal(got, &resp); err != nil {
		t.Fatalf("substitute frame did not unmarshal as RPCResponse: %v", err)
	}
	if resp.Error == nil {
		t.Fatalf("substitute frame carried no error; got %+v", resp)
	}
	if resp.Error.Code != errCodeFrameTooLarge {
		t.Fatalf("error code = %d, want %d (errCodeFrameTooLarge)", resp.Error.Code, errCodeFrameTooLarge)
	}
	if resp.ID == nil {
		t.Fatalf("substitute error dropped the request id; client cannot route the rejection")
	}
}
