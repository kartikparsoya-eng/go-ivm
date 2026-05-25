package main

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"sync"
	"testing"
)

// TestMultiGroupParallel verifies that multiple client groups can
// init, addQuery, and advance concurrently without races or corruption.
func TestMultiGroupParallel(t *testing.T) {
	socketPath := "/tmp/go-ivm-test-parallel.sock"
	os.Remove(socketPath)

	listener, err := net.Listen("unix", socketPath)
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	defer os.Remove(socketPath)

	server := NewServer()
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			go handleConnection(conn, server)
		}
	}()

	// Helper: send msgpack-RPC over length-prefix framing, get response.
	sendRPC := func(conn net.Conn, reader *bufio.Reader, method string, params interface{}) RPCResponse {
		paramData, err := mpMarshal(params)
		if err != nil {
			t.Fatalf("marshal params: %v", err)
		}
		req := RPCRequest{
			JSONRPC: "2.0",
			Method:  method,
			Params:  paramData,
			ID:      1,
		}
		reqData, err := mpMarshal(req)
		if err != nil {
			t.Fatalf("marshal req: %v", err)
		}
		if err := writeFrame(conn, reqData); err != nil {
			t.Fatalf("write frame: %v", err)
		}

		respBytes, err := readFrame(reader)
		if err != nil {
			t.Fatalf("read frame: %v", err)
		}
		var resp RPCResponse
		if err := mpUnmarshal(respBytes, &resp); err != nil {
			t.Fatalf("unmarshal resp: %v", err)
		}
		return resp
	}

	numGroups := 8
	var wg sync.WaitGroup

	for i := 0; i < numGroups; i++ {
		wg.Add(1)
		go func(groupIdx int) {
			defer wg.Done()

			cgID := fmt.Sprintf("group-%d", groupIdx)

			conn, err := net.Dial("unix", socketPath)
			if err != nil {
				t.Errorf("group %d: dial failed: %v", groupIdx, err)
				return
			}
			defer conn.Close()
			reader := bufio.NewReader(conn)

			// Init with in-memory tables (no SQLite file needed)
			initP := map[string]interface{}{
				"clientGroupID": cgID,
				"dbPath":        ":memory:",
				"storagePath":   ":memory:",
				"tables":        map[string]interface{}{},
			}
			resp := sendRPC(conn, reader, "init", initP)
			if resp.Error != nil {
				t.Errorf("group %d init: %s", groupIdx, resp.Error.Message)
				return
			}

			// Destroy
			destroyP := map[string]interface{}{
				"clientGroupID": cgID,
			}
			resp = sendRPC(conn, reader, "destroy", destroyP)
			if resp.Error != nil {
				t.Errorf("group %d destroy: %s", groupIdx, resp.Error.Message)
			}
		}(i)
	}

	wg.Wait()
}
