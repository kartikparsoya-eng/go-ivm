package engine

// Throwaway profiling harness for the "SHM ring buffer vs Unix socket" decision.
// Isolates, per representative advance size, the three costs of moving an advance
// frame across the sidecar boundary:
//
//	encode  — msgpack.Marshal of []RowChange   (serialization; SHM does NOT remove)
//	decode  — msgpack.Unmarshal back           (serialization; SHM does NOT remove)
//	xfer    — round-trip over a REAL unix socket: small request out + B-byte
//	          response back, length-prefixed    (transport; SHM REMOVES this)
//
// "SHM-removable fraction" = xfer / (encode + xfer + decode). If that is small,
// shared memory shaves the cheap part and leaves the dominant serialization
// untouched. Run:  go test ./engine -run TestTransportProfile -v
// Delete after reading; not part of the suite's intent.

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/kartikparsoya-eng/go-ivm/ivm"
	"github.com/vmihailenco/msgpack/v5"
)

func mpEnc(v interface{}) []byte {
	var buf bytes.Buffer
	enc := msgpack.NewEncoder(&buf)
	enc.SetCustomStructTag("json")
	enc.UseCompactInts(true)
	if err := enc.Encode(v); err != nil {
		panic(err)
	}
	return buf.Bytes()
}

func mpDec(data []byte, v interface{}) {
	dec := msgpack.NewDecoder(bytes.NewReader(data))
	dec.SetCustomStructTag("json")
	if err := dec.Decode(v); err != nil {
		panic(err)
	}
}

// makeChanges builds n RowChanges with a realistic ~15-column row (mixed
// strings / numbers / bool / nulls), deterministic so sizes are stable.
func makeChanges(n int) []RowChange {
	out := make([]RowChange, n)
	for i := 0; i < n; i++ {
		id := fmt.Sprintf("conv_%012d", i)
		out[i] = RowChange{
			Type:    i % 3,
			QueryID: "q_channelConversationsPaginatedV3_abc123",
			Table:   "conversations",
			RowKey:  map[string]interface{}{"conversationId": id},
			Row: ivm.Row{
				"conversationId":           id,
				"channelId":                fmt.Sprintf("chan_%08d", i%5000),
				"workspaceId":              "ws_000007",
				"createdBy":                fmt.Sprintf("user_%06d", i%900),
				"title":                    "Re: incremental view maintenance throughput thread",
				"createdAt":                float64(1779813865070 + i*1000),
				"updatedAt":                float64(1779813999070 + i*1000),
				"lastMessageAt":            float64(1779814100070 + i*1000),
				"participantCount":         float64(i % 40),
				"unreadCount":              float64(i % 12),
				"messageCount":             float64(100 + i%4000),
				"isPinned":                 i%7 == 0,
				"isArchived":               i%11 == 0,
				"parentMessageId":          nil,
				"conversationSeenCutoffAt": nil,
			},
		}
	}
	return out
}

func TestTransportProfile(t *testing.T) {
	if os.Getenv("GO_IVM_BENCH") == "" {
		t.Skip("wire-boundary microbench; set GO_IVM_BENCH=1 to run")
	}
	// Real unix-domain socket echo server: read a length-prefixed request,
	// reply with a length-prefixed B-byte response. This is the actual kernel
	// copy + syscall path SHM would eliminate.
	dir := t.TempDir()
	sock := filepath.Join(dir, "xfer.sock")
	ln, err := net.Listen("unix", sock)
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	// One response buffer per size, prepared by the server on demand.
	respBySize := map[int][]byte{}

	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		hdr := make([]byte, 4)
		for {
			if _, err := readFull(conn, hdr); err != nil {
				return
			}
			reqLen := binary.BigEndian.Uint32(hdr)
			req := make([]byte, reqLen)
			if _, err := readFull(conn, req); err != nil {
				return
			}
			// req payload's first 4 bytes encode which response size to send back.
			key := int(binary.BigEndian.Uint32(req[:4]))
			resp := respBySize[key]
			var lp [4]byte
			binary.BigEndian.PutUint32(lp[:], uint32(len(resp)))
			conn.Write(lp[:])
			conn.Write(resp)
		}
	}()

	conn, err := net.Dial("unix", sock)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	sizes := []int{10, 100, 1000, 10000}
	const iters = 200

	fmt.Printf("\n%-8s %-10s %-12s %-12s %-12s %-14s\n",
		"N", "bytes", "encode_us", "decode_us", "xfer_us", "xfer_%_of_ser+xfer")
	for _, n := range sizes {
		changes := makeChanges(n)
		encoded := mpEnc(changes)
		respBySize[n] = encoded

		// request payload: 4-byte size key + small filler (model the tiny advance req)
		req := make([]byte, 64)
		binary.BigEndian.PutUint32(req[:4], uint32(n))

		// --- encode ---
		tEnc := time.Now()
		for i := 0; i < iters; i++ {
			_ = mpEnc(changes)
		}
		encNs := time.Since(tEnc).Nanoseconds() / iters

		// --- decode ---
		tDec := time.Now()
		for i := 0; i < iters; i++ {
			var dst []RowChange
			mpDec(encoded, &dst)
		}
		decNs := time.Since(tDec).Nanoseconds() / iters

		// --- transport round-trip (req out, B-byte resp back) ---
		hdr := make([]byte, 4)
		rbuf := make([]byte, len(encoded))
		tXfer := time.Now()
		for i := 0; i < iters; i++ {
			var lp [4]byte
			binary.BigEndian.PutUint32(lp[:], uint32(len(req)))
			conn.Write(lp[:])
			conn.Write(req)
			if _, err := readFull(conn, hdr); err != nil {
				t.Fatal(err)
			}
			rlen := binary.BigEndian.Uint32(hdr)
			if int(rlen) != len(rbuf) {
				rbuf = make([]byte, rlen)
			}
			if _, err := readFull(conn, rbuf); err != nil {
				t.Fatal(err)
			}
		}
		xferNs := time.Since(tXfer).Nanoseconds() / iters

		serXfer := float64(encNs + decNs + xferNs)
		pct := 100 * float64(xferNs) / serXfer
		fmt.Printf("%-8d %-10d %-12.1f %-12.1f %-12.1f %-14.1f\n",
			n, len(encoded),
			float64(encNs)/1000, float64(decNs)/1000, float64(xferNs)/1000, pct)
	}
	fmt.Println()
}

func readFull(c net.Conn, buf []byte) (int, error) {
	got := 0
	for got < len(buf) {
		n, err := c.Read(buf[got:])
		if n > 0 {
			got += n
		}
		if err != nil {
			return got, err
		}
	}
	return got, nil
}
