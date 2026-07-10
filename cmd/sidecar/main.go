package main

// MessagePack-RPC engine host. One Engine per client group, each running in
// its own goroutine so different groups execute in parallel. Served ONLY via
// the in-process NAPI transport (abi.go / napi_lib.go), which pumps
// length-prefixed msgpack frames through handleConnection over a net.Pipe.
// Wire format: 4-byte big-endian length prefix, then a MessagePack payload.

import (
	"bufio"
	"bytes"
	"context"
	"database/sql"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"net/http"
	_ "net/http/pprof" // registers /debug/pprof on http.DefaultServeMux when active
	"os"
	"reflect"
	"runtime"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/kartikparsoya-eng/go-ivm/builder"
	"github.com/kartikparsoya-eng/go-ivm/engine"
	"github.com/kartikparsoya-eng/go-ivm/internal/snapshotter"
	"github.com/kartikparsoya-eng/go-ivm/internal/tablesource"
	"github.com/kartikparsoya-eng/go-ivm/ivm"
	"github.com/kartikparsoya-eng/go-ivm/sqlite"
	"github.com/vmihailenco/msgpack/v5"
)

// --- Wire format helpers ---
//
// Framing: 4-byte big-endian uint32 length, followed by N bytes of MessagePack
// payload. Reads/writes are stream-oriented; multiple goroutines writing to the
// same connection must hold the connection's write mutex to keep frames intact.

const maxFrameSize = 64 * 1024 * 1024 // 64MB safety cap per message

// mpMarshal encodes v using MessagePack, honoring the existing `json:"..."`
// struct tags (so the same structs work without dual-tagging).
//
// UseCompactInts writes integers using the smallest msgpack type that fits.
// Without this, vmihailenco/msgpack encodes every uint64 as a full 9-byte
// uint64, which msgpackr on the JS side decodes as BigInt — breaking the
// numeric `id` round-trip in the RPC envelope (Map.get(BigInt) != Map.get(Number)).
func mpMarshal(v interface{}) ([]byte, error) {
	var buf bytes.Buffer
	enc := msgpack.NewEncoder(&buf)
	enc.SetCustomStructTag("json")
	enc.UseCompactInts(true)
	if err := enc.Encode(v); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// mpUnmarshal decodes MessagePack bytes into v, honoring `json:"..."` tags.
//
// UseLooseInterfaceDecoding promotes int8/16/32 → int64 and uint8/16/32 → uint64
// when decoding into interface{} (e.g., map[string]interface{} row values).
// Without this, msgpack picks the smallest type that fits, which breaks the IVM's
// CompareValues / NormalizeRow paths.
//
// After decoding we walk all interface{} values and convert every integer type
// to float64. This matches TS's single-Number-type model — TS never has int vs
// float distinction, so AST literals and row values share one numeric space.
// Without this, a row value normalized to float64(5) would not equal a literal
// decoded as int64(5) at any code path that bypasses CompareValues/ValuesEqual
// (e.g., a future Go interface == comparison).
//
// Typed struct fields (e.g., `Limit *int`) are unaffected because msgpack
// decodes them straight to int via reflection, not through DecodeInterface.
func mpUnmarshal(data []byte, v interface{}) error {
	dec := msgpack.NewDecoder(bytes.NewReader(data))
	dec.SetCustomStructTag("json")
	dec.UseLooseInterfaceDecoding(true)
	if err := dec.Decode(v); err != nil {
		return err
	}
	normalizeNumericInterfaces(v)
	return nil
}

// normalizeNumericInterfaces walks v via reflection and rewrites any int/uint
// type held in an interface{} (map values, slice elements, exported struct
// fields of type interface{}) to float64. Typed struct fields (e.g.,
// `Limit *int`) keep their declared types — only interface{} positions are
// rewritten, since those are the only ones that can hide a numeric type
// mismatch from the IVM's comparison functions.
func normalizeNumericInterfaces(v interface{}) {
	rv := reflect.ValueOf(v)
	if !rv.IsValid() {
		return
	}
	if rv.Kind() == reflect.Ptr {
		if rv.IsNil() {
			return
		}
		rv = rv.Elem()
	}
	walkForNumericNormalize(rv)
}

// walkForNumericNormalize traverses a reflect.Value, descending into maps,
// slices, arrays, structs, and pointers. When it finds an interface{} holding
// a numeric concrete type, it replaces the interface contents with float64.
func walkForNumericNormalize(rv reflect.Value) {
	if !rv.IsValid() {
		return
	}
	switch rv.Kind() {
	case reflect.Interface:
		if rv.IsNil() {
			return
		}
		inner := rv.Elem()
		if f, ok := numericToFloat64(inner); ok && rv.CanSet() {
			rv.Set(reflect.ValueOf(f))
			return
		}
		// Recurse into the concrete value held by the interface — e.g., a
		// map[string]interface{} held in an interface{} field.
		walkForNumericNormalize(inner)
	case reflect.Ptr:
		if rv.IsNil() {
			return
		}
		walkForNumericNormalize(rv.Elem())
	case reflect.Map:
		iter := rv.MapRange()
		for iter.Next() {
			val := iter.Value()
			// Map values are not directly addressable. Replace via SetMapIndex
			// if the value is an interface{} holding a numeric.
			if val.Kind() == reflect.Interface && !val.IsNil() {
				inner := val.Elem()
				if f, ok := numericToFloat64(inner); ok {
					rv.SetMapIndex(iter.Key(), reflect.ValueOf(f))
					continue
				}
				// Recurse into the inner concrete value. Since map values are
				// not addressable, this only mutates maps/slices reachable
				// through pointers — sufficient for our use.
				walkForNumericNormalize(inner)
			}
		}
	case reflect.Slice, reflect.Array:
		for i := 0; i < rv.Len(); i++ {
			walkForNumericNormalize(rv.Index(i))
		}
	case reflect.Struct:
		for i := 0; i < rv.NumField(); i++ {
			f := rv.Field(i)
			if f.CanSet() {
				walkForNumericNormalize(f)
			}
		}
	}
}

// numericToFloat64 returns (v, true) if the reflect.Value holds a Go numeric
// type other than float64 (in which case it's returned as float64) or non-int
// types (returns false). float64 already in float64 returns false (no change).
func numericToFloat64(rv reflect.Value) (float64, bool) {
	switch rv.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return float64(rv.Int()), true
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return float64(rv.Uint()), true
	case reflect.Float32:
		return rv.Float(), true
	}
	return 0, false
}

// minFrameSize is the smallest legal RPC frame body. A 0-byte body would
// mean "no payload at all" — there's no valid msgpack encoding for that.
// 1 byte is the smallest msgpack value (fixmap0 / nil / etc.); a real
// RPCRequest object always serializes to ~15+ bytes, but we use 1 to avoid
// false-rejects if the protocol ever adds a minimal pong-style frame.
// Pre-fix readFrame accepted len=0 and io.ReadFull happily returned a
// zero-length data slice; unmarshal would then throw and tight-loop
// against a stream of 4-byte-zero prefixes (DoS amplifier with cheap
// upstream cost). Reject early so the connection closes deterministically.
const minFrameSize = 1

// readFrame reads one length-prefixed frame from r. Returns io.EOF on clean
// connection close.
func readFrame(r *bufio.Reader) ([]byte, error) {
	var lenBuf [4]byte
	if _, err := io.ReadFull(r, lenBuf[:]); err != nil {
		return nil, err
	}
	n := binary.BigEndian.Uint32(lenBuf[:])
	if n < minFrameSize {
		return nil, fmt.Errorf("frame too small: %d < %d (protocol violation)", n, minFrameSize)
	}
	if n > maxFrameSize {
		return nil, fmt.Errorf("frame too large: %d > %d", n, maxFrameSize)
	}
	data := make([]byte, n)
	if _, err := io.ReadFull(r, data); err != nil {
		return nil, err
	}
	return data, nil
}

// writeFrame writes one length-prefixed frame to w. Caller must hold the
// connection's write mutex if multiple goroutines write concurrently.
func writeFrame(w io.Writer, data []byte) error {
	buf := make([]byte, 4+len(data))
	binary.BigEndian.PutUint32(buf[0:4], uint32(len(data)))
	copy(buf[4:], data)
	_, err := w.Write(buf)
	return err
}

// errCodeFrameTooLarge is returned when a response frame would exceed the wire
// cap. The TS reader SILENTLY SKIPS frames larger than its matching
// MAX_FRAME_SIZE (go-ivm-client.ts), orphaning the RPC into a 60s timeout that
// freezes the client group. We convert oversize frames into this attributable
// error so the call rejects immediately instead. The real fix is to chunk large
// results via the streaming RPCs (addQueriesStream / advanceToHeadStream);
// this is the defense-in-depth net for any non-streaming path that slips
// through (e.g. a single node whose subtree exceeds softChunkBytes).
const errCodeFrameTooLarge = -32011

// capFrameBytes returns the bytes to actually write for a response. If the
// marshaled frame exceeds maxFrameSize it returns a marshaled RPC error for the
// SAME request id (a small, valid frame the reader will accept and route as a
// rejection) plus true to signal the substitution. Otherwise it returns the
// original bytes and false.
func capFrameBytes(id interface{}, data []byte, maxFrameSize int) ([]byte, bool) {
	if len(data) <= maxFrameSize {
		return data, false
	}
	errData, _ := mpMarshal(rpcError(id, errCodeFrameTooLarge,
		fmt.Sprintf("response too large: %d bytes exceeds %d-byte frame cap; "+
			"this query must use a streaming RPC", len(data), maxFrameSize)))
	return errData, true
}

// --- Performance metrics ---

type perfMetrics struct {
	// Concurrency counters (how many engines doing work right now)
	advancesInFlight atomic.Int64
	hydratesInFlight atomic.Int64

	// Latency tracking (last 10s window)
	mu               sync.Mutex
	advanceLatencies []time.Duration
	hydrateLatencies []time.Duration
	advanceCount     atomic.Int64
	hydrateCount     atomic.Int64
	peakAdvConc      atomic.Int64 // peak concurrent advances seen
	peakHydConc      atomic.Int64 // peak concurrent hydrates seen

	// Chunk-count tracking (last 10s window) — answers "is the streaming
	// path actually chunking, or are payloads always single-frame?" One
	// entry per finalized query for hydrate, one per finalized call for
	// advance. A 1 means the call hit the fast-path (single chunk); higher
	// values mean the payload crossed advanceChunkSize / hydrateChunkSize.
	hydrateChunkCounts []int
	advanceChunkCounts []int
	// Row-mode advance delivers one record PER ROW (chunkSize=1), so its
	// "chunk count" is really a ROW count. Tracked separately so the
	// advance-chunks histogram stays comparable across transports — socket
	// (frames of ~100) vs napi rowMode (rows) — which an A/B rollout dashboard
	// would otherwise read apples-vs-oranges (REVIEW-napi-transport P2).
	advanceRowCounts []int

	// Cold-start reader-pool bind outcomes (last 10s window). Lets us
	// correlate bind-success-rate with replicator commit frequency: under fast
	// drive-mode writes the converge loop races a moving head, so a falling
	// bind-rate / rising avg-converge-attempts is the signal the pin is losing
	// and cold hydrates are degrading to the serial single-conn path.
	readerPoolBindCoread       atomic.Int64 // K readers latched to anchor via wal2 co-read
	readerPoolBindConverge     atomic.Int64 // K readers converged-upward to head
	readerPoolBindSerial       atomic.Int64 // no pool bound — serial single-conn fallback
	readerPoolConvergeAttempts atomic.Int64 // summed converge passes consumed

	// Warm-hydrate pool bind outcomes (addQueriesStream on a live-pipeline CG).
	// coread = K readers latched to curr's frame via co-read; serial = co-read
	// unavailable (non-wal2 / capture error / frame mismatch) so the warm add ran
	// single-conn. There is deliberately no "converge" bucket: the warm path
	// never converges to head (it would desync the new query from live pipelines).
	readerPoolWarmCoread atomic.Int64
	readerPoolWarmSerial atomic.Int64
	// lastReaderCacheHits/Misses hold the previous window's cumulative
	// reader-shell cache counters (tablesource.ReaderShellCacheCounters) so
	// reportAndReset prints per-window deltas. Touched only by the single
	// reporter goroutine.
	lastReaderCacheHits   int64
	lastReaderCacheMisses int64

	// napiDeliverStalls counts deliveries/flushes that found the addon's
	// TSFN queue FULL and entered a PARK (rowplane.go parkSlice — stage
	// hard bound or frame delivery; abi.go deliverPumpFrame). Under ABI v5
	// staging, ordinary congestion stages records instead of parking, so
	// this counts genuine waits only. napiDeliverTimeouts counts parks
	// that hit GO_IVM_DELIVER_TIMEOUT — incident-class, paired with the
	// [GO-IVM][DELIVER-TIMEOUT] marker. napiStagedRecords counts records
	// that found the queue full and staged (the congestion volume);
	// napiBatchFlushes counts kind-5 batch items shipped — staged/batches
	// is the mean coalescing factor.
	napiDeliverStalls   atomic.Int64
	napiDeliverTimeouts atomic.Int64
	napiStagedRecords   atomic.Int64
	napiBatchFlushes    atomic.Int64
}

var metrics = &perfMetrics{}

func (m *perfMetrics) recordAdvance(d time.Duration) {
	m.advanceCount.Add(1)
	m.mu.Lock()
	m.advanceLatencies = append(m.advanceLatencies, d)
	m.mu.Unlock()
}

func (m *perfMetrics) recordHydrate(d time.Duration) {
	m.hydrateCount.Add(1)
	m.mu.Lock()
	m.hydrateLatencies = append(m.hydrateLatencies, d)
	m.mu.Unlock()
}

// recordHydrateChunks logs the chunk count for ONE query that just
// finalized in addQueriesStream. n=1 means single-chunk fast path; n>1
// means the query crossed hydrateChunkSize and exercised the multi-frame
// wire path.
func (m *perfMetrics) recordHydrateChunks(n int) {
	m.mu.Lock()
	m.hydrateChunkCounts = append(m.hydrateChunkCounts, n)
	m.mu.Unlock()
}

// recordAdvanceChunks logs the chunk count for ONE advanceToHeadStream call
// that just finalized. n=1 means the entire diff fit in one frame; n>1
// means it crossed advanceChunkSize.
func (m *perfMetrics) recordAdvanceChunks(n int) {
	m.mu.Lock()
	m.advanceChunkCounts = append(m.advanceChunkCounts, n)
	m.mu.Unlock()
}

// recordAdvanceRows logs the per-row delivery count for ONE rowMode advance
// call (chunkSize=1). Separate from recordAdvanceChunks because a "chunk" is
// a row in that mode; see advanceRowCounts (REVIEW-napi-transport P2).
func (m *perfMetrics) recordAdvanceRows(n int) {
	m.mu.Lock()
	m.advanceRowCounts = append(m.advanceRowCounts, n)
	m.mu.Unlock()
}

// startPprofServer opens the pprof + block/mutex profiling endpoint when
// GO_IVM_PPROF_ADDR is set (nil when unset — off by default). Started by the
// in-process NAPI host (REVIEW-napi-transport O1). pprof pinned the EXISTS
// N+1, the pin race, and the GC ceiling on this project, so it must exist
// in-process.
//
// A bare ":port" addr derives a per-WORKER port from the PID: napi syncer
// workers are separate PROCESSES that would otherwise all bind the same
// fixed port and all but one would fail. A fully-qualified host:port is
// honored verbatim (operator owns per-worker uniqueness). S3 bind guard
// preserved: a hostless addr defaults to loopback — pprof is an RCE-grade
// surface (reads heap, dumps goroutines, can trigger GC).
func startPprofServer() *http.Server {
	addr := os.Getenv("GO_IVM_PPROF_ADDR")
	if addr == "" {
		return nil
	}
	if strings.HasPrefix(addr, ":") {
		if p, err := strconv.Atoi(strings.TrimPrefix(addr, ":")); err == nil {
			// Spread workers across a small band off the base port.
			addr = fmt.Sprintf(":%d", p+os.Getpid()%1000)
		}
		addr = "127.0.0.1" + addr
	}
	runtime.SetBlockProfileRate(1)
	runtime.SetMutexProfileFraction(1)
	srv := &http.Server{Addr: addr, Handler: http.DefaultServeMux}
	go func() {
		fmt.Fprintf(os.Stderr, "[GO-IVM] pprof listening on %s\n", addr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			fmt.Fprintf(os.Stderr, "[GO-IVM] pprof server exited: %v\n", err)
		}
	}()
	return srv
}

// runPerfReporter runs the 10-second [GO-IVM][PERF] window reporter plus the
// replica-pool-pressure watch until ctx is cancelled. Started by the NAPI
// host (REVIEW-napi-transport O1 — the PERF line is what every soak greps).
// Blocking; run in a goroutine.
func (s *Server) runPerfReporter(ctx context.Context) {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	var lastReadWait, lastWriteWait int64
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		metrics.reportAndReset()
		// Replica-pool pressure: WaitCount growth means goroutines are
		// blocking on conn acquisition — the precursor to TS-side RPC
		// timeouts. Surface it BEFORE it becomes reset storms.
		s.replicaMu.Lock()
		rdb, wdb := s.replicaDB, s.replicaWritableDB
		s.replicaMu.Unlock()
		if rdb != nil && wdb != nil {
			rs, ws := rdb.Stats(), wdb.Stats()
			// Read vs writable waits reported SEPARATELY: the summed count
			// made the 2026-07-06 latency forensics ambiguous (read-pool
			// saturation from warm-pool K bursts vs writable prev-conn
			// contention need different remedies).
			if rs.WaitCount > lastReadWait || ws.WaitCount > lastWriteWait {
				fmt.Fprintf(os.Stderr,
					"[GO-IVM] replica pool pressure: read +%d waits (in-use %d/%d, wait %s) "+
						"writable +%d waits (in-use %d/%d, wait %s) in last 10s — "+
						"consider raising GO_IVM_MAX_OPEN_CONNS\n",
					rs.WaitCount-lastReadWait, rs.InUse, rs.MaxOpenConnections,
					rs.WaitDuration.Round(time.Millisecond),
					ws.WaitCount-lastWriteWait, ws.InUse, ws.MaxOpenConnections,
					ws.WaitDuration.Round(time.Millisecond))
			} else if rs.MaxOpenConnections > 0 && rs.InUse >= rs.MaxOpenConnections {
				// Saturation without NEW waits is the deadlock signature the
				// 2026-07-06 incident hid: blocked acquirers bump WaitCount
				// exactly once, so a wedged-full pool goes silent under the
				// growth-only gate above. Log it every window until it clears.
				fmt.Fprintf(os.Stderr,
					"[GO-IVM] replica read pool SATURATED: in-use %d/%d for a full 10s window "+
						"(writable in-use %d/%d) — acquires are queueing; sustained saturation "+
						"suggests leaked readers or GO_IVM_MAX_OPEN_CONNS too low\n",
					rs.InUse, rs.MaxOpenConnections, ws.InUse, ws.MaxOpenConnections)
			}
			lastReadWait, lastWriteWait = rs.WaitCount, ws.WaitCount
		}
	}
}

// poolBindOutcome is HOW a cold-start hydrate got its K-reader frame: latched
// to the anchor frame via wal2 co-read (coread-fast), converged-upward across K
// independent BEGINs (the shipped fallback), or neither — no pool bound, so the
// hydrate runs serial on the single curr conn.
type poolBindOutcome int

const (
	poolBindSerial   poolBindOutcome = iota // no pool — serial single-conn fallback
	poolBindConverge                        // K readers converged-upward to head
	poolBindCoread                          // K readers latched to anchor via co-read
)

// recordReaderPoolBind tracks one cold-start reader-pool convergence outcome.
// poolBindCoread/poolBindConverge both mean a parallel-hydrate pool bound (the
// pin worked); poolBindSerial means the converge loop exhausted its attempts (or
// curr couldn't refresh) and the hydrate fell back to the serial single-conn
// path. attempts is the converge passes consumed (1 = bound first try; higher =
// the replicator advanced mid-pin and forced retries). Reported as
// [GO-IVM][PERF-POOL].
func (m *perfMetrics) recordReaderPoolBind(outcome poolBindOutcome, attempts int) {
	m.readerPoolConvergeAttempts.Add(int64(attempts))
	switch outcome {
	case poolBindCoread:
		m.readerPoolBindCoread.Add(1)
	case poolBindConverge:
		m.readerPoolBindConverge.Add(1)
	default:
		m.readerPoolBindSerial.Add(1)
	}
}

// recordWarmReaderPoolBind tracks whether a warm hydrate ran parallel (co-read
// pool bound at curr's frame) or fell back to the serial single-conn path.
func (m *perfMetrics) recordWarmReaderPoolBind(bound bool) {
	if bound {
		m.readerPoolWarmCoread.Add(1)
	} else {
		m.readerPoolWarmSerial.Add(1)
	}
}

func (m *perfMetrics) reportAndReset() {
	m.mu.Lock()
	advLats := m.advanceLatencies
	hydLats := m.hydrateLatencies
	hydChunks := m.hydrateChunkCounts
	advChunks := m.advanceChunkCounts
	advRows := m.advanceRowCounts
	m.advanceLatencies = nil
	m.hydrateLatencies = nil
	m.hydrateChunkCounts = nil
	m.advanceChunkCounts = nil
	m.advanceRowCounts = nil
	m.mu.Unlock()

	advCount := m.advanceCount.Swap(0)
	hydCount := m.hydrateCount.Swap(0)
	peakAdv := m.peakAdvConc.Swap(0)
	peakHyd := m.peakHydConc.Swap(0)
	bindCoread := m.readerPoolBindCoread.Swap(0)
	bindConverge := m.readerPoolBindConverge.Swap(0)
	bindSerial := m.readerPoolBindSerial.Swap(0)
	convergeAttempts := m.readerPoolConvergeAttempts.Swap(0)
	warmCoread := m.readerPoolWarmCoread.Swap(0)
	warmSerial := m.readerPoolWarmSerial.Swap(0)
	deliverStalls := m.napiDeliverStalls.Swap(0)
	deliverTimeouts := m.napiDeliverTimeouts.Swap(0)
	stagedRecords := m.napiStagedRecords.Swap(0)
	batchFlushes := m.napiBatchFlushes.Swap(0)
	cacheHits, cacheMisses := tablesource.ReaderShellCacheCounters()
	dHits, dMisses := cacheHits-m.lastReaderCacheHits, cacheMisses-m.lastReaderCacheMisses
	m.lastReaderCacheHits, m.lastReaderCacheMisses = cacheHits, cacheMisses

	if advCount == 0 && hydCount == 0 && bindCoread == 0 && bindConverge == 0 &&
		bindSerial == 0 && warmCoread == 0 && warmSerial == 0 && dHits == 0 && dMisses == 0 &&
		deliverStalls == 0 && deliverTimeouts == 0 && stagedRecords == 0 && batchFlushes == 0 {
		return
	}

	var advP50, advP95, advMax time.Duration
	if len(advLats) > 0 {
		sortDurations(advLats)
		advP50 = advLats[len(advLats)/2]
		advP95 = advLats[int(float64(len(advLats))*0.95)]
		advMax = advLats[len(advLats)-1]
	}

	var hydP50, hydP95, hydMax time.Duration
	if len(hydLats) > 0 {
		sortDurations(hydLats)
		hydP50 = hydLats[len(hydLats)/2]
		hydP95 = hydLats[int(float64(len(hydLats))*0.95)]
		hydMax = hydLats[len(hydLats)-1]
	}

	// Chunk-count distribution: lets operators answer "is the streaming
	// path actually chunking" without parsing per-frame logs. A median
	// of 1 means almost every call hits the single-frame fast path.
	hydChunkP50, hydChunkP95, hydChunkMax := chunkStats(hydChunks)
	advChunkP50, advChunkP95, advChunkMax := chunkStats(advChunks)
	// Row-mode advance rows reported as a DISTINCT segment (not folded into
	// advance chunks) so socket-vs-napi comparisons stay honest (P2).
	advRowP50, advRowP95, advRowMax := chunkStats(advRows)

	fmt.Fprintf(os.Stderr,
		"[GO-IVM][PERF-CHUNKS] 10s window: hydrate chunks (p50=%d p95=%d max=%d n=%d) advance chunks (p50=%d p95=%d max=%d n=%d) advance rows (p50=%d p95=%d max=%d n=%d)\n",
		hydChunkP50, hydChunkP95, hydChunkMax, len(hydChunks),
		advChunkP50, advChunkP95, advChunkMax, len(advChunks),
		advRowP50, advRowP95, advRowMax, len(advRows))

	fmt.Fprintf(os.Stderr, "[GO-IVM][PERF] 10s window: advances=%d (p50=%v p95=%v max=%v peakConc=%d) hydrates=%d (p50=%v p95=%v max=%v peakConc=%d)\n",
		advCount, advP50, advP95, advMax, peakAdv,
		hydCount, hydP50, hydP95, hydMax, peakHyd)

	// Cold-start reader-pool pin breakdown (drive mode only). coread = latched to
	// the anchor frame via wal2 co-read; converge = K readers converged-upward;
	// serial = no pool bound (the pin lost the race to the replicator and the
	// hydrate ran single-conn). pin-rate falling + avg-converge-attempts rising
	// under load = the pin losing margin to the replicator commit rate; a coread
	// share collapsing toward converge means the anchor capture is erroring (e.g.
	// the replica dropped out of wal2 mode).
	if bindCoread > 0 || bindConverge > 0 || bindSerial > 0 {
		bound := bindCoread + bindConverge
		total := bound + bindSerial
		pinRate := float64(bound) / float64(total) * 100
		avgAttempts := float64(convergeAttempts) / float64(total)
		fmt.Fprintf(os.Stderr,
			"[GO-IVM][PERF-POOL] 10s window: cold-hydrate pins coread=%d converge=%d serial=%d (pin-rate=%.1f%% avg-converge-attempts=%.1f)\n",
			bindCoread, bindConverge, bindSerial, pinRate, avgAttempts)
	}

	// Warm-hydrate pool breakdown (GO_IVM_WARM_HYDRATE_POOL). coread = the warm
	// add hydrated in parallel on curr's frame; serial = co-read was unavailable
	// (non-wal2 / capture error / frame moved) and it ran single-conn. A serial
	// share climbing means warm adds are losing parallelism — check the replica
	// is in wal2 mode and the add isn't racing an advance that moved curr.
	if warmCoread > 0 || warmSerial > 0 {
		warmTotal := warmCoread + warmSerial
		warmRate := float64(warmCoread) / float64(warmTotal) * 100
		fmt.Fprintf(os.Stderr,
			"[GO-IVM][PERF-POOL] 10s window: warm-hydrate pins coread=%d serial=%d (pin-rate=%.1f%%)\n",
			warmCoread, warmSerial, warmRate)
	}

	// Reader-shell cache (reader_cache.go): pool builds provisioning conns
	// from the worker-wide cache vs fresh SQLite opens. A reuse-rate
	// collapsing toward 0 under steady churn means teardowns aren't feeding
	// the cache (or the TTL sweep is outrunning the churn interval).
	if dHits > 0 || dMisses > 0 {
		reuse := float64(dHits) / float64(dHits+dMisses) * 100
		fmt.Fprintf(os.Stderr,
			"[GO-IVM][PERF-POOL] 10s window: reader-shell cache hits=%d misses=%d (reuse-rate=%.1f%%)\n",
			dHits, dMisses, reuse)
	}

	// NAPI deliver backpressure (ABI v5): staged = records that found the
	// TSFN queue full and coalesced into the stage (the producer kept
	// producing — no park); batches = kind-5 items shipped (staged/batches
	// ≈ coalescing factor); stalls = genuine PARKS (stage hard bound /
	// frame delivery / pump), event-woken by the drain signal; timeouts =
	// parks that outlived GO_IVM_DELIVER_TIMEOUT (incident — see
	// [GO-IVM][DELIVER-TIMEOUT]). Sustained staging tracks JS-event-loop
	// busyness; sustained STALLS mean even batches can't ship.
	if deliverStalls > 0 || deliverTimeouts > 0 || stagedRecords > 0 || batchFlushes > 0 {
		fmt.Fprintf(os.Stderr,
			"[GO-IVM][PERF-NAPI] 10s window: deliver queue-full stalls=%d timeouts=%d staged=%d batchFlushes=%d\n",
			deliverStalls, deliverTimeouts, stagedRecords, batchFlushes)
	}
}

// updatePeak performs an atomic max — fixes parallelism review HIGH-2.
// The old Load+Store pattern was racy: two goroutines could both observe
// peak=10, then one stores 15, the other stores 12 — peak ends at 12.
// With CompareAndSwap we retry until our value either wins or is obsolete.
func updatePeak(peak *atomic.Int64, n int64) {
	for {
		old := peak.Load()
		if n <= old || peak.CompareAndSwap(old, n) {
			return
		}
	}
}

func sortDurations(d []time.Duration) {
	// Simple insertion sort — fine for <=1000 entries per window
	for i := 1; i < len(d); i++ {
		for j := i; j > 0 && d[j] < d[j-1]; j-- {
			d[j], d[j-1] = d[j-1], d[j]
		}
	}
}

// chunkStats returns p50, p95, and max of an int slice. Returns (0,0,0)
// when empty (caller suppresses output on the empty case). In-place sort
// is fine because the caller already swapped the slice out under lock.
func chunkStats(c []int) (p50, p95, max int) {
	if len(c) == 0 {
		return 0, 0, 0
	}
	for i := 1; i < len(c); i++ {
		for j := i; j > 0 && c[j] < c[j-1]; j-- {
			c[j], c[j-1] = c[j-1], c[j]
		}
	}
	return c[len(c)/2], c[int(float64(len(c))*0.95)], c[len(c)-1]
}

// Version handshake — bumped when wire format or RPC semantics change so
// the TS client can refuse to talk to an incompatible sidecar
// (REVIEW-final MED-CROSS-5).
const (
	sidecarVersion     = "0.7.0"
	sidecarProtocolRev = 12 // bumped: advanceToHeadStream header frame.
)

// rpcCodeStaleInitEpoch signals that a mutating RPC arrived with an
// initEpoch that doesn't match the cgID's current epoch. Caller is from a
// torn-down view-syncer instance and must not be allowed to mutate engine
// state. The TS client treats this as a no-op (the live instance will
// reconcile via its own init).
const rpcCodeStaleInitEpoch = -32101

// parallelThreshold is the min connection count per MemorySource at which
// genPushAndWriteParallel kicks in. 0 means "use the engine default". Read
// once at startup from GO_IVM_PARALLEL_THRESHOLD; same value applied to every
// new client group's engine. We pull this from env (not RPC) because
// per-init RPC configurability would require a wire bump and most operators
// want one global value tuned to their workload.
var parallelThreshold = func() int {
	v := os.Getenv("GO_IVM_PARALLEL_THRESHOLD")
	if v == "" {
		return 0 // let engine pick its default
	}
	n, err := strconv.Atoi(v)
	if err != nil || n < 1 {
		fmt.Fprintf(os.Stderr, "[GO-IVM] invalid GO_IVM_PARALLEL_THRESHOLD=%q, using default\n", v)
		return 0
	}
	return n
}()

// envPositiveInt reads a positive integer from env, returning def when
// unset/invalid (with a stderr note on invalid).
func envPositiveInt(name string, def int) int {
	v := os.Getenv(name)
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil || n < 1 {
		fmt.Fprintf(os.Stderr, "[GO-IVM] invalid %s=%q, using default %d\n", name, v, def)
		return def
	}
	return n
}

// connMaxIdleFromEnv resolves GO_IVM_CONN_MAX_IDLE_SEC into the tablesource
// pool's idle-conn deadline. Unset → 0 (package default, 90s). A value of 0
// or a negative sentinel disables the deadline (conns park until closed) —
// use only for A/B against the pre-fix behavior. Invalid → default.
func connMaxIdleFromEnv() time.Duration {
	v := os.Getenv("GO_IVM_CONN_MAX_IDLE_SEC")
	if v == "" {
		return 0 // tablesource applies its default
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		fmt.Fprintf(os.Stderr,
			"[GO-IVM] invalid GO_IVM_CONN_MAX_IDLE_SEC=%q, using default\n", v)
		return 0
	}
	if n <= 0 {
		return -1 * time.Second // sentinel: disable the idle deadline
	}
	return time.Duration(n) * time.Second
}

// advanceBudgetMs is the wall-clock budget for ONE advanceToHead[Stream]
// call (derive + Collect + engine apply + emit). User's-audit item: a
// pathologically slow advance pins the WAL2 frame the diff was derived
// against for its whole duration (blocking checkpointing of that range) — so
// this bounds the pin even when the TS-economic abort is not armed (old TS,
// shadow paths, suppressAbort). On budget exceed the RPC errors mid-stream
// with the TYPED abort (rpcCodeAdvanceAborted → TS maps it to
// ResetPipelinesSignal('advancement-timeout') — reset + re-hydrate with
// bounded time, exactly like the GO_IVM_MAX_DIFF_CHANGES refusal bounds
// memory). It must NOT be a plain -32000: since the follow-TS failure model,
// 'unclassified' RETHROWS (CG teardown) — a time-bound overrun is an
// economics decision, not a bug. Var, not const, for tests. Practically
// disable by setting it very large.
var advanceBudgetMs = envPositiveInt("GO_IVM_ADVANCE_BUDGET_MS", 60_000)

// --- RPC types ---

type RPCRequest struct {
	JSONRPC string             `json:"jsonrpc"`
	Method  string             `json:"method"`
	Params  msgpack.RawMessage `json:"params"`
	ID      interface{}        `json:"id"`
	// W3C traceparent forwarded by the TS client (REVIEW-final MED-CROSS-4).
	// Logged for slow handlers; full Go-side OTel SDK integration is a
	// separate feature.
	Traceparent string `json:"traceparent,omitempty"`
}

type RPCResponse struct {
	JSONRPC string      `json:"jsonrpc"`
	Result  interface{} `json:"result,omitempty"`
	Error   *RPCError   `json:"error,omitempty"`
	ID      interface{} `json:"id"`
}

type RPCError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	// Data carries an optional structured payload for typed error codes.
	Data interface{} `json:"data,omitempty"`
}

// --- ClientGroup: one Engine per client group ---

// ClientGroup holds the Engine and an ordered request queue for a single client group.
// Requests are processed sequentially in FIFO order via a channel, preserving the
// ordering that TS relies on (e.g., advance must complete before removeQuery).
// Different groups run fully in parallel.
//
// Lifecycle: created lazily in getGroup; destroyed in removeGroup/closeAll
// which close `done` (signalling senders to bail out and the worker
// goroutine to exit). reqC is NEVER closed — closing it would race with
// concurrent trySendReq calls and panic on send-after-close. The
// `done` channel decouples shutdown signalling from the data channel,
// eliminating both that panic risk and the previous pattern where
// trySendReq held g.mu during a blocking channel send (which serialized
// the connection reader across slow CGs and delayed shutdownGroup until
// the worker finished its current handler).
type ClientGroup struct {
	mu   sync.Mutex
	eng  *engine.Engine
	reqC chan clientGroupReq // ordered request queue, never closed
	done chan struct{}       // closed by shutdownGroup to signal teardown
	// closeOnce guards close(done) so concurrent shutdownGroup / closeAll
	// calls don't panic on double-close.
	closeOnce sync.Once
	// initEpoch monotonically increments on every handleInit (atomic Add
	// under mu). Mutating RPCs (addQuery* / advance* / destroy) carry the
	// epoch they were issued under; mismatch → rejected with
	// rpcCodeStaleInitEpoch. This catches a torn-down view-syncer instance
	// whose late-arriving mutation would otherwise corrupt the freshly
	// init'd engine of a new instance for the same cgID.
	// Cross-GENERATION protection (destroy→re-init creates a fresh
	// ClientGroup) comes from Server.lastEpochs: creation seeds this from
	// the graveyard so epochs never restart at 0 for a cgID the server has
	// seen before. Atomic so deletion sites (removeGroup / reaper /
	// closeAll) can persist it to the graveyard under s.mu without taking
	// group.mu (no s.mu→group.mu nesting).
	initEpoch atomic.Uint64
	// lastUsedNs is set on every request arrival; the idle reaper compares
	// against `now - groupIdleTimeout` to garbage-collect abandoned groups
	// (REVIEW-final HIGH-CROSS-2 / HIGH-CROSS-3). Accessed via atomic so the
	// reaper doesn't need mu.
	lastUsedNs atomic.Int64
	// inFlight is true while the worker is executing a handler (dequeue
	// through respCh delivery). The reaper must never reap a group whose
	// worker is mid-handler (scale-review A4): lastUsedNs is stamped at
	// DEQUEUE, so a handler outliving the idle window (long hydrate under
	// backpressure) made a LIVE group reap-eligible — it was deleted from
	// s.groups while its handler streamed, and the next RPC for the same
	// cgID created a SECOND group+engine over the same storage
	// (split-brain). Set/cleared only by the worker goroutine; read by the
	// reaper. The worker stamps lastUsedNs fresh BEFORE clearing this flag
	// (sync/atomic is seq-cst), so a reaper that observes inFlight==false
	// is guaranteed to then observe the post-completion timestamp — a
	// just-finished group is never "idle since dequeue".
	inFlight atomic.Bool

	// curReq describes the request the worker is CURRENTLY executing —
	// stamped at dequeue, cleared after the respCh send. The wedge watchdog
	// (wedgewatch.go) reads it lock-free to detect handlers running past
	// GO_IVM_WEDGE_WATCHDOG_SEC. Written only by the worker goroutine.
	curReq atomic.Pointer[activeReq]
	// wedgeDumped latches the once-per-incident all-goroutine stack dump
	// (set by the watchdog on first detection, re-armed by the worker when
	// the handler completes) so a wedge produces exactly one dump, however
	// many scan ticks it spans.
	wedgeDumped atomic.Bool

	// sendMu closes the orphaned-respCh race between trySendReq and the
	// worker's post-done drain (full-scale review 2026-07-03). trySendReq
	// holds RLock across its done pre-check AND the reqC send; the exiting
	// worker takes Lock ONCE (a barrier) after its drain grace expires —
	// waiting out any sender still mid-send — then does a final
	// non-blocking sweep of reqC. Pre-fix, a sender preempted >50ms between
	// the pre-check and the select commit could land a request in reqC
	// AFTER the worker exited: its respCh never got a reader, the
	// connection's writer goroutine blocked forever, and handleConnection's
	// writerWg.Wait() hung the teardown. Senders acquiring RLock after the
	// barrier see done closed at the pre-check and bail deterministically.
	sendMu sync.RWMutex

	// snap is this group's Snapshotter — the Go-side leapfrog that derives
	// its own snapshot diff from the replica's changeLog2 (internal/snapshotter).
	// The advanceToHeadStream RPC applies its diff instead of any TS-shipped
	// SnapshotChange[]. Built
	// in handleInit (pinned at the then-current head, matching the hydrate
	// version), torn down in shutdownGroup / re-init. snapSpecs/snapAllNames
	// are the syncable TableSpecs and the full replicated table-name set,
	// captured at init for the Diff's syncable/non-syncable classification.
	snap         *snapshotter.Snapshotter
	snapSpecs    map[string]*snapshotter.TableSpec
	snapAllNames map[string]bool

	// readerPool is the cold-start parallel-hydrate reader pool (drive mode,
	// GO_IVM_HYDRATE_READERS>1). Built+bound in buildSnapshotterLocked at curr's
	// stateVersion; torn down at the first advance (tearDownReaderPool), on
	// shutdownGroup / re-init, and by the reaper once readerPoolBoundAt is
	// older than coldPoolTTL (scale review: an advance-less CG kept alive by
	// non-advance RPCs pinned the init-time WAL frame indefinitely — on a
	// busy replica wal2 cannot checkpoint past the pinned frame, so the WAL
	// grows without bound). Nil when the feature is off or the pool couldn't
	// pin (replica advanced past curr) — both fall back to the single-conn path.
	readerPool *tablesource.ReaderPool
	// readerPoolBoundAt is when the CURRENT cold pool was bound (zero when
	// readerPool is nil). Guarded by group.mu, like readerPool itself.
	readerPoolBoundAt time.Time
	coread            *tablesource.CoRead
}

type clientGroupReq struct {
	req    RPCRequest
	respCh chan RPCResponse
	// enqueuedAt is when trySendReq accepted the request into reqC — the
	// worker computes FIFO queue-wait from it (teardown-window measurement:
	// a destroy queued behind a long hydrate/advance is the dominant term
	// of the TS-side zombie window, so it must be attributable).
	enqueuedAt time.Time
	// streamW is set for streaming methods (currently addQueriesStream).
	// The handler emits partial frames via streamW; the final frame still
	// goes through respCh and the per-request writer goroutine.
	streamW streamWriter
	// group is the *ClientGroup the reader resolved when dispatching.
	// Handlers MUST use this rather than re-resolving via s.getGroup(cgID)
	// — concurrent removeGroup between dispatch and handler invocation
	// would otherwise spawn an orphan empty ClientGroup (and a leaked
	// worker goroutine) that handlers would then fail against and the
	// 30-min reaper would only collect much later.
	group *ClientGroup
	// cgID is the clientGroupID the dispatcher already extracted to route
	// this request (handleConnection) — threaded through so the worker's
	// per-request observability (wedge-watchdog stamp, TEARDOWN/SLOW/
	// WEDGE-CLEAR lines) never re-parses params on the hot path. Empty for
	// direct trySendReq callers (tests); the worker falls back to
	// extractClientGroupID then.
	cgID string
}

// streamWriter writes a partial frame carrying part of a streaming RPC
// response. Enqueues to the single flusher so partials and the final "done"
// frame share one FIFO — frames are atomic on the wire and partials
// structurally precede "done".
type streamWriter func(reqID interface{}, partial interface{})

// --- Server: manages multiple client groups ---

type Server struct {
	mu     sync.RWMutex
	groups map[string]*ClientGroup // clientGroupID → ClientGroup
	// lastEpochs is the initEpoch GRAVEYARD (scale-review C3): the highest
	// epoch each cgID ever reached, surviving group destruction. A destroyed
	// cgID's re-init seeds the fresh ClientGroup from here, so the new
	// generation's first epoch is strictly greater than anything the old
	// generation handed out. Without it, destroy→re-init restarted the count
	// at 0 and old-gen epoch N == new-gen epoch N: a late mutation from the
	// torn-down instance passed checkInitEpoch and corrupted the new
	// engine — the exact corruption the epoch exists to stop.
	// Guarded by mu (same critical sections that create/delete groups).
	lastEpochs map[string]uint64

	// abiDeliver, when non-nil, is the in-process (NAPI) transport's
	// out-of-band delivery callback: (kind, payload) entries land on the
	// addon's single ordered TSFN queue; the returned status reports the
	// NONBLOCKING enqueue outcome (deliverOK / deliverFull / deliverClosed
	// — ABI v4; the CALLER owns retry policy, see rowplane.go's lock
	// discipline). Set ONCE by the ABI host before
	// handleConnection starts (never mutated after) — handlers read it
	// lock-free. nil disables row mode (e.g. pipe-only unit fixtures):
	// rowMode requests then stream ordinary msgpack partials via streamW.
	// Payload bytes are valid only for the duration of the call (the
	// receiver copies ON deliverOK), so encoders may reuse their buffers.
	abiDeliver func(kind int32, payload []byte) int32

	// streamGates is the pull-hydration (ABI v3) demand-gate registry: one
	// gate per in-flight pullMode addQueriesStream RPC, keyed by the f64
	// reqID. Grant/cancel arrive as DIRECT JS-thread calls through the
	// goivm_stream_credit/goivm_stream_cancel exports (napi_lib.go) — a
	// leaf-locked registry, deliberately independent of s.mu (see
	// streamgate.go). Zero-value ready.
	streamGates streamGateRegistry

	// Path-and-lazy-open for the read-side replica pool. The replica is
	// authoritative for every table (tablesource.Source per (cg, table));
	// the actual open is deferred until the first init RPC — by that time
	// the TS replicator has finished writing the SQLite header.
	//
	// Singleflight design (C13): pre-fix this used a single mutex held
	// across the entire 60-second retry loop. N concurrent first-init
	// callers serialized behind the first; each failed open's deadline
	// expiration forced the next waiter to redo a fresh 60s loop from
	// scratch — so a chronic replica-unreachable condition multiplied
	// init latency by N. Now exactly one goroutine probes at a time;
	// others wait on a channel that closes when the probe completes,
	// then read the result under the mutex. The mutex is only held
	// for tiny critical sections (check cache, register probe, store
	// result) — never across the slow retry loop.
	replicaPath       string
	replicaMu         sync.Mutex
	replicaDB         *sql.DB       // populated on successful probe (under replicaMu)
	replicaWritableDB *sql.DB       // writable companion pool — used for each Source's prev-snapshot tx conn
	replicaProbe      chan struct{} // non-nil while a probe is in flight; closed when done
	replicaErr        error         // last probe's terminal error (under replicaMu)

	// appID names the app whose `${appID}.permissions` table the Snapshotter's
	// Diff watches for permissions-change resets. From GO_IVM_APP_ID (or the
	// per-init AppID field).
	appID string

	// hydrateReaders is the per-CG frame-pinned reader-pool floor used to
	// parallelize hydrate (drive mode only). It defaults to
	// 2×GO_IVM_HYDRATE_PARALLELISM and is overridden by GO_IVM_HYDRATE_READERS.
	// Option B resource model: K = max(hydrateReaders, hydrateLanes) is the
	// concurrent-hydrate ADMISSION width — each pipeline holds exactly ONE
	// reader for its whole drain (nested fetches interleave cursors on it);
	// batches wider than K queue at AcquireForPipeline while holding nothing.
	// K<=1 keeps the legacy single-conn serial path: no pool is built. The
	// cold pool is torn down at the first advance.
	hydrateReaders int

	// hydrateLanes is the number of worker lanes (P) that hydrate queries in
	// parallel on the non-pull path. It defaults to
	// GO_IVM_HYDRATE_PARALLELISM and is overridden by GO_IVM_HYDRATE_LANES.
	// Also a floor for the reader-pool width (K = max(hydrateReaders,
	// hydrateLanes)) so every lane can hold its one pipeline reader without
	// queueing. Default 4. 1 = serial (legacy).
	hydrateLanes int

	// warmHydratePoolEnabled extends the parallel-hydrate reader pool to WARM
	// hydrates (addQueriesStream on a CG that already has live pipelines), not
	// just the first cold one. From GO_IVM_WARM_HYDRATE_POOL=true; default OFF so
	// the shipped default behavior is unchanged until the flag flips.
	warmHydratePoolEnabled bool

	// wedgeThreshold is how long one handler may run before the wedge
	// watchdog (wedgewatch.go) reports it and the worker emits WEDGE-CLEAR
	// on its eventual return. From GO_IVM_WEDGE_WATCHDOG_SEC (default 90s,
	// below the TS 120s RPC deadline). Set once in NewServer; tests
	// override the field directly before traffic starts.
	wedgeThreshold time.Duration
}

func NewServer(replicaPath string) *Server {
	return &Server{
		groups:         make(map[string]*ClientGroup),
		lastEpochs:     make(map[string]uint64),
		replicaPath:    replicaPath,
		hydrateReaders: 1,
		hydrateLanes:   4,
		wedgeThreshold: wedgeWatchdogThreshold(),
	}
}

var (
	replicaOpenTimeout        = 60 * time.Second
	replicaOpenInitialBackoff = 500 * time.Millisecond
)

// getReplicaDB opens the SQLite replica on first call and caches the
// *sql.DB. Subsequent calls return the cached pool. Internal retry of
// up to 60s with progressive backoff handles the cold-start race where
// the TS replicator is still laying down the SQLite header when the
// sidecar's first init RPC arrives. Returns an error if the deadline
// is hit so the calling handler can surface the fallback to TS.
func (s *Server) getReplicaDB() (*sql.DB, error) {
	// Phase 1: fast path — cache hit OR another goroutine is already
	// probing, in which case we wait for it instead of starting our own.
	s.replicaMu.Lock()
	if s.replicaDB != nil {
		db := s.replicaDB
		s.replicaMu.Unlock()
		return db, nil
	}
	if s.replicaPath == "" {
		s.replicaMu.Unlock()
		return nil, fmt.Errorf("getReplicaDB: replicaPath unset")
	}
	if s.replicaProbe != nil {
		// A probe is already in flight. Wait for it to finish, then
		// re-check the cache + last error.
		probe := s.replicaProbe
		s.replicaMu.Unlock()
		<-probe
		s.replicaMu.Lock()
		if s.replicaDB != nil {
			db := s.replicaDB
			s.replicaMu.Unlock()
			return db, nil
		}
		err := s.replicaErr
		s.replicaMu.Unlock()
		if err == nil {
			err = fmt.Errorf("getReplicaDB: probe completed with no result")
		}
		return nil, err
	}

	// Phase 2: I'm the prober. Register intent under mu, then release
	// before doing the slow open so concurrent callers can wait on the
	// channel rather than blocking on the mutex during retries.
	probe := make(chan struct{})
	s.replicaProbe = probe
	s.replicaErr = nil
	s.replicaMu.Unlock()

	openTimeout := replicaOpenTimeout
	deadline := time.Now().Add(openTimeout)
	backoff := replicaOpenInitialBackoff
	var db *sql.DB
	var writableDB *sql.DB
	var lastErr error
	// Pool sizing must scale with concurrent CG count: the package default
	// (256) is sized for ~36 CGs in push mode / ~128 in drive mode (2 pinned
	// snapshotter conns per CG), and production runs hundreds of CGs per
	// shared sidecar. Exhaustion shows up as indefinite Conn() blocking →
	// RPC timeouts on the TS side → CG reset storms. Size via env:
	// GO_IVM_MAX_OPEN_CONNS ≥ 3× expected concurrent CGs is a safe rule of
	// thumb in drive mode (each SQLite conn costs ~2MB page cache).
	// CacheSizeKB caps each conn's SQLite page cache (C-side malloc —
	// invisible to GOMEMLIMIT). Worst-case C-side memory ≈ 2 pools ×
	// MaxOpenConns × cache, so at 1024 conns the SQLite default (~2MB)
	// costs up to ~4GB that no Go-side limiter can see.
	poolOpts := tablesource.OpenOptions{
		MaxOpenConns: envPositiveInt("GO_IVM_MAX_OPEN_CONNS", 0),
		MaxIdleConns: envPositiveInt("GO_IVM_MAX_IDLE_CONNS", 0),
		CacheSizeKB:  envPositiveInt("GO_IVM_CONN_CACHE_KB", 0),
		// Idle-conn reclaim: after a churn burst subsides, the pool cleaner
		// closes conns idle longer than this, returning their fd + C-side
		// page cache to the OS (the ART memory-growth fix). Env-tunable;
		// 0 → package default (90s). A dedicated -1 sentinel disables it.
		ConnMaxIdle: connMaxIdleFromEnv(),
	}
	if poolOpts.MaxOpenConns > 0 {
		fmt.Fprintf(os.Stderr, "[GO-IVM] replica pool: max open conns %d (GO_IVM_MAX_OPEN_CONNS)\n",
			poolOpts.MaxOpenConns)
	}
	for time.Now().Before(deadline) {
		var err error
		db, err = tablesource.Open(s.replicaPath, poolOpts)
		if err != nil {
			lastErr = err
			fmt.Fprintf(os.Stderr,
				"[GO-IVM] replica not ready yet (%v) — retrying in %v\n",
				err, backoff)
			time.Sleep(backoff)
			if backoff < 5*time.Second {
				backoff *= 2
			}
			db = nil
			continue
		}
		writableDB, err = tablesource.OpenWritable(s.replicaPath, poolOpts)
		if err != nil {
			db.Close()
			db = nil
			lastErr = err
			fmt.Fprintf(os.Stderr,
				"[GO-IVM] writable pool not ready yet (%v) — retrying in %v\n",
				err, backoff)
			time.Sleep(backoff)
			if backoff < 5*time.Second {
				backoff *= 2
			}
			continue
		}
		fmt.Fprintf(os.Stderr,
			"[GO-IVM] opened replica %s (WAL mode; query_only read pool + writable prev-tx pool)\n",
			s.replicaPath)
		// Reader-shell cache (reader_cache.go): pool builds reuse idle raw
		// conns (+ their prepared-stmt caches) across pool generations
		// instead of paying K SQLite opens per build — what made the
		// build-slot gate (and its skip-to-serial) deletable. Cap 0 disables.
		cacheCap := 32
		if v := os.Getenv("GO_IVM_READER_CACHE_CAP"); v != "" {
			if n, err := strconv.Atoi(v); err == nil && n >= 0 {
				cacheCap = n
			}
		}
		if cacheCap > 0 {
			tablesource.EnableReaderShellCache(db, cacheCap)
			fmt.Fprintf(os.Stderr,
				"[GO-IVM] reader-shell cache enabled: cap=%d ttl=%v (GO_IVM_READER_CACHE_CAP / GO_IVM_READER_CACHE_TTL_SEC)\n",
				cacheCap, readerCacheTTL())
		}
		break
	}

	// Phase 3: publish result under mu, signal waiters, and clear the
	// probe registration so future callers can probe again (e.g., the
	// next caller after a deadline-expiration retry).
	s.replicaMu.Lock()
	if db != nil && writableDB != nil {
		s.replicaDB = db
		s.replicaWritableDB = writableDB
		s.replicaErr = nil
	} else if lastErr != nil {
		s.replicaErr = fmt.Errorf("getReplicaDB: gave up after %v: %w", openTimeout, lastErr)
	} else {
		s.replicaErr = fmt.Errorf("getReplicaDB: deadline expired with no attempts")
	}
	s.replicaProbe = nil
	s.replicaMu.Unlock()
	close(probe)

	if db != nil {
		return db, nil
	}
	return nil, s.replicaErr
}

// getReplicaWritableDB returns the writable companion pool created
// alongside replicaDB during the singleflight probe. Must be called AFTER
// getReplicaDB has succeeded (it sets both in the same critical section).
// Returns nil if the probe never opened the writable pool.
func (s *Server) getReplicaWritableDB() *sql.DB {
	s.replicaMu.Lock()
	defer s.replicaMu.Unlock()
	return s.replicaWritableDB
}

// getOrCreateGroup returns the ClientGroup for the given ID, creating if needed.
// Each group has a dedicated worker goroutine that processes requests in FIFO order.
// getGroup returns the existing ClientGroup for id, or creates one if
// createIfMissing is true. Non-init handlers MUST pass createIfMissing=false
// — without that guard, a handler mid-flight whose group was concurrently
// destroyed by removeGroup would silently spawn an orphan empty ClientGroup
// (plus a leaked worker goroutine that lives until the 30-min reaper).
// The orphan would then fail every subsequent RPC with "engine not
// initialized" until the reaper finally collected it.
//
// Returns nil when the group is absent and createIfMissing=false.
func (s *Server) getGroup(id string, createIfMissing bool) *ClientGroup {
	s.mu.RLock()
	g := s.groups[id]
	s.mu.RUnlock()
	if g != nil {
		return g
	}
	if !createIfMissing {
		return nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	// Double-check after upgrade
	if g = s.groups[id]; g != nil {
		return g
	}
	g = &ClientGroup{
		reqC: make(chan clientGroupReq, 64),
		done: make(chan struct{}),
	}
	// C3: seed the epoch from the graveyard so a re-created cgID continues
	// its predecessor's count instead of restarting at 0 (see lastEpochs).
	g.initEpoch.Store(s.lastEpochs[id])
	g.lastUsedNs.Store(time.Now().UnixNano())
	s.groups[id] = g
	// Start a worker goroutine that processes requests in order.
	go g.worker(s)
	return g
}

// groupIdleTimeout — groups untouched for this long are eligible for
// reaping. Picked to be longer than typical client churn (>30 min) but
// short enough to bound memory after a wave of disconnects.
const groupIdleTimeout = 30 * time.Minute

// reaperInterval is how often runReaper scans for idle groups. Env-tunable
// (GO_IVM_REAPER_INTERVAL_SEC) so the memory-leak soak can force fast
// reaping; default 5 min.
func reaperInterval() time.Duration {
	if v := os.Getenv("GO_IVM_REAPER_INTERVAL_SEC"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return time.Duration(n) * time.Second
		}
	}
	return 5 * time.Minute
}

// reaperIdleTimeout is the age threshold for reaping. Env-tunable
// (GO_IVM_REAPER_IDLE_SEC) alongside the interval; default groupIdleTimeout.
func reaperIdleTimeout() time.Duration {
	if v := os.Getenv("GO_IVM_REAPER_IDLE_SEC"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return time.Duration(n) * time.Second
		}
	}
	return groupIdleTimeout
}

// coldPoolTTL is how long a cold-start reader pool may stay bound before the
// reaper tears it down (scale review). The pool exists to parallelize the
// cold-start hydrate burst and is normally dropped at the FIRST advance —
// but a CG that hydrates and then never advances (client keeps it alive
// with non-advance RPCs, or advances simply stop being driven) kept K
// readers pinned at the init-time WAL frame indefinitely, blocking wal2
// checkpointing past that frame: unbounded WAL growth on a busy replica.
// Five minutes comfortably covers any legitimate cold-start window (the
// TTL clock starts at BIND, and hydrates in progress hold group.mu, which
// the sweep never waits on). Env-tunable via GO_IVM_COLD_POOL_TTL_SEC.
func coldPoolTTL() time.Duration {
	if v := os.Getenv("GO_IVM_COLD_POOL_TTL_SEC"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return time.Duration(n) * time.Second
		}
	}
	return 5 * time.Minute
}

// pullIdleTimeout bounds how long a pull-hydrate producer may stay parked
// at zero credit with no grants before its gate is auto-cancelled (ABI v3,
// DESIGN-duplex-streaming D7). This is the pull lane's analogue of
// GO_IVM_ADVANCE_BUDGET_MS: it bounds the WAL-frame pin (a parked hydrate
// holds its read snapshot) and the group.mu hold (same-CG advances queue
// behind a parked hydrate — TS-faithful, but TS never parks on a vanished
// client). Auto-cancel takes the exact same unwind as a client cancel; the
// client receives a terminal error frame and re-hydrates. Env-tunable via
// GO_IVM_PULL_IDLE_TIMEOUT_SEC; default 60s.
func pullIdleTimeout() time.Duration {
	if v := os.Getenv("GO_IVM_PULL_IDLE_TIMEOUT_SEC"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return time.Duration(n) * time.Second
		}
	}
	return 60 * time.Second
}

// runPullIdleSweeper cancels pull gates whose producers have been parked
// past pullIdleTimeout (D7). Started by BOTH transports next to runReaper.
// The sweep is O(registered gates) over a leaf-locked registry — it never
// touches s.mu or group.mu, so it can never wedge behind a slow handler.
// Covers the crashed-client-no-cancel case together with shutdownGroup's
// teardown broadcast. Blocking call; run in its own goroutine.
func (s *Server) runPullIdleSweeper(ctx context.Context) {
	idle := pullIdleTimeout()
	tick := idle / 4
	if tick > 5*time.Second {
		tick = 5 * time.Second
	}
	if tick < 50*time.Millisecond {
		tick = 50 * time.Millisecond
	}
	ticker := time.NewTicker(tick)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			if n := s.streamGates.sweepIdle(now, idle); n > 0 {
				fmt.Fprintf(os.Stderr,
					"[GO-IVM] pull idle-timeout: cancelled %d parked stream(s) (no credit for > %v)\n",
					n, idle)
			}
		}
	}
}

// readerCacheTTL bounds how long an idle reader shell may sit in the
// reader-shell cache before the reaper tick closes it (returns its fd +
// stmt-cache C-heap). Env-tunable via GO_IVM_READER_CACHE_TTL_SEC; default
// 5 min — same order as coldPoolTTL, comfortably covering CG churn
// intervals while bounding the standing footprint after churn subsides.
func readerCacheTTL() time.Duration {
	if v := os.Getenv("GO_IVM_READER_CACHE_TTL_SEC"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return time.Duration(n) * time.Second
		}
	}
	return 5 * time.Minute
}

// runReaper periodically reaps idle client groups until ctx is cancelled.
// Started by BOTH transports — main() for the socket sidecar AND the NAPI
// ABI host (abi.go). Before the ABI host wired this, in-process (napi) mode
// had NO reaper at all: abandoned CGs (missed TS teardown, network
// partition) accumulated engines + prev-tx conns for the life of the worker
// — a napi-only leak on top of the shared idle-conn one. Blocking call; run
// in its own goroutine.
func (s *Server) runReaper(ctx context.Context) {
	ticker := time.NewTicker(reaperInterval())
	defer ticker.Stop()
	idle := reaperIdleTimeout()
	poolTTL := coldPoolTTL()
	cacheTTL := readerCacheTTL()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			n := s.reapIdleGroups(now.Add(-idle))
			if n > 0 {
				fmt.Fprintf(os.Stderr, "[GO-IVM] reaped %d idle client groups\n", n)
			}
			if p := s.reapStaleColdPools(now, poolTTL); p > 0 {
				fmt.Fprintf(os.Stderr, "[GO-IVM] tore down %d stale cold reader pool(s) past TTL\n", p)
			}
			// Reader-shell cache TTL: close shells idle past cacheTTL
			// (bounds standing fds + stmt-cache C-heap after churn subsides).
			s.replicaMu.Lock()
			rdb := s.replicaDB
			s.replicaMu.Unlock()
			if rdb != nil {
				if c := tablesource.SweepReaderShellCache(rdb, cacheTTL); c > 0 {
					fmt.Fprintf(os.Stderr, "[GO-IVM] closed %d idle reader-shell conn(s) past TTL\n", c)
				}
			}
		}
	}
}

// reapStaleColdPools tears down every group's cold-start reader pool whose
// bind is older than ttl (see coldPoolTTL for why). Skips groups whose
// worker is mid-handler and uses TryLock so a long hydrate (which holds
// group.mu for its whole duration) can never wedge the reaper goroutine —
// the sweep just retries next tick. Returns the number of pools dropped.
func (s *Server) reapStaleColdPools(now time.Time, ttl time.Duration) int {
	s.mu.RLock()
	groups := make([]*ClientGroup, 0, len(s.groups))
	for _, g := range s.groups {
		groups = append(groups, g)
	}
	s.mu.RUnlock()

	dropped := 0
	for _, g := range groups {
		if g.inFlight.Load() {
			continue // its handler likely holds group.mu; next tick
		}
		if !g.mu.TryLock() {
			continue
		}
		if g.readerPool != nil && !g.readerPoolBoundAt.IsZero() &&
			now.Sub(g.readerPoolBoundAt) > ttl {
			s.tearDownReaderPool(g)
			dropped++
		}
		g.mu.Unlock()
	}
	return dropped
}

// reapIdleGroups scans the group map and destroys any group whose lastUsedNs
// is older than `cutoff`. Returns the number of groups reaped.
func (s *Server) reapIdleGroups(cutoff time.Time) int {
	cutoffNs := cutoff.UnixNano()

	// Snapshot the IDs we want to consider (read lock). We don't reap under
	// the write lock because shutdownGroup takes g.mu and can block.
	s.mu.RLock()
	candidates := make([]struct {
		id string
		g  *ClientGroup
	}, 0, len(s.groups))
	for id, g := range s.groups {
		if g.inFlight.Load() {
			continue // A4: worker mid-handler — alive by definition
		}
		if g.lastUsedNs.Load() < cutoffNs {
			candidates = append(candidates, struct {
				id string
				g  *ClientGroup
			}{id, g})
		}
	}
	s.mu.RUnlock()

	reaped := 0
	for _, c := range candidates {
		// Double-check under write lock: someone may have refreshed lastUsed
		// since the snapshot.
		s.mu.Lock()
		current, stillThere := s.groups[c.id]
		if !stillThere || current != c.g {
			s.mu.Unlock()
			continue
		}
		if current.inFlight.Load() || current.lastUsedNs.Load() >= cutoffNs {
			s.mu.Unlock()
			continue
		}
		s.saveEpochLocked(c.id, current)
		delete(s.groups, c.id)
		s.mu.Unlock()
		s.shutdownGroup(c.g, c.id, "reaper")
		reaped++
	}
	return reaped
}

// worker processes requests for this client group sequentially in FIFO order.
// Exits when `done` is closed; on exit, drains any remaining buffered requests
// in reqC and responds to each with a "group destroyed" error so respCh
// readers don't hang.
func (g *ClientGroup) worker(s *Server) {
	for {
		var req clientGroupReq
		select {
		case req = <-g.reqC:
			// fall through to handle
		case <-g.done:
			// Drain buffered requests with an error so respCh readers
			// unblock. The grace deadline absorbs the common race window:
			// a trySendReq whose select observed done-not-closed AND
			// reqC-has-space can commit to the reqC send case AFTER we
			// noticed done was closed. 50ms covers any normally-scheduled
			// sender; the sendMu barrier below covers the pathological one.
			deadline := time.NewTimer(50 * time.Millisecond)
			defer deadline.Stop()
		drain:
			for {
				select {
				case r := <-g.reqC:
					r.respCh <- RPCResponse{
						JSONRPC: "2.0",
						Error:   &RPCError{Code: -32000, Message: "client group destroyed"},
						ID:      r.req.ID,
					}
				case <-deadline.C:
					break drain
				}
			}
			// Barrier: wait out any trySendReq still holding RLock (it will
			// either commit to reqC or bail via done), then sweep whatever
			// landed. After this Lock, every future sender's done pre-check
			// runs strictly after close(done) — deterministically bails — so
			// nothing can enter reqC once the sweep finishes. Closes the
			// >50ms-preempted-sender orphan (see ClientGroup.sendMu).
			g.sendMu.Lock()
			g.sendMu.Unlock() //nolint:staticcheck // empty critical section IS the barrier
			for {
				select {
				case r := <-g.reqC:
					r.respCh <- RPCResponse{
						JSONRPC: "2.0",
						Error:   &RPCError{Code: -32000, Message: "client group destroyed"},
						ID:      r.req.ID,
					}
				default:
					return
				}
			}
		}
		g.lastUsedNs.Store(time.Now().UnixNano())
		g.inFlight.Store(true) // A4: reap-proof while the handler runs
		var start time.Time
		method := req.req.Method
		dequeued := time.Now()
		cg := req.cgID
		if cg == "" {
			// Direct trySendReq callers (tests) skip the dispatcher's
			// extraction; parse once here so the stamp + logs still carry it.
			cg = extractClientGroupID(req.req)
		}
		var qWait time.Duration
		if !req.enqueuedAt.IsZero() {
			qWait = dequeued.Sub(req.enqueuedAt)
		}
		// Wedge-watchdog stamp (wedgewatch.go): makes this handler's
		// execution OBSERVABLE while in flight — the 7fbeed43 incident was
		// undiagnosable precisely because a running handler was invisible
		// (nothing logs until it returns, and a successful return logged
		// nothing at all).
		g.curReq.Store(&activeReq{
			method:    method,
			cgID:      cg,
			reqID:     req.req.ID,
			start:     dequeued,
			queueWait: qWait,
		})

		// Teardown-window measurement: the destroy RPC's FIFO queue-wait is
		// the TS zombie window's dominant Go-side term when the worker is
		// busy (the CG worker serializes handlers, so a destroy behind an
		// in-flight hydrate/advance waits for ALL of it). Logged per destroy
		// — destroys are CG-lifecycle-rate, not row-rate.
		if method == "destroy" && !req.enqueuedAt.IsZero() {
			fmt.Fprintf(os.Stderr, "[GO-IVM][TEARDOWN] destroy dequeued cg=%s queueWait=%v\n",
				cg, qWait)
		}

		// advanceToHeadStream is THE advance: TS ships no changes; Go derives
		// + applies its own diff. Counted here so [GO-IVM][PERF] lines report
		// real advance traffic (advances=N), not just PERF-CHUNKS row counts.
		switch method {
		case "advanceToHeadStream":
			start = time.Now()
			n := metrics.advancesInFlight.Add(1)
			updatePeak(&metrics.peakAdvConc, n)
		case "addQueriesStream":
			start = time.Now()
			n := metrics.hydratesInFlight.Add(1)
			updatePeak(&metrics.peakHydConc, n)
		}

		ctx := extractTraceparent(context.Background(), req.req.Traceparent)
		_, endSpan := startHandlerSpan(ctx, method)

		var resp RPCResponse
		if req.streamW != nil && method == "addQueriesStream" {
			// Streaming variant: per-query partial frames go through streamW;
			// the terminal "done" RPCResponse still flows through respCh.
			// C1: the stream handlers run OUTSIDE handleRequest's recover, and
			// the engine deliberately re-raises panics on this (worker)
			// goroutine — so without this recover such a panic aborts
			// the whole multi-CG process. Convert it to an error response (the
			// TS client rejects the call) instead.
			resp = s.handleStreamWithRecover(req.req, req.streamW, s.handleAddQueriesStream)
		} else if req.streamW != nil && method == "advanceToHeadStream" {
			// Streaming advance (drive): chunked RowChanges
			// go through streamW; terminal "done" RPCResponse flows through respCh.
			resp = s.handleStreamWithRecover(req.req, req.streamW, s.handleAdvanceToHeadStream)
		} else {
			resp = s.handleRequest(req.req)
		}

		endSpan(resp.Error)

		switch method {
		case "advanceToHeadStream":
			metrics.advancesInFlight.Add(-1)
			metrics.recordAdvance(time.Since(start))
		case "addQueriesStream":
			metrics.hydratesInFlight.Add(-1)
			metrics.recordHydrate(time.Since(start))
		}

		// Slow-handler log doubles as a fallback breadcrumb when OTel is off.
		// UNCONDITIONAL on traceparent (7fbeed43 forensics): the old
		// `Traceparent != ""` gate made a slow-but-successful hydrate
		// invisible when the request arrived without one — the exact
		// blindspot that left a ~140s silent return indistinguishable from a
		// permanent wedge across two ART builds. cg is included so the line
		// correlates without a params re-parse.
		if !start.IsZero() {
			if elapsed := time.Since(start); elapsed > 500*time.Millisecond {
				fmt.Fprintf(os.Stderr, "[GO-IVM][SLOW] method=%s cg=%s elapsed=%v traceparent=%s\n",
					method, cg, elapsed, req.req.Traceparent)
			}
		}

		req.respCh <- resp
		// WEDGE-CLEAR: a handler that ran past the watchdog threshold has
		// RETURNED — the release-side timestamp that discriminates "wedged
		// forever" from "silently un-stuck at ~120s" (see wedgewatch.go).
		// Fires for EVERY method (an init/destroy that took 90s matters as
		// much as a hydrate), incident-rate by construction.
		if elapsed := time.Since(dequeued); elapsed > s.wedgeThreshold {
			fmt.Fprintf(wedgeLogW, "[GO-IVM][WEDGE-CLEAR] cg=%s method=%s elapsed=%v err=%v\n",
				cg, method, elapsed.Round(time.Millisecond), resp.Error != nil)
		}
		g.curReq.Store(nil)
		g.wedgeDumped.Store(false) // re-arm the once-per-incident dump latch
		// Completion stamp BEFORE clearing inFlight (see the field comment):
		// without it, a handler that ran longer than the idle window left
		// lastUsedNs at its DEQUEUE time — instantly reap-eligible the
		// moment inFlight cleared, despite having JUST finished work.
		g.lastUsedNs.Store(time.Now().UnixNano())
		g.inFlight.Store(false)
	}
}

// trySendReq enqueues req on the group's request channel. Returns false if
// the group has been destroyed — caller should respond with an error.
//
// The select-with-done pattern replaces the previous mutex-guarded send:
//  1. No g.mu contention. The connection reader no longer serializes
//     across slow CGs on the same connection.
//  2. No send-on-closed-channel risk. reqC is never closed; teardown
//     signals via close(done) which is safe to read in parallel.
//  3. shutdownGroup completes immediately even if a handler is mid-flight,
//     instead of blocking until the worker finishes its current request.
//
// If the buffer is full AND done is not closed, this blocks waiting for
// the worker to drain. That's intentional backpressure on a single CG;
// the connection's outer dispatcher is free to handle other CGs since
// it no longer holds g.mu while waiting here.
//
// We pre-check done with a non-blocking select so a caller that arrives
// after shutdown is deterministically rejected. Without this, Go's select
// would pick randomly between "send on reqC (buffer has space)" and
// "<-g.done (closed)" — so half the time a post-shutdown send would
// succeed and the worker's drain (with bounded grace) might still miss it.
// The pre-check + bounded drain together guarantee no orphaned respCh.
//
// lastUsedNs is stamped on arrival, BEFORE the send. The previous design
// only refreshed it on worker dequeue — so a long handler holding the
// worker meant queued reqs didn't keep the group alive, and the reaper
// could destroy a group with live work still pending. With this stamp
// the reaper's double-check (under s.mu in reapIdleGroups) sees the
// fresh timestamp and skips eviction.
func (g *ClientGroup) trySendReq(req clientGroupReq) bool {
	// RLock brackets the pre-check + send so the exiting worker's sendMu
	// barrier can wait out an in-flight send before its final reqC sweep
	// (see ClientGroup.sendMu). Uncontended RLock is nanoseconds — noise
	// against the msgpack decode already on this path. Holding it across
	// the blocking send is safe: the worker never takes sendMu while
	// serving, so backpressure drains normally.
	g.sendMu.RLock()
	defer g.sendMu.RUnlock()
	select {
	case <-g.done:
		return false
	default:
	}
	g.lastUsedNs.Store(time.Now().UnixNano())
	req.enqueuedAt = time.Now()
	select {
	case g.reqC <- req:
		return true
	case <-g.done:
		return false
	}
}

// saveEpochLocked persists a group's initEpoch to the graveyard (C3) so a
// future re-creation of the same cgID cannot restart the epoch count.
// MUST hold s.mu (write). Monotonic: never lowers an existing entry.
func (s *Server) saveEpochLocked(id string, g *ClientGroup) {
	if e := g.initEpoch.Load(); e > s.lastEpochs[id] {
		s.lastEpochs[id] = e
	}
}

// removeGroup destroys a client group and its engine. Closes reqC so the
// worker goroutine exits cleanly; without this, every destroy leaked a
// goroutine + the channel + the closed engine reference.
func (s *Server) removeGroup(id string) {
	s.mu.Lock()
	g := s.groups[id]
	if g != nil {
		s.saveEpochLocked(id, g)
	}
	delete(s.groups, id)
	s.mu.Unlock()
	if g != nil {
		s.shutdownGroup(g, id, "destroy-rpc")
	}
}

// shutdownGroup performs the per-group teardown: signals the worker to
// exit via close(done), then closes the engine. Idempotent via closeOnce
// so concurrent removeGroup / closeAll don't race on the close.
//
// id/reason are observability-only (teardown-window measurement): every
// teardown logs one [GO-IVM][TEARDOWN] line with per-phase timings so the
// TS zombie-window (view-syncer stop → ServiceRunner delete, which awaits
// this via the destroy RPC — pipeline-driver.ts:1147 MED-5) can be
// classified into mu-wait vs actual cleanup cost.
//
// Does NOT close reqC — closing the data channel would race with concurrent
// trySendReq calls and panic on send-after-close. The worker treats `done`
// as the exit signal and drains any remaining buffered requests with an
// error response before returning. New senders past this point see done
// closed and bail out without touching reqC.
func (s *Server) shutdownGroup(g *ClientGroup, id, reason string) {
	t0 := time.Now()
	g.closeOnce.Do(func() {
		close(g.done)
	})
	// Pull gates FIRST (ABI v3, D6): a pull-hydrate producer parked at zero
	// credit holds group.mu via its RPC handler — taking g.mu below would
	// wait on the client's think-time (or forever, for a vanished client).
	// Cancelling the group's gates unparks those producers; they unwind
	// (ErrStreamCancelled), their handler returns and releases group.mu,
	// and the teardown proceeds. Owner is the group POINTER, so this can
	// never touch a re-created generation of the same cgID.
	s.streamGates.cancelOwner(g)
	gatesDone := time.Now()
	// Engine cleanup needs g.mu because handlers also take it (and we may
	// race with a handler that just dequeued before done was closed; the
	// handler will finish and respCh-send before re-entering the worker
	// loop, at which point the drain branch fires).
	g.mu.Lock()
	muAcquired := time.Now()
	poolK := 0
	if g.readerPool != nil {
		poolK = g.readerPool.Size()
	}
	s.tearDownReaderPool(g)
	poolDone := time.Now()
	if g.eng != nil {
		g.eng.Close()
		g.eng = nil
	}
	engDone := time.Now()
	if g.snap != nil {
		g.snap.Destroy()
		g.snap = nil
	}
	snapDone := time.Now()
	g.mu.Unlock()
	fmt.Fprintf(os.Stderr,
		"[GO-IVM][TEARDOWN] cg=%s reason=%s total=%v gates=%v muWait=%v pool=%v(k=%d) eng=%v snap=%v\n",
		id, reason, snapDone.Sub(t0),
		gatesDone.Sub(t0),
		muAcquired.Sub(gatesDone),
		poolDone.Sub(muAcquired), poolK,
		engDone.Sub(poolDone),
		snapDone.Sub(engDone))
}

// closeAll shuts down all engines and their worker goroutines.
func (s *Server) closeAll() {
	s.mu.Lock()
	groups := s.groups
	for id, g := range groups {
		s.saveEpochLocked(id, g)
	}
	s.groups = make(map[string]*ClientGroup)
	s.mu.Unlock()
	for id, g := range groups {
		s.shutdownGroup(g, id, "close-all")
	}
	// Reader-shell cache last: the group teardowns above RETURN shells to
	// it (pool.Close), so draining before them would strand those. Cached
	// raw conns are invisible to the *sql.DB pools — they must be closed
	// explicitly or they outlive the server.
	s.replicaMu.Lock()
	rdb := s.replicaDB
	s.replicaMu.Unlock()
	if rdb != nil {
		tablesource.CloseReaderShellCache(rdb)
	}
}

// rpcCodeDataError marks a recovered panic as a DETERMINISTIC, NON-RETRYABLE
// data/schema error (ivm.DataError — bad replica value, e.g. non-JSON in a
// json column, int beyond MAX_SAFE_INTEGER, cross-type compare). The TS
// view-syncer (RPC_CODE_DATA_ERROR in go-ivm-client.ts) tears down the CG
// instead of escalating to a pipeline reset, which would re-read the same bad
// row and loop forever. Generic panics keep -32000 ('unclassified' → rethrow
// → teardown under the follow-TS failure model).
const rpcCodeDataError = -32102

// rpcCodeScalarReset marks a recovered *engine.ScalarResetError: a resolved
// scalar subquery's value changed mid-advance, so the main query's baked-in
// literal is stale. TS's own companion push throws
// ResetPipelinesSignal('scalar-subquery') here (pipeline-driver.ts:1717-1723) —
// a RESET + re-hydrate, NOT a teardown — so this must not ride -32000
// ('unclassified' → teardown). The TS client maps this code back to the
// same ResetPipelinesSignal('scalar-subquery').
const rpcCodeScalarReset = -32105

// panicErrorCode returns the RPC error code for a recovered panic value:
// rpcCodeDataError for an *ivm.DataError, rpcCodeAdvanceAborted for the
// economic advancement-abort (advance_abort.go — sink-site aborts panic
// because the engine sink has no error return), rpcCodeScalarReset for the
// companion scalar-subquery reset, -32000 otherwise.
func panicErrorCode(r any) int {
	if _, ok := r.(*ivm.DataError); ok {
		return rpcCodeDataError
	}
	if _, ok := r.(*advanceAbortedError); ok {
		return rpcCodeAdvanceAborted
	}
	if _, ok := r.(*engine.ScalarResetError); ok {
		return rpcCodeScalarReset
	}
	return -32000
}

// panicErrorMessage renders a recovered panic for the wire. The economic
// abort must arrive byte-identical to TS's advancement-timeout message (the
// TS side surfaces it as the ResetPipelinesSignal message), so it must NOT
// get the "panic: " prefix diagnostics use. Same for the scalar reset,
// whose message mirrors TS's ResetPipelinesSignal('scalar-subquery') text.
func panicErrorMessage(r any) string {
	if e, ok := r.(*advanceAbortedError); ok {
		return e.Error()
	}
	if e, ok := r.(*engine.ScalarResetError); ok {
		return e.Error()
	}
	return fmt.Sprintf("panic: %v", r)
}

// hydrateErrorResponse maps a RETURNED hydrate error to the right RPC code —
// the returned-error twin of panicErrorCode. A hydrate lane's panic is
// recovered per-lane (panics can't cross goroutines) and surfaced as an error
// by firstHydratePanic, so a *ivm.DataError read during the hydrate scan
// arrives here wrapped, not as a live panic. errors.As unwraps it → the SAME
// rpcCodeDataError the advance path emits via panicErrorCode, so TS classifies
// a bad replica value identically whether it surfaces in hydrate or advance.
// Everything else keeps -32000 ('unclassified' → teardown).
func hydrateErrorResponse(reqID interface{}, prefix string, err error) RPCResponse {
	var de *ivm.DataError
	if errors.As(err, &de) {
		return rpcError(reqID, rpcCodeDataError, prefix+err.Error())
	}
	return rpcError(reqID, -32000, prefix+err.Error())
}

// handleStreamWithRecover runs a streaming handler with a panic recover (C1).
// The streaming handlers dispatch directly from the worker goroutine, bypassing
// handleRequest's recover; the engine also re-raises panics onto
// this goroutine. Without this, such a panic would abort the whole process.
// Any partial frames already written are harmless — the error RPCResponse makes
// the TS client reject the call rather than awaiting a "done" that never comes.
func (s *Server) handleStreamWithRecover(
	req RPCRequest,
	streamW streamWriter,
	handler func(RPCRequest, streamWriter) RPCResponse,
) (resp RPCResponse) {
	defer func() {
		if r := recover(); r != nil {
			stack := make([]byte, 4096)
			n := runtime.Stack(stack, false)
			fmt.Fprintf(os.Stderr, "[GO-IVM] PANIC in %s (stream): %v\n%s\n", req.Method, r, stack[:n])
			resp = RPCResponse{
				JSONRPC: "2.0",
				Error:   &RPCError{Code: panicErrorCode(r), Message: panicErrorMessage(r)},
				ID:      req.ID,
			}
		}
	}()
	return handler(req, streamW)
}

func (s *Server) handleRequest(req RPCRequest) (resp RPCResponse) {
	defer func() {
		if r := recover(); r != nil {
			stack := make([]byte, 4096)
			n := runtime.Stack(stack, false)
			fmt.Fprintf(os.Stderr, "[GO-IVM] PANIC in %s: %v\n%s\n", req.Method, r, stack[:n])
			resp = RPCResponse{
				JSONRPC: "2.0",
				Error:   &RPCError{Code: panicErrorCode(r), Message: fmt.Sprintf("panic: %v", r)},
				ID:      req.ID,
			}
		}
	}()
	switch req.Method {
	case "ping":
		return RPCResponse{JSONRPC: "2.0", Result: "pong", ID: req.ID}
	case "version":
		return RPCResponse{
			JSONRPC: "2.0",
			Result: map[string]interface{}{
				"version":     sidecarVersion,
				"protocolRev": sidecarProtocolRev,
			},
			ID: req.ID,
		}
	case "init":
		return s.handleInit(req)
	case "removeQuery":
		return s.handleRemoveQuery(req)
	case "destroy":
		return s.handleDestroy(req)
	default:
		return RPCResponse{
			JSONRPC: "2.0",
			Error:   &RPCError{Code: -32601, Message: "Method not found: " + req.Method},
			ID:      req.ID,
		}
	}
}

// --- init: set up Engine for a client group ---

type initParams struct {
	ClientGroupID string                       `json:"clientGroupID"`
	Storage       string                       `json:"storagePath"`
	Tables        map[string]tableSchemaParams `json:"tables"`
	// AppID names the app whose `${appID}.permissions` table the Snapshotter
	// watches for permissions-change resets. Optional; falls back to the
	// process-level GO_IVM_APP_ID. Only used when advanceToHead is enabled.
	AppID string `json:"appID,omitempty"`
}

type tableSchemaParams struct {
	Columns    map[string]sqlite.ColumnSchema `json:"columns"`
	PrimaryKey []string                       `json:"primaryKey"`
	// UniqueKeys: all column sets with a unique index on this table (includes
	// PrimaryKey). Forwarded from TS-side liteTableSpec.uniqueKeys; consumed
	// by the scalar-subquery resolver to identify subqueries returning at
	// most one row. Empty/nil disables the resolver for this table.
	UniqueKeys [][]string `json:"uniqueKeys,omitempty"`
	// MinRowVersion: the table's minRowVersion (TS liteTableSpec.minRowVersion),
	// set after a RESET during incremental catchup. Forwarded so streamNodes can
	// bump an emitted row's _0_version up to it when below (audit item K, port of
	// pipeline-driver.ts:2843-2850). Empty/absent means no bump for this table.
	MinRowVersion string `json:"minRowVersion,omitempty"`
}

func (s *Server) handleInit(req RPCRequest) RPCResponse {
	var p initParams
	if err := mpUnmarshal(req.Params, &p); err != nil {
		return rpcError(req.ID, -32602, err.Error())
	}

	// Default clientGroupID for backward compat
	cgID := p.ClientGroupID
	if cgID == "" {
		cgID = "default"
	}

	// Init is the only handler that creates the group; non-init handlers
	// pass createIfMissing=false so a concurrent removeGroup can't be
	// papered over by an orphan empty-engine ClientGroup spawn.
	group := s.getGroup(cgID, true)
	group.mu.Lock()
	defer group.mu.Unlock()

	// Storage path (per client group)
	storagePath := p.Storage
	if storagePath == "" {
		storagePath = ":memory:"
	}

	replicaDB, err := s.getReplicaDB()
	if err != nil {
		return rpcError(req.ID, -32000, "replica not ready: "+err.Error())
	}
	writableDB := s.getReplicaWritableDB()
	if writableDB == nil {
		return rpcError(req.ID, -32000, "writable replica pool not ready")
	}
	tables := p.Tables
	eng, err := engine.NewEngine(engine.EngineConfig{
		StoragePath:       storagePath,
		ParallelThreshold: parallelThreshold,
		SourceFactory: func(tableName string) (engine.Source, error) {
			schema, ok := tables[tableName]
			if !ok {
				return nil, nil
			}
			return tablesource.New(replicaDB, writableDB, tableName, schema.Columns, schema.PrimaryKey)
		},
	})
	if err != nil {
		return rpcError(req.ID, -32000, "create engine: "+err.Error())
	}
	committed := false
	var snapState *initSnapshotterState
	defer func() {
		if committed {
			return
		}
		_ = eng.Close()
		if snapState != nil {
			snapState.destroy()
		}
	}()

	minRowVersions := make(map[string]string)
	for tableName, schema := range p.Tables {
		if err := tablesource.Validate(replicaDB, tableName, schema.Columns, schema.PrimaryKey); err != nil {
			return rpcError(req.ID, -32000,
				"tablesource.Validate for "+tableName+": "+err.Error())
		}
		if schema.MinRowVersion != "" {
			minRowVersions[tableName] = schema.MinRowVersion
		}
		// Forward unique-key metadata for the scalar-subquery resolver
		// (scalar resolution is upstream of the leaf source).
		if len(schema.UniqueKeys) > 0 {
			eng.SetTableUniqueKeys(tableName, schema.UniqueKeys)
		}
	}

	// Install the per-table minRowVersion map for the streamNodes bump
	// (audit item K). Empty map is fine — bumpRowVersions is a no-op then.
	eng.SetMinRowVersions(minRowVersions)

	// Build the per-CG Snapshotter — the advanceToHeadStream drive path's
	// leapfrog. Pinned at the current replica head, which is the same frame
	// the hydrate (tablesource fetch) reads from, so the first advance diff
	// is computed against the version the engine was hydrated at. With the
	// TS-shipped advance path removed there is no fallback for a CG whose
	// snapshotter failed to build — fail the init loudly instead (TS's own
	// Snapshotter constructor throws on failure, tearing the syncer down).
	snapState, err = s.buildSnapshotterState(&p)
	if err != nil {
		return rpcError(req.ID, -32000, "snapshotter init: "+err.Error())
	}
	// Drive: the engine's tablesource leaves read from the Snapshotter's
	// frame, not their own per-Source tx. Sticky-bind them to curr now so the
	// initial hydrate reads the same frame the Snapshotter is pinned at. Each
	// advance flips the binding to prev for the apply, then back to curr.
	eng.BindTableSourcesToConn(snapState.current.Conn())

	// Report the snapshotter's pinned stateVersion — the frame the FIRST
	// hydrate reads at (refreshSnapForInitialHydrateLocked deliberately does
	// not re-pin). TS stamps its CVR hydrate updater at
	// max(tsVersion, THIS) so hydrated rows written after TS's own (earlier)
	// snapshot pin are never received under an unbumped CVR version —
	// the cvr.ts:778 "Expected CVR version to have been bumped" teardown
	// (gen-6). Fail loudly if the just-built snapshotter can't report it:
	// silently omitting the field would resurrect that bug for this CG.
	cur := snapState.current

	// Init commits atomically: the old generation stays live until every new
	// generation step above has succeeded. Only now do we tear down old
	// resources, publish eng/snap/specs, and bump initEpoch.
	s.tearDownReaderPool(group)
	if group.eng != nil {
		group.eng.Close()
	}
	if group.snap != nil {
		group.snap.Destroy()
	}
	group.eng = eng
	group.snap = snapState.snap
	group.snapSpecs = snapState.specs
	group.snapAllNames = snapState.allNames
	currentEpoch := group.initEpoch.Add(1)
	committed = true

	return RPCResponse{
		JSONRPC: "2.0",
		Result: map[string]interface{}{
			"status":    "ok",
			"initEpoch": currentEpoch,
			"version":   cur.Version(),
		},
		ID: req.ID,
	}
}

// checkInitEpoch verifies the caller's epoch matches the cg's current
// epoch under group.mu. Caller must already hold group.mu. Returns
// an RPCResponse with rpcCodeStaleInitEpoch if stale, else (response, false).
func checkInitEpoch(group *ClientGroup, reqID interface{}, callerEpoch uint64) (RPCResponse, bool) {
	if cur := group.initEpoch.Load(); callerEpoch != cur {
		return rpcError(reqID, rpcCodeStaleInitEpoch,
			fmt.Sprintf("stale init epoch: caller=%d current=%d", callerEpoch, cur),
		), true
	}
	return RPCResponse{}, false
}

// --- addQueriesParams: shared by addQueriesStream (push + pull modes) ---

type addQueriesParams struct {
	ClientGroupID string `json:"clientGroupID"`
	Queries       []struct {
		QueryID string      `json:"queryID"`
		AST     builder.AST `json:"ast"`
	} `json:"queries"`
	InitEpoch uint64 `json:"initEpoch"`
	// RowMode: see advanceParams.RowMode — same contract for hydrate.
	RowMode bool `json:"rowMode,omitempty"`
	// PullMode (ABI v3, DESIGN-duplex-streaming): hydrate streams must use
	// credit-gated row delivery on the NAPI row plane. Older frame-mode
	// compatibility is deliberately not part of the production contract.
	PullMode bool `json:"pullMode,omitempty"`
	// PullWindow is the OPENING credit for the pull gate (the client's
	// window W). It rides the request — not a first goivm_stream_credit
	// call — because a grant racing ahead of gate registration is a silent
	// no-op: the opening window would be lost and the producer would park
	// until the idle sweep. 0/absent = zero opening credit (every row waits
	// for an explicit grant — the lockstep test mode).
	PullWindow int `json:"pullWindow,omitempty"`
}

// --- addQueriesStream: build pipelines + parallel hydrate, emit per-query
// results as they finish instead of batching the whole response ---
//
// Wire shape: for each query, one OR MORE "partial" frames with the same id,
// each carrying `{queryID, changes, chunkIndex, final, timingMs}`. A query's
// frames have monotonically increasing chunkIndex starting at 0; exactly one
// frame per query has final=true (the last one). After all queries finish,
// exactly one terminal frame whose Result is the literal string "done".
// TS client uses "done" to resolve the call promise.
//
// Single-result queries emit one frame with chunkIndex=0 + final=true
// (preserves the fast path: small queries don't pay framing overhead).
//
// TimingMs is the per-query wall time; it's identical across all chunks of
// the same query (recorded once when the query's fetch finishes).

type addQueriesStreamPartial struct {
	QueryID string `json:"queryID"`
	// Positional (rev 9) RowChange encoding — see positional.go. Replaces the
	// legacy `changes` array; keys are sent once per group in Dict.
	Dict       []dictEntry     `json:"d,omitempty"`
	Rows       [][]interface{} `json:"r,omitempty"`
	ChunkIndex int             `json:"chunkIndex"`
	Final      bool            `json:"final"`
	TimingMs   float64         `json:"timingMs"`
	SigDelta   string          `json:"sigDelta,omitempty"`
}

func (s *Server) handleAddQueriesStream(req RPCRequest, streamW streamWriter) RPCResponse {
	var p addQueriesParams
	if err := mpUnmarshal(req.Params, &p); err != nil {
		return rpcError(req.ID, -32602, err.Error())
	}

	cgID := p.ClientGroupID
	if cgID == "" {
		cgID = "default"
	}

	group := s.getGroup(cgID, false)
	if group == nil {
		return rpcError(req.ID, -32000, "engine not initialized (call init first)")
	}
	group.mu.Lock()
	defer group.mu.Unlock()

	if group.eng == nil {
		return rpcError(req.ID, -32000, "engine not initialized (call init first)")
	}
	if resp, stale := checkInitEpoch(group, req.ID, p.InitEpoch); stale {
		return resp
	}

	specs := make([]engine.QuerySpec, len(p.Queries))
	for i, q := range p.Queries {
		specs[i] = engine.QuerySpec{QueryID: q.QueryID, AST: q.AST}
	}

	// On the Final frame, ChunkIndex+1 is the total chunk count for that
	// query — engine_streaming_test.go locks in the invariant that Final
	// is always on the last (highest-ChunkIndex) frame.
	s.refreshSnapForInitialHydrateLocked(cgID, group)
	// Warm hydrate (live-pipeline CG): parallelize the added queries' fetches on
	// a co-read pool pinned to curr's current frame. No-op for the cold first
	// hydrate (handled above) or when GO_IVM_WARM_HYDRATE_POOL is off. Ephemeral
	// — torn down right after AddQueriesStream so the next advance is unaffected.
	warmPool, warmCR := s.buildWarmReaderPoolLocked(group, cgID)
	if warmPool != nil {
		defer s.tearDownWarmReaderPool(group, warmPool, warmCR)
	}
	// Production stream contract: per-row records via the NAPI row plane,
	// credit-gated by pullMode. Each query's Final partial still ships as a
	// kind-1 frame (per-query TimingMs + completion signal). onResult runs
	// concurrently from hydrate lanes; rowPlane's mutex serializes delivery.
	rp := newRowPlane(s, req.ID, p.RowMode, cgID, group.done)
	if !p.RowMode || !p.PullMode || rp == nil {
		return rpcError(req.ID, -32000,
			"addQueriesStream: requires row-mode pull NAPI transport")
	}

	// Pull mode (ABI v3): register the per-RPC demand gate. Row-bearing
	// deliveries acquire one credit each; group defs, Final frames, and error
	// frames ride free because gating them would deadlock the client.
	rid, _ := numericReqID(req.ID) // non-numeric already refused by newRowPlane
	gate := s.streamGates.register(rid, group, int64(p.PullWindow), func() {
		group.lastUsedNs.Store(time.Now().UnixNano())
	})
	if gate == nil {
		return rpcError(req.ID, -32000,
			"addQueriesStream: pull gate registration failed")
	}
	// The gate's cancel must also unpark a delivery stuck on a full TSFN
	// queue; fold it into the plane's cancellation check.
	rp.setPullGate(gate)
	defer s.streamGates.unregister(rid)
	err := group.eng.AddQueriesStreamPull(specs, 1, func(r engine.QueryResult) bool {
		if len(r.Changes) > 0 && !acquirePullCredit(gate, rp) {
			return false
		}
		if !rp.emitHydratePartial(r) {
			return false
		}
		if r.Final {
			metrics.recordHydrateChunks(r.ChunkIndex + 1)
		}
		return true
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "[GO-IVM] addQueriesStream(pullMode) ERROR cg=%s: %v\n", cgID, err)
		return hydrateErrorResponse(req.ID, "addQueriesStream: ", err)
	}

	// "done" sentinel — TS client uses this to resolve the call promise.
	return RPCResponse{JSONRPC: "2.0", Result: "done", ID: req.ID}
}

// --- removeQuery ---

type removeQueryParams struct {
	ClientGroupID string `json:"clientGroupID"`
	QueryID       string `json:"queryID"`
	InitEpoch     uint64 `json:"initEpoch"`
}

func (s *Server) handleRemoveQuery(req RPCRequest) RPCResponse {
	var p removeQueryParams
	if err := mpUnmarshal(req.Params, &p); err != nil {
		return rpcError(req.ID, -32602, err.Error())
	}

	cgID := p.ClientGroupID
	if cgID == "" {
		cgID = "default"
	}

	group := s.getGroup(cgID, false)
	if group == nil {
		return rpcError(req.ID, -32000, "engine not initialized (call init first)")
	}
	group.mu.Lock()
	defer group.mu.Unlock()

	if group.eng == nil {
		return rpcError(req.ID, -32000, "engine not initialized")
	}
	if resp, stale := checkInitEpoch(group, req.ID, p.InitEpoch); stale {
		return resp
	}

	group.eng.RemoveQuery(p.QueryID)
	return RPCResponse{JSONRPC: "2.0", Result: "ok", ID: req.ID}
}

// --- destroy: tear down a client group ---

type destroyParams struct {
	ClientGroupID string `json:"clientGroupID"`
	// InitEpoch must match the cgID's current epoch (returned by handleInit).
	// A stale destroy from a torn-down view-syncer whose RPC raced past a
	// fresh init for the same cgID must not tear down the live successor's
	// engine. 0 = caller didn't send one (pre-protocolRev-9 client) → reject.
	InitEpoch uint64 `json:"initEpoch"`
}

func (s *Server) handleDestroy(req RPCRequest) RPCResponse {
	var p destroyParams
	if err := mpUnmarshal(req.Params, &p); err != nil {
		return rpcError(req.ID, -32602, err.Error())
	}

	cgID := p.ClientGroupID
	if cgID == "" {
		cgID = "default"
	}

	// Epoch guard: verify the caller's epoch matches before tearing down.
	// We look up the group under s.mu (released immediately, same as every
	// other handler via getGroup), then check epoch under group.mu. We canNOT
	// hold group.mu across removeGroup because shutdownGroup takes group.mu
	// — holding it would self-deadlock. The TOCTOU between the epoch check
	// and removeGroup is benign: a stale view-syncer's destroy and the live
	// instance's init are separated by network latency + TS startup time,
	// not microseconds.
	group := s.getGroup(cgID, false)
	if group != nil {
		// The worker serializes this CG's handlers, so this Lock is normally
		// free; contention here means the REAPER (or a concurrent teardown
		// path) holds g.mu — worth a breadcrumb when it stalls the destroy.
		muWait0 := time.Now()
		group.mu.Lock()
		if w := time.Since(muWait0); w > 5*time.Millisecond {
			fmt.Fprintf(os.Stderr, "[GO-IVM][TEARDOWN] cg=%s destroy epoch-check muWait=%v\n", cgID, w)
		}
		if resp, stale := checkInitEpoch(group, req.ID, p.InitEpoch); stale {
			group.mu.Unlock()
			return resp
		}
		group.mu.Unlock()
	}

	s.removeGroup(cgID)
	return RPCResponse{JSONRPC: "2.0", Result: "ok", ID: req.ID}
}

// --- helpers ---

func rpcError(id interface{}, code int, msg string) RPCResponse {
	return RPCResponse{JSONRPC: "2.0", Error: &RPCError{Code: code, Message: msg}, ID: id}
}

// extractClientGroupID extracts the clientGroupID from an RPC request's params.
// Returns "" for requests without a clientGroupID (e.g., ping).
func extractClientGroupID(req RPCRequest) string {
	if req.Method == "ping" {
		return ""
	}
	var p struct {
		ClientGroupID string `json:"clientGroupID"`
	}
	if err := mpUnmarshal(req.Params, &p); err != nil {
		return ""
	}
	if p.ClientGroupID == "" {
		return "default"
	}
	return p.ClientGroupID
}

// --- connection handler ---

func handleConnection(conn net.Conn, server *Server) {
	defer conn.Close()
	reader := bufio.NewReaderSize(conn, 64*1024)

	// Single flusher: one goroutine owns the socket write, draining a bounded
	// chan []byte. All response paths (immediate, respCh, streamW partials,
	// "done") enqueue encoded frames here. This structurally guarantees FIFO
	// ordering — partials enqueued by lanes before wg.Wait precede the "done"
	// frame enqueued after — and replaces writeFrameLocked+writeMu+writerSem.
	// See DESIGN-streaming-hydrate.md §3g.
	//
	// Backpressure: the flusher's bounded channel (cap 256) blocks the
	// enqueuing goroutine when the socket is slow. That goroutine is one of
	// the dispatcher's per-response goroutines (below), so a full flusher
	// transitively fills outC (cap 1024) which blocks the reader, applying
	// TCP backpressure. The bound is outC.cap + flushCh.cap ≈ 1280 in-flight
	// goroutines — tighter than the old writerSem's 4096.
	encodeFrame := func(resp RPCResponse) []byte {
		data, err := mpMarshal(resp)
		if err != nil {
			data, _ = mpMarshal(rpcError(resp.ID, -32603, "encode response: "+err.Error()))
		}
		// Defense-in-depth: never silently emit a frame the TS reader will skip
		// (which orphans the RPC into a 60s timeout). Substitute a loud error.
		if capped, over := capFrameBytes(resp.ID, data, maxFrameSize); over {
			fmt.Fprintf(os.Stderr,
				"[GO-IVM] response frame too large: %d > %d (id=%v) — sending error instead of orphaning the RPC\n",
				len(data), maxFrameSize, resp.ID)
			data = capped
		}
		return data
	}
	flushCh := make(chan []byte, 256)
	flushDone := make(chan struct{})
	go func() {
		defer close(flushDone)
		for data := range flushCh {
			_ = writeFrame(conn, data) // socket close handled by reader-loop exit
		}
	}()

	type pendingResp struct {
		respCh chan RPCResponse
		// If respCh is nil, immediate is the response to write directly
		// (used for parse errors and group-destroyed errors).
		immediate *RPCResponse
	}
	outC := make(chan pendingResp, 1024)
	dispatchDone := make(chan struct{})
	var writerWg sync.WaitGroup
	go func() {
		defer close(dispatchDone)
		for p := range outC {
			if p.immediate != nil {
				writerWg.Add(1)
				go func(resp RPCResponse) {
					defer writerWg.Done()
					flushCh <- encodeFrame(resp)
				}(*p.immediate)
				continue
			}
			writerWg.Add(1)
			go func(respCh chan RPCResponse) {
				defer writerWg.Done()
				resp := <-respCh
				flushCh <- encodeFrame(resp)
			}(p.respCh)
		}
	}()
	defer func() {
		close(outC)
		<-dispatchDone
		writerWg.Wait()
		close(flushCh)
		<-flushDone
	}()

	enqueueImmediate := func(resp RPCResponse) {
		outC <- pendingResp{immediate: &resp}
	}

	// Partial-frame writer for streaming RPCs. Enqueues to the single flusher
	// so partials and the final "done" frame share one FIFO queue — partials
	// are enqueued synchronously by the lane before it finishes; "done" is
	// enqueued after wg.Wait returns (all lanes complete) → FIFO guarantees
	// partials precede "done". No bypass, no exceptions.
	streamW := streamWriter(func(reqID interface{}, partial interface{}) {
		flushCh <- encodeFrame(RPCResponse{
			JSONRPC: "2.0",
			Result:  partial,
			ID:      reqID,
		})
	})

	for {
		frame, err := readFrame(reader)
		if err != nil {
			return
		}

		var req RPCRequest
		if err := mpUnmarshal(frame, &req); err != nil {
			enqueueImmediate(rpcError(nil, -32700, "Parse error: "+err.Error()))
			continue
		}

		// Preserve FIFO ordering per client group: enqueue SYNCHRONOUSLY
		// (in read-loop order). The writer goroutine drains responses in
		// the same order.
		cgID := extractClientGroupID(req)
		if cgID != "" {
			// Only init can create a new group. Non-init RPCs for an absent
			// group are rejected at dispatch time so the reader never spawns
			// an orphan empty ClientGroup whose only purpose is to fail
			// every subsequent handler and wait for the 30-min reaper.
			createIfMissing := req.Method == "init"
			group := server.getGroup(cgID, createIfMissing)
			if group == nil {
				enqueueImmediate(rpcError(req.ID, -32000, "engine not initialized (call init first)"))
				continue
			}
			respCh := make(chan RPCResponse, 1)
			// Streaming methods get a streamWriter; non-streaming methods
			// don't (streamW field stays nil).
			var sw streamWriter
			if req.Method == "addQueriesStream" || req.Method == "advanceToHeadStream" {
				sw = streamW
			}
			if !group.trySendReq(clientGroupReq{req: req, respCh: respCh, streamW: sw, group: group, cgID: cgID}) {
				enqueueImmediate(rpcError(req.ID, -32000, "client group destroyed"))
			} else {
				outC <- pendingResp{respCh: respCh}
			}
		} else {
			// Non-group requests (ping) — run inline in a goroutine that
			// fulfills its own respCh. Keeps writer ordered.
			respCh := make(chan RPCResponse, 1)
			outC <- pendingResp{respCh: respCh}
			go func(r RPCRequest) {
				respCh <- server.handleRequest(r)
			}(req)
		}
	}
}

// --- main ---

// tuneRuntime relaxes the garbage collector for this allocation-heavy server.
// Each hydrate of a ~1k-row query allocates ~8.6k objects (the per-row Row map
// dominates — see PERF-REVIEW.md). At the Go default GOGC=100 the GC saturates
// under concurrent multi-CG load and becomes the ceiling on multi-core scaling:
// the cross-CG parallel speedup measured 2.6x at 16 CGs on a 14-core box at
// GOGC=100, but 4.9x at GOGC=800 (TableSourceMulti benchmarks) — the cores and
// the parallelization code were never the bottleneck, the GC was.
//
// Defaults to a moderate 2x (GOGC=200), which is overridable:
//   - GO_IVM_GOGC=<n|off>   — GC target percent (this namespace, logged).
//   - GO_IVM_GOMEMLIMIT=<bytes> — soft memory cap (debug.SetMemoryLimit). The
//     principled high-throughput config: set this to the container budget and
//     run a high/off GOGC so GC only fires near the cap.
//
// The standard GOGC env (already applied by the runtime) takes precedence when
// GO_IVM_GOGC is unset, so existing deployments that tuned GOGC are unaffected.

// parseByteSize parses a byte count with an optional binary suffix
// (B, KiB, MiB, GiB, TiB) — the same shapes the Go runtime's own GOMEMLIMIT
// accepts, because operators habitually write "4GiB" for GO_IVM_GOMEMLIMIT
// too (pre-fix that parsed as an error and silently disabled every memory
// fallback; see tuneRuntime).
func parseByteSize(s string) (int64, bool) {
	mult := int64(1)
	num := s
	for _, suf := range []struct {
		s string
		m int64
	}{
		{"KiB", 1 << 10}, {"MiB", 1 << 20}, {"GiB", 1 << 30}, {"TiB", 1 << 40}, {"B", 1},
	} {
		if strings.HasSuffix(s, suf.s) {
			mult = suf.m
			num = strings.TrimSuffix(s, suf.s)
			break
		}
	}
	n, err := strconv.ParseInt(strings.TrimSpace(num), 10, 64)
	if err != nil || n < 0 || (mult > 1 && n > math.MaxInt64/mult) {
		return 0, false
	}
	return n * mult, true
}

func tuneRuntime() {
	// Crash forensics (REVIEW-napi-transport C1): a Go runtime FATAL (not a
	// recovered panic — concurrent map write, stack overflow, etc.) prints its
	// goroutine dump then kills the process. In napi mode that process is the
	// syncer WORKER, and a K8s restart makes the trace easy to miss — so
	// guarantee there IS a trace even if the environment set GOTRACEBACK=none.
	// SetTraceback can only RAISE the level, so this is a floor, never an
	// override of an operator who asked for more ("all"/"crash").
	debug.SetTraceback("single")
	if v := os.Getenv("GO_IVM_GOGC"); v != "" {
		if v == "off" {
			debug.SetGCPercent(-1)
			fmt.Fprintf(os.Stderr, "[GO-IVM] GC disabled (GO_IVM_GOGC=off)\n")
		} else if n, err := strconv.Atoi(v); err == nil {
			debug.SetGCPercent(n)
			fmt.Fprintf(os.Stderr, "[GO-IVM] GC percent set to %d (GO_IVM_GOGC)\n", n)
		}
	} else if os.Getenv("GOGC") == "" {
		// No operator GC tuning at all → apply the moderate default.
		debug.SetGCPercent(200)
		fmt.Fprintf(os.Stderr, "[GO-IVM] GC percent defaulted to 200 (override GO_IVM_GOGC / GOGC)\n")
	}
	if v := os.Getenv("GO_IVM_GOMEMLIMIT"); v != "" {
		if n, ok := parseByteSize(v); ok && n > 0 {
			debug.SetMemoryLimit(n)
			fmt.Fprintf(os.Stderr, "[GO-IVM] soft memory limit set to %d bytes (GO_IVM_GOMEMLIMIT=%s)\n", n, v)
			return
		}
		// Scale review: a malformed value used to `return` here anyway —
		// silently disabling EVERY fallback below (GOMEMLIMIT, cgroup
		// percent, absolute) — so one typo ran the engine with no memory
		// ceiling at GOGC=200: heap balloons to 3x live data, container
		// OOM. Warn loudly and FALL THROUGH to the fallback chain.
		fmt.Fprintf(os.Stderr,
			"[GO-IVM] WARNING: unparseable GO_IVM_GOMEMLIMIT=%q ignored "+
				"(want bytes or a KiB/MiB/GiB/TiB suffix); falling back to the default budget\n", v)
	}
	if os.Getenv("GOMEMLIMIT") != "" {
		return // runtime already applied it
	}
	// No operator memory limit at all → default to a fraction of the cgroup
	// limit. Without ANY limit, GOGC=200 lets the heap balloon to 3× live
	// data and the kernel OOM-kills the whole container — which takes down
	// every CG in the shared sidecar AND the zero-cache workers next to it.
	// The sidecar shares its container with ~20 node syncer workers, so it
	// must not claim the whole budget: default to 40% (override with
	// GO_IVM_GOMEMLIMIT, GOMEMLIMIT, or GO_IVM_GOMEMLIMIT_PERCENT).
	// GOMEMLIMIT is soft — GC works harder near the cap instead of the
	// process dying — and it does not cover C-side SQLite page cache.
	if limit := readCgroupMemoryLimit(); limit > 0 {
		pct := int64(envPositiveInt("GO_IVM_GOMEMLIMIT_PERCENT", 40))
		soft := limit * pct / 100
		debug.SetMemoryLimit(soft)
		fmt.Fprintf(os.Stderr,
			"[GO-IVM] soft memory limit defaulted to %d bytes (%d%% of cgroup limit %d; "+
				"override GO_IVM_GOMEMLIMIT / GO_IVM_GOMEMLIMIT_PERCENT)\n",
			soft, pct, limit)
		return
	}
	// Cgroup limit unreadable or unlimited (bare metal, dev machine,
	// non-standard cgroup layout) — without ANY ceiling, GOGC=200 lets the
	// heap balloon to 3× live data with nothing pushing back. Apply a
	// generous absolute fallback; it's a SOFT limit (GC works harder near
	// it, nothing dies) sized well above typical sidecar working sets.
	// Override with GO_IVM_GOMEMLIMIT for bigger deployments.
	const fallbackSoftLimit = int64(8) << 30 // 8GiB
	debug.SetMemoryLimit(fallbackSoftLimit)
	fmt.Fprintf(os.Stderr,
		"[GO-IVM] soft memory limit defaulted to %d bytes (no cgroup limit found; "+
			"override GO_IVM_GOMEMLIMIT)\n", fallbackSoftLimit)
}

// readCgroupMemoryLimit returns the container memory limit in bytes, or 0
// when unlimited / not in a container / unreadable. Supports cgroup v2
// (memory.max) and v1 (memory.limit_in_bytes).
func readCgroupMemoryLimit() int64 {
	for _, p := range []string{
		"/sys/fs/cgroup/memory.max",
		"/sys/fs/cgroup/memory/memory.limit_in_bytes",
	} {
		b, err := os.ReadFile(p)
		if err != nil {
			continue
		}
		s := strings.TrimSpace(string(b))
		if s == "max" {
			return 0
		}
		n, err := strconv.ParseInt(s, 10, 64)
		// cgroup v1 reports "unlimited" as a huge sentinel (~2^63); treat
		// anything implausibly large (>4TB) as no limit.
		if err != nil || n <= 0 || n > int64(4)<<40 {
			return 0
		}
		return n
	}
	return 0
}

// main is a stub: the socket transport was removed in the RPC-surface
// removal sweep (protocolRev 10). The ONLY supported deployment is the
// in-process NAPI transport — build the c-shared library with
// `-tags napilib -buildmode=c-shared` and dlopen it from the zero-cache
// syncer worker (see napi_lib.go / abi.go). This package remains `main`
// so the plain `go build ./cmd/sidecar` compile gate keeps working.
func main() {
	fmt.Fprintln(os.Stderr,
		"go-ivm-sidecar: the socket transport was removed; "+
			"build with -tags napilib -buildmode=c-shared and load in-process via NAPI")
	os.Exit(1)
}
