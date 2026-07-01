package main

// MessagePack-RPC server over a Unix socket. One Engine per client group,
// each running in its own goroutine so different groups execute in parallel.
// Wire format: 4-byte big-endian length prefix, then a MessagePack payload.

import (
	"bufio"
	"bytes"
	"context"
	"database/sql"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"net/http"
	_ "net/http/pprof" // registers /debug/pprof on http.DefaultServeMux when active
	"os"
	"os/signal"
	"reflect"
	"runtime"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
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
// results via the streaming RPCs (addQueriesStream / advanceStream / *Stream);
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

// recordAdvanceChunks logs the chunk count for ONE advanceStream call
// that just finalized. n=1 means the entire diff fit in one frame; n>1
// means it crossed advanceChunkSize.
func (m *perfMetrics) recordAdvanceChunks(n int) {
	m.mu.Lock()
	m.advanceChunkCounts = append(m.advanceChunkCounts, n)
	m.mu.Unlock()
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
	m.advanceLatencies = nil
	m.hydrateLatencies = nil
	m.hydrateChunkCounts = nil
	m.advanceChunkCounts = nil
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

	if advCount == 0 && hydCount == 0 && bindCoread == 0 && bindConverge == 0 &&
		bindSerial == 0 && warmCoread == 0 && warmSerial == 0 {
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

	fmt.Fprintf(os.Stderr,
		"[GO-IVM][PERF-CHUNKS] 10s window: hydrate chunks (p50=%d p95=%d max=%d n=%d) advance chunks (p50=%d p95=%d max=%d n=%d)\n",
		hydChunkP50, hydChunkP95, hydChunkMax, len(hydChunks),
		advChunkP50, advChunkP95, advChunkMax, len(advChunks))

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

const defaultSocket = "/tmp/go-ivm.sock"

// Version handshake — bumped when wire format or RPC semantics change so
// the TS client can refuse to talk to an incompatible sidecar
// (REVIEW-final MED-CROSS-5).
const (
	sidecarVersion     = "0.7.0"
	sidecarProtocolRev = 9 // bumped: streamed RowChange chunks (addQueriesStream, advanceStream, advanceToHeadStream) now use the positional wire encoding (see positional.go) — keys sent once per (queryID,table) group instead of per row. Rev 8 added advanceToHeadStream. Rev 7 added advanceToHead. Rev 6 added refreshSnapshot.
)

// rpcCodeStaleInitEpoch signals that a mutating RPC arrived with an
// initEpoch that doesn't match the cgID's current epoch. Caller is from a
// torn-down view-syncer instance and must not be allowed to mutate engine
// state. The TS client treats this as a no-op (the live instance will
// reconcile via its own init+loadRows).
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

// maxDiffChanges caps how many change-log entries advanceToHead[Stream] will
// materialize via diff.Collect (full row values per entry — see the guards at
// the Collect call sites). Above the cap the RPC returns an error and the TS
// caller resets/re-hydrates with bounded memory. 50k fat rows ≈ low hundreds
// of MB transient per CG — past that, replaying the diff is slower than a
// re-hydrate anyway.
var maxDiffChanges = envPositiveInt("GO_IVM_MAX_DIFF_CHANGES", 50_000)

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
	// Data carries an optional structured payload. Used by the drift
	// signal (code -32100) to ship *ivm.DriftError details — table, op,
	// PK, has_count — so TS can log specifics before re-init.
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
	// initEpoch monotonically increments on every handleInit (under mu).
	// Mutating RPCs (loadRows / addQuery* / advance*) carry the epoch they
	// were issued under; mismatch → rejected with rpcCodeStaleInitEpoch.
	// This catches a torn-down view-syncer instance whose late-arriving
	// loadRows would otherwise mix old-snapshot rows into the freshly
	// init'd engine of a new instance for the same cgID.
	initEpoch uint64
	// lastUsedNs is set on every request arrival; the idle reaper compares
	// against `now - groupIdleTimeout` to garbage-collect abandoned groups
	// (REVIEW-final HIGH-CROSS-2 / HIGH-CROSS-3). Accessed via atomic so the
	// reaper doesn't need mu.
	lastUsedNs atomic.Int64

	// snap is this group's Snapshotter — the Go-side leapfrog that derives
	// its own snapshot diff from the replica's changeLog2 (internal/snapshotter).
	// Non-nil only when GO_IVM_ADVANCE_TO_HEAD=true AND sourceMode==table; the
	// advanceToHead RPC uses it instead of TS-shipped SnapshotChange[]. Built
	// in handleInit (pinned at the then-current head, matching the hydrate
	// version), torn down in shutdownGroup / re-init. snapSpecs/snapAllNames
	// are the syncable TableSpecs and the full replicated table-name set,
	// captured at init for the Diff's syncable/non-syncable classification.
	snap         *snapshotter.Snapshotter
	snapSpecs    map[string]*snapshotter.TableSpec
	snapAllNames map[string]bool

	// readerPool is the cold-start parallel-hydrate reader pool (drive mode,
	// GO_IVM_HYDRATE_READERS>1). Built+bound in buildSnapshotterLocked at curr's
	// stateVersion; torn down at the first advance (tearDownReaderPool) and on
	// shutdownGroup / re-init. Nil when the feature is off or the pool couldn't
	// pin (replica advanced past curr) — both fall back to the single-conn path.
	readerPool *tablesource.ReaderPool
	coread     *tablesource.CoRead
}

type clientGroupReq struct {
	req    RPCRequest
	respCh chan RPCResponse
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

	// Leaf-source mode for this sidecar process. ModeMemory uses the
	// classic loadRows-populated MemorySource; ModeTable constructs a
	// tablesource.Source per (cg, table) over replicaDB and treats
	// loadRows as a no-op (the SQLite file is already authoritative).
	sourceMode tablesource.Mode

	// Path-and-lazy-open for the read-side replica pool. In table mode
	// we keep main()'s accept loop unblocked by deferring the actual
	// open until the first init RPC — by that time the TS replicator
	// has finished writing the SQLite header.
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
	// per-init AppID field). Only consulted when advanceToHead is enabled.
	appID string

	// advanceToHeadEnabled gates the Go-derived-diff path. When true (and
	// sourceMode==table) handleInit builds a per-CG Snapshotter and the
	// advanceToHead RPC becomes available. Off by default — the legacy
	// TS-ships-the-diff advance path is unaffected (design §7 P1: behind a flag).
	advanceToHeadEnabled bool

	// advanceDriveEnabled (P2) turns advanceToHead from a pure derivation into a
	// self-consistent driven advance: the engine's tablesource leaves are
	// frame-bound to the Snapshotter's pinned conn (curr for hydrate, prev for
	// the apply), so Go's own derived diff drives Go's engine with no TS-shipped
	// changes and no frame-timing drift. Implies advanceToHeadEnabled.
	advanceDriveEnabled bool

	// hydrateReaders is GO_IVM_HYDRATE_READERS — the size of the per-CG
	// frame-pinned reader pool used to parallelize cold-start hydrate (drive
	// mode only). 1 (default) keeps the legacy single-conn serial path: no pool
	// is built and the feature is fully dark. >1 builds a K-connection pool at
	// init (all pinned to curr's stateVersion) so the per-query hydrate
	// goroutines read in parallel; torn down at the first advance.
	hydrateReaders int

	// hydrateLanes is GO_IVM_HYDRATE_LANES — the number of worker lanes (P)
	// that hydrate queries in parallel. Replaces the unbounded per-query
	// goroutine spawn with P workers. The reader pool is sized to at least P
	// (K = P × Cmax; Cmax=1 while operators are eager → K=P) so every lane
	// can always acquire a reader. Default 4. 1 = serial (legacy).
	hydrateLanes int

	// warmHydratePoolEnabled extends the parallel-hydrate reader pool to WARM
	// hydrates (addQueriesStream on a CG that already has live pipelines), not
	// just the first cold one. From GO_IVM_WARM_HYDRATE_POOL=true; default OFF so
	// the shipped cold-only path is untouched. Unlike the cold pool, the warm
	// pool is co-read-ONLY and pinned to curr's EXISTING frame (no
	// RefreshCurrentToHead): a converge-upward fallback would read a NEWER frame
	// than the live pipelines and desync the new query, so on any co-read failure
	// the warm path stays serial (reads the bound curr.Conn()) rather than
	// converging. Requires hydrateReaders>1 + advanceDrive.
	warmHydratePoolEnabled bool
}

func NewServer(mode tablesource.Mode, replicaPath string) *Server {
	return &Server{
		groups:         make(map[string]*ClientGroup),
		sourceMode:     mode,
		replicaPath:    replicaPath,
		hydrateReaders: 1,
		hydrateLanes:   4,
	}
}

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
		return nil, fmt.Errorf("getReplicaDB: replicaPath unset (sourceMode=%s)", s.sourceMode)
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

	const openTimeout = 60 * time.Second
	deadline := time.Now().Add(openTimeout)
	backoff := 500 * time.Millisecond
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
		if current.lastUsedNs.Load() >= cutoffNs {
			s.mu.Unlock()
			continue
		}
		delete(s.groups, c.id)
		s.mu.Unlock()
		s.shutdownGroup(c.g)
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
			// unblock. The grace deadline absorbs a small race window:
			// a trySendReq whose select observed done-not-closed AND
			// reqC-has-space can commit to the reqC send case AFTER we
			// noticed done was closed. Without the grace period we'd
			// exit on `default` and orphan that send (respCh hangs
			// forever). 50ms is far longer than the goroutine commits
			// involved, so any racy sender lands in time.
			deadline := time.NewTimer(50 * time.Millisecond)
			defer deadline.Stop()
			for {
				select {
				case r := <-g.reqC:
					r.respCh <- RPCResponse{
						JSONRPC: "2.0",
						Error:   &RPCError{Code: -32000, Message: "client group destroyed"},
						ID:      r.req.ID,
					}
				case <-deadline.C:
					return
				}
			}
		}
		g.lastUsedNs.Store(time.Now().UnixNano())
		var start time.Time
		method := req.req.Method

		switch method {
		case "advanceStream":
			start = time.Now()
			n := metrics.advancesInFlight.Add(1)
			updatePeak(&metrics.peakAdvConc, n)
		case "addQuery", "addQueries", "addQueriesStream":
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
			// AdvanceStream deliberately re-raises non-drift panics on this
			// (worker) goroutine — so without this recover such a panic aborts
			// the whole multi-CG process. Convert it to an error response (the
			// TS client rejects the call) instead.
			resp = s.handleStreamWithRecover(req.req, req.streamW, s.handleAddQueriesStream)
		} else if req.streamW != nil && method == "advanceStream" {
			// Streaming variant of advance: partial frames go through streamW;
			// terminal "done" RPCResponse still flows through respCh.
			resp = s.handleStreamWithRecover(req.req, req.streamW, s.handleAdvanceStream)
		} else if req.streamW != nil && method == "advanceToHeadStream" {
			// Streaming variant of advanceToHead (drive mode): chunked RowChanges
			// go through streamW; terminal "done" RPCResponse flows through respCh.
			resp = s.handleStreamWithRecover(req.req, req.streamW, s.handleAdvanceToHeadStream)
		} else {
			resp = s.handleRequest(req.req)
		}

		endSpan(resp.Error)

		switch method {
		case "advanceStream":
			metrics.advancesInFlight.Add(-1)
			metrics.recordAdvance(time.Since(start))
		case "addQuery", "addQueries", "addQueriesStream":
			metrics.hydratesInFlight.Add(-1)
			metrics.recordHydrate(time.Since(start))
		}

		// Slow-handler log doubles as a fallback breadcrumb when OTel is off.
		if !start.IsZero() {
			if elapsed := time.Since(start); elapsed > 500*time.Millisecond && req.req.Traceparent != "" {
				fmt.Fprintf(os.Stderr, "[GO-IVM][SLOW] method=%s elapsed=%v traceparent=%s\n",
					method, elapsed, req.req.Traceparent)
			}
		}

		req.respCh <- resp
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
	select {
	case <-g.done:
		return false
	default:
	}
	g.lastUsedNs.Store(time.Now().UnixNano())
	select {
	case g.reqC <- req:
		return true
	case <-g.done:
		return false
	}
}

// removeGroup destroys a client group and its engine. Closes reqC so the
// worker goroutine exits cleanly; without this, every destroy leaked a
// goroutine + the channel + the closed engine reference.
func (s *Server) removeGroup(id string) {
	s.mu.Lock()
	g := s.groups[id]
	delete(s.groups, id)
	s.mu.Unlock()
	if g != nil {
		s.shutdownGroup(g)
	}
}

// shutdownGroup performs the per-group teardown: signals the worker to
// exit via close(done), then closes the engine. Idempotent via closeOnce
// so concurrent removeGroup / closeAll don't race on the close.
//
// Does NOT close reqC — closing the data channel would race with concurrent
// trySendReq calls and panic on send-after-close. The worker treats `done`
// as the exit signal and drains any remaining buffered requests with an
// error response before returning. New senders past this point see done
// closed and bail out without touching reqC.
func (s *Server) shutdownGroup(g *ClientGroup) {
	g.closeOnce.Do(func() {
		close(g.done)
	})
	// Engine cleanup needs g.mu because handlers also take it (and we may
	// race with a handler that just dequeued before done was closed; the
	// handler will finish and respCh-send before re-entering the worker
	// loop, at which point the drain branch fires).
	g.mu.Lock()
	s.tearDownReaderPool(g)
	if g.eng != nil {
		g.eng.Close()
		g.eng = nil
	}
	if g.snap != nil {
		g.snap.Destroy()
		g.snap = nil
	}
	g.mu.Unlock()
}

// closeAll shuts down all engines and their worker goroutines.
func (s *Server) closeAll() {
	s.mu.Lock()
	groups := s.groups
	s.groups = make(map[string]*ClientGroup)
	s.mu.Unlock()
	for _, g := range groups {
		s.shutdownGroup(g)
	}
}

// rpcCodeDataError marks a recovered panic as a DETERMINISTIC, NON-RETRYABLE
// data/schema error (ivm.DataError — bad replica value, e.g. non-JSON in a
// json column, int beyond MAX_SAFE_INTEGER, cross-type compare). The TS
// view-syncer (RPC_CODE_DATA_ERROR in go-ivm-client.ts) tears down the CG
// instead of escalating to a pipeline reset, which would re-read the same bad
// row and loop forever. Generic panics keep -32000 (transient → reset).
const rpcCodeDataError = -32102

// panicErrorCode returns the RPC error code for a recovered panic value:
// rpcCodeDataError for an *ivm.DataError, -32000 otherwise.
func panicErrorCode(r any) int {
	if _, ok := r.(*ivm.DataError); ok {
		return rpcCodeDataError
	}
	return -32000
}

// handleStreamWithRecover runs a streaming handler with a panic recover (C1).
// The streaming handlers dispatch directly from the worker goroutine, bypassing
// handleRequest's recover; AdvanceStream also re-raises non-drift panics onto
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
				Error:   &RPCError{Code: panicErrorCode(r), Message: fmt.Sprintf("panic: %v", r)},
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
				// sourceMode lets TS skip shipping row contents at init when
				// the sidecar reads SQLite directly (loadRows is a no-op in
				// table mode). Additive field — clients that don't read it
				// keep the old ship-everything behavior.
				"sourceMode": s.sourceMode.String(),
			},
			ID: req.ID,
		}
	case "init":
		return s.handleInit(req)
	case "loadRows":
		return s.handleLoadRows(req)
	case "addQuery":
		return s.handleAddQuery(req)
	case "addQueries":
		return s.handleAddQueries(req)
	case "removeQuery":
		return s.handleRemoveQuery(req)
	case "advanceToHead":
		return s.handleAdvanceToHead(req)
	case "destroy":
		return s.handleDestroy(req)
	case "refreshSnapshot":
		return s.handleRefreshSnapshot(req)
	case "pipelineCount":
		return s.handlePipelineCount(req)
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
	DBPath        string                       `json:"dbPath"` // deprecated, kept for compat
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
	// pipeline-driver.ts:3172-3178). Empty/absent means no bump for this table.
	MinRowVersion string `json:"minRowVersion,omitempty"`
	// Rows are typed as ivm.Row (not []map[string]interface{}) so the custom
	// Row.DecodeMsgpack runs at decode time — performing the in-place
	// int->float64 coercion that the SnapshotChange path already gets via
	// engine.SnapshotChange.PrevValues being typed as ivm.Row. Without this
	// the init/loadRows paths produce int* values that survive into
	// downstream comparisons in MemorySource mode; the FromSQLiteType
	// coercion below catches schema-mapped columns but the no-schema
	// fallback (e.g. json columns) leaves the raw decoded type asymmetric
	// vs the advance path. Audit's latent ModeMemory<->ModeTable divergence.
	Rows []ivm.Row `json:"rows"`
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

	// Close previous engine if re-initializing this group
	if group.eng != nil {
		group.eng.Close()
	}
	// Tear down a previous Snapshotter (re-init re-pins from scratch).
	if group.snap != nil {
		group.snap.Destroy()
		group.snap = nil
		group.snapSpecs = nil
		group.snapAllNames = nil
	}

	// Bump epoch BEFORE we create the new engine so any in-flight loadRows
	// from the prior epoch is rejected even if it races the engine swap.
	group.initEpoch++
	currentEpoch := group.initEpoch

	// Storage path (per client group)
	storagePath := p.Storage
	if storagePath == "" {
		storagePath = ":memory:"
	}

	eng, err := engine.NewEngine(engine.EngineConfig{
		StoragePath:       storagePath,
		ParallelThreshold: parallelThreshold,
	})
	if err != nil {
		return rpcError(req.ID, -32000, "create engine: "+err.Error())
	}
	group.eng = eng

	// For each table: register the leaf source. In ModeMemory we build a
	// MemorySource and bulk-load the rows TS sent in the init payload.
	// In ModeTable we build a tablesource.Source over the shared replica
	// pool — schema.Rows is ignored (the SQLite file is authoritative).
	minRowVersions := make(map[string]string)
	for tableName, schema := range p.Tables {
		if schema.MinRowVersion != "" {
			minRowVersions[tableName] = schema.MinRowVersion
		}
		columns := make(map[string]string, len(schema.Columns))
		for col, cs := range schema.Columns {
			columns[col] = cs.Type
		}

		if s.sourceMode == tablesource.ModeTable {
			db, err := s.getReplicaDB()
			if err != nil {
				return rpcError(req.ID, -32000,
					"replica not ready: "+err.Error())
			}
			writableDB := s.getReplicaWritableDB()
			if writableDB == nil {
				return rpcError(req.ID, -32000,
					"writable replica pool not ready")
			}
			src, err := tablesource.New(db, writableDB, tableName, schema.Columns, schema.PrimaryKey)
			if err != nil {
				return rpcError(req.ID, -32000,
					"tablesource.New for "+tableName+": "+err.Error())
			}
			eng.RegisterSource(src)
		} else {
			// Inject FromSQLiteType as the column converter so MemorySource.NormalizeRow
			// (called on every advance / loadRows row) produces the same type shapes
			// as the init path for ALL column types — including json/string/blob,
			// which the legacy partial converter missed. REVIEW-ts-integration CRITICAL-3.
			ms := ivm.NewMemorySourceWithConverter(
				tableName, columns, schema.PrimaryKey,
				func(v interface{}, colType string) ivm.Value {
					return sqlite.FromSQLiteType(v, colType)
				},
			)

			// Convert raw JSON rows to typed ivm.Row using column schemas
			rows := make([]ivm.Row, 0, len(schema.Rows))
			for _, rawRow := range schema.Rows {
				row := make(ivm.Row, len(rawRow))
				for col, val := range rawRow {
					if cs, ok := schema.Columns[col]; ok {
						row[col] = sqlite.FromSQLiteType(val, cs.Type)
					} else {
						row[col] = val
					}
				}
				rows = append(rows, row)
			}
			ms.BulkInsert(rows)

			eng.RegisterMemorySource(ms)
		}

		// Forward unique-key metadata for the scalar-subquery resolver.
		// Same call for both source modes (scalar resolution is upstream
		// of the leaf source).
		if len(schema.UniqueKeys) > 0 {
			eng.SetTableUniqueKeys(tableName, schema.UniqueKeys)
		}
	}

	// Install the per-table minRowVersion map for the streamNodes bump
	// (audit item K). Empty map is fine — bumpRowVersions is a no-op then.
	eng.SetMinRowVersions(minRowVersions)

	// Build the per-CG Snapshotter when the Go-derived-diff path is armed.
	// Pinned at the current replica head, which is the same frame the
	// hydrate (tablesource fetch) reads from, so the first advanceToHead diff
	// is computed against the version the engine was hydrated at. Failures
	// here are non-fatal: advanceToHead simply stays unavailable for this CG
	// and the legacy TS-shipped advance path keeps working.
	if s.advanceToHeadEnabled && s.sourceMode == tablesource.ModeTable {
		if err := s.buildSnapshotterLocked(group, &p); err != nil {
			fmt.Fprintf(os.Stderr,
				"[GO-IVM] advanceToHead disabled for cg=%s: %v\n", cgID, err)
		}
	}

	return RPCResponse{
		JSONRPC: "2.0",
		Result: map[string]interface{}{
			"status":    "ok",
			"initEpoch": currentEpoch,
		},
		ID: req.ID,
	}
}

// --- loadRows: append a chunked batch of rows to a registered MemorySource ---
//
// init carries schema only; loadRows streams data in batches sized to fit
// comfortably under the 64MB frame cap. Each batch is normalized via
// FromSQLiteType per column before insertion, matching the init path.
// REVIEW-ts-integration CRITICAL-2.

type loadRowsParams struct {
	ClientGroupID string `json:"clientGroupID"`
	Table         string `json:"table"`
	// Rows typed as ivm.Row so Row.DecodeMsgpack runs uniformly with the
	// SnapshotChange path; see tableSchemaParams.Rows for full rationale.
	Rows []ivm.Row `json:"rows"`
	// InitEpoch must match the cgID's current epoch (returned by handleInit).
	// Stale callers (torn-down view-syncer whose loadRows raced past the
	// init from a new instance for the same cgID) are rejected with
	// rpcCodeStaleInitEpoch instead of corrupting the new engine's state.
	// 0 = caller didn't send one (pre-protocolRev-5 client) → reject.
	InitEpoch uint64 `json:"initEpoch"`
}

// checkInitEpoch verifies the caller's epoch matches the cg's current
// epoch under group.mu. Caller must already hold group.mu. Returns
// an RPCResponse with rpcCodeStaleInitEpoch if stale, else (response, false).
func checkInitEpoch(group *ClientGroup, reqID interface{}, callerEpoch uint64) (RPCResponse, bool) {
	if callerEpoch != group.initEpoch {
		return rpcError(reqID, rpcCodeStaleInitEpoch,
			fmt.Sprintf("stale init epoch: caller=%d current=%d", callerEpoch, group.initEpoch),
		), true
	}
	return RPCResponse{}, false
}

func (s *Server) handleLoadRows(req RPCRequest) RPCResponse {
	var p loadRowsParams
	if err := mpUnmarshal(req.Params, &p); err != nil {
		return rpcError(req.ID, -32602, err.Error())
	}

	// In table mode the SQLite replica is authoritative — TS still sends
	// loadRows for protocol compatibility, but we have nothing to do
	// with the rows. Return success without touching engine state.
	if s.sourceMode == tablesource.ModeTable {
		return RPCResponse{
			JSONRPC: "2.0",
			Result:  map[string]interface{}{"status": "ok"},
			ID:      req.ID,
		}
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

	ms := group.eng.GetMemorySource(p.Table)
	if ms == nil {
		return rpcError(req.ID, -32000, "no MemorySource registered for table: "+p.Table)
	}

	columns := ms.Columns()
	rows := make([]ivm.Row, 0, len(p.Rows))
	for _, rawRow := range p.Rows {
		row := make(ivm.Row, len(rawRow))
		for col, val := range rawRow {
			if colType, ok := columns[col]; ok {
				row[col] = sqlite.FromSQLiteType(val, colType)
			} else {
				row[col] = val
			}
		}
		rows = append(rows, row)
	}
	ms.BulkInsert(rows)
	return RPCResponse{JSONRPC: "2.0", Result: "ok", ID: req.ID}
}

// --- addQuery: build pipeline + hydrate ---

type addQueryParams struct {
	ClientGroupID string      `json:"clientGroupID"`
	QueryID       string      `json:"queryID"`
	AST           builder.AST `json:"ast"`
	InitEpoch     uint64      `json:"initEpoch"`
}

type addQueryResult struct {
	Changes  []engine.RowChange `json:"changes"`
	TimingMs float64            `json:"timingMs,omitempty"`
}

func (s *Server) handleAddQuery(req RPCRequest) RPCResponse {
	var p addQueryParams
	if err := mpUnmarshal(req.Params, &p); err != nil {
		// Diagnostic: dump first 64 bytes so we can see what wire format arrived.
		dump := req.Params
		if len(dump) > 64 {
			dump = dump[:64]
		}
		fmt.Fprintf(os.Stderr, "[GO-IVM] addQuery decode FAIL err=%v paramsLen=%d firstBytes=%x\n",
			err, len(req.Params), dump)
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

	s.refreshSnapForInitialHydrateLocked(cgID, group, []engine.QuerySpec{{AST: p.AST}})
	changes, timingMs, err := group.eng.AddQuery(p.QueryID, p.AST)
	if err != nil {
		fmt.Fprintf(os.Stderr, "[GO-IVM] addQuery ERROR cg=%s query=%s: %v\n", cgID, p.QueryID, err)
		return rpcError(req.ID, -32000, "addQuery: "+err.Error())
	}

	return RPCResponse{JSONRPC: "2.0", Result: addQueryResult{Changes: changes, TimingMs: timingMs}, ID: req.ID}
}

// --- addQueries: batch build + parallel hydrate ---

type addQueriesParams struct {
	ClientGroupID string `json:"clientGroupID"`
	Queries       []struct {
		QueryID string      `json:"queryID"`
		AST     builder.AST `json:"ast"`
	} `json:"queries"`
	InitEpoch uint64 `json:"initEpoch"`
}

type addQueriesResult struct {
	Results []addQueryResult `json:"results"`
}

func (s *Server) handleAddQueries(req RPCRequest) RPCResponse {
	var p addQueriesParams
	if err := mpUnmarshal(req.Params, &p); err != nil {
		// Find position of first 0xd4 in params for diagnosis
		d4Pos := -1
		for i, b := range req.Params {
			if b == 0xd4 {
				d4Pos = i
				break
			}
		}
		// Dump bytes around the d4 position (±20 bytes) plus tail of message
		start := d4Pos - 20
		if start < 0 {
			start = 0
		}
		end := d4Pos + 20
		if end > len(req.Params) {
			end = len(req.Params)
		}
		ctx := req.Params[start:end]
		tail := req.Params
		if len(tail) > 64 {
			tail = tail[len(tail)-64:]
		}
		fmt.Fprintf(os.Stderr, "[GO-IVM] addQueries decode FAIL err=%v len=%d d4Pos=%d ctx[%d:%d]=%x lastBytes=%x\n",
			err, len(req.Params), d4Pos, start, end, ctx, tail)
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

	s.refreshSnapForInitialHydrateLocked(cgID, group, specs)
	warmPool, warmCR := s.buildWarmReaderPoolLocked(group, engine.ConservativeHydrateCmaxForSpecs(specs))
	if warmPool != nil {
		defer s.tearDownWarmReaderPool(group, warmPool, warmCR)
	}
	results, err := group.eng.AddQueries(specs)
	if err != nil {
		fmt.Fprintf(os.Stderr, "[GO-IVM] addQueries ERROR cg=%s: %v\n", cgID, err)
		return rpcError(req.ID, -32000, "addQueries: "+err.Error())
	}

	resultList := make([]addQueryResult, len(results))
	for i, r := range results {
		resultList[i] = addQueryResult{Changes: r.Changes, TimingMs: r.TimingMs}
	}

	return RPCResponse{JSONRPC: "2.0", Result: addQueriesResult{Results: resultList}, ID: req.ID}
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
	s.refreshSnapForInitialHydrateLocked(cgID, group, specs)
	// Warm hydrate (live-pipeline CG): parallelize the added queries' fetches on
	// a co-read pool pinned to curr's current frame. No-op for the cold first
	// hydrate (handled above) or when GO_IVM_WARM_HYDRATE_POOL is off. Ephemeral
	// — torn down right after AddQueriesStream so the next advance is unaffected.
	warmPool, warmCR := s.buildWarmReaderPoolLocked(group, engine.ConservativeHydrateCmaxForSpecs(specs))
	if warmPool != nil {
		defer s.tearDownWarmReaderPool(group, warmPool, warmCR)
	}
	err := group.eng.AddQueriesStream(specs, func(r engine.QueryResult) {
		pc := toPositional(r.Changes)
		streamW(req.ID, addQueriesStreamPartial{
			QueryID:    r.QueryID,
			Dict:       pc.Dict,
			Rows:       pc.Rows,
			ChunkIndex: r.ChunkIndex,
			Final:      r.Final,
			TimingMs:   r.TimingMs,
		})
		if r.Final {
			metrics.recordHydrateChunks(r.ChunkIndex + 1)
		}
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "[GO-IVM] addQueriesStream ERROR cg=%s: %v\n", cgID, err)
		return rpcError(req.ID, -32000, "addQueriesStream: "+err.Error())
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

// --- advance: push snapshot diffs through all pipelines ---

type advanceParams struct {
	ClientGroupID string                  `json:"clientGroupID"`
	Changes       []engine.SnapshotChange `json:"changes"`
	InitEpoch     uint64                  `json:"initEpoch"`
}

// rpcCodeDrift is the JSON-RPC error code for source-drift detection.
// TS-side go-ivm-client recognizes this code and triggers re-init via the
// existing onRestart pipeline, then emits the partial Changes the drift
// data carries to clients before re-hydrating (matching TS's view-syncer
// where assert-and-throw streams pre-throw RowChanges to pokers before
// snapshot revert).
const rpcCodeDrift = -32100

// driftRPCData is the JSON shape Go ships in the rpcCodeDrift error's
// Data field. Carries the standard *ivm.DriftError diagnostic fields
// PLUS the partial Advance output produced by Pushes that completed
// successfully BEFORE the panic. TS-side accumulates these into
// DriftError.partialChanges so GoComputeBackend's drift recovery can
// forward them to the view-syncer instead of dropping them.
type driftRPCData struct {
	Table          string               `json:"table"`
	Op             string               `json:"op"`
	PK             map[string]ivm.Value `json:"pk"`
	HasCount       int                  `json:"hasCount"`
	PartialChanges []engine.RowChange   `json:"partialChanges,omitempty"`
	PartialTimings []engine.TableTiming `json:"partialTimings,omitempty"`
}

// --- advanceStream: streaming variant of the push-mode advance ---
//
// Wire shape: one OR MORE "partial" frames with the same id, each carrying
// `{changes, chunkIndex, final, timings?}`. Frames arrive in monotonic
// chunkIndex order (the engine's AdvanceStream calls onResult synchronously
// from one goroutine). Exactly one frame has final=true (the last); only
// that frame carries timings. After the final partial frame, exactly one
// terminal frame whose Result is the literal string "done" — TS client
// uses "done" to resolve the call promise.
//
// Reuses `advanceParams` (the {clientGroupID, changes, initEpoch} request
// shape).

type advanceStreamPartial struct {
	// Positional (rev 9) RowChange encoding — see positional.go.
	Dict       []dictEntry          `json:"d,omitempty"`
	Rows       [][]interface{}      `json:"r,omitempty"`
	ChunkIndex int                  `json:"chunkIndex"`
	Final      bool                 `json:"final"`
	Timings    []engine.TableTiming `json:"timings,omitempty"`
	// Drift is non-nil only on the Final frame and only when MemorySource
	// raised *ivm.DriftError mid-advance. TS client treats this as a
	// signal to discard the whole stream + trigger re-init.
	Drift *ivm.DriftError `json:"drift,omitempty"`
}

func (s *Server) handleAdvanceStream(req RPCRequest, streamW streamWriter) RPCResponse {
	var p advanceParams
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

	err := group.eng.AdvanceStream(p.Changes, func(r engine.AdvanceStreamPartial) {
		pc := toPositional(r.Changes)
		streamW(req.ID, advanceStreamPartial{
			Dict:       pc.Dict,
			Rows:       pc.Rows,
			ChunkIndex: r.ChunkIndex,
			Final:      r.Final,
			Timings:    r.Timings,
			Drift:      r.Drift,
		})
		if r.Drift != nil {
			fmt.Fprintf(os.Stderr, "[GO-IVM][drift] advanceStream cg=%s %s\n", cgID, r.Drift.Error())
		}
		// One advanceStream call → one record on the terminal frame.
		// Final's ChunkIndex+1 is the total chunk count for this call.
		if r.Final {
			metrics.recordAdvanceChunks(r.ChunkIndex + 1)
		}
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "[GO-IVM] advanceStream ERROR cg=%s: %v\n", cgID, err)
		return rpcError(req.ID, -32000, "advanceStream: "+err.Error())
	}

	// "done" sentinel — TS client uses this to resolve the call promise.
	return RPCResponse{JSONRPC: "2.0", Result: "done", ID: req.ID}
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
		group.mu.Lock()
		if resp, stale := checkInitEpoch(group, req.ID, p.InitEpoch); stale {
			group.mu.Unlock()
			return resp
		}
		group.mu.Unlock()
	}

	s.removeGroup(cgID)
	return RPCResponse{JSONRPC: "2.0", Result: "ok", ID: req.ID}
}

// refreshSnapshotParams shares the clientGroupID-only shape with other
// per-CG RPCs.
type refreshSnapshotParams struct {
	ClientGroupID string `json:"clientGroupID"`
	InitEpoch     uint64 `json:"initEpoch"`
}

// pipelineCountParams: cgID-only health probe, no initEpoch (we want this
// to succeed during/after recovery to detect freeze, not be rejected as
// stale).
type pipelineCountParams struct {
	ClientGroupID string `json:"clientGroupID"`
}

// handlePipelineCount returns the number of queries currently registered
// on this CG's engine. Drift audit uses it to detect the C2 freeze: TS
// believes N queries are active, Go reports 0 → per-CG recovery dropped
// pipeline state somewhere along the way. Returns 0 (not an error) when
// the engine isn't initialized — the absence-of-engine is the answer,
// and forcing the audit to handle an error response would just add noise.
func (s *Server) handlePipelineCount(req RPCRequest) RPCResponse {
	var p pipelineCountParams
	if err := mpUnmarshal(req.Params, &p); err != nil {
		return rpcError(req.ID, -32602, err.Error())
	}
	cgID := p.ClientGroupID
	if cgID == "" {
		cgID = "default"
	}
	group := s.getGroup(cgID, false)
	if group == nil {
		// pipelineCount is a health probe; an absent group is a valid
		// answer (0 pipelines), not an error. Returning rpcError would
		// surface as a generic Error in TS and bypass the freeze-
		// detection path that compares numbers.
		return RPCResponse{JSONRPC: "2.0", Result: 0, ID: req.ID}
	}
	group.mu.Lock()
	defer group.mu.Unlock()
	count := 0
	if group.eng != nil {
		count = group.eng.PipelineCount()
	}
	return RPCResponse{JSONRPC: "2.0", Result: count, ID: req.ID}
}

// handleRefreshSnapshot rolls each registered Source's pinned snapshot
// (no-op for MemorySource). Used by the TS drift audit so its
// comparison reads see Go's view of the current replica state, not the
// snapshot pinned since the last Push — which would otherwise produce
// transient set differences during sustained writes.
//
// initEpoch guard matches the other mutating RPCs: a stale audit from a
// torn-down view-syncer must not roll the new instance's tx.
func (s *Server) handleRefreshSnapshot(req RPCRequest) RPCResponse {
	var p refreshSnapshotParams
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
	// Snapshot eng + initEpoch under a brief mu hold, then release before
	// the (potentially slow) RefreshAllSources call. Pre-fix this held
	// group.mu for the whole RefreshAllSources duration, serializing with
	// the per-CG worker's currently-running handler — which defeated the
	// reader-loop's deliberate decision to route this RPC AROUND the FIFO
	// (see the routing comment in handleConnection). Holding mu briefly
	// keeps eng+epoch read race-free; releasing before the call lets
	// engine.RefreshAllSources execute concurrently with whatever the
	// worker is doing (advance/hydrate). RefreshAllSources is itself
	// lock-free against e.mu after the atomic.Pointer COW refactor, so
	// no contention remains at any level.
	//
	// Lifecycle safety: once we hold a non-nil eng pointer locally, Go's
	// GC keeps the engine alive even if shutdownGroup races and nils
	// group.eng concurrently. RefreshAllSources called on a closed engine
	// is a no-op (sources.Load() returns nil after Close).
	group.mu.Lock()
	eng := group.eng
	if eng == nil {
		group.mu.Unlock()
		return rpcError(req.ID, -32000, "engine not initialized (call init first)")
	}
	resp, stale := checkInitEpoch(group, req.ID, p.InitEpoch)
	group.mu.Unlock()
	if stale {
		return resp
	}
	eng.RefreshAllSources()
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
		//
		// refreshSnapshot bypasses the per-CG worker FIFO. It only bumps
		// per-source mutexes and is a no-op when overlay!=nil, so
		// running concurrently with the CG's advance/hydrate is safe.
		// Routing it through the FIFO meant queueing behind multi-second
		// hydrates, and the audit's 30s/120s RPC budget couldn't absorb
		// that under sustained multi-CG load.
		cgID := extractClientGroupID(req)
		if cgID != "" && req.Method != "refreshSnapshot" {
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
			if req.Method == "addQueriesStream" || req.Method == "advanceStream" ||
				req.Method == "advanceToHeadStream" {
				sw = streamW
			}
			if !group.trySendReq(clientGroupReq{req: req, respCh: respCh, streamW: sw, group: group}) {
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
func tuneRuntime() {
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
		if n, err := strconv.ParseInt(v, 10, 64); err == nil && n > 0 {
			debug.SetMemoryLimit(n)
			fmt.Fprintf(os.Stderr, "[GO-IVM] soft memory limit set to %d bytes (GO_IVM_GOMEMLIMIT)\n", n)
		}
		return
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

func main() {
	socketPath := defaultSocket
	if len(os.Args) > 1 {
		socketPath = os.Args[1]
	}

	// Relax GC before serving — the allocation rate, not the cores, caps
	// multi-CG parallel scaling (see tuneRuntime).
	tuneRuntime()

	// Log the per-Take state-cache bound: it caps retained Go heap from
	// high-cardinality Take partitions (the dominant pre-OOM allocation found
	// by heap-profiling). 0 = unbounded (the old, OOM-prone behavior).
	if m := sqlite.TakeStateCacheMax(); m > 0 {
		fmt.Fprintf(os.Stderr,
			"[GO-IVM] take-state cache: max %d entries per operator (GO_IVM_TAKE_STATE_CACHE_MAX)\n", m)
	} else {
		fmt.Fprintf(os.Stderr,
			"[GO-IVM] take-state cache: UNBOUNDED (GO_IVM_TAKE_STATE_CACHE_MAX=0)\n")
	}

	// CRIT-6: fail loud at boot if the init/advance value-coercion contract is
	// broken (a colType that doesn't map both raw and JS shapes to the same
	// canonical value) — better a startup crash than silent client-data
	// corruption once a CG is serving.
	if err := sqlite.SelfCheckCoercion(); err != nil {
		fmt.Fprintf(os.Stderr, "[GO-IVM] FATAL: %v\n", err)
		os.Exit(1)
	}

	// Shutdown context for background goroutines (metrics reporter, idle reaper).
	// Cancelled by the SIGINT/SIGTERM handler so goroutines exit cleanly.
	shutdownCtx, shutdownCancel := context.WithCancel(context.Background())
	defer shutdownCancel()
	var pprofServer *http.Server

	// Optional pprof endpoint. Off by default — opens only when
	// GO_IVM_PPROF_ADDR is set (e.g., "127.0.0.1:6060" in a sandbox or
	// "0.0.0.0:6060" when the container exposes the port). Enabling block
	// + mutex profiling carries ~5% overhead at sample rate 1, which is
	// acceptable during a profile run but should stay off in prod. The
	// _ "net/http/pprof" import registers handlers on http.DefaultServeMux.
	//
	// S3: a bare port (":6060") with no host binds 0.0.0.0 and exposes pprof on
	// every interface — pprof is an RCE-grade surface (read heap, fetch goroutine
	// state, even trigger GC/allocs). Default a hostless addr to 127.0.0.1 so it
	// stays loopback-only unless the operator EXPLICITLY asks for a bind (an
	// addr already carrying a host, including "0.0.0.0:…", is honored verbatim).
	if addr := os.Getenv("GO_IVM_PPROF_ADDR"); addr != "" {
		if strings.HasPrefix(addr, ":") {
			addr = "127.0.0.1" + addr
		}
		runtime.SetBlockProfileRate(1)
		runtime.SetMutexProfileFraction(1)
		pprofServer = &http.Server{Addr: addr, Handler: http.DefaultServeMux}
		go func() {
			fmt.Fprintf(os.Stderr, "[GO-IVM] pprof listening on %s\n", addr)
			if err := pprofServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
				fmt.Fprintf(os.Stderr, "[GO-IVM] pprof server exited: %v\n", err)
			}
		}()
	}

	// Remove stale socket — but ONLY if the path is an actual leftover socket,
	// not a live file or directory someone (or another process) placed there.
	// Blind os.Remove would clobber a non-socket file and silently mask a
	// misconfiguration (S2). A missing path is the normal first-run case.
	if info, err := os.Stat(socketPath); err == nil {
		if info.Mode()&os.ModeSocket != 0 {
			if err := os.Remove(socketPath); err != nil {
				fmt.Fprintf(os.Stderr, "Failed to remove stale socket %s: %v\n", socketPath, err)
				os.Exit(1)
			}
		} else {
			fmt.Fprintf(os.Stderr, "Socket path %s exists but is not a socket (mode=%v); refusing to remove\n", socketPath, info.Mode())
			os.Exit(1)
		}
	}

	listener, err := net.Listen("unix", socketPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to listen: %v\n", err)
		os.Exit(1)
	}
	// Restrict the socket to owner-only access (S1). The sidecar is a local
	// subprocess of zero-cache, so only the same-uid zero-cache process needs
	// to connect; 0600 prevents any other local user from talking to it.
	if err := os.Chmod(socketPath, 0o600); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to chmod socket %s: %v\n", socketPath, err)
		_ = listener.Close()
		os.Exit(1)
	}
	defer listener.Close()

	otelShutdown, err := otelInit(context.Background())
	if err != nil {
		// Telemetry must never take the sidecar down — log and continue noop.
		fmt.Fprintf(os.Stderr, "[GO-IVM] OTel init failed (continuing without traces): %v\n", err)
		otelShutdown = func(context.Context) error { return nil }
	}

	// Leaf-source selector (see DESIGN-tablesource-port.md). Unknown /
	// empty values default to memory so misconfiguration cannot silently
	// activate the read-from-replica path.
	sourceMode := tablesource.ParseMode()

	// When in table mode, open the TS replica file once for the whole
	// process — pool is shared across CGs (per-CG read-tx isolation is
	// the TxCache's job, not the pool's). Refuse to start if the path
	// is missing or the file isn't in WAL — failing fast here beats a
	// confusing per-CG init error later.
	// In table mode, validate that the path is set but DO NOT open the
	// replica synchronously. The TS replicator takes 5-30s to finish
	// initializing replica.db in a cold-start container; blocking here
	// would also block this process from servicing the TS ping that the
	// view-syncer worker sends to verify the sidecar is alive. The
	// listener wouldn't accept until the open returns, ping times out,
	// TS falls back to TS-only path for the lifetime of the container.
	//
	// Instead, the path is stashed on the Server and the first init RPC
	// for a table-mode CG triggers a synchronous (per-call) open with
	// retry. By that time the replicator is definitely done.
	var replicaPath string
	if sourceMode == tablesource.ModeTable {
		// Prefer the dedicated override so callers can point Go at a
		// different replica than TS for testing. In normal deploys both
		// processes share the same SQLite file, so fall back to the TS
		// replica path — saves operators from setting the same value twice
		// and from the silent-misconfigure trap when one is forgotten.
		replicaPath = os.Getenv("GO_IVM_REPLICA_DB_PATH")
		if replicaPath == "" {
			replicaPath = os.Getenv("ZERO_REPLICA_FILE")
		}
		if replicaPath == "" {
			fmt.Fprintln(os.Stderr,
				"[GO-IVM] GO_IVM_SOURCE_MODE=table but neither GO_IVM_REPLICA_DB_PATH nor ZERO_REPLICA_FILE is set — refusing to start")
			os.Exit(1)
		}
		fmt.Fprintf(os.Stderr,
			"[GO-IVM] table mode armed; replica %s will open lazily on first init\n",
			replicaPath)
	}
	fmt.Printf("Go IVM sidecar listening on %s (multi-engine, source=%s)\n",
		socketPath, sourceMode)

	server := NewServer(sourceMode, replicaPath)
	server.appID = os.Getenv("GO_IVM_APP_ID")
	server.advanceToHeadEnabled = os.Getenv("GO_IVM_ADVANCE_TO_HEAD") == "true"
	// GO_IVM_ADVANCE_DRIVE implies advanceToHead (P2 self-consistent advance).
	server.advanceDriveEnabled = os.Getenv("GO_IVM_ADVANCE_DRIVE") == "true"
	if server.advanceDriveEnabled {
		server.advanceToHeadEnabled = true
	}
	// GO_IVM_HYDRATE_READERS RAISES the cold-hydrate reader-pool floor above the
	// default K = hydrateLanes × Cmax. Streaming-by-default (this branch): the
	// pool is built unconditionally under drive mode, so this env no longer
	// enables/disables streaming — it only sets a higher K floor.
	if v := os.Getenv("GO_IVM_HYDRATE_READERS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 1 {
			server.hydrateReaders = n
		}
	}
	// GO_IVM_HYDRATE_LANES sets P (worker lane count for bounded parallel
	// hydrate). The reader pool is sized to max(hydrateReaders, hydrateLanes×Cmax)
	// so every lane can acquire all Cmax readers it needs (deadlock-freedom: §3d).
	if v := os.Getenv("GO_IVM_HYDRATE_LANES"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			server.hydrateLanes = n
		}
	}
	// Warm hydrate (adds on existing-pipeline CGs) now ALSO streams via the
	// co-read pool by default under drive mode — no GO_IVM_WARM_HYDRATE_POOL gate.
	// The env is still read for the boot log but no longer gates anything.
	server.warmHydratePoolEnabled = os.Getenv("GO_IVM_WARM_HYDRATE_POOL") == "true"
	fmt.Fprintf(os.Stderr,
		"[GO-IVM] hydrate config: streaming=%v (default-on under drive) readers=%d(floor) lanes=%d advanceDrive=%v\n",
		server.advanceDriveEnabled, server.hydrateReaders, server.hydrateLanes, server.advanceDriveEnabled)
	if server.advanceToHeadEnabled {
		if sourceMode != tablesource.ModeTable {
			fmt.Fprintln(os.Stderr,
				"[GO-IVM] GO_IVM_ADVANCE_TO_HEAD=true ignored: requires GO_IVM_SOURCE_MODE=table")
			server.advanceToHeadEnabled = false
			server.advanceDriveEnabled = false
		} else {
			mode := "derive-only (P1 shadow)"
			if server.advanceDriveEnabled {
				mode = "DRIVE (P2 frame-coordinated self-consistent advance)"
			}
			fmt.Fprintf(os.Stderr,
				"[GO-IVM] advanceToHead ARMED [%s] (appID=%q)\n", mode, server.appID)
		}
	}

	// Start periodic metrics reporter
	go func() {
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()
		var lastWait int64
		for {
			select {
			case <-shutdownCtx.Done():
				return
			case <-ticker.C:
			}
			metrics.reportAndReset()
			// Replica-pool pressure: WaitCount growth means goroutines are
			// blocking on conn acquisition — the precursor to TS-side RPC
			// timeouts. Surface it BEFORE it becomes reset storms so ops can
			// raise GO_IVM_MAX_OPEN_CONNS (rule of thumb: ≥3× concurrent CGs).
			server.replicaMu.Lock()
			rdb, wdb := server.replicaDB, server.replicaWritableDB
			server.replicaMu.Unlock()
			if rdb != nil && wdb != nil {
				rs, ws := rdb.Stats(), wdb.Stats()
				wait := rs.WaitCount + ws.WaitCount
				if wait > lastWait {
					fmt.Fprintf(os.Stderr,
						"[GO-IVM] replica pool pressure: +%d conn waits in last 10s "+
							"(read in-use %d/%d, writable in-use %d/%d, total wait %s) — "+
							"consider raising GO_IVM_MAX_OPEN_CONNS\n",
						wait-lastWait, rs.InUse, rs.MaxOpenConnections,
						ws.InUse, ws.MaxOpenConnections,
						(rs.WaitDuration + ws.WaitDuration).Round(time.Millisecond))
				}
				lastWait = wait
			}
		}
	}()

	// Start idle-group reaper. Without this, ClientGroups that the TS side
	// fails to explicitly destroy (network partition, process crash, missed
	// teardown) accumulate indefinitely, with their engine state + worker
	// goroutine + MemorySource data resident. Reaps groups untouched for
	// groupIdleTimeout. REVIEW-final HIGH-CROSS-2 / HIGH-CROSS-3.
	go func() {
		ticker := time.NewTicker(5 * time.Minute)
		defer ticker.Stop()
		for {
			select {
			case <-shutdownCtx.Done():
				return
			case now := <-ticker.C:
				n := server.reapIdleGroups(now.Add(-groupIdleTimeout))
				if n > 0 {
					fmt.Fprintf(os.Stderr, "[GO-IVM] reaped %d idle client groups\n", n)
				}
			}
		}
	}()

	// Graceful shutdown
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sig
		fmt.Println("\nShutting down...")
		shutdownCancel()
		if pprofServer != nil {
			pprofServer.Shutdown(context.Background())
		}
		server.closeAll()
		listener.Close()
		os.Remove(socketPath)
		if err := otelShutdown(context.Background()); err != nil {
			fmt.Fprintf(os.Stderr, "[GO-IVM] OTel shutdown error: %v\n", err)
		}
		os.Exit(0)
	}()

	for {
		conn, err := listener.Accept()
		if err != nil {
			break
		}
		go handleConnection(conn, server)
	}
}
