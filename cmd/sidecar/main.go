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
	"strconv"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/kartikparsoya-eng/go-ivm/builder"
	"github.com/kartikparsoya-eng/go-ivm/engine"
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

// --- Performance metrics ---

type perfMetrics struct {
	// Concurrency counters (how many engines doing work right now)
	advancesInFlight atomic.Int64
	hydratesInFlight atomic.Int64

	// Latency tracking (last 10s window)
	mu              sync.Mutex
	advanceLatencies []time.Duration
	hydrateLatencies []time.Duration
	advanceCount    atomic.Int64
	hydrateCount    atomic.Int64
	peakAdvConc     atomic.Int64 // peak concurrent advances seen
	peakHydConc     atomic.Int64 // peak concurrent hydrates seen

	// Chunk-count tracking (last 10s window) — answers "is the streaming
	// path actually chunking, or are payloads always single-frame?" One
	// entry per finalized query for hydrate, one per finalized call for
	// advance. A 1 means the call hit the fast-path (single chunk); higher
	// values mean the payload crossed advanceChunkSize / hydrateChunkSize.
	hydrateChunkCounts []int
	advanceChunkCounts []int
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

	if advCount == 0 && hydCount == 0 {
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
	sidecarVersion     = "0.6.0"
	sidecarProtocolRev = 6 // bumped: refreshSnapshot RPC added (rolls TableSource pinned tx — drift audit calls it to align comparison reads with Go's view; safe to call against MemorySource too as it's a no-op there).
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

// --- RPC types ---

type RPCRequest struct {
	JSONRPC     string             `json:"jsonrpc"`
	Method      string             `json:"method"`
	Params      msgpack.RawMessage `json:"params"`
	ID          interface{}        `json:"id"`
	// W3C traceparent forwarded by the TS client (REVIEW-final MED-CROSS-4).
	// Logged for slow handlers; full Go-side OTel SDK integration is a
	// separate feature.
	Traceparent string             `json:"traceparent,omitempty"`
}

type RPCResponse struct {
	JSONRPC string      `json:"jsonrpc"`
	Result  interface{} `json:"result,omitempty"`
	Error   *RPCError   `json:"error,omitempty"`
	ID      interface{} `json:"id"`
}

type RPCError struct {
	Code    int         `json:"code"`
	Message string      `json:"message"`
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
// response. Closes over the connection's writeMu so frames stay atomic on
// the wire across multiple goroutines emitting concurrently.
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
	replicaPath        string
	replicaMu          sync.Mutex
	replicaDB          *sql.DB       // populated on successful probe (under replicaMu)
	replicaWritableDB  *sql.DB       // writable companion pool — used for each Source's prev-snapshot tx conn
	replicaProbe       chan struct{} // non-nil while a probe is in flight; closed when done
	replicaErr         error         // last probe's terminal error (under replicaMu)
}

func NewServer(mode tablesource.Mode, replicaPath string) *Server {
	return &Server{
		groups:      make(map[string]*ClientGroup),
		sourceMode:  mode,
		replicaPath: replicaPath,
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
	for time.Now().Before(deadline) {
		var err error
		db, err = tablesource.Open(s.replicaPath, tablesource.OpenOptions{})
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
		writableDB, err = tablesource.OpenWritable(s.replicaPath, tablesource.OpenOptions{})
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
		case "advance", "advanceStream":
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
			resp = s.handleAddQueriesStream(req.req, req.streamW)
		} else if req.streamW != nil && method == "advanceStream" {
			// Streaming variant of advance: partial frames go through streamW;
			// terminal "done" RPCResponse still flows through respCh.
			resp = s.handleAdvanceStream(req.req, req.streamW)
		} else {
			resp = s.handleRequest(req.req)
		}

		endSpan(resp.Error)

		switch method {
		case "advance", "advanceStream":
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
	if g.eng != nil {
		g.eng.Close()
		g.eng = nil
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

func (s *Server) handleRequest(req RPCRequest) (resp RPCResponse) {
	defer func() {
		if r := recover(); r != nil {
			stack := make([]byte, 4096)
			n := runtime.Stack(stack, false)
			fmt.Fprintf(os.Stderr, "[GO-IVM] PANIC in %s: %v\n%s\n", req.Method, r, stack[:n])
			resp = RPCResponse{
				JSONRPC: "2.0",
				Error:   &RPCError{Code: -32000, Message: fmt.Sprintf("panic: %v", r)},
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
	case "loadRows":
		return s.handleLoadRows(req)
	case "addQuery":
		return s.handleAddQuery(req)
	case "addQueries":
		return s.handleAddQueries(req)
	case "removeQuery":
		return s.handleRemoveQuery(req)
	case "advance":
		return s.handleAdvance(req)
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
	QueryID    string             `json:"queryID"`
	Changes    []engine.RowChange `json:"changes"`
	ChunkIndex int                `json:"chunkIndex"`
	Final      bool               `json:"final"`
	TimingMs   float64            `json:"timingMs"`
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
	err := group.eng.AddQueriesStream(specs, func(r engine.QueryResult) {
		streamW(req.ID, addQueriesStreamPartial{
			QueryID:    r.QueryID,
			Changes:    r.Changes,
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

type advanceResult struct {
	Changes []engine.RowChange   `json:"changes"`
	Timings []engine.TableTiming `json:"timings,omitempty"`
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

func (s *Server) handleAdvance(req RPCRequest) RPCResponse {
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

	result := group.eng.Advance(p.Changes)
	if result.Drift != nil {
		// Self-heal signal: TS will re-init. Log clearly so prod ops can
		// see frequency vs the panic line that used to crash the sidecar.
		// Carry the partial Changes alongside the drift signal so the TS
		// view-syncer can emit them to clients before re-hydrating —
		// matches TS's assert-and-throw behavior where partial output
		// already streamed to pokers before the snapshot revert.
		fmt.Fprintf(os.Stderr, "[GO-IVM][drift] advance cg=%s %s\n", cgID, result.Drift.Error())
		return RPCResponse{
			JSONRPC: "2.0",
			Error: &RPCError{
				Code:    rpcCodeDrift,
				Message: result.Drift.Error(),
				Data: driftRPCData{
					Table:          result.Drift.Table,
					Op:             result.Drift.Op,
					PK:             result.Drift.PK,
					HasCount:       result.Drift.HasCount,
					PartialChanges: result.Changes,
					PartialTimings: result.Timings,
				},
			},
			ID: req.ID,
		}
	}
	return RPCResponse{JSONRPC: "2.0", Result: advanceResult{Changes: result.Changes, Timings: result.Timings}, ID: req.ID}
}

// --- advanceStream: streaming variant of advance ---
//
// Wire shape: one OR MORE "partial" frames with the same id, each carrying
// `{changes, chunkIndex, final, timings?}`. Frames arrive in monotonic
// chunkIndex order (the engine's AdvanceStream calls onResult synchronously
// from one goroutine). Exactly one frame has final=true (the last); only
// that frame carries timings. After the final partial frame, exactly one
// terminal frame whose Result is the literal string "done" — TS client
// uses "done" to resolve the call promise.
//
// Reuses `advanceParams` since the request shape matches the non-streaming
// `advance` method exactly.

type advanceStreamPartial struct {
	Changes    []engine.RowChange   `json:"changes"`
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
		streamW(req.ID, advanceStreamPartial{
			Changes:    r.Changes,
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

	// Per-request writer goroutines + a shared write mutex (REVIEW-final
	// CRITICAL-CROSS-1). The previous single-sequential-writer was queue-
	// order on the wire but caused HOL blocking: one slow group's response
	// at position K held back every group's responses at positions K+1...
	// Now each pending response waits on its own respCh independently and
	// races for `writeMu` when ready; cross-group requests no longer
	// serialize. TS uses ID-based response correlation (see go-ivm-client.ts
	// #onData), so out-of-order responses on the wire are safe.
	//
	// Goroutine bound (writerSem, cap maxWriterGoroutines): pre-fix this
	// site claimed bounded by outC.cap=1024 — but that only bounded the
	// reader→dispatcher queue. Each item dispatcher reads spawns a goroutine
	// that lives until respCh fires; the reader can keep pushing new items
	// (and dispatcher keeps spawning new goroutines) while existing ones
	// wait on slow handlers. Under sustained adversarial load N CGs each
	// with a slow handler, writer-goroutine count grows to N regardless of
	// outC.cap. With the semaphore the dispatcher blocks on a full sem,
	// which transitively blocks the reader at outC, applying real
	// backpressure to the Node side. 4096 is generous for typical
	// per-connection concurrency (one conn per worker; thousands of
	// in-flight RPCs is already a hot worker).
	const maxWriterGoroutines = 4096
	writerSem := make(chan struct{}, maxWriterGoroutines)
	var writeMu sync.Mutex
	writeFrameLocked := func(resp RPCResponse) {
		data, err := mpMarshal(resp)
		if err != nil {
			data, _ = mpMarshal(rpcError(resp.ID, -32603, "encode response: "+err.Error()))
		}
		writeMu.Lock()
		_ = writeFrame(conn, data) // socket close handled by reader-loop exit
		writeMu.Unlock()
	}

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
			// Acquire sem before spawning. Blocks when maxWriterGoroutines
			// are already in flight; the reader sees outC fill (cap 1024)
			// and itself blocks on enqueue, applying TCP backpressure.
			writerSem <- struct{}{}
			if p.immediate != nil {
				writerWg.Add(1)
				go func(resp RPCResponse) {
					defer writerWg.Done()
					defer func() { <-writerSem }()
					writeFrameLocked(resp)
				}(*p.immediate)
				continue
			}
			writerWg.Add(1)
			go func(respCh chan RPCResponse) {
				defer writerWg.Done()
				defer func() { <-writerSem }()
				resp := <-respCh
				writeFrameLocked(resp)
			}(p.respCh)
		}
	}()
	defer func() {
		close(outC)
		<-dispatchDone
		writerWg.Wait()
	}()

	enqueueImmediate := func(resp RPCResponse) {
		outC <- pendingResp{immediate: &resp}
	}

	// Partial-frame writer for streaming RPCs. Goes through writeFrameLocked
	// so concurrent goroutines from a single streaming handler stay frame-
	// atomic; bypasses outC because partials don't have a respCh — the
	// final "done" frame still goes through respCh.
	streamW := streamWriter(func(reqID interface{}, partial interface{}) {
		writeFrameLocked(RPCResponse{
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
			if req.Method == "addQueriesStream" || req.Method == "advanceStream" {
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

func main() {
	socketPath := defaultSocket
	if len(os.Args) > 1 {
		socketPath = os.Args[1]
	}

	// CRIT-6: fail loud at boot if the init/advance value-coercion contract is
	// broken (a colType that doesn't map both raw and JS shapes to the same
	// canonical value) — better a startup crash than silent client-data
	// corruption once a CG is serving.
	if err := sqlite.SelfCheckCoercion(); err != nil {
		fmt.Fprintf(os.Stderr, "[GO-IVM] FATAL: %v\n", err)
		os.Exit(1)
	}

	// Optional pprof endpoint. Off by default — opens only when
	// GO_IVM_PPROF_ADDR is set (e.g., "127.0.0.1:6060" in a sandbox or
	// "0.0.0.0:6060" when the container exposes the port). Enabling block
	// + mutex profiling carries ~5% overhead at sample rate 1, which is
	// acceptable during a profile run but should stay off in prod. The
	// _ "net/http/pprof" import registers handlers on http.DefaultServeMux.
	if addr := os.Getenv("GO_IVM_PPROF_ADDR"); addr != "" {
		runtime.SetBlockProfileRate(1)
		runtime.SetMutexProfileFraction(1)
		go func() {
			fmt.Fprintf(os.Stderr, "[GO-IVM] pprof listening on %s\n", addr)
			if err := http.ListenAndServe(addr, nil); err != nil {
				fmt.Fprintf(os.Stderr, "[GO-IVM] pprof server exited: %v\n", err)
			}
		}()
	}

	// Remove stale socket
	os.Remove(socketPath)

	listener, err := net.Listen("unix", socketPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to listen: %v\n", err)
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

	// Start periodic metrics reporter
	go func() {
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()
		for range ticker.C {
			metrics.reportAndReset()
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
		for now := range ticker.C {
			n := server.reapIdleGroups(now.Add(-groupIdleTimeout))
			if n > 0 {
				fmt.Fprintf(os.Stderr, "[GO-IVM] reaped %d idle client groups\n", n)
			}
		}
	}()

	// Graceful shutdown
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sig
		fmt.Println("\nShutting down...")
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
