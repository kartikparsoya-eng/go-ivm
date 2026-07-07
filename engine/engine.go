package engine

// Per-clientGroup IVM engine: owns sources and pipelines, hydrates queries
// from current state, and drives advance diffs through the operator trees.
// Hydrations run in parallel goroutines; advances are serialized on e.mu so
// pushes don't race with concurrent fetches.

import (
	"database/sql"
	"errors"
	"fmt"
	"iter"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/kartikparsoya-eng/go-ivm/builder"
	"github.com/kartikparsoya-eng/go-ivm/internal/procclock"
	"github.com/kartikparsoya-eng/go-ivm/ivm"
	"github.com/kartikparsoya-eng/go-ivm/sqlite"
)

// envChunkSize reads name from env and returns its int value, or def if
// unset/unparseable/non-positive. Used to make hydrateChunkSize and
// advanceChunkSize tunable at deploy time without a code change (e.g., to
// 100 to exercise multi-frame streaming on a small sandbox dataset).
func envChunkSize(name string, def int) int {
	v := os.Getenv(name)
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil || n <= 0 {
		return def
	}
	return n
}

// ErrEngineClosed is returned by Engine methods invoked after Close().
// Streaming methods (AddQueriesStream, AdvanceStream) check this so a
// late-arriving RPC doesn't silently produce an empty result against a
// nil pipelines/sources map.
var ErrEngineClosed = errors.New("engine is closed")

// ErrStreamCancelled is returned by AddQueriesStreamPull when the consumer
// aborted the stream: the pull gate was cancelled (client .return()/.throw(),
// group teardown, or the pull idle timeout) and onResult returned false, so
// the producer broke its fetch range and unwound (cursor closed, pool reader
// released). DESIGN-duplex-streaming D4.
//
// I9 (reset-classification pin): this error is CLIENT-INITIATED and must
// never join the reset ladder — a storm of tab-closes must not become a
// reset storm. The sidecar maps it to a plain -32000 terminal error frame
// for bookkeeping symmetry (the client already left); it must not map to
// rpcCodeDataError or any reset-triggering class.
var ErrStreamCancelled = errors.New("hydrate stream cancelled by consumer")

// Source is the interface that engine sources must implement.
//
// Close releases any external resource the leaf holds (the tablesource leaf's
// dedicated writable *sql.Conn + open prev tx). It is invoked by Engine.Close
// on every registered source so group teardown / re-init returns those conns to
// the writable pool instead of leaking them. MemorySource leaves implement it as
// a no-op. Close must be idempotent.
type Source interface {
	TableName() string
	PrimaryKey() []string
	NormalizeRow(ivm.Row)
	Push(ivm.SourceChange)
	Connect(sort ivm.Ordering, filter *builder.Condition, filterPredicate func(ivm.Row) bool, splitEditKeys map[string]bool) ivm.Input
	Close() error
}

// memorySourceAdapter wraps *ivm.MemorySource to implement Source.
type memorySourceAdapter struct {
	ms *ivm.MemorySource
}

func (a *memorySourceAdapter) TableName() string                     { return a.ms.TableName() }
func (a *memorySourceAdapter) PrimaryKey() []string                  { return a.ms.PrimaryKey() }
func (a *memorySourceAdapter) NormalizeRow(row ivm.Row)              { a.ms.NormalizeRow(row) }
func (a *memorySourceAdapter) Push(sc ivm.SourceChange) { a.ms.Push(sc) }
func (a *memorySourceAdapter) Connect(sort ivm.Ordering, filter *builder.Condition, filterPredicate func(ivm.Row) bool, splitEditKeys map[string]bool) ivm.Input {
	return a.ms.Connect(sort, filterPredicate, splitEditKeys)
}

// Close is a no-op: MemorySource holds no external resource (no SQLite conn or
// open tx). Present only to satisfy engine.Source so the tablesource leaf's
// Close — which DOES roll back the prev tx and return its writable conn to the
// pool — is callable polymorphically from Engine.Close.
func (a *memorySourceAdapter) Close() error { return nil }

func (a *memorySourceAdapter) OnAdvanceEnd() {
	a.ms.ClearBatchState()
}

// SnapshotChange represents a single row change from the snapshot diff.
type SnapshotChange struct {
	Table      string    `json:"table"`
	PrevValues []ivm.Row `json:"prevValues"` // rows being removed/replaced
	NextValue  ivm.Row   `json:"nextValue"`  // new row value, nil for delete
}

// AdvanceResult is returned by Engine.Advance().
//
// Timings is one entry per (table, sourceChange) — the same granularity TS
// records to its `ivm.advance-time` histogram. Lets TS attribute wall time to
// the responsible table/op instead of seeing a single opaque RPC duration.
type AdvanceResult struct {
	Changes []RowChange
	Timings []TableTiming
}

// TableTiming reports the wall time spent processing a single source change
// for a table during Engine.Advance.
type TableTiming struct {
	Table   string  `json:"table"`
	ChangeT int     `json:"type"` // ivm.ChangeType (0=add,1=remove,2=edit)
	Ms      float64 `json:"ms"`   // wall-time millis
}

// PipelineEntry tracks a registered pipeline.
type pipelineEntry struct {
	queryID  string
	pipeline *builder.Pipeline
	schema   *ivm.SourceSchema
	// companions are pipelines built for scalar subqueries resolved away by
	// the AST resolver. Each holds a live Connection to its subquery's
	// source, so the source's Push fans out to it on every advance — that's
	// how scalar EXISTS replacement stays live without explicit re-eval.
	// Tracked here so removeQuery can destroy them alongside the main
	// pipeline. Empty for queries with no resolved scalar subqueries.
	companions []*companionEntry

	// delegate is the per-query BuilderDelegate shared by the main pipeline
	// and all companion sub-pipelines. Held so removeQueryLocked can call
	// delegate.cgs.Destroy() to DELETE this query's operator-storage rows
	// (keyed by cgID == queryID). Without this the rows leaked — Destroy was
	// never invoked anywhere (HIGH-4). cgs is nil for queries with no
	// storage-using operator (e.g. no Take).
	delegate *engineDelegate
}

// companionEntry is the Go-side companion record. The TS analogue is
// CompanionPipeline in pipeline-driver.ts. On advance, the companion's
// output runs the live "scalar value changed" check: if a push moves the
// resolved scalar's child field to a different value, the baked-in literal
// in the main
// query's plan is stale, so we raise a *ScalarResetError — the sidecar maps
// it to the reset RPC code and TS resets + re-registers the query, which
// re-runs ResolveSimpleScalarSubqueries against current
// truth and bakes the NEW value. Matches TS's ResetPipelinesSignal
// ('scalar-subquery') end behavior. Unchanged-value pushes accumulate
// normally.
type companionEntry struct {
	pipeline   *builder.Pipeline
	schema     *ivm.SourceSchema
	matchedRow ivm.Row // captured by the executor at resolve time; nil = no match
	childField string

	// resolvedValue is the scalar's child-field value captured at resolve
	// time (nil == SQL/JS null, which is also what an unmatched subquery
	// resolves to — TS stores the same single resolvedValue with no
	// separate "matched" flag). The companion's advance-time output
	// compares each push's child value against this.
	resolvedValue ivm.Value
}

// Engine is the IVM engine that manages sources, pipelines, and the advance loop.
//
// sources concurrency: held as atomic.Pointer to a map snapshot (copy-on-write).
// Mutators (RegisterSource / Close) take e.mu, build a new map, and Store the
// new pointer atomically. Readers Load the pointer without taking any lock.
//
// The map snapshot itself is treated as immutable after Store — never mutated
// in place. Reads via sourcesView() return the live snapshot pointer; iterating
// it under no lock is safe because no writer ever mutates it.
type Engine struct {
	mu        sync.Mutex
	sources   atomic.Pointer[map[string]Source] // immutable snapshots; updated COW under e.mu
	pipelines map[string]*pipelineEntry         // query ID → pipeline
	streamer  *Streamer
	storage   *sqlite.DatabaseStorage
	closed    bool // set true by Close; guards against post-Close calls

	// tableUniqueKeys: per-table list of unique key column sets. Used by the
	// scalar-subquery resolver to detect "simple" subqueries — those whose
	// WHERE constrains all columns of at least one unique key, guaranteeing
	// at most one matching row. Set by handleInit from TS-side tableSpecs;
	// nil/missing for a table means "no known unique keys" (resolver will
	// treat all that table's scalar subqueries as non-simple and leave the
	// EXISTS rewrite in place).
	tableUniqueKeys map[string][][]string

	// parallelThreshold: if a source has more connections than this, fan-out in parallel
	parallelThreshold int

	// minRowVersions: per-table minRowVersion forwarded from TS-side
	// tableSpec.minRowVersion (set after a RESET during incremental catchup).
	// Used to bump an emitted row's _0_version up to minRowVersion when the
	// row's stored version is below it — direct port of TS streamNodes
	// (pipeline-driver.ts:2843-2850). Empty/missing for a table means no bump.
	// The bump is a no-op in steady state (minRowVersion unset or rows already
	// at/above it), so it only activates in the rare post-RESET window.
	minRowVersions map[string]string
}

// zeroVersionColumn is the row-version bookkeeping column (TS
// ZERO_VERSION_COLUMN_NAME, replication-state.ts). Present on rows read from
// the replica; compared/bumped against minRowVersion. Not part of the zql
// spec.
const zeroVersionColumn = "_0_version"

// SetMinRowVersions installs the per-table minRowVersion map (from
// handleInit). Safe to call before any advance/hydrate; nil clears it.
func (e *Engine) SetMinRowVersions(m map[string]string) {
	e.mu.Lock()
	e.minRowVersions = m
	e.mu.Unlock()
}

// bumpRowVersions applies the TS streamNodes minRowVersion bump
// (pipeline-driver.ts:2843-2850) to a finished RowChange slice: for each
// non-REMOVE change whose table has a minRowVersion and whose row's stored
// _0_version is below it, rewrite _0_version up to minRowVersion. Bumps on a
// COPY of the row so the source's row map is never mutated. No-op (returns the
// input untouched) when no minRowVersions are set — the common steady state.
func bumpRowVersions(changes []RowChange, mrv map[string]string) []RowChange {
	if len(mrv) == 0 {
		return changes
	}
	for i := range changes {
		c := &changes[i]
		if c.Type == RowChangeRemove || c.Row == nil {
			continue
		}
		want := mrv[c.Table]
		if want == "" {
			continue
		}
		cur, ok := c.Row[zeroVersionColumn].(string)
		if !ok || cur >= want {
			continue
		}
		nr := make(ivm.Row, len(c.Row))
		for k, v := range c.Row {
			nr[k] = v
		}
		nr[zeroVersionColumn] = want
		c.Row = nr
	}
	return changes
}

// EngineConfig configures the engine.
type EngineConfig struct {
	StoragePath       string // path for operator storage DB
	ParallelThreshold int    // min connections for parallel fan-out (default: 2, set below when 0)
}

// NewEngine creates a new IVM engine.
func NewEngine(cfg EngineConfig) (*Engine, error) {
	storage, err := sqlite.NewDatabaseStorage(cfg.StoragePath)
	if err != nil {
		return nil, fmt.Errorf("create engine storage: %w", err)
	}

	threshold := cfg.ParallelThreshold
	if threshold == 0 {
		// Default lowered from 4 to 2. The conservative 4 was originally picked
		// to avoid goroutine-spawn overhead dominating tiny per-connection work,
		// but dashboard-style workloads typically have 2-3 queries per source
		// per cg, so sources never crossed the threshold and parallel push was
		// effectively dead code. At 2 connections, fan-out still wins on
		// branchy operator trees (joins, filter chains); on trivial trees the
		// overhead is bounded by len(activeConns) * goroutine-spawn (~1µs).
		threshold = 2
	}

	e := &Engine{
		pipelines:         make(map[string]*pipelineEntry),
		streamer:          NewStreamer(),
		storage:           storage,
		tableUniqueKeys:   make(map[string][][]string),
		parallelThreshold: threshold,
	}
	empty := make(map[string]Source)
	e.sources.Store(&empty)
	return e, nil
}

// sourcesView returns the current sources snapshot. Caller MUST NOT mutate
// the returned map — it's the live snapshot that may be observed by other
// goroutines. Mutators must build a fresh map and Store it (see RegisterSource).
// Returns nil after Close.
func (e *Engine) sourcesView() map[string]Source {
	p := e.sources.Load()
	if p == nil {
		return nil
	}
	return *p
}

// SetTableUniqueKeys registers the unique-key column sets for a table.
// Called by the sidecar's handleInit per table; consumed by the scalar
// subquery resolver at query-build time. Safe to call before any
// pipelines are registered.
func (e *Engine) SetTableUniqueKeys(tableName string, uniqueKeys [][]string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if uniqueKeys == nil {
		delete(e.tableUniqueKeys, tableName)
		return
	}
	e.tableUniqueKeys[tableName] = uniqueKeys
}

// PipelineCount returns the number of registered queries (pipelines) on
// this engine. Consumed by the sidecar's warm-pool sizing (an existing
// pipeline means a warm add must hydrate at the live pipelines' frame) and
// by the cold-hydrate seam's "first hydrate" check.
func (e *Engine) PipelineCount() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return len(e.pipelines)
}

// TableUniqueKeys returns a snapshot of the registered unique keys keyed by
// table name. Returned map is a fresh copy — safe for the caller to retain.
func (e *Engine) TableUniqueKeys() map[string][][]string {
	e.mu.Lock()
	defer e.mu.Unlock()
	out := make(map[string][][]string, len(e.tableUniqueKeys))
	for k, v := range e.tableUniqueKeys {
		out[k] = v
	}
	return out
}

// Close shuts down the engine: destroys all pipelines, clears sources, and
// closes the operator-storage database. Idempotent — repeated calls return
// the first storage close error (or nil).
//
// Callers (e.g., the sidecar's group lifecycle) must guarantee no in-flight
// operations on this engine when Close is invoked; concurrent Advance/AddQuery
// during Close would race on map state.
func (e *Engine) Close() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed {
		return nil
	}
	e.closed = true
	for _, entry := range e.pipelines {
		entry.pipeline.Input.Destroy()
		for _, ce := range entry.companions {
			ce.pipeline.Input.Destroy()
		}
	}
	e.pipelines = nil

	// Close each registered leaf source AFTER its pipeline inputs are
	// destroyed. Input.Destroy → disconnect only drops the connection from
	// the source's connection slice; it never releases the source's own
	// writable *sql.Conn + open prev tx. Without this loop, every group
	// teardown / re-init leaks one writable conn + one open tx per
	// queried table (tablesource acquires them lazily on the first hydrate
	// Fetch via ensurePrevTxLocked). Under sustained client-group churn that
	// exhausts the writable pool — the reconnect-flood root cause that the
	// pool-256 + 30s acquire-timeout (be2f2a1) only deferred. tablesource
	// leaves roll back + return the conn here; MemorySource leaves no-op.
	// Done under e.mu and before Store(nil) so no concurrent sourcesView()
	// reader observes a half-closed source.
	var firstErr error
	for _, src := range e.sourcesView() {
		if err := src.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	e.sources.Store(nil)
	e.tableUniqueKeys = nil
	if e.storage != nil {
		if err := e.storage.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
		e.storage = nil
	}
	return firstErr
}

// RegisterSource registers a Source for a given table. Copy-on-write: builds a
// fresh map and atomically stores the new pointer so concurrent lock-free
// readers always see a complete snapshot.
func (e *Engine) RegisterSource(source Source) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed {
		return
	}
	cur := e.sourcesView()
	next := make(map[string]Source, len(cur)+1)
	for k, v := range cur {
		next[k] = v
	}
	next[source.TableName()] = source
	e.sources.Store(&next)
}

// signalAdvanceEnd notifies every registered source that the current
// advance batch is complete. Sources that don't implement OnAdvanceEnd
// (MemorySource — state evolves naturally via writeChange) are silently
// skipped. TableSource uses this to rotate its snapshot to the post-batch
// frame and clear its batch-scoped delta — without this hook every Push
// would pull in the replicator's already-committed future-batch mutations
// and trip Take's stale-bound trap (see tablesource/source.go OnAdvanceEnd).
func (e *Engine) signalAdvanceEnd() {
	for _, src := range e.sourcesView() {
		if h, ok := src.(interface{ OnAdvanceEnd() }); ok {
			h.OnAdvanceEnd()
		}
		// Clear per-batch state (intra-batch removed-PK set) at the true
		// batch boundary. This is deliberately separate from OnAdvanceEnd
		// so the dedup set is only ever dropped between batches, not
		// mid-batch. (MemorySource clears via its adapter's OnAdvanceEnd;
		// TableSource needs this explicit hook.)
		if c, ok := src.(interface{ ClearBatchState() }); ok {
			c.ClearBatchState()
		}
	}
}

// RegisterMemorySource registers a MemorySource directly.
// Enables parallel fan-out if the engine's parallelThreshold > 0.
func (e *Engine) RegisterMemorySource(ms *ivm.MemorySource) {
	if e.parallelThreshold > 0 {
		ms.SetParallel(true, e.parallelThreshold)
	}
	e.RegisterSource(&memorySourceAdapter{ms: ms})
}

// connBinder is implemented by sources that can redirect their prev-tx
// reads/writes to an externally-pinned connection — tablesource.Source, for
// P2 frame-coordination. MemorySource adapters don't implement it.
type connBinder interface {
	BindConn(*sql.Conn)
	UnbindConn()
}

// BindTableSourcesToConn binds every connBinder leaf to conn — the Snapshotter's
// pinned BEGIN CONCURRENT frame — so a Snapshotter-derived diff is applied into
// the exact frame it was derived against (no independent per-Source re-pin, no
// frame-timing drift). Must be paired with UnbindTableSources after the advance.
func (e *Engine) BindTableSourcesToConn(conn *sql.Conn) {
	for _, src := range e.sourcesView() {
		if b, ok := src.(connBinder); ok {
			b.BindConn(conn)
		}
	}
}

// UnbindTableSources detaches every connBinder leaf from its external conn.
func (e *Engine) UnbindTableSources() {
	for _, src := range e.sourcesView() {
		if b, ok := src.(connBinder); ok {
			b.UnbindConn()
		}
	}
}

// readerPoolBinder is implemented by sources that can route their hydrate reads
// through a CG-shared frame-pinned reader pool (tablesource.Source). The pool is
// passed as `any` so this package need not import internal/tablesource (which
// imports this package — that would cycle). MemorySource adapters don't
// implement it.
type readerPoolBinder interface {
	BindReaderPool(pool any)
	UnbindReaderPool()
}

// BindTableSourcesToReaderPool routes every leaf source's hydrate reads through
// pool — a set of connections all pinned to one WAL frame — so the per-query
// hydrate goroutines read in parallel instead of serializing on the single
// bound conn. Bind ONLY during the advance-free cold-start window; pair with
// UnbindTableSourcesReaderPool before the first advance (the pool's pinned frame
// goes stale once curr rotates). pool is the opaque *tablesource.ReaderPool.
func (e *Engine) BindTableSourcesToReaderPool(pool any) {
	for _, src := range e.sourcesView() {
		if b, ok := src.(readerPoolBinder); ok {
			b.BindReaderPool(pool)
		}
	}
}

// UnbindTableSourcesReaderPool detaches the reader pool from every leaf source;
// reads revert to the single-conn path. Does not Close the pool.
func (e *Engine) UnbindTableSourcesReaderPool() {
	for _, src := range e.sourcesView() {
		if b, ok := src.(readerPoolBinder); ok {
			b.UnbindReaderPool()
		}
	}
}

// GetMemorySource returns the registered MemorySource for tableName, or nil
// if either no source is registered for the table or the registered source is
// not a MemorySource. Used by the sidecar's loadRows handler to append rows
// to an existing source after init.
func (e *Engine) GetMemorySource(tableName string) *ivm.MemorySource {
	s, ok := e.sourcesView()[tableName]
	if !ok {
		return nil
	}
	if a, ok := s.(*memorySourceAdapter); ok {
		return a.ms
	}
	return nil
}

// AddQuery builds a pipeline from an AST and registers it.
// Returns initial hydration RowChanges (the current state as ADDs) and the
// wall-time spent fetching + flattening (excludes pipeline build, which is
// shared bookkeeping cost).
func (e *Engine) AddQuery(queryID string, ast builder.AST) ([]RowChange, float64, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	// Bail on a closed engine. Close sets e.pipelines = nil; without this guard
	// buildAndRegisterLocked would write e.pipelines[queryID] = entry on a nil
	// map and panic. The sidecar handler serializes AddQuery vs Close on
	// group.mu (handleAddQuery:1427 vs shutdownGroup:1012), so this is not
	// reachable from the production handler path — but the Engine is a reusable
	// library surface, and the invariant currently lives in a different file
	// than the nil-map write. The one-line guard is cheap insurance.
	if e.closed {
		return nil, 0, ErrEngineClosed
	}

	// Remove existing pipeline if any
	e.removeQueryLocked(queryID)

	entry := e.buildAndRegisterLocked(queryID, ast)

	// Hydrate: fetch current state and return as ADD changes (plus any
	// resolver-recorded companion rows).
	start := time.Now()
	hydration := bumpRowVersions(hydrateEntry(entry), e.minRowVersions)
	timingMs := float64(time.Since(start).Microseconds()) / 1000.0

	// HIGH-11: wire companion outputs after hydrate.
	e.wireCompanionOutputsLocked(entry)

	return hydration, timingMs, nil
}

// buildAndRegisterLocked is the shared body of AddQuery / AddQueries /
// AddQueriesStream. It resolves scalar EXISTS via the unique-key resolver,
// builds the main pipeline + any companion sub-pipelines, wires outputs to
// the streamer (companions emit under the MAIN queryID so their rows reach
// the same downstream consumer), and stores the entry.
//
// Must be called with e.mu held. The caller is responsible for hydration.
func (e *Engine) buildAndRegisterLocked(queryID string, ast builder.AST) *pipelineEntry {
	delegate := &engineDelegate{engine: e, queryID: queryID}

	// Resolver executor: each call builds a mini-pipeline against the
	// subquery, fetches at most one row, captures it for hydration, and
	// returns the childField value. The pipeline is kept alive so the
	// subquery's source push fans out to it on advance (companion live
	// tracking is implicit — MemorySource.Push fans to every Connection).
	var companions []*companionEntry
	executor := func(subqueryAST builder.AST, childField string) (interface{}, bool) {
		subPipeline := builder.BuildPipeline(subqueryAST, delegate)
		subSchema := subPipeline.Input.GetSchema()
		var firstNode ivm.Node
		var matched bool
		for node := range subPipeline.Input.Fetch(ivm.FetchRequest{}) {
			firstNode = node
			matched = true
			break
		}
		ce := &companionEntry{
			pipeline:   subPipeline,
			schema:     subSchema,
			childField: childField,
		}
		var value interface{}
		if matched {
			ce.matchedRow = firstNode.Row
			value = firstNode.Row[childField]
		}
		// Capture the resolved scalar value so the companion's advance-time
		// output can detect a later change (port of TS CompanionPipeline's
		// resolvedValue). nil means the subquery matched no row (or matched
		// a null) — both are SQL/JS null, exactly as TS treats them.
		ce.resolvedValue = value
		companions = append(companions, ce)
		return value, matched
	}

	result := builder.ResolveSimpleScalarSubqueries(ast, e.tableUniqueKeys, executor)

	mainPipeline := builder.BuildPipeline(result.AST, delegate)
	schema := mainPipeline.Input.GetSchema()

	entry := &pipelineEntry{
		queryID:    queryID,
		pipeline:   mainPipeline,
		schema:     schema,
		companions: companions,
		delegate:   delegate,
	}
	e.pipelines[queryID] = entry

	mainPipeline.Input.SetOutput(&pipelineOutput{
		engine:  e,
		queryID: queryID,
		schema:  schema,
	})

	// HIGH-11: companion outputs are wired AFTER hydrate (see
	// wireCompanionOutputsLocked), matching TS which wires the main pipeline
	// before hydrate but companions after (pipeline-driver.ts:1615-1747). They
	// only emit on advance (source.Push), never during the hydrate Fetch, so
	// deferring the wiring keeps companion emissions out of the hydrate window
	// even if the build+hydrate locking is ever loosened for throughput.
	return entry
}

// buildBatchLocked builds+registers a batch of query pipelines sequentially
// (Phase 1 of AddQueries / AddQueriesStream). Must be called with e.mu held.
//
// Unwind-on-panic: BuildPipeline panics on bad input (*ivm.DataError for an
// unknown table / unknown condition type). Without the recover below, a panic
// on query k would leave queries 0..k-1 REGISTERED with wired outputs but
// never hydrated — orphan pipelines the TS side doesn't know about (the whole
// batch RPC rejects). Their operators (e.g. Take, whose bound is only set by
// the hydrate fetch) would then receive advance pushes in an un-hydrated
// state and panic, turning one bad query in a batch into an advance-time
// reset loop. Unwind everything this call registered, then re-raise so the
// RPC handler's recover classifies the original panic (*ivm.DataError keeps
// its -32102 teardown code).
//
// Note: the panicking query's own partially-built operators may have
// connected to sources before the panic (connections with nil output —
// skipped by push fan-out). Those are a bounded memory leak until the CG is
// destroyed, not a correctness hazard, and are not unwound here.
func (e *Engine) buildBatchLocked(queries []QuerySpec) []*pipelineEntry {
	built := make([]*pipelineEntry, 0, len(queries))
	defer func() {
		if r := recover(); r != nil {
			for _, entry := range built {
				e.removeQueryLocked(entry.queryID)
			}
			panic(r)
		}
	}()
	for _, q := range queries {
		e.removeQueryLocked(q.QueryID)
		built = append(built, e.buildAndRegisterLocked(q.QueryID, q.AST))
	}
	return built
}

// wireCompanionOutputsLocked attaches each companion sub-pipeline's output to
// the streamer (tagged with the MAIN queryID) and the scalar-value-changed
// reset check. Called by the AddQuery* paths AFTER hydrateEntry, so a companion
// can never emit during hydrate. Must be called with e.mu held.
func (e *Engine) wireCompanionOutputsLocked(entry *pipelineEntry) {
	for _, ce := range entry.companions {
		ce.pipeline.Input.SetOutput(&companionOutput{
			pipelineOutput: pipelineOutput{
				engine:  e,
				queryID: entry.queryID,
				schema:  ce.schema,
			},
			childField:    ce.childField,
			resolvedValue: ce.resolvedValue,
		})
	}
}

// hydrateEntry fetches the main pipeline's current state and emits all
// matched companion rows as ADDs under the same queryID. Caller is
// responsible for timing.
func hydrateEntry(entry *pipelineEntry) []RowChange {
	var hydration []RowChange
	for node := range entry.pipeline.Input.Fetch(ivm.FetchRequest{}) {
		hydration = append(hydration, streamNodes(entry.queryID, entry.schema, RowChangeAdd, node)...)
	}
	// Companion rows: emit each matched subquery row as an ADD so the
	// client can re-evaluate its own EXISTS against the same data the
	// resolver saw. Unmatched companions contribute nothing — there is
	// no row to ship.
	for _, ce := range entry.companions {
		if ce.matchedRow == nil {
			continue
		}
		node := ivm.Node{Row: ce.matchedRow}
		hydration = append(hydration, streamNodes(entry.queryID, ce.schema, RowChangeAdd, node)...)
	}
	return hydration
}

// AddQueries builds multiple pipelines and hydrates them in parallel.
// Pipelines are built sequentially (mutates source connections), but
// hydration fetches run concurrently (read-only against source data).
func (e *Engine) AddQueries(queries []QuerySpec) ([]QueryResult, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	// Bail on a closed engine — see AddQuery for the nil-map-write rationale.
	if e.closed {
		return nil, ErrEngineClosed
	}

	// Phase 1: Build all pipelines sequentially (mutates shared state).
	// Scalar-subquery resolution runs here too — see buildAndRegisterLocked.
	built := e.buildBatchLocked(queries)

	// Phase 2: Hydrate all pipelines via P worker lanes (bounded parallelism).
	// P bounds both the goroutine count and — with K = P × Cmax reader-pool
	// connections — the concurrent-cursor demand. See
	// DESIGN-streaming-hydrate.md §3a/§3d.
	results := make([]QueryResult, len(built))
	mrv := e.minRowVersions
	// C1: a panic inside a hydrate goroutine (e.g. pkValue on a nil-PK row)
	// cannot be caught by the RPC handler's recover — panics don't cross
	// goroutine boundaries, so an uncaught one aborts the WHOLE multi-CG
	// process. Capture per-lane and surface as an error (mirrors the
	// per-goroutine recover in ivm/parallel.go's push fan-out).
	hydratePanics := make([]any, len(built))
	p := hydrateLanes
	if p > len(built) {
		p = len(built)
	}
	if p < 1 {
		p = 1
	}
	type hydrateJob struct {
		idx   int
		entry *pipelineEntry
	}
	jobs := make(chan hydrateJob, len(built))
	for i, entry := range built {
		jobs <- hydrateJob{i, entry}
	}
	close(jobs)
	var wg sync.WaitGroup
	for w := 0; w < p; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for job := range jobs {
				func() {
					defer func() {
						if r := recover(); r != nil {
							hydratePanics[job.idx] = r
						}
					}()
					start := time.Now()
					hydration := bumpRowVersions(hydrateEntry(job.entry), mrv)
					timingMs := float64(time.Since(start).Microseconds()) / 1000.0
					results[job.idx] = QueryResult{
						QueryID:    job.entry.queryID,
						Changes:    hydration,
						ChunkIndex: 0,
						Final:      true,
						TimingMs:   timingMs,
					}
				}()
			}
		}()
	}
	wg.Wait()
	if err := firstHydratePanic(built, hydratePanics); err != nil {
		return nil, err
	}

	// HIGH-11: wire companion outputs after all hydrates complete.
	for _, entry := range built {
		e.wireCompanionOutputsLocked(entry)
	}

	return results, nil
}

// AddQueriesStream is like AddQueries but invokes `onResult` from each
// hydration goroutine as soon as that query finishes, instead of collecting
// all results first. Used by the sidecar's streaming RPC handler to flush
// fast queries to the wire while slower ones in the same batch are still
// running.
//
// onResult may be called concurrently from multiple goroutines; the caller
// is responsible for serializing wire writes (the sidecar does this via
// its writeMu).
//
// Returns after all goroutines have finished. Engine.mu is held for the
// BUILD and POST phases only; the drain runs outside it (D5,
// DESIGN-duplex-streaming). "Source state stays read-only across the
// hydration window" is the CALLER's per-group serialization invariant
// (worker FIFO + group.mu in the sidecar — TS's per-CG model).
func (e *Engine) AddQueriesStream(
	queries []QuerySpec,
	onResult func(QueryResult),
) error {
	return e.addQueriesStreamChunked(queries, hydrateChunkSize, false,
		func(r QueryResult) bool { onResult(r); return true })
}

// AddQueriesStreamChunked is AddQueriesStream with a per-call chunk-size
// override. chunkSize=1 yields one QueryResult per RowChange — the NAPI
// row plane uses this so each row crosses the Go↔JS boundary the moment
// the (lazy) fetch produces it, instead of being re-batched into
// hydrateChunkSize frames. chunkSize<=0 falls back to hydrateChunkSize.
//
// onResult returns whether the consumer wants MORE results (D4): false
// means "consumer gone — stop producing". The producer breaks its fetch
// range, which unwinds the operator chain via the iter.Seq defers (cursor
// close, pool-reader release — the Go dual of TS generator .return()), and
// the call returns ErrStreamCancelled. Callers that never cancel pass a
// closure returning true unconditionally — zero behavior change.
func (e *Engine) AddQueriesStreamChunked(
	queries []QuerySpec,
	chunkSize int,
	onResult func(QueryResult) bool,
) error {
	if chunkSize <= 0 {
		chunkSize = hydrateChunkSize
	}
	return e.addQueriesStreamChunked(queries, chunkSize, false, onResult)
}

// AddQueriesStreamPull is AddQueriesStreamChunked for pull-mode (ABI v3)
// hydrates. Two differences (DESIGN-duplex-streaming D5/D6):
//
//   - Each query drains on its OWN goroutine instead of the shared
//     hydrate-lane pool. A pull producer parks on client demand (the
//     sidecar's onResult blocks in streamGate.acquire); parking a shared
//     lane would starve sibling queries for client-think-time. Pull
//     concurrency is client-bounded by credits, so the P-lane bound is
//     redundant here; non-pull hydrates keep the pool (K = P × Cmax).
//   - The reader-demand bound shifts accordingly: a pull batch can demand
//     up to len(queries) concurrent readers instead of P. The sidecar's
//     warm-pool sizing already uses ConservativeHydrateCmaxForSpecs which
//     is per-spec, and pool acquisition falls back to serial when
//     exhausted — bounded degradation, not failure.
func (e *Engine) AddQueriesStreamPull(
	queries []QuerySpec,
	chunkSize int,
	onResult func(QueryResult) bool,
) error {
	if chunkSize <= 0 {
		chunkSize = hydrateChunkSize
	}
	return e.addQueriesStreamChunked(queries, chunkSize, true, onResult)
}

func (e *Engine) addQueriesStreamChunked(
	queries []QuerySpec,
	chunkSize int,
	pull bool,
	onResult func(QueryResult) bool,
) error {
	// D5 lock structure (DESIGN-duplex-streaming): build under e.mu, drain
	// OUTSIDE e.mu, post-wiring under e.mu again. The drain phase only
	// READS source state; every source-mutating entry point is serialized
	// against this call at the sidecar level (per-CG worker FIFO +
	// group.mu held across the whole RPC — main.go handleAddQueriesStream)
	// — the same per-CG serialization TS's view-syncer provides.
	// Rationale: a pull producer parked on client demand while holding
	// e.mu would freeze the engine (advances, Close) for
	// client-think-time.
	e.mu.Lock()

	// Bail explicitly on closed engine. Without this, the loop below
	// would iterate a nil pipelines/sources map and silently return
	// success with no callbacks fired — caller sees "0 queries
	// hydrated" with no error.
	if e.closed {
		e.mu.Unlock()
		return ErrEngineClosed
	}

	// Phase 1 (build, under e.mu): Build all pipelines sequentially
	// (mutates shared state). Scalar-subquery resolution + companion
	// wiring happens here too — see buildAndRegisterLocked. The closure's
	// deferred unlock covers build-phase PANICS (unknown table → DataError
	// panic, addqueries_build_unwind_test.go): the panic must escape to
	// the caller with e.mu released, exactly as the pre-D5 whole-function
	// defer provided.
	var built []*pipelineEntry
	var mrv map[string]string
	func() {
		defer e.mu.Unlock()
		built = e.buildBatchLocked(queries)
		mrv = e.minRowVersions
	}()

	// Phase 2 (drain, OUTSIDE e.mu): Hydrate via P worker lanes (or one
	// goroutine per query in pull mode — see AddQueriesStreamPull),
	// streaming per-query results in chunks as each query's fetch
	// progresses. Each query may emit multiple chunks (one per
	// hydrateChunkSize RowChanges); the last chunk has Final=true. P
	// bounds both goroutine count and connection demand (K = P × Cmax;
	// see DESIGN-streaming-hydrate.md §3a/§3d).
	//
	// The consumer streams each node as Fetch yields it — no intermediate
	// slices.Collect materialization. Go-side peak memory is bounded by the
	// current chunk (hydrateChunkSize RowChanges), not the full result set.
	//
	// C1: capture per-lane panics (see AddQueries) so a nil-PK panic in one
	// query's hydrate becomes a returned error instead of a process abort.
	// The query may have already emitted partial (Final=false) frames; the
	// handler turns the returned error into an rpcError, which rejects the
	// whole addQueriesStream call on the TS side — a clean failure, not a crash.
	hydratePanics := make([]any, len(built))
	// cancelled flips when any onResult returns false (one gate serves the
	// whole RPC, so one refusal means the client abandoned the whole call);
	// other producers notice at their next flush or job pickup and stop.
	var cancelled atomic.Bool

	hydrateOne := func(idx int, entry *pipelineEntry) {
		defer func() {
			if r := recover(); r != nil {
				hydratePanics[idx] = r
			}
		}()
		start := time.Now()
		var chunk []RowChange
		chunkBytes := 0
		chunkIndex := 0
		flush := func(final bool) bool {
			// TimingMs only on the final chunk (TS accumulator uses it
			// for per-query attribution). Avoids leaking incremental
			// fetch-time noise into the histogram.
			var timingMs float64
			if final {
				timingMs = float64(time.Since(start).Microseconds()) / 1000.0
			}
			if !onResult(QueryResult{
				QueryID:    entry.queryID,
				Changes:    bumpRowVersions(chunk, mrv),
				ChunkIndex: chunkIndex,
				Final:      final,
				TimingMs:   timingMs,
			}) {
				cancelled.Store(true)
				return false
			}
			// T1-5: reuse the backing array (see the matching note in
			// AdvanceStream's flush). onResult encodes Changes
			// synchronously via the sidecar's streamW before returning, so
			// the array is free to reuse. Revert to `chunk = nil` if any
			// caller retains Changes asynchronously.
			chunk = chunk[:0]
			chunkBytes = 0
			chunkIndex++
			return true
		}

		for node := range entry.pipeline.Input.Fetch(ivm.FetchRequest{}) {
			nodeChanges := streamNodes(entry.queryID, entry.schema, RowChangeAdd, node)
			chunk = append(chunk, nodeChanges...)
			chunkBytes += estimateRowChangesBytes(nodeChanges)
			if len(chunk) >= chunkSize || chunkBytes >= softChunkBytes {
				if !flush(false) {
					// Returning breaks the range mid-iteration: iter.Seq
					// yield sees false, every operator frame's defers run,
					// the SQLite cursor closes, the pool reader returns to
					// the warm pool (D4 — the .return() dual).
					return
				}
			}
		}
		if cancelled.Load() {
			// Another producer's consumer refusal (or this RPC's gate
			// cancel between flushes) — stop before companion emit; the
			// RPC is settling as cancelled, nothing may be delivered.
			return
		}
		// Emit companion rows after the main pipeline's nodes so the
		// client receives all rows for one queryID in one logical run.
		// They're tagged with the same queryID + each companion's own
		// schema (table name), so the streamer/client demultiplex
		// correctly. Chunk-bounded so a query with many companions
		// still respects hydrateChunkSize.
		for _, ce := range entry.companions {
			if ce.matchedRow == nil {
				continue
			}
			node := ivm.Node{Row: ce.matchedRow}
			nodeChanges := streamNodes(entry.queryID, ce.schema, RowChangeAdd, node)
			chunk = append(chunk, nodeChanges...)
			chunkBytes += estimateRowChangesBytes(nodeChanges)
			if len(chunk) >= chunkSize || chunkBytes >= softChunkBytes {
				if !flush(false) {
					return
				}
			}
		}
		// Always emit a terminal frame with Final=true, even if empty —
		// the TS accumulator uses it as the per-query completion signal.
		// For queries whose total RowChanges hit an exact multiple of
		// hydrateChunkSize, the terminal frame carries zero rows; one
		// extra small frame per such query is the tradeoff for a simple
		// invariant ("every query ends with Final=true").
		flush(true)
	}

	var wg sync.WaitGroup
	if pull {
		// D6: one goroutine per query — a parked pull producer must not
		// occupy a shared lane (see AddQueriesStreamPull).
		for i, entry := range built {
			wg.Add(1)
			go func(idx int, entry *pipelineEntry) {
				defer wg.Done()
				if cancelled.Load() {
					return
				}
				hydrateOne(idx, entry)
			}(i, entry)
		}
	} else {
		p := hydrateLanes
		if p > len(built) {
			p = len(built)
		}
		if p < 1 {
			p = 1
		}
		type hydrateJob struct {
			idx   int
			entry *pipelineEntry
		}
		jobs := make(chan hydrateJob, len(built))
		for i, entry := range built {
			jobs <- hydrateJob{i, entry}
		}
		close(jobs)
		for w := 0; w < p; w++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for job := range jobs {
					if cancelled.Load() {
						continue // cancelled RPC: skip queued queries entirely
					}
					hydrateOne(job.idx, job.entry)
				}
			}()
		}
	}
	wg.Wait()

	// Phase 3 (post, under e.mu again).
	e.mu.Lock()
	defer e.mu.Unlock()
	if err := firstHydratePanic(built, hydratePanics); err != nil {
		return err
	}
	if cancelled.Load() {
		// Pipelines stay registered, exactly like the panic path: the TS
		// side rejects the whole addQueriesStream (I3 all-or-nothing) and
		// a retry re-adds the queries (buildAndRegisterLocked removes the
		// stale entry first).
		return ErrStreamCancelled
	}

	// HIGH-11: wire companion outputs after all hydrates complete. Skipped
	// if the engine closed mid-drain (Close contract says callers prevent
	// that, but the check is cheap and the wiring would touch destroyed
	// pipelines).
	if !e.closed {
		for _, entry := range built {
			e.wireCompanionOutputsLocked(entry)
		}
	}
	return nil
}

// firstHydratePanic converts the first non-nil per-goroutine hydrate panic into
// an error so the RPC handler can return an error frame instead of letting the
// panic abort the whole sidecar process (C1). Any value is wrapped
// with its query ID for diagnosis.
func firstHydratePanic(built []*pipelineEntry, panics []any) error {
	for i, p := range panics {
		if p == nil {
			continue
		}
		qid := ""
		if i < len(built) && built[i] != nil {
			qid = built[i].queryID
		}
		// Preserve a typed *ivm.DataError (unsafe int / bad JSON surfaced by
		// FromSQLiteType during the hydrate scan) through the %w chain, so the
		// sidecar handler maps it to rpcCodeDataError — IDENTICAL to how the
		// advance path surfaces the same panic via panicErrorCode. Before this,
		// the SAME DataError produced -32102 in advance but a generic -32000 in
		// hydrate, so TS re-initialized cleanly on an advance-time bad value but
		// took the generic-failure path on a hydrate-time one (parity gap).
		if err, ok := p.(error); ok {
			return fmt.Errorf("hydrate panic (query %s): %w", qid, err)
		}
		return fmt.Errorf("hydrate panic (query %s): %v", qid, p)
	}
	return nil
}

// QuerySpec is a query to add in a batch.
type QuerySpec struct {
	QueryID string
	AST     builder.AST
}

// QueryResult is the hydration result for one query in a batch.
//
// For AddQueries (non-streaming), every QueryResult has ChunkIndex=0 and
// Final=true — the slice carries the query's complete result.
//
// For AddQueriesStream, a single query may produce multiple QueryResult
// chunks. ChunkIndex is monotonically increasing per query starting at 0;
// exactly one chunk per query has Final=true (the last). TimingMs is the
// per-query wall time, recorded once on the final chunk (zero on
// non-final chunks) so the TS accumulator can attribute timing per query.
type QueryResult struct {
	QueryID    string
	Changes    []RowChange
	ChunkIndex int
	Final      bool
	TimingMs   float64 // wall-time millis for this query's fetch+stream (final chunk only)
}

// defaultChunkSize is the ONE production knob for streamed-frame
// granularity: GO_IVM_CHUNK_SIZE sets BOTH hydrateChunkSize and
// advanceChunkSize. Default 100 — the production path streams by default
// (the validated streaming-tablesrc setting; aligns with the TS
// view-syncer's cursor page size). The per-facet vars below remain as
// fine-grained overrides for A/B work; deployments should set only this.
//
// TRANSPORT-AGNOSTIC (REVIEW-napi-transport O2): this default is engine-
// level, so it applies to the SOCKET transport too, not just napi. A
// socket deployment on this binary emits ~100× more (smaller) frames per
// large hydrate/advance than the old 10000 default — each frame pays a
// length-prefix + write() syscall + TS-side decode dispatch. This is
// intended (streaming-by-default is a deliberate product decision) and is
// the exact config the streaming-tablesrc image variant already ships and
// soaked over a socket. A socket deployment that wants the old
// coarse-frame behavior sets GO_IVM_CHUNK_SIZE=10000 (or the per-facet
// GO_IVM_HYDRATE_CHUNK_SIZE / GO_IVM_ADVANCE_CHUNK_SIZE).
var defaultChunkSize = envChunkSize("GO_IVM_CHUNK_SIZE", 100)

// hydrateChunkSize is the max number of RowChanges per partial frame in
// AddQueriesStream. Matches the TS view-syncer's CURSOR_PAGE_SIZE so chunks
// align with the downstream poke-batching boundary (no point chunking
// finer than the consumer batches).
//
// Declared as var (not const) so tests can shrink it to exercise chunk
// boundaries without allocating 10k-row payloads per case.
var hydrateChunkSize = envChunkSize("GO_IVM_HYDRATE_CHUNK_SIZE", defaultChunkSize)

// hydrateLanes is the number of worker lanes (P) that hydrate queries in
// parallel. Replaces the unbounded per-query goroutine spawn with P workers
// draining a job channel, bounding both goroutine count and — with K = P ×
// Cmax reader-pool connections — concurrent-cursor demand. See
// DESIGN-streaming-hydrate.md §3a/§3d.
//
// Default 4; GO_IVM_PARALLELISM is the ONE production parallelism knob (it
// also sizes the sidecar's reader-pool floor at 2×P — see newServerFromEnv);
// GO_IVM_HYDRATE_LANES overrides the lane count individually. The pool must
// be sized to at least P (K = P × Cmax; Cmax=1 while operators are eager →
// K=P) so every lane can always acquire a reader (deadlock-freedom: §3d).
var hydrateLanes = envChunkSize("GO_IVM_HYDRATE_LANES", envChunkSize("GO_IVM_PARALLELISM", 4))

// softChunkBytes is the estimated-payload budget per streamed partial frame.
// The row-count caps (hydrateChunkSize / advanceChunkSize) bound COUNT but
// not BYTES: 10k rows averaging >6.4KB each (large message bodies, canvas
// JSON, ...) msgpack-encode past the 64MB wire frame cap and the receiver
// rejects the frame. For hydrate that is a DETERMINISTIC failure loop — the
// same query re-fetches the same fat rows on every retry. Chunks flush early
// when the running size estimate crosses this budget. The estimate ignores
// msgpack framing overhead (undercounts slightly); the 8MB-vs-64MB margin
// absorbs that. A SINGLE source-change or node whose changes alone exceed
// the budget still ships in one frame (same caveat as the count cap —
// splitting those needs operator-level chunking).
var softChunkBytes = envChunkSize("GO_IVM_CHUNK_SOFT_BYTES", 8*1024*1024)

// estimateRowChangeBytes is a cheap one-pass size estimate of a RowChange's
// wire footprint, for softChunkBytes accounting. Strings/blobs dominate fat
// rows, so they're measured exactly; every other value counts a fixed 16.
func estimateRowChangeBytes(rc RowChange) int {
	n := 64 + len(rc.QueryID) + len(rc.Table)
	for k, v := range rc.RowKey {
		n += len(k) + estimateValueBytes(v)
	}
	for k, v := range rc.Row {
		n += len(k) + estimateValueBytes(v)
	}
	return n
}

func estimateValueBytes(v interface{}) int {
	switch t := v.(type) {
	case string:
		return len(t) + 8
	case []byte:
		return len(t) + 8
	default:
		return 16
	}
}

func estimateRowChangesBytes(rcs []RowChange) int {
	n := 0
	for _, rc := range rcs {
		n += estimateRowChangeBytes(rc)
	}
	return n
}

// RemoveQuery destroys a pipeline.
func (e *Engine) RemoveQuery(queryID string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.removeQueryLocked(queryID)
}

func (e *Engine) removeQueryLocked(queryID string) {
	entry, ok := e.pipelines[queryID]
	if !ok {
		return
	}
	entry.pipeline.Input.Destroy()
	// Tear down companion sub-pipelines; their Connections to the subquery
	// sources are otherwise leaked and would keep receiving Push fan-outs
	// after the parent query was removed.
	for _, ce := range entry.companions {
		ce.pipeline.Input.Destroy()
	}
	// HIGH-4: DELETE this query's operator-storage rows (Take windows, etc.).
	// CreateClientGroupStorage DELETEs on construction but Destroy was never
	// called, so rows accumulated per distinct queryID over the engine's
	// lifetime. cgs is nil when no storage-using operator was built.
	if entry.delegate != nil && entry.delegate.cgs != nil {
		entry.delegate.cgs.Destroy()
	}
	delete(e.pipelines, queryID)
}

// Advance processes a batch of snapshot changes through all affected sources.
// Returns flat RowChanges representing the effect on all registered pipelines.
//
// Failure model (follow-TS): the source's pre-Push validation (tablesource
// driftCheckLocked in prod; the MemorySource genPush asserts in the engine
// test fixture) and every downstream operator assert PANIC with a plain
// error — the direct twin of TS's assert-throws. The panic re-raises out of
// this method after the streamer is drained (HIGH-10) and signalAdvanceEnd
// has rotated the sources; the sidecar handler converts it to an RPC error
// and TS tears the client group down.
func (e *Engine) Advance(changes []SnapshotChange) *AdvanceResult {
	e.mu.Lock()
	defer e.mu.Unlock()
	// End-of-batch hook for every registered source — rotates TableSource
	// snapshots + clears their batch-scoped delta (incl. the removedInBatch
	// dedup set) so the next batch starts clean. Deferred (not inline after the
	// push loop) so it ALSO fires when a panic re-raises out of the
	// recover()-guarded loop — matching AdvanceStream and guaranteeing the set
	// never leaks across a batch boundary. Registered after e.mu.Unlock so it
	// still runs while e.mu is held.
	defer e.signalAdvanceEnd()

	var allRowChanges []RowChange
	var timings []TableTiming

	func() {
		defer func() {
			if r := recover(); r != nil {
				// HIGH-10: drain the streamer before re-raising, matching
				// AdvanceStream's recover. Otherwise this aborted advance's
				// accumulated entries survive and the NEXT successful
				// Advance's first streamer.Stream() surfaces them as if
				// produced by that advance — wrong RowChanges to clients.
				_ = e.streamer.Stream()
				panic(r)
			}
		}()

		// Snapshot sources once; COW + atomic.Pointer guarantees this slice
		// stays consistent for the duration of the advance loop.
		sources := e.sourcesView()
		// Group changes by table for potential cross-table parallelism (future)
		for _, change := range changes {
			source, ok := sources[change.Table]
			if !ok {
				continue // no pipelines read this table
			}

			sourceChanges := snapshotToSourceChanges(change, source)

			// Push each source change and collect streamer output. Time each
			// one individually so TS can attribute wall time to the
			// responsible (table, op) pair — matches the granularity of TS's
			// #advanceTime histogram (pipeline-driver.ts:2545).
			for _, sc := range sourceChanges {
				start := time.Now()
				source.Push(sc)
				rowChanges := e.streamer.Stream()
				ms := float64(time.Since(start).Microseconds()) / 1000.0
				allRowChanges = append(allRowChanges, rowChanges...)
				timings = append(timings, TableTiming{
					Table:   change.Table,
					ChangeT: int(sc.Type),
					Ms:      ms,
				})
			}
		}
	}()

	return &AdvanceResult{Changes: bumpRowVersions(allRowChanges, e.minRowVersions), Timings: timings}
}

// AdvanceStreamPartial is one frame in the AdvanceStream output. Multiple
// frames may be emitted per advance call; the frame with Final=true is the
// terminal frame. Timings is populated only on the terminal frame so the TS
// caller can record per-(table,op) histogram entries once, atomically with
// the completion signal.
//
// See Engine.AdvanceStream for the chunking contract.
type AdvanceStreamPartial struct {
	Changes    []RowChange   `json:"changes"`
	ChunkIndex int           `json:"chunkIndex"`
	Final      bool          `json:"final"`
	Timings    []TableTiming `json:"timings,omitempty"`
}

// advanceChunkSize is the max number of RowChanges per partial frame in
// AdvanceStream. Matches hydrateChunkSize and the TS view-syncer's
// CURSOR_PAGE_SIZE so chunks align with the downstream poke-batching
// boundary. Var (not const) for test override. Set GO_IVM_CHUNK_SIZE to
// control both this and hydrateChunkSize together (the production knob).
var advanceChunkSize = envChunkSize("GO_IVM_ADVANCE_CHUNK_SIZE", defaultChunkSize)

// AdvanceStream is the streaming variant of Advance: same source-push +
// streamer-drain loop, but flushes a partial frame every advanceChunkSize
// accumulated RowChanges instead of buffering the whole AdvanceResult in
// one msgpack frame. The TS client reassembles the frames into the same
// AdvanceResult shape Advance returns, so view-syncer code is agnostic to
// which path was used.
//
// Frame invariants (mirror AddQueriesStream):
//   - exactly one frame has Final=true (always the last)
//   - ChunkIndex is monotonically increasing per call starting at 0
//   - Timings is populated only on the Final frame
//   - empty advances still emit one frame with Final=true (no changes,
//     no timings) so the TS accumulator has a uniform completion signal
//
// Like AddQueriesStream, this reduces Go-side memory pressure (each chunk
// is encoded + flushed + freed before the next accumulates) and improves
// time-to-first-byte for large advance batches. Go-side peak buffer is
// bounded to ~one chunk even for a single source-change whose fan-out
// exceeds the chunk size: the streamer's chunkSink (SetChunkSink below)
// flushes full chunks DURING the flatten, and only the sub-threshold
// residual per pipeline coalesces into the streamer's slice.
//
// onResult may be called multiple times from this goroutine before the
// function returns. Engine.mu is held throughout, matching Advance's
// lock discipline.
func (e *Engine) AdvanceStream(
	changes []SnapshotChange,
	onResult func(AdvanceStreamPartial),
) error {
	return e.advanceStreamChunked(changes, advanceChunkSize, onResult)
}

// AdvanceStreamChunked is AdvanceStream with a per-call chunk-size override.
// chunkSize=1 yields one partial per RowChange — the NAPI row plane uses
// this so each row crosses the Go↔JS boundary the moment the push's flatten
// produces it (straight off the lazy SQLite cursor),
// instead of being re-batched into advanceChunkSize frames. chunkSize<=0
// falls back to advanceChunkSize.
func (e *Engine) AdvanceStreamChunked(
	changes []SnapshotChange,
	chunkSize int,
	onResult func(AdvanceStreamPartial),
) error {
	if chunkSize <= 0 {
		chunkSize = advanceChunkSize
	}
	return e.advanceStreamChunked(changes, chunkSize, onResult)
}

// AdvanceStreamChunkedSeq is AdvanceStreamChunked over a LAZY change
// sequence (DESIGN-duplex-streaming D9): the snapshotter's changelog
// cursor feeds the push loop one SnapshotChange at a time, so peak memory
// is O(chunk) instead of O(diff) — the GO_IVM_MAX_DIFF_CHANGES cap and its
// reset failure mode are unnecessary for callers of this variant. This is
// TS's shape: #advance iterates its changelog cursor lazily and pushes
// per-change; nothing materializes the diff.
//
// The seq's error slot carries the CURSOR's failure (reset signal /
// invalid-diff / SQL error) in-band. On a yielded error the loop stops and
// the error is returned WITHOUT the terminal Final flush: the engine may
// have applied a prefix of the diff, so the stream must settle as an ERROR
// (the sidecar's rpcError — classified into the caller's reset path, which
// discards the half-advanced engine), never as a clean Final that a
// consumer could mistake for a complete advance. signalAdvanceEnd still
// runs (sources rotate to a sane frame for the teardown window), matching
// the drift/panic paths.
func (e *Engine) AdvanceStreamChunkedSeq(
	changes iter.Seq2[SnapshotChange, error],
	chunkSize int,
	onResult func(AdvanceStreamPartial),
) error {
	if chunkSize <= 0 {
		chunkSize = advanceChunkSize
	}
	return e.advanceStreamChunkedSeq(changes, chunkSize, nil, onResult)
}

// advanceClockCarrier is implemented by sources whose push fan-out runs on
// worker goroutines (tablesource.Source): the engine hands them the
// advance's processing-clock accumulator so those worker threads' CPU is
// bracketed into the same budget the sidecar's economic advancement-abort
// evaluates (cmd/sidecar/advance_abort.go). ivm.MemorySource (engine-test
// fixture, not on the production path) deliberately does not implement it.
type advanceClockCarrier interface {
	SetAdvanceClock(*procclock.Accumulator)
}

// AdvanceStreamChunkedSeqClocked is AdvanceStreamChunkedSeq with a
// processing-clock accumulator threaded down to the sources' parallel
// push-fanout workers. nil clk ≡ AdvanceStreamChunkedSeq (zero cost).
func (e *Engine) AdvanceStreamChunkedSeqClocked(
	changes iter.Seq2[SnapshotChange, error],
	chunkSize int,
	clk *procclock.Accumulator,
	onResult func(AdvanceStreamPartial),
) error {
	if chunkSize <= 0 {
		chunkSize = advanceChunkSize
	}
	return e.advanceStreamChunkedSeq(changes, chunkSize, clk, onResult)
}

func (e *Engine) advanceStreamChunked(
	changes []SnapshotChange,
	chunkSize int,
	onResult func(AdvanceStreamPartial),
) error {
	return e.advanceStreamChunkedSeq(func(yield func(SnapshotChange, error) bool) {
		for _, c := range changes {
			if !yield(c, nil) {
				return
			}
		}
	}, chunkSize, nil, onResult)
}

func (e *Engine) advanceStreamChunkedSeq(
	changes iter.Seq2[SnapshotChange, error],
	chunkSize int,
	clk *procclock.Accumulator,
	onResult func(AdvanceStreamPartial),
) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	// Bail explicitly on closed engine — without this, the loop below
	// would skip every source (e.sources is nil after Close) and silently
	// emit one empty Final frame, indistinguishable from a no-op advance.
	if e.closed {
		return ErrEngineClosed
	}

	// Snapshot sources once — COW + atomic.Pointer keeps the map consistent
	// for the whole advance. Hand the processing clock to sources whose
	// fan-out spawns worker goroutines, and clear it on ALL exits (defer
	// runs on the panic path too) so this advance's clock can't leak into
	// the next.
	sources := e.sourcesView()
	if clk != nil {
		for _, src := range sources {
			if c, ok := src.(advanceClockCarrier); ok {
				c.SetAdvanceClock(clk)
			}
		}
		defer func() {
			for _, src := range sources {
				if c, ok := src.(advanceClockCarrier); ok {
					c.SetAdvanceClock(nil)
				}
			}
		}()
	}

	var pending []RowChange
	pendingBytes := 0
	var timings []TableTiming
	chunkIndex := 0
	var flushMu sync.Mutex

	// emitLocked writes ONE partial/final frame to the wire. Callers MUST hold
	// flushMu — that is what keeps chunkIndex monotonic and frame SEND order
	// equal to it, even when parallel push-fanout goroutines flush mid-flatten
	// chunks (via the streamer's chunkSink) concurrently. Timings ride the
	// final frame only.
	// `rows` is consumed synchronously — the sidecar's streamW → mpMarshal
	// encodes it into a separate byte buffer before onResult returns, so
	// callers may reuse the backing array after (T1-5 invariant).
	emitLocked := func(rows []RowChange, final bool) {
		var t []TableTiming
		if final {
			t = timings
		}
		onResult(AdvanceStreamPartial{
			Changes:    bumpRowVersions(rows, e.minRowVersions),
			ChunkIndex: chunkIndex,
			Final:      final,
			Timings:    t,
		})
		chunkIndex++
	}

	// flushPendingLocked ships the buffered sub-threshold residual of EARLIER
	// pushes as its own partial frame. Callers MUST hold flushMu.
	//
	// Cross-push wire order (scale-review C1): the chunkSink flushes full
	// chunks of the CURRENT push's fan-out directly to the wire mid-flatten.
	// If a previous push's residual were still sitting in `pending`, the newer
	// rows would overtake it on the wire — remove(X) (push N, buffered) +
	// add(X) (push N+1, chunk-flushed) would arrive at the client as
	// add-then-remove, permanently deleting the row, while chunkIndex stays
	// monotonic so nothing downstream detects it. Draining `pending` before
	// every chunk emission keeps the wire in push order. Memory safety: the
	// chunkSink only fires while the main goroutine is blocked inside
	// source.Push (both fanout paths wg.Wait before returning — see
	// ivm/parallel.go and tablesource/parallel_fanout.go), so main-goroutine
	// access to pending never overlaps a sink call; the goroutine start/join
	// edges plus flushMu give cross-goroutine visibility.
	flushPendingLocked := func() {
		if len(pending) == 0 {
			return
		}
		emitLocked(pending, false)
		pending = pending[:0]
		pendingBytes = 0
	}

	flush := func(final bool) {
		flushMu.Lock()
		// Deferred unlock: onResult is caller-supplied and may panic (e.g. a
		// wire-write failure surfacing as panic). A bare Unlock after the call
		// would leave flushMu held on that panic, and the guaranteed terminal
		// flush(true) below would then deadlock on flushMu.Lock() — wedging the
		// engine with e.mu held, i.e. every CG sharing this engine.
		defer flushMu.Unlock()
		if final {
			// Terminal frame always goes out, even with empty changes.
			emitLocked(pending, true)
			pending = pending[:0]
			pendingBytes = 0
			return
		}
		flushPendingLocked()
	}

	// Operator-level streaming (DESIGN-streaming-advance §Win-2): a single
	// source-change's fan-out flushes full chunks DURING the flatten via the
	// streamer's chunkSink, so peak buffer is bounded to one chunk instead of the
	// whole delta (a 50k-child ADD ships as N frames, not one 50k frame — matching
	// TS #streamNodes yield* + processChanges poke-every-CURSOR_PAGE_SIZE). The
	// <chunkSize residual coalesces into the streamer's rows and is drained per
	// source-change below. Cleared on return (under e.mu, before Unlock) so
	// hydrate/companion Accumulate stay in slice mode.
	e.streamer.SetChunkSink(func(chunk []RowChange) {
		flushMu.Lock()
		defer flushMu.Unlock() // deferred: onResult may panic (see flush)
		// C1: ship earlier pushes' buffered residual BEFORE this mid-flatten
		// chunk so the wire never carries newer rows ahead of older ones.
		flushPendingLocked()
		emitLocked(chunk, false)
	}, chunkSize, softChunkBytes)
	defer e.streamer.SetChunkSink(nil, 0, 0)

	// Panic capture: pre-fix this re-raised inline (panic(r)
	// in the deferred recover), skipping the terminal flush(true) below
	// and leaving the TS-side accumulator throwing
	// "finished without a final chunk". That cascaded to C5's protocol-
	// violation path which treats the wire as corrupted. By capturing
	// the panic and re-raising AFTER flush(true), TS always sees a
	// clean terminal frame — empty changes signal "advance abandoned"
	// rather than wire protocol corruption.
	var capturedPanic any
	var seqErr error
	func() {
		defer func() {
			if r := recover(); r != nil {
				// Capture for re-raise after flush. This includes the
				// source/operator asserts (plain-error drift panics): TS's
				// twin throws → the view-syncer tears the client group
				// down, so no partial output may settle as a clean stream.
				capturedPanic = r
				// Drop partial output: a panic mid-loop
				// means Go's state may have advanced partially; sending
				// only the partial diff to TS would leave the CVR
				// out of sync with Go. Safer to send empty Final and
				// let the caller's teardown machinery rebuild from
				// scratch.
				pending = nil
				_ = e.streamer.Stream()
			}
		}()

		// Sources snapshot hoisted above (shared with the clock plumbing).
		for change, cerr := range changes {
			if cerr != nil {
				// Lazy cursor failed mid-diff (D9): stop consuming; the
				// error settles the whole stream (see AdvanceStreamChunkedSeq).
				seqErr = cerr
				return
			}
			source, ok := sources[change.Table]
			if !ok {
				continue // no pipelines read this table
			}

			sourceChanges := snapshotToSourceChanges(change, source)

			for _, sc := range sourceChanges {
				start := time.Now()
				source.Push(sc)
				rowChanges := e.streamer.Stream()
				ms := float64(time.Since(start).Microseconds()) / 1000.0
				pending = append(pending, rowChanges...)
				pendingBytes += estimateRowChangesBytes(rowChanges)
				timings = append(timings, TableTiming{
					Table:   change.Table,
					ChangeT: int(sc.Type),
					Ms:      ms,
				})

				// Flush mid-batch if we've crossed the chunk threshold —
				// row count OR estimated bytes (fat rows blow the 64MB wire
				// frame long before 10k rows; see softChunkBytes). Note the
				// residual drained above is what the chunkSink did NOT flush
				// mid-flatten: up to one sub-threshold tail per pipeline, so
				// `pending` can briefly exceed chunkSize with many pipelines
				// — checked AFTER appending, flushed as one frame here.
				if len(pending) >= chunkSize || pendingBytes >= softChunkBytes {
					flush(false)
				}
			}
		}
	}()

	// End-of-batch hook — rotate TableSource snapshots + clear their
	// batch-scoped delta. See Engine.Advance for the full rationale; same
	// invariant applies to the streaming path. Runs OUTSIDE the
	// recover()-guarded func so it fires on the panic path too.
	e.signalAdvanceEnd()

	// D9: a lazy-cursor failure settles the stream as an ERROR — no
	// terminal Final frame (the caller's rpcError is the terminal; a clean
	// Final here would let a consumer mistake a half-applied diff for a
	// complete advance).
	if seqErr != nil {
		return seqErr
	}

	// Always emit a terminal Final frame. Carries cumulative timings on
	// success; carries empty Changes on a captured panic (the caller's
	// teardown machinery rebuilds).
	flush(true)

	// Re-raise the captured panic AFTER flush so TS sees a clean wire.
	// The sidecar RPC handler has its own
	// recover that converts panic to an error response — the wire
	// already carries the Final marker so TS doesn't trip the
	// protocol-violation path.
	if capturedPanic != nil {
		panic(capturedPanic)
	}
	return nil
}

// snapshotToSourceChanges converts a SnapshotChange to IVM SourceChanges.
func snapshotToSourceChanges(change SnapshotChange, source Source) []ivm.SourceChange {
	var result []ivm.SourceChange

	// Normalize row types to match what the source expects.
	source.NormalizeRow(change.NextValue)
	for _, prevRow := range change.PrevValues {
		source.NormalizeRow(prevRow)
	}

	var editOldRow ivm.Row

	// Process removals
	for _, prevRow := range change.PrevValues {
		if change.NextValue != nil && primaryKeysMatch(prevRow, change.NextValue, source.PrimaryKey()) {
			// This prev row matches the next row's PK — it's an edit
			editOldRow = prevRow
		} else {
			// Pure removal (or constraint conflict removal)
			result = append(result, ivm.SourceChange{
				Type: ivm.ChangeTypeRemove,
				Row:  prevRow,
			})
		}
	}

	// Process addition/edit
	if change.NextValue != nil {
		if editOldRow != nil {
			result = append(result, ivm.SourceChange{
				Type:   ivm.ChangeTypeEdit,
				Row:    change.NextValue,
				OldRow: editOldRow,
			})
		} else {
			result = append(result, ivm.SourceChange{
				Type: ivm.ChangeTypeAdd,
				Row:  change.NextValue,
			})
		}
	}

	return result
}

// primaryKeysMatch checks if two rows have the same primary key values.
func primaryKeysMatch(a, b ivm.Row, pk []string) bool {
	for _, key := range pk {
		if !ivm.ValuesEqual(a[key], b[key]) {
			return false
		}
	}
	return true
}

// --- pipelineOutput implements ivm.Output ---
// It captures IVM changes into the engine's streamer.

type pipelineOutput struct {
	engine  *Engine
	queryID string
	schema  *ivm.SourceSchema
}

func (po *pipelineOutput) Push(change ivm.Change, pusher ivm.InputBase) {
	// Flatten-in-push (DESIGN-streaming-advance.md): Accumulate flattens the
	// change tree to RowChanges NOW, while Output.Push is on the stack and the
	// mutation overlay + join in-progress child state are still live (§3). No
	// eager materializeChange deep-copy — streamNodesInto walks the lazy
	// relationship closures directly, matching TS's #streamNodes generator.
	po.engine.streamer.Accumulate(po.queryID, po.schema, []ivm.Change{change})
}

// companionOutput wraps pipelineOutput for a resolved scalar-subquery
// companion pipeline. Before accumulating the companion's change it runs
// the scalar-value-changed reset check — a direct port of TS's live
// companion push. A push that moves the
// resolved scalar's child field to a different value makes the main
// query's baked-in literal stale, so it panics with *ScalarResetError; the
// sidecar maps it to the scalar-reset RPC code and TS resets +
// re-registers the query, re-running ResolveSimpleScalarSubqueries against
// current truth and baking the NEW value — TS's own companion push throws
// ResetPipelinesSignal('scalar-subquery') at the same point
// (pipeline-driver.ts:1717). Unchanged-value pushes
// accumulate exactly as the plain pipelineOutput would.
type companionOutput struct {
	pipelineOutput
	childField    string
	resolvedValue ivm.Value
}

// ScalarResetError is the panic a companionOutput raises when a resolved
// scalar subquery's value changes — the twin of TS's
// ResetPipelinesSignal('scalar-subquery') (pipeline-driver.ts:1717-1723).
// Unlike the source/operator asserts (whose TS twins throw → teardown),
// TS's disposition here is a RESET + re-hydrate, so the sidecar maps this
// type to its own RPC code instead of the generic -32000. Message mirrors
// the TS signal's message.
type ScalarResetError struct {
	Table    string
	Resolved string // JS-String rendering of the resolved (baked) value
	New      string // JS-String rendering of the pushed value
}

func (e *ScalarResetError) Error() string {
	return fmt.Sprintf("Scalar subquery value changed for %s: %s -> %s",
		e.Table, e.Resolved, e.New)
}

// jsScalarString approximates JS String(v) for the scalar values a
// resolvable subquery yields (string/number/bool/null/undefined). Message
// rendering only — never compared or parsed.
func jsScalarString(v ivm.Value, undefined bool) string {
	switch {
	case undefined:
		return "undefined"
	case v == nil:
		return "null"
	}
	switch x := v.(type) {
	case string:
		return x
	case bool:
		if x {
			return "true"
		}
		return "false"
	case float64:
		return strconv.FormatFloat(x, 'f', -1, 64)
	default:
		return fmt.Sprintf("%v", v)
	}
}

func (co *companionOutput) Push(change ivm.Change, pusher ivm.InputBase) {
	changed := false
	var newValue ivm.Value
	newUndefined := false
	switch change.Type {
	case ivm.ChangeTypeAdd, ivm.ChangeTypeEdit:
		// New scalar value is the child field of the pushed (new) node.
		// TS: newValue = change.node.row[childField] ?? null.
		newValue = change.Node.Row[co.childField]
		changed = !scalarValuesEqual(newValue, co.resolvedValue)
	case ivm.ChangeTypeRemove:
		// TS: newValue = undefined for REMOVE, and scalarValuesEqual(
		// undefined, resolvedValue) is always false (resolvedValue is never
		// undefined) — so removing the scalar's source row always resets.
		changed = true
		newUndefined = true
	case ivm.ChangeTypeChild:
		// TS returns [] for CHILD: a relationship-only change does not move
		// the scalar value — neither accumulate nor reset.
		return
	}
	if changed {
		panic(&ScalarResetError{
			Table:    co.schema.TableName,
			Resolved: jsScalarString(co.resolvedValue, false),
			New:      jsScalarString(newValue, newUndefined),
		})
	}
	co.pipelineOutput.Push(change, pusher)
}

// scalarValuesEqual ports TS's scalarValuesEqual (pipeline-driver.ts:3029-3034,
// strict `a === b`) for the resolved-scalar child-field comparison. Go's
// interface `==` matches JS `===` for scalar literals (the only thing a
// resolvable scalar subquery yields), with nil == SQL/JS null. The recover
// guards the rare non-comparable dynamic type (JSON map/slice would panic
// on ==); treating those as unequal mirrors TS's reference-inequality for
// object-typed values (→ reset), which is the safe direction.
func scalarValuesEqual(a, b ivm.Value) (eq bool) {
	defer func() {
		if recover() != nil {
			eq = false
		}
	}()
	return a == b
}

// --- engineDelegate implements builder.Delegate ---

type engineDelegate struct {
	engine  *Engine
	queryID string
	cgs     *sqlite.ClientGroupStorage
}

// GetSource resolves a table name to a builder.Source for use during pipeline
// build. Reads the engine's sources snapshot lock-free — the snapshot pointer
// is COW-updated by RegisterSource so iteration is always against a consistent
// post-Store view (see type Engine doc comment).
//
// The returned wrapper carries this delegate's queryID as the CONNECT GROUP:
// every leaf connection the builder makes for this query (main pipeline
// branches, EXISTS/related subquery legs, resolved-scalar companions — all
// built under the same delegate) is tagged with it. tablesource.Source uses
// the tag as the parallel-advance serialization unit: connections of one
// query share spine operators and MUST push serially; connections of
// different queries share nothing above the source and may push in parallel.
func (d *engineDelegate) GetSource(tableName string) builder.Source {
	source, ok := d.engine.sourcesView()[tableName]
	if !ok {
		return nil
	}
	return &engineSource{source: source, group: d.queryID}
}

func (d *engineDelegate) CreateStorage(name string) ivm.TakeStorage {
	return d.ensureCGS().CreateTakeStorage()
}

func (d *engineDelegate) CreateCapStorage(name string) ivm.CapStorage {
	return d.ensureCGS().CreateCapStorage()
}

// ensureCGS lazily creates the per-query ClientGroupStorage. Shared by both
// storage factories: CreateClientGroupStorage DELETEs the queryID's rows at
// creation, so a second instance mid-build would wipe storages already
// vended to earlier operators of the same pipeline.
func (d *engineDelegate) ensureCGS() *sqlite.ClientGroupStorage {
	if d.cgs == nil {
		d.cgs = d.engine.storage.CreateClientGroupStorage(d.queryID)
	}
	return d.cgs
}

// --- engineSource wraps Source as builder.Source ---

type engineSource struct {
	source Source
	// group is the owning query's ID — the parallel-advance serialization
	// unit. Threaded to the source at Connect time via the optional
	// connGroupTagger interface (tablesource implements it; MemorySource
	// doesn't and keeps its own threshold-based parallel fanout).
	group string
}

// connGroupTagger is implemented by sources whose next Connect call should
// tag the new connection with a pipeline group. Set-then-Connect is atomic
// here because pipeline builds run under Engine.mu and Connect is synchronous
// inside builder.BuildPipeline.
type connGroupTagger interface {
	SetNextConnectGroup(string)
}

func (es *engineSource) Connect(opts builder.ConnectOptions) ivm.Input {
	if t, ok := es.source.(connGroupTagger); ok {
		t.SetNextConnectGroup(es.group)
	}
	return es.source.Connect(opts.Sort, opts.Filter, opts.FilterPredicate, opts.SplitEditKeys)
}

func (es *engineSource) PrimaryKey() []string {
	return es.source.PrimaryKey()
}

func (es *engineSource) NormalizeRow(row ivm.Row) {
	es.source.NormalizeRow(row)
}
