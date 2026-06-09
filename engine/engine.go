package engine

// Per-clientGroup IVM engine: owns sources and pipelines, hydrates queries
// from current state, and drives advance diffs through the operator trees.
// Hydrations run in parallel goroutines; advances are serialized on e.mu so
// pushes don't race with concurrent fetches.

import (
	"database/sql"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/kartikparsoya-eng/go-ivm/builder"
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
	Push(ivm.SourceChange) []ivm.Change
	Connect(sort ivm.Ordering, filterPredicate func(ivm.Row) bool, splitEditKeys map[string]bool) ivm.Input
	Close() error
}

// memorySourceAdapter wraps *ivm.MemorySource to implement Source.
type memorySourceAdapter struct {
	ms *ivm.MemorySource
}

func (a *memorySourceAdapter) TableName() string      { return a.ms.TableName() }
func (a *memorySourceAdapter) PrimaryKey() []string   { return a.ms.PrimaryKey() }
func (a *memorySourceAdapter) NormalizeRow(row ivm.Row) { a.ms.NormalizeRow(row) }
func (a *memorySourceAdapter) Push(sc ivm.SourceChange) []ivm.Change { return a.ms.Push(sc) }
func (a *memorySourceAdapter) Connect(sort ivm.Ordering, filterPredicate func(ivm.Row) bool, splitEditKeys map[string]bool) ivm.Input {
	return a.ms.Connect(sort, filterPredicate, splitEditKeys)
}

// Close is a no-op: MemorySource holds no external resource (no SQLite conn or
// open tx). Present only to satisfy engine.Source so the tablesource leaf's
// Close — which DOES roll back the prev tx and return its writable conn to the
// pool — is callable polymorphically from Engine.Close.
func (a *memorySourceAdapter) Close() error { return nil }

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
//
// Drift is non-nil iff the engine recovered an *ivm.DriftError mid-advance.
// In that case Changes is empty (any partial output is dropped — TS will
// re-init from current SQLite truth so re-applying partial output risks
// double-application) and Timings holds whatever was measured before the
// drift was detected (useful for diagnostics, harmless to ship).
type AdvanceResult struct {
	Changes []RowChange
	Timings []TableTiming
	Drift   *ivm.DriftError
}

// TableTiming reports the wall time spent processing a single source change
// for a table during Engine.Advance.
type TableTiming struct {
	Table   string  `json:"table"`
	ChangeT int     `json:"type"`           // ivm.ChangeType (0=add,1=remove,2=edit)
	Ms      float64 `json:"ms"`             // wall-time millis
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
// output runs the live "scalar value changed" check (port of
// pipeline-driver.ts:2017-2047): if a push moves the resolved scalar's
// child field to a different value, the baked-in literal in the main
// query's plan is stale, so we raise a reset (DriftError) that flows
// through the engine's existing recover→re-hydrate path — TS re-registers
// the query, which re-runs ResolveSimpleScalarSubqueries against current
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
// new pointer atomically. Readers — including the drift-audit's
// RefreshAllSources path which deliberately bypasses the per-CG worker FIFO —
// Load the pointer without taking any lock. This is what makes the bypass
// actually bypass: pre-fix, RefreshAllSources took e.mu and serialized behind
// multi-second AddQueriesStream / Advance holders, so the audit's 30s/120s
// budget couldn't absorb sustained multi-CG load. With COW, refresh proceeds
// concurrently regardless of how long other handlers run.
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
	// (pipeline-driver.ts:3172-3178). Empty/missing for a table means no bump.
	// The bump is a no-op in steady state (minRowVersion unset or rows already
	// at/above it), so it only activates in the rare post-RESET window.
	minRowVersions map[string]string
}

// zeroVersionColumn is the row-version bookkeeping column (TS
// ZERO_VERSION_COLUMN_NAME, replication-state.ts). Present on rows read from
// the replica; compared/bumped against minRowVersion. Not part of the zql
// spec, so it is stripped before client comparison — see the drift-audit
// projection in pipeline-driver.ts.
const zeroVersionColumn = "_0_version"

// SetMinRowVersions installs the per-table minRowVersion map (from
// handleInit). Safe to call before any advance/hydrate; nil clears it.
func (e *Engine) SetMinRowVersions(m map[string]string) {
	e.mu.Lock()
	e.minRowVersions = m
	e.mu.Unlock()
}

// bumpRowVersions applies the TS streamNodes minRowVersion bump
// (pipeline-driver.ts:3172-3178) to a finished RowChange slice: for each
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
	ParallelThreshold int    // min connections for parallel fan-out (default: 4)
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
// this engine. Used by the TS drift audit as a cross-validation signal:
// if TS sees N registered queries but Go reports 0, the per-CG recovery
// machinery has silently dropped Go's pipeline state (the C2 freeze
// condition) — every advance will return empty changes and the client
// view is permanently divergent until reconnect. The audit logs an
// error + triggers resetEngine when this happens.
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

// RefreshAllSources rolls each source's pinned snapshot (if any) to the
// current backing storage state. Sources without pinned snapshots
// (MemorySource) are silently skipped. Used by the drift audit so its
// comparison reads are at the same point in time as the underlying
// replica writer, rather than against a snapshot pinned by the last
// Push (which would show transient set differences during sustained
// writes).
func (e *Engine) RefreshAllSources() {
	// Lock-free read of the sources snapshot — this is the load-bearing part
	// of the refreshSnapshot FIFO-bypass. See the type Engine doc comment for
	// the full rationale. Per-source RefreshSnapshot is independently safe
	// to invoke concurrently with the source's Push (see source.go's
	// "No-op while a Push is in progress" guard).
	for _, src := range e.sourcesView() {
		if r, ok := src.(interface{ RefreshSnapshot() }); ok {
			r.RefreshSnapshot()
		}
	}
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
		nodes := subPipeline.Input.Fetch(ivm.FetchRequest{})
		ce := &companionEntry{
			pipeline:   subPipeline,
			schema:     subSchema,
			childField: childField,
		}
		var value interface{}
		var matched bool
		if len(nodes) > 0 {
			ce.matchedRow = nodes[0].Row
			value = nodes[0].Row[childField]
			matched = true
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
	// before hydrate but companions after (pipeline-driver.ts:1936-2026). They
	// only emit on advance (source.Push), never during the hydrate Fetch, so
	// deferring the wiring keeps companion emissions out of the hydrate window
	// even if the build+hydrate locking is ever loosened for throughput.
	return entry
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
	nodes := entry.pipeline.Input.Fetch(ivm.FetchRequest{})
	for _, node := range nodes {
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

	// Phase 1: Build all pipelines sequentially (mutates shared state).
	// Scalar-subquery resolution runs here too — see buildAndRegisterLocked.
	built := make([]*pipelineEntry, 0, len(queries))
	for _, q := range queries {
		e.removeQueryLocked(q.QueryID)
		built = append(built, e.buildAndRegisterLocked(q.QueryID, q.AST))
	}

	// Phase 2: Hydrate all pipelines in parallel (read-only fetches).
	results := make([]QueryResult, len(built))
	// Snapshot minRowVersions under the held e.mu so the parallel hydrate
	// goroutines read a stable map (K: minRowVersion bump).
	mrv := e.minRowVersions
	// C1: a panic inside a hydrate goroutine (e.g. pkValue on a nil-PK row)
	// cannot be caught by the RPC handler's recover — panics don't cross
	// goroutine boundaries, so an uncaught one aborts the WHOLE multi-CG
	// process. Capture per-goroutine and surface as an error (mirrors the
	// per-goroutine recover in ivm/parallel.go's push fan-out).
	hydratePanics := make([]any, len(built))
	var wg sync.WaitGroup
	wg.Add(len(built))
	for i, entry := range built {
		go func(idx int, entry *pipelineEntry) {
			defer wg.Done()
			defer func() {
				if r := recover(); r != nil {
					hydratePanics[idx] = r
				}
			}()
			start := time.Now()
			hydration := bumpRowVersions(hydrateEntry(entry), mrv)
			timingMs := float64(time.Since(start).Microseconds()) / 1000.0
			// Non-streaming path: result is the single (final) chunk.
			results[idx] = QueryResult{
				QueryID:    entry.queryID,
				Changes:    hydration,
				ChunkIndex: 0,
				Final:      true,
				TimingMs:   timingMs,
			}
		}(i, entry)
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
// duration (consistent with AddQueries' lock discipline so source state
// stays read-only across the hydration window).
func (e *Engine) AddQueriesStream(
	queries []QuerySpec,
	onResult func(QueryResult),
) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	// Bail explicitly on closed engine. Without this, the loop below
	// would iterate a nil pipelines/sources map and silently return
	// success with no callbacks fired — caller sees "0 queries
	// hydrated" with no error.
	if e.closed {
		return ErrEngineClosed
	}

	// Phase 1: Build all pipelines sequentially (mutates shared state).
	// Scalar-subquery resolution + companion wiring happens here too —
	// see buildAndRegisterLocked.
	built := make([]*pipelineEntry, 0, len(queries))
	for _, q := range queries {
		e.removeQueryLocked(q.QueryID)
		built = append(built, e.buildAndRegisterLocked(q.QueryID, q.AST))
	}

	// Phase 2: Hydrate in parallel, stream per-query results in chunks as
	// each query's fetch progresses. Each query may emit multiple chunks
	// (one per hydrateChunkSize RowChanges); the last chunk has Final=true.
	//
	// Chunking is a wire-level optimization: it reduces TS-side peak memory
	// (decode + process + discard per chunk vs decode-whole-frame) and
	// improves time-to-first-byte for large hydrations. Go-side memory is
	// NOT bounded — pipeline.Input.Fetch still returns the full result
	// slice up front; operator-level chunking would be a separate refactor.
	// Snapshot minRowVersions under the held e.mu so the parallel hydrate
	// goroutines read a stable map (K: minRowVersion bump).
	mrv := e.minRowVersions
	// C1: capture per-goroutine panics (see AddQueries) so a nil-PK panic in
	// one query's hydrate becomes a returned error instead of a process abort.
	// The query may have already emitted partial (Final=false) frames; the
	// handler turns the returned error into an rpcError, which rejects the
	// whole addQueriesStream call on the TS side — a clean failure, not a crash.
	hydratePanics := make([]any, len(built))
	var wg sync.WaitGroup
	wg.Add(len(built))
	for bi, b := range built {
		go func(idx int, entry *pipelineEntry) {
			defer wg.Done()
			defer func() {
				if r := recover(); r != nil {
					hydratePanics[idx] = r
				}
			}()
			start := time.Now()
			nodes := entry.pipeline.Input.Fetch(ivm.FetchRequest{})

			var chunk []RowChange
			chunkIndex := 0
			flush := func(final bool) {
				// TimingMs only on the final chunk (TS accumulator uses it
				// for per-query attribution). Avoids leaking incremental
				// fetch-time noise into the histogram.
				var timingMs float64
				if final {
					timingMs = float64(time.Since(start).Microseconds()) / 1000.0
				}
				onResult(QueryResult{
					QueryID:    entry.queryID,
					Changes:    bumpRowVersions(chunk, mrv),
					ChunkIndex: chunkIndex,
					Final:      final,
					TimingMs:   timingMs,
				})
				// T1-5: reuse the backing array (see the matching note in
				// AdvanceStream's flush). onResult encodes Changes
				// synchronously via the sidecar's streamW before returning, so
				// the array is free to reuse. Revert to `chunk = nil` if any
				// caller retains Changes asynchronously.
				chunk = chunk[:0]
				chunkIndex++
			}

			for _, node := range nodes {
				nodeChanges := streamNodes(entry.queryID, entry.schema, RowChangeAdd, node)
				chunk = append(chunk, nodeChanges...)
				if len(chunk) >= hydrateChunkSize {
					flush(false)
				}
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
				if len(chunk) >= hydrateChunkSize {
					flush(false)
				}
			}
			// Always emit a terminal frame with Final=true, even if empty —
			// the TS accumulator uses it as the per-query completion signal.
			// For queries whose total RowChanges hit an exact multiple of
			// hydrateChunkSize, the terminal frame carries zero rows; one
			// extra small frame per such query is the tradeoff for a simple
			// invariant ("every query ends with Final=true").
			flush(true)
		}(bi, b)
	}
	wg.Wait()
	if err := firstHydratePanic(built, hydratePanics); err != nil {
		return err
	}

	// HIGH-11: wire companion outputs after all hydrates complete.
	for _, entry := range built {
		e.wireCompanionOutputsLocked(entry)
	}
	return nil
}

// firstHydratePanic converts the first non-nil per-goroutine hydrate panic into
// an error so the RPC handler can return an error frame instead of letting the
// panic abort the whole sidecar process (C1). A *DriftError is preserved as-is
// (the caller's drift handling expects that type); any other value is wrapped
// with its query ID for diagnosis.
func firstHydratePanic(built []*pipelineEntry, panics []any) error {
	for i, p := range panics {
		if p == nil {
			continue
		}
		if d, ok := p.(*ivm.DriftError); ok {
			return d
		}
		qid := ""
		if i < len(built) && built[i] != nil {
			qid = built[i].queryID
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

// hydrateChunkSize is the max number of RowChanges per partial frame in
// AddQueriesStream. Matches the TS view-syncer's CURSOR_PAGE_SIZE so chunks
// align with the downstream poke-batching boundary (no point chunking
// finer than the consumer batches).
//
// Declared as var (not const) so tests can shrink it to exercise chunk
// boundaries without allocating 10k-row payloads per case.
var hydrateChunkSize = envChunkSize("GO_IVM_HYDRATE_CHUNK_SIZE", 10000)

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
// Drift recovery: if MemorySource's pre-Push validation detects an Edit/Remove
// against a missing row (or duplicate Add), it panics with *ivm.DriftError —
// raised BEFORE any state mutation, so we can recover cleanly. On drift we
// drop the entire advance: clear the streamer (it may hold valid output
// from earlier source.Push calls in this loop, but TS will re-init from
// fresh SQLite truth, so applying those would risk double-application),
// and return a result with Drift set so the sidecar can signal TS.
func (e *Engine) Advance(changes []SnapshotChange) *AdvanceResult {
	e.mu.Lock()
	defer e.mu.Unlock()

	var allRowChanges []RowChange
	var timings []TableTiming
	var drift *ivm.DriftError

	func() {
		defer func() {
			if r := recover(); r != nil {
				if d, ok := r.(*ivm.DriftError); ok {
					drift = d
					// Match TS view-syncer behavior: emit whatever output
					// successful prior Push calls in this loop already
					// produced, then signal drift so the caller re-hydrates.
					// TS's assert-and-throw path emits its partial computation
					// before snapshot revert; previously Go discarded prior
					// output here, which produced observable output divergence
					// between Go and TS for the corrupt advance (see 30-min
					// shadow soak: TS=N changes, Go=0 changes for the failing
					// batch). Drain anything still in the streamer in case the
					// panicking Push emitted partial output before the panic.
					if drained := e.streamer.Stream(); len(drained) > 0 {
						allRowChanges = append(allRowChanges, drained...)
					}
					return
				}
				// HIGH-10: drain the streamer before re-raising a non-drift
				// panic, matching AdvanceStream's recover. Otherwise this
				// aborted advance's accumulated entries survive and the NEXT
				// successful Advance's first streamer.Stream() surfaces them as
				// if produced by that advance — wrong RowChanges to clients.
				_ = e.streamer.Stream()
				panic(r) // re-raise non-drift panics
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
			// #advanceTime histogram (pipeline-driver.ts:1351).
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

	// End-of-batch hook for every registered source — rotates TableSource
	// snapshots + clears their batch-scoped delta so the next batch
	// reads from a frame that reflects this batch's committed mutations.
	// Runs on BOTH the success and drift paths: on drift the engine
	// will be re-inited from SQLite truth anyway, but until that lands
	// we want batchDelta cleared so a follow-up Advance call between
	// drift and re-init starts clean.
	e.signalAdvanceEnd()

	if drift != nil {
		// Partial-emit-then-drift: prior successful pushes' output goes to
		// the caller so clients see it (TS behavior). The drift signal
		// triggers re-hydrate which catches up any missed deltas.
		return &AdvanceResult{Changes: bumpRowVersions(allRowChanges, e.minRowVersions), Timings: timings, Drift: drift}
	}
	return &AdvanceResult{Changes: bumpRowVersions(allRowChanges, e.minRowVersions), Timings: timings}
}

// AdvanceStreamPartial is one frame in the AdvanceStream output. Multiple
// frames may be emitted per advance call; the frame with Final=true is the
// terminal frame. Timings is populated only on the terminal frame so the TS
// caller can record per-(table,op) histogram entries once, atomically with
// the completion signal.
//
// Drift is non-nil only on the Final frame and only when MemorySource
// raised *ivm.DriftError mid-advance. The TS client must discard ALL
// partial frames received for this stream (their data is overlapping with
// what the post-drift re-init will replay) and trigger sidecar re-init.
//
// See Engine.AdvanceStream for the chunking contract.
type AdvanceStreamPartial struct {
	Changes    []RowChange      `json:"changes"`
	ChunkIndex int              `json:"chunkIndex"`
	Final      bool             `json:"final"`
	Timings    []TableTiming    `json:"timings,omitempty"`
	Drift      *ivm.DriftError  `json:"drift,omitempty"`
}

// advanceChunkSize is the max number of RowChanges per partial frame in
// AdvanceStream. Matches hydrateChunkSize and the TS view-syncer's
// CURSOR_PAGE_SIZE so chunks align with the downstream poke-batching
// boundary. Var (not const) for test override.
var advanceChunkSize = envChunkSize("GO_IVM_ADVANCE_CHUNK_SIZE", 10000)

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
// time-to-first-byte for large advance batches. It does NOT bound Go-side
// memory below the chunk size — single source-change outputs above
// advanceChunkSize are still buffered intact (operator-level chunking
// would be a separate refactor).
//
// onResult may be called multiple times from this goroutine before the
// function returns. Engine.mu is held throughout, matching Advance's
// lock discipline.
func (e *Engine) AdvanceStream(
	changes []SnapshotChange,
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

	var pending []RowChange
	var timings []TableTiming
	chunkIndex := 0
	var drift *ivm.DriftError

	flush := func(final bool) {
		var t []TableTiming
		if final {
			t = timings
		}
		onResult(AdvanceStreamPartial{
			Changes:    bumpRowVersions(pending, e.minRowVersions),
			ChunkIndex: chunkIndex,
			Final:      final,
			Timings:    t,
			Drift:      drift, // nil unless this is the terminal drift signal
		})
		// T1-5: reuse the backing array instead of `pending = nil`. SAFE only
		// because onResult consumes `Changes` SYNCHRONOUSLY — the sidecar's
		// streamW → writeFrameLocked → mpMarshal encodes the slice into a
		// separate byte buffer before this call returns (cmd/sidecar/main.go
		// streamW). By here the array is no longer referenced downstream.
		// INVARIANT: if a caller's onResult ever retains Changes past return
		// (async encode/buffer), revert this to `pending = nil`.
		pending = pending[:0]
		chunkIndex++
	}

	// Non-Drift panic capture: pre-fix this re-raised inline (panic(r)
	// in the deferred recover), skipping the terminal flush(true) below
	// and leaving the TS-side accumulator throwing
	// "finished without a final chunk". That cascaded to C5's protocol-
	// violation path which treats the wire as corrupted. By capturing
	// the panic and re-raising AFTER flush(true), TS always sees a
	// clean terminal frame — empty changes signal "advance abandoned"
	// rather than wire protocol corruption.
	var nonDriftPanic any
	func() {
		defer func() {
			if r := recover(); r != nil {
				if d, ok := r.(*ivm.DriftError); ok {
					drift = d
					// Match TS view-syncer: emit whatever partial output
					// successful prior pushes produced in this advance, then
					// signal drift so TS re-hydrates. Previously Go dropped
					// the partial output (`pending = nil`), producing
					// observable output divergence vs TS for the corrupt
					// advance (30-min shadow soak: TS emitted partial changes
					// for the batch, Go emitted none). Drain anything still
					// in the streamer in case the panicking Push left
					// residual output before the panic point.
					if drained := e.streamer.Stream(); len(drained) > 0 {
						pending = append(pending, drained...)
					}
					return
				}
				// Capture non-Drift panic for re-raise after flush.
				nonDriftPanic = r
				// Drop partial output: an unrecovered panic mid-loop
				// means Go's state may have advanced partially; sending
				// only the partial diff to TS would leave the CVR
				// out of sync with Go. Safer to send empty Final and
				// let the caller's restart machinery rebuild from
				// scratch.
				pending = nil
				_ = e.streamer.Stream()
			}
		}()

		// Snapshot sources once; COW + atomic.Pointer guarantees this slice
		// stays consistent for the duration of the advance loop.
		sources := e.sourcesView()
		for _, change := range changes {
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
				timings = append(timings, TableTiming{
					Table:   change.Table,
					ChangeT: int(sc.Type),
					Ms:      ms,
				})

				// Flush mid-batch if we've crossed the chunk threshold. We
				// only check AFTER appending so a single source-change that
				// produces >chunkSize rows still ships in one frame (we
				// don't split an individual source-change's RowChange list
				// — that would require operator-level chunking).
				if len(pending) >= advanceChunkSize {
					flush(false)
				}
			}
		}
	}()

	// End-of-batch hook — rotate TableSource snapshots + clear their
	// batch-scoped delta. See Engine.Advance for the full rationale; same
	// invariant applies to the streaming path. Runs OUTSIDE the
	// recover()-guarded func so it fires on the drift path too.
	e.signalAdvanceEnd()

	// Always emit a terminal Final frame. Carries cumulative timings on
	// success; carries Drift + empty Changes on drift recovery (TS
	// discards the whole stream and re-inits); carries empty Changes
	// on non-Drift panic (caller's restart machinery rebuilds).
	flush(true)

	// Re-raise non-Drift panic AFTER flush so TS sees a clean wire.
	// engine.Advance's caller (sidecar RPC handler) has its own
	// recover that converts panic to an error response — the wire
	// already carries the Final marker so TS doesn't trip the
	// protocol-violation path.
	if nonDriftPanic != nil {
		panic(nonDriftPanic)
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

func (po *pipelineOutput) Push(change ivm.Change, pusher ivm.InputBase) []ivm.Change {
	// Materialize relationships eagerly (like TS Catch.expandNode does during push).
	// This captures the relationship state while the overlay is still active.
	materialized := materializeChange(change)
	po.engine.streamer.Accumulate(po.queryID, po.schema, []ivm.Change{materialized})
	return nil
}

// companionOutput wraps pipelineOutput for a resolved scalar-subquery
// companion pipeline. Before accumulating the companion's change it runs
// the scalar-value-changed reset check — direct port of TS's live
// companion push (pipeline-driver.ts:2017-2047). A push that moves the
// resolved scalar's child field to a different value makes the main
// query's baked-in literal stale, so it raises a *ivm.DriftError to ride
// the engine's existing recover→re-hydrate path: TS re-registers the
// query, re-running ResolveSimpleScalarSubqueries against current truth
// and baking the NEW value. Net behavior matches TS's
// ResetPipelinesSignal('scalar-subquery'). Unchanged-value pushes
// accumulate exactly as the plain pipelineOutput would.
type companionOutput struct {
	pipelineOutput
	childField    string
	resolvedValue ivm.Value
}

func (co *companionOutput) Push(change ivm.Change, pusher ivm.InputBase) []ivm.Change {
	changed := false
	switch change.Type {
	case ivm.ChangeTypeAdd, ivm.ChangeTypeEdit:
		// New scalar value is the child field of the pushed (new) node.
		// TS: newValue = change.node.row[childField] ?? null.
		newValue := change.Node.Row[co.childField]
		changed = !scalarValuesEqual(newValue, co.resolvedValue)
	case ivm.ChangeTypeRemove:
		// TS: newValue = undefined for REMOVE, and scalarValuesEqual(
		// undefined, resolvedValue) is always false (resolvedValue is never
		// undefined) — so removing the scalar's source row always resets.
		changed = true
	case ivm.ChangeTypeChild:
		// TS returns [] for CHILD: a relationship-only change does not move
		// the scalar value — neither accumulate nor reset.
		return nil
	}
	if changed {
		panic(&ivm.DriftError{
			Table:    co.schema.TableName,
			Op:       "ScalarSubquery",
			PK:       map[string]ivm.Value{},
			HasCount: 0,
		})
	}
	return co.pipelineOutput.Push(change, pusher)
}

// scalarValuesEqual ports TS's scalarValuesEqual (pipeline-driver.ts:3278,
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

// materializeChange eagerly evaluates all lazy relationship closures in a change.
func materializeChange(change ivm.Change) ivm.Change {
	result := ivm.Change{
		Type:  change.Type,
		Child: change.Child,
	}
	result.Node = materializeNode(change.Node)
	if change.OldNode != nil {
		old := materializeNode(*change.OldNode)
		result.OldNode = &old
	}
	return result
}

// materializeNode evaluates lazy relationship closures and returns a node with concrete data.
func materializeNode(node ivm.Node) ivm.Node {
	result := ivm.Node{Row: node.Row}
	if node.Relationships != nil {
		result.Relationships = make(map[string]func() []ivm.Node, len(node.Relationships))
		for relName, fn := range node.Relationships {
			// Evaluate the closure NOW (while overlay is active)
			children := fn()
			// Recursively materialize children
			materialized := make([]ivm.Node, len(children))
			for i, child := range children {
				materialized[i] = materializeNode(child)
			}
			// Capture in a non-lazy closure
			captured := materialized
			result.Relationships[relName] = func() []ivm.Node { return captured }
		}
	}
	return result
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
func (d *engineDelegate) GetSource(tableName string) builder.Source {
	source, ok := d.engine.sourcesView()[tableName]
	if !ok {
		return nil
	}
	return &engineSource{source: source}
}

func (d *engineDelegate) CreateStorage(name string) ivm.TakeStorage {
	if d.cgs == nil {
		d.cgs = d.engine.storage.CreateClientGroupStorage(d.queryID)
	}
	return d.cgs.CreateTakeStorage()
}

// rowPKSummary returns a brief string showing the row's primary key values.
func rowPKSummary(row ivm.Row, pk []string) string {
	if row == nil {
		return "<nil>"
	}
	parts := make([]string, len(pk))
	for i, col := range pk {
		parts[i] = fmt.Sprintf("%s=%v", col, row[col])
	}
	return fmt.Sprintf("{%s}", strings.Join(parts, ","))
}

// --- engineSource wraps Source as builder.Source ---

type engineSource struct {
	source Source
}

func (es *engineSource) Connect(opts builder.ConnectOptions) ivm.Input {
	return es.source.Connect(opts.Sort, opts.FilterPredicate, opts.SplitEditKeys)
}

func (es *engineSource) PrimaryKey() []string {
	return es.source.PrimaryKey()
}

func (es *engineSource) NormalizeRow(row ivm.Row) {
	es.source.NormalizeRow(row)
}
