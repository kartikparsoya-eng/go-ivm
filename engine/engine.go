package engine

// Per-clientGroup IVM engine: owns sources and pipelines, hydrates queries
// from current state, and drives advance diffs through the operator trees.
// Hydrations run in parallel goroutines; advances are serialized on e.mu so
// pushes don't race with concurrent fetches.

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/kartikparsoya-eng/go-ivm/builder"
	"github.com/kartikparsoya-eng/go-ivm/ivm"
	"github.com/kartikparsoya-eng/go-ivm/sqlite"
)

// ErrEngineClosed is returned by Engine methods invoked after Close().
// Streaming methods (AddQueriesStream, AdvanceStream) check this so a
// late-arriving RPC doesn't silently produce an empty result against a
// nil pipelines/sources map.
var ErrEngineClosed = errors.New("engine is closed")

// Source is the interface that engine sources must implement.
type Source interface {
	TableName() string
	PrimaryKey() []string
	NormalizeRow(ivm.Row)
	Push(ivm.SourceChange) []ivm.Change
	Connect(sort ivm.Ordering, filterPredicate func(ivm.Row) bool, splitEditKeys map[string]bool) ivm.Input
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
}

// companionEntry is the Go-side companion record. The TS analogue is
// CompanionPipeline in pipeline-driver.ts; this version omits the live
// "scalar value changed" reset because almost all real workloads never
// change a scalar's child field — they only ADD/REMOVE rows that flip
// existence. If a value-change case becomes load-bearing, we can add a
// reset signal here that piggybacks on the engine's existing reset path.
type companionEntry struct {
	pipeline   *builder.Pipeline
	schema     *ivm.SourceSchema
	matchedRow ivm.Row // captured by the executor at resolve time; nil = no match
	childField string
}

// Engine is the IVM engine that manages sources, pipelines, and the advance loop.
type Engine struct {
	mu        sync.Mutex
	sources   map[string]Source         // table name → source
	pipelines map[string]*pipelineEntry // query ID → pipeline
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

	return &Engine{
		sources:           make(map[string]Source),
		pipelines:         make(map[string]*pipelineEntry),
		streamer:          NewStreamer(),
		storage:           storage,
		tableUniqueKeys:   make(map[string][][]string),
		parallelThreshold: threshold,
	}, nil
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
	e.sources = nil
	e.tableUniqueKeys = nil
	if e.storage == nil {
		return nil
	}
	err := e.storage.Close()
	e.storage = nil
	return err
}

// RegisterSource registers a Source for a given table.
func (e *Engine) RegisterSource(source Source) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.sources[source.TableName()] = source
}

// RegisterMemorySource registers a MemorySource directly.
// Enables parallel fan-out if the engine's parallelThreshold > 0.
func (e *Engine) RegisterMemorySource(ms *ivm.MemorySource) {
	if e.parallelThreshold > 0 {
		ms.SetParallel(true, e.parallelThreshold)
	}
	e.RegisterSource(&memorySourceAdapter{ms: ms})
}

// GetMemorySource returns the registered MemorySource for tableName, or nil
// if either no source is registered for the table or the registered source is
// not a MemorySource. Used by the sidecar's loadRows handler to append rows
// to an existing source after init.
func (e *Engine) GetMemorySource(tableName string) *ivm.MemorySource {
	e.mu.Lock()
	defer e.mu.Unlock()
	s, ok := e.sources[tableName]
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
	hydration := hydrateEntry(entry)
	timingMs := float64(time.Since(start).Microseconds()) / 1000.0

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
	}
	e.pipelines[queryID] = entry

	mainPipeline.Input.SetOutput(&pipelineOutput{
		engine:  e,
		queryID: queryID,
		schema:  schema,
	})

	// Wire each companion's output to the streamer tagged with the MAIN
	// queryID — when source.Push fans out and the companion's pipeline
	// produces a Change, it lands in the same accumulated batch the main
	// query's changes do and ships to the client as a peer row.
	for _, ce := range companions {
		ce.pipeline.Input.SetOutput(&pipelineOutput{
			engine:  e,
			queryID: queryID,
			schema:  ce.schema,
		})
	}

	return entry
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
	var wg sync.WaitGroup
	wg.Add(len(built))
	for i, entry := range built {
		go func(idx int, entry *pipelineEntry) {
			defer wg.Done()
			start := time.Now()
			hydration := hydrateEntry(entry)
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
	var wg sync.WaitGroup
	wg.Add(len(built))
	for _, b := range built {
		go func(entry *pipelineEntry) {
			defer wg.Done()
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
					Changes:    chunk,
					ChunkIndex: chunkIndex,
					Final:      final,
					TimingMs:   timingMs,
				})
				chunk = nil
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
		}(b)
	}
	wg.Wait()
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
var hydrateChunkSize = 10000

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
	delete(e.pipelines, queryID)
}

// Advance processes a batch of snapshot changes through all affected sources.
// Returns flat RowChanges representing the effect on all registered pipelines.
func (e *Engine) Advance(changes []SnapshotChange) *AdvanceResult {
	e.mu.Lock()
	defer e.mu.Unlock()

	var allRowChanges []RowChange
	var timings []TableTiming

	// Group changes by table for potential cross-table parallelism (future)
	for _, change := range changes {
		source, ok := e.sources[change.Table]
		if !ok {
			continue // no pipelines read this table
		}

		// Determine source changes from the snapshot change
		sourceChanges := snapshotToSourceChanges(change, source)

		// Push each source change and collect streamer output. Time each one
		// individually so TS can attribute wall time to the responsible
		// (table, op) pair — matches the granularity of TS's #advanceTime
		// histogram (pipeline-driver.ts:1351).
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

	return &AdvanceResult{Changes: allRowChanges, Timings: timings}
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
// boundary. Var (not const) for test override.
var advanceChunkSize = 10000

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

	flush := func(final bool) {
		var t []TableTiming
		if final {
			t = timings
		}
		onResult(AdvanceStreamPartial{
			Changes:    pending,
			ChunkIndex: chunkIndex,
			Final:      final,
			Timings:    t,
		})
		pending = nil
		chunkIndex++
	}

	for _, change := range changes {
		source, ok := e.sources[change.Table]
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
			// produces >chunkSize rows still ships in one frame (we don't
			// split an individual source-change's RowChange list — that
			// would require operator-level chunking).
			if len(pending) >= advanceChunkSize {
				flush(false)
			}
		}
	}

	// Always emit a terminal frame with Final=true and the cumulative
	// timings, even when no changes accumulated (empty advance) or the
	// final source change exactly hit the chunk threshold.
	flush(true)
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

func (d *engineDelegate) GetSource(tableName string) builder.Source {
	source, ok := d.engine.sources[tableName]
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
