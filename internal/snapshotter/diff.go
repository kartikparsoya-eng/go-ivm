package snapshotter

import (
	"encoding/json"
	"fmt"

	"github.com/kartikparsoya-eng/go-ivm/ivm"
	"github.com/kartikparsoya-eng/go-ivm/sqlite"
)

// Change-log ops (change-log.ts:43-46). Exact bytes.
const (
	opSet      = "s" // set (insert/update)
	opDel      = "d" // delete
	opTruncate = "t" // table truncate
	opReset    = "r" // table reset (schema change)
)

// Change is the diff output unit. Mirrors snapshotter.ts Change (209-220) and
// is shape-compatible with engine.SnapshotChange (PrevValues/NextValue) plus
// the rowKey TS also carries.
//
//   - PrevValues: rows to REMOVE — the old value on a delete, or every row that
//     unique-conflicts with NextValue on a set.
//   - NextValue:  the new row, or nil for a delete.
//   - RowKey:     the primary/row key of the change.
type Change struct {
	Table      string
	PrevValues []ivm.Row
	NextValue  ivm.Row // nil for delete
	RowKey     ivm.Row
}

// Reset reasons (snapshotter.ts ResetPipelinesReason). The scalar-subquery and
// advancement-timeout reasons live in the engine, not the snapshot diff.
const (
	ReasonSchemaChange      = "schema-change"
	ReasonTruncation        = "truncation"
	ReasonPermissionsChange = "permissions-change"
)

// ResetSignal aborts diff iteration and tells the caller to re-hydrate all
// pipelines at curr. Mirrors ResetPipelinesSignal (265-273).
type ResetSignal struct {
	Reason string
	Msg    string
}

func (e *ResetSignal) Error() string { return e.Msg }

// IsReset reports whether err is a *ResetSignal, optionally extracting it.
func IsReset(err error) (*ResetSignal, bool) {
	r, ok := err.(*ResetSignal)
	return r, ok
}

// InvalidDiffError is raised when a diff is consumed after its snapshots have
// advanced. Mirrors InvalidDiffError (586-590).
type InvalidDiffError struct{ Msg string }

func (e *InvalidDiffError) Error() string { return e.Msg }

// Diff is the lazy difference between two snapshots. Mirrors class Diff (398).
type Diff struct {
	permissionsTable string
	syncable         map[string]*TableSpec
	allNames         map[string]bool
	prev             *Snapshot
	curr             *Snapshot

	// Changes is the number of change-log entries between the snapshots (not
	// necessarily the number of emitted Changes — a TRUNCATE is one entry).
	Changes int
}

func newDiff(
	appID string,
	syncable map[string]*TableSpec,
	allNames map[string]bool,
	prev, curr *Snapshot,
) (*Diff, error) {
	n, err := curr.NumChangesSince(prev.version)
	if err != nil {
		return nil, err
	}
	return &Diff{
		permissionsTable: appID + ".permissions",
		syncable:         syncable,
		allNames:         allNames,
		prev:             prev,
		curr:             curr,
		Changes:          n,
	}, nil
}

// Prev returns the previous snapshot (its version is Prev().Version()).
func (d *Diff) Prev() *Snapshot { return d.prev }

// Curr returns the current snapshot.
func (d *Diff) Curr() *Snapshot { return d.curr }

// Each iterates the diff, invoking emit for every emitted Change in
// (stateVersion ASC, pos ASC) order. It returns:
//   - *ResetSignal      on a reset/truncate/permissions-change op,
//   - *InvalidDiffError on a stale-diff version check,
//   - a plain error     on an unknown table / missing value / SQL failure,
//   - whatever emit returns (non-nil stops iteration).
//
// Mirrors the Diff[Symbol.iterator] body (421-554) step for step.
func (d *Diff) Each(emit func(Change) error) error {
	entries, err := d.curr.ChangesSince(d.prev.version)
	if err != nil {
		return err
	}

	for _, e := range entries {
		// (1) RESET → abort & rehydrate (schema change). (2) TRUNCATE → abort
		// & rehydrate. Table-wide ops sort first (pos=-1) so this happens as
		// early as possible. (snapshotter.ts:444-457)
		switch e.op {
		case opReset:
			return &ResetSignal{
				Reason: ReasonSchemaChange,
				Msg:    fmt.Sprintf("schema for table %s has changed", e.table),
			}
		case opTruncate:
			return &ResetSignal{
				Reason: ReasonTruncation,
				Msg:    fmt.Sprintf("table %s has been truncated", e.table),
			}
		}

		// (3) Non-syncable: skip if known, error if truly unknown. (459-463)
		spec := d.syncable[e.table]
		if spec == nil {
			if d.allNames[e.table] {
				continue
			}
			return fmt.Errorf("change for unknown table %s", e.table)
		}

		// (4) Catch-up invariant: every change-log op has stateVersion strictly
		// greater than the table's minRowVersion (a minRowVersion set is always
		// followed by a RESET, so incremental traversal is at a later version).
		// (474-479)
		if !(d.lessThan(spec.MinRowVersion, e.stateVersion)) {
			return fmt.Errorf(
				"unexpected change @%s for table %s with minRowVersion %q: %s(%s)",
				e.stateVersion, e.table, spec.MinRowVersion, e.op, e.rowKey)
		}

		// (rowKey must be present for row changes — t/r already returned.) (481)
		rowKey, err := parseRowKey(e.rowKey)
		if err != nil {
			return err
		}

		// (5) nextValue: the new contents for a set, null for a delete. (482-483)
		var nextRaw map[string]any
		nextFound := false
		if e.op == opSet {
			nextRaw, nextFound, err = d.curr.GetRow(spec, rowKey)
			if err != nil {
				return err
			}
		}

		// (6) prevValues: unique-conflicts on a set, or the old row on a
		// delete. (485-494)
		var prevValues []map[string]any
		if nextFound {
			prevValues, err = d.prev.GetRows(spec, spec.UniqueKeys, nextRaw)
			if err != nil {
				return err
			}
		} else {
			pv, ok, err := d.prev.GetRow(spec, rowKey)
			if err != nil {
				return err
			}
			if ok {
				prevValues = []map[string]any{pv}
			}
		}

		// (7) A set whose row is missing in curr is a hard inconsistency. (495-499)
		if e.op == opSet && !nextFound {
			return fmt.Errorf("missing value for %s %s", e.table, e.rowKey)
		}

		// (8) Stale-diff detection. (501, 556-583)
		if err := d.checkValid(e.stateVersion, e.op, prevValues, nextRaw); err != nil {
			return err
		}

		// (9) No-op filter: delete of a row absent in prev. (503-507)
		if len(prevValues) == 0 && !nextFound {
			continue
		}

		// (10) Permissions change → abort & rehydrate. (509-524)
		if e.table == d.permissionsTable && nextFound {
			for _, pv := range prevValues {
				if !rawScalarEqual(pv["permissions"], nextRaw["permissions"]) {
					return &ResetSignal{
						Reason: ReasonPermissionsChange,
						Msg: fmt.Sprintf("Permissions have changed %v => %v",
							rawScalar(pv["hash"]), rawScalar(nextRaw["hash"])),
					}
				}
			}
		}

		// (11) Emit — coerce raw SQLite values to ZQL values here (the diff
		// checks above ran on raw values, matching TS). (529-540)
		change := Change{
			Table:      e.table,
			PrevValues: coerceRows(prevValues, spec),
			RowKey:     jsonRow(rowKey),
		}
		if nextFound {
			change.NextValue = coerceRow(nextRaw, spec)
		}
		if err := emit(change); err != nil {
			return err
		}
	}
	return nil
}

// Collect drains the whole diff into a slice. Convenience over Each for callers
// (and tests) that want the full ordered Change sequence; returns the same
// errors Each does.
func (d *Diff) Collect() ([]Change, error) {
	var out []Change
	err := d.Each(func(c Change) error {
		out = append(out, c)
		return nil
	})
	return out, err
}

// checkValid replicates checkThatDiffIsValid (556-583) — all three version
// checks that detect a diff consumed after its snapshots advanced.
func (d *Diff) checkValid(stateVersion, op string, prevValues []map[string]any, nextRaw map[string]any) error {
	if stateVersion > d.curr.version {
		return &InvalidDiffError{Msg: fmt.Sprintf(
			"Diff is no longer valid. curr db has advanced past %s", d.curr.version)}
	}
	for _, pv := range prevValues {
		// TS: (prevValue[ROW_VERSION] ?? '~') — a missing version sorts high so
		// it trips the check (defensive; _0_version is always present in practice).
		ver := "~"
		if raw, ok := pv[zeroVersionColumn]; ok && raw != nil {
			ver = versionOf(raw)
		}
		if ver > d.prev.version {
			return &InvalidDiffError{Msg: fmt.Sprintf(
				"Diff is no longer valid. prev db has advanced past %s.", d.prev.version)}
		}
	}
	if op == opSet {
		ver := versionOf(nextRaw[zeroVersionColumn])
		if ver != stateVersion {
			return &InvalidDiffError{Msg: "Diff is no longer valid. curr db has advanced."}
		}
	}
	return nil
}

// lessThan compares two LexiVersions. They are lexicographically sortable, so
// Go string comparison matches TS's `<` exactly. "" < any non-empty version.
func (d *Diff) lessThan(a, b string) bool { return a < b }

// parseRowKey parses the change-log's JSON rowKey text into a map.
func parseRowKey(s string) (map[string]any, error) {
	var m map[string]any
	if err := json.Unmarshal([]byte(s), &m); err != nil {
		return nil, fmt.Errorf("snapshotter: parse rowKey %q: %w", s, err)
	}
	return m, nil
}

// coerceRow converts a raw SQLite row to a ZQL ivm.Row via FromSQLiteType,
// per the column types in spec. Mirrors fromSQLiteTypes(zqlSpec, row, table).
func coerceRow(raw map[string]any, spec *TableSpec) ivm.Row {
	out := make(ivm.Row, len(raw))
	for c, val := range raw {
		t := "string"
		if cs, ok := spec.Columns[c]; ok {
			t = cs.Type
		}
		out[c] = sqlite.FromSQLiteType(val, t)
	}
	return out
}

func coerceRows(raws []map[string]any, spec *TableSpec) []ivm.Row {
	if len(raws) == 0 {
		return nil
	}
	out := make([]ivm.Row, len(raws))
	for i, r := range raws {
		out[i] = coerceRow(r, spec)
	}
	return out
}

// jsonRow shallow-copies the parsed rowKey into an ivm.Row. The rowKey carries
// raw JSON-decoded scalars (TS emits the rowKey as-is, not fromSQLiteTypes-d).
func jsonRow(m map[string]any) ivm.Row {
	out := make(ivm.Row, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}

// versionOf renders a raw `_0_version` (TEXT) value as its plain LexiVersion
// string, tolerant of the string/[]byte split the driver may use for TEXT.
// Unlike rawScalar this adds no type prefix — the result is compared directly
// against a stateVersion string.
func versionOf(v any) string {
	switch x := v.(type) {
	case nil:
		return ""
	case string:
		return x
	case []byte:
		return string(x)
	default:
		return fmt.Sprintf("%v", v)
	}
}

// rawScalarEqual compares two raw SQLite scalar values for equality, tolerant
// of the string/[]byte split the driver may use for TEXT columns.
func rawScalarEqual(a, b any) bool { return rawScalar(a) == rawScalar(b) }

// rawScalar renders a raw SQLite scalar to a comparable string key. TEXT may
// arrive as string or []byte; both fold to the same key so equal text compares
// equal regardless of representation.
func rawScalar(v any) string {
	switch x := v.(type) {
	case nil:
		return "\x00null"
	case []byte:
		return "s:" + string(x)
	case string:
		return "s:" + x
	default:
		return fmt.Sprintf("%T:%v", v, v)
	}
}
