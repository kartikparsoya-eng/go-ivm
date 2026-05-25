package sqlite

// TableSource is the production source backed by a SQLite table.
// Values are written to the backing table AFTER being vended by the source
// (to ensure self-join correctness with overlays).

import (
	"database/sql"
	"fmt"
	"strings"
	"sync"

	"github.com/kartikparsoya-eng/go-ivm/ivm"

	_ "modernc.org/sqlite"
)

// TableSource implements the IVM Source interface backed by SQLite.
type TableSource struct {
	mu          sync.Mutex
	db          *sql.DB
	tableName   string
	columns     map[string]ColumnSchema
	primaryKey  []string
	connections []*TableConnection
	overlay     *ivm.Overlay
	pushEpoch   int

	// Prepared statements
	insertSQL     string
	deleteSQL     string
	updateSQL     string
	checkExistSQL string
}

// TableConnection is a connection from TableSource to a downstream pipeline.
type TableConnection struct {
	Input           ivm.Input
	Output          ivm.Output
	Sort            ivm.Ordering
	SplitEditKeys   map[string]bool
	CompareRows     ivm.Comparator
	FilterPredicate func(ivm.Row) bool
	FilterCondition *Condition
	LastPushedEpoch int
}

// NewTableSource creates a TableSource for the given table.
func NewTableSource(db *sql.DB, tableName string, columns map[string]ColumnSchema, primaryKey []string) *TableSource {
	ts := &TableSource{
		db:         db,
		tableName:  tableName,
		columns:    columns,
		primaryKey: primaryKey,
	}

	// Build prepared SQL strings
	colNames := sortedColumnNames(columns)
	quotedCols := make([]string, len(colNames))
	placeholders := make([]string, len(colNames))
	for i, c := range colNames {
		quotedCols[i] = quoteIdent(c)
		placeholders[i] = "?"
	}

	ts.insertSQL = fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)",
		quoteIdent(tableName),
		strings.Join(quotedCols, ", "),
		strings.Join(placeholders, ", "))

	pkWhere := make([]string, len(primaryKey))
	for i, k := range primaryKey {
		pkWhere[i] = fmt.Sprintf("%s = ?", quoteIdent(k))
	}
	ts.deleteSQL = fmt.Sprintf("DELETE FROM %s WHERE %s",
		quoteIdent(tableName), strings.Join(pkWhere, " AND "))

	ts.checkExistSQL = fmt.Sprintf("SELECT 1 FROM %s WHERE %s LIMIT 1",
		quoteIdent(tableName), strings.Join(pkWhere, " AND "))

	// UPDATE (only if there are non-PK columns)
	nonPKCols := nonPrimaryKeys(columns, primaryKey)
	if len(nonPKCols) > 0 {
		setClauses := make([]string, len(nonPKCols))
		for i, c := range nonPKCols {
			setClauses[i] = fmt.Sprintf("%s = ?", quoteIdent(c))
		}
		ts.updateSQL = fmt.Sprintf("UPDATE %s SET %s WHERE %s",
			quoteIdent(tableName),
			strings.Join(setClauses, ", "),
			strings.Join(pkWhere, " AND "))
	}

	return ts
}

// TableName returns the table name.
func (ts *TableSource) TableName() string { return ts.tableName }

// PrimaryKey returns the primary key columns.
func (ts *TableSource) PrimaryKey() []string { return ts.primaryKey }

// ColumnSchemas returns the column schema map for type normalization.
func (ts *TableSource) ColumnSchemas() map[string]ColumnSchema { return ts.columns }

// NormalizeRow coerces each value in a row to match the column schema types.
// This is needed when rows arrive from JSON (where all numbers are float64 and
// booleans are Go bool) but need to match the types produced by FromSQLiteType.
func (ts *TableSource) NormalizeRow(row ivm.Row) ivm.Row {
	if row == nil {
		return nil
	}
	for col, val := range row {
		colSchema, ok := ts.columns[col]
		if !ok {
			continue
		}
		row[col] = FromSQLiteType(val, colSchema.Type)
	}
	return row
}

// ConnectPipeline creates a connection with a predicate filter (used by engine/builder).
func (ts *TableSource) ConnectPipeline(sort ivm.Ordering, filterPred func(ivm.Row) bool, splitEditKeys map[string]bool) ivm.Input {
	primaryKeySort := make(ivm.Ordering, len(ts.primaryKey))
	for i, k := range ts.primaryKey {
		primaryKeySort[i] = [2]string{k, "asc"}
	}

	var comparator ivm.Comparator
	if sort != nil {
		comparator = ivm.MakeComparator(sort, false)
	} else {
		comparator = ivm.MakeComparator(primaryKeySort, false)
	}

	conn := &TableConnection{
		Sort:            sort,
		SplitEditKeys:   splitEditKeys,
		CompareRows:     comparator,
		FilterPredicate: filterPred,
	}

	schema := &ivm.SourceSchema{
		TableName:     ts.tableName,
		Columns:       ts.columnsAsStringMap(),
		PrimaryKey:    ts.primaryKey,
		Relationships: map[string]*ivm.SourceSchema{},
		IsHidden:      false,
		System:        "client",
		CompareRows:   comparator,
	}
	if sort != nil {
		schema.Sort = sort
	}

	input := &tableSourceInput{
		ts:     ts,
		conn:   conn,
		schema: schema,
	}
	conn.Input = input
	conn.Output = ivm.ThrowOutput

	ts.mu.Lock()
	ts.connections = append(ts.connections, conn)
	ts.mu.Unlock()

	return input
}

// Connect creates a new connection with specified sort and optional filter.
func (ts *TableSource) Connect(sort ivm.Ordering, filter *Condition, splitEditKeys map[string]bool) ivm.Input {
	primaryKeySort := make(ivm.Ordering, len(ts.primaryKey))
	for i, k := range ts.primaryKey {
		primaryKeySort[i] = [2]string{k, "asc"}
	}

	var comparator ivm.Comparator
	if sort != nil {
		comparator = ivm.MakeComparator(sort, false)
	} else {
		comparator = ivm.MakeComparator(primaryKeySort, false)
	}

	conn := &TableConnection{
		Sort:          sort,
		SplitEditKeys: splitEditKeys,
		CompareRows:   comparator,
	}

	if filter != nil {
		conn.FilterCondition = filter
		conn.FilterPredicate = createPredicate(filter)
	}

	schema := &ivm.SourceSchema{
		TableName:     ts.tableName,
		Columns:       ts.columnsAsStringMap(),
		PrimaryKey:    ts.primaryKey,
		Relationships: map[string]*ivm.SourceSchema{},
		IsHidden:      false,
		System:        "client",
		CompareRows:   comparator,
	}
	if sort != nil {
		schema.Sort = sort
	}

	input := &tableSourceInput{
		ts:     ts,
		conn:   conn,
		schema: schema,
	}
	conn.Input = input
	conn.Output = ivm.ThrowOutput

	ts.mu.Lock()
	ts.connections = append(ts.connections, conn)
	ts.mu.Unlock()

	return input
}

// Push pushes a source change through all connections, then writes to SQLite.
func (ts *TableSource) Push(change ivm.SourceChange) []ivm.Change {
	ts.pushEpoch++
	epoch := ts.pushEpoch

	// Debug assertions: validate row existence (matches TS genPush assertions)
	if ts.checkExistSQL != "" {
		switch change.Type {
		case ivm.ChangeTypeAdd:
			if ts.Exists(change.Row) {
				panic(fmt.Sprintf("Row already exists: %v", change.Row))
			}
		case ivm.ChangeTypeRemove:
			if !ts.Exists(change.Row) {
				panic(fmt.Sprintf("Row not found for remove: %v", change.Row))
			}
		case ivm.ChangeTypeEdit:
			if !ts.Exists(change.OldRow) {
				panic(fmt.Sprintf("Row not found for edit: %v", change.OldRow))
			}
		}
	}

	// Set overlay before pushing (connections can read during their push)
	ts.overlay = &ivm.Overlay{Epoch: epoch, Change: change}
	defer func() { ts.overlay = nil }()

	var allChanges []ivm.Change

	for _, conn := range ts.connections {
		if conn.Output == nil {
			continue
		}

		// Build the IVM Change from the SourceChange
		ivmChange := sourceChangeToChange(change)
		if ivmChange == nil {
			continue
		}

		// Apply filter — only push if change passes filter
		if conn.FilterPredicate != nil {
			if !conn.FilterPredicate(change.Row) {
				if change.Type == ivm.ChangeTypeEdit && conn.FilterPredicate(change.OldRow) {
					// Edit where new row doesn't match filter but old did → remove
					removeChange := ivm.MakeRemoveChange(ivm.Node{Row: change.OldRow})
					changes := conn.Output.Push(removeChange, conn.Input)
					allChanges = append(allChanges, changes...)
				}
				continue
			}
			if change.Type == ivm.ChangeTypeEdit && !conn.FilterPredicate(change.OldRow) {
				// Edit where old row didn't match filter but new does → add
				addChange := ivm.MakeAddChange(ivm.Node{Row: change.Row})
				changes := conn.Output.Push(addChange, conn.Input)
				allChanges = append(allChanges, changes...)
				continue
			}
		}

		// Handle split-edit: if edit changes a split-edit key, split into remove+add
		if change.Type == ivm.ChangeTypeEdit && conn.SplitEditKeys != nil {
			if editChangesSplitKeys(change, conn.SplitEditKeys) {
				removeChange := ivm.MakeRemoveChange(ivm.Node{Row: change.OldRow})
				changes := conn.Output.Push(removeChange, conn.Input)
				allChanges = append(allChanges, changes...)

				addChange := ivm.MakeAddChange(ivm.Node{Row: change.Row})
				changes = conn.Output.Push(addChange, conn.Input)
				allChanges = append(allChanges, changes...)
				continue
			}
		}

		changes := conn.Output.Push(*ivmChange, conn.Input)
		allChanges = append(allChanges, changes...)
		conn.LastPushedEpoch = epoch
	}

	// Write to SQLite AFTER pushing (overlay ensures correctness for self-joins)
	ts.writeChange(change)

	return allChanges
}

// Fetch fetches nodes from SQLite with overlay applied.
func (ts *TableSource) Fetch(req ivm.FetchRequest, conn *TableConnection) []ivm.Node {
	query := BuildSelectQuery(
		ts.tableName,
		ts.columns,
		req.Constraint,
		conn.FilterCondition,
		conn.Sort,
		req.Reverse,
		req.Start,
	)

	rows, err := ts.db.Query(query.SQL, query.Params...)
	if err != nil {
		panic(fmt.Sprintf("TableSource.Fetch query failed: %v\nSQL: %s", err, query.SQL))
	}
	defer rows.Close()

	colNames, err := rows.Columns()
	if err != nil {
		panic(fmt.Sprintf("TableSource.Fetch columns failed: %v", err))
	}

	var result []ivm.Node

	for rows.Next() {
		values := make([]interface{}, len(colNames))
		valuePtrs := make([]interface{}, len(colNames))
		for i := range values {
			valuePtrs[i] = &values[i]
		}
		if err := rows.Scan(valuePtrs...); err != nil {
			panic(fmt.Sprintf("TableSource.Fetch scan failed: %v", err))
		}

		row := make(ivm.Row)
		for i, col := range colNames {
			colSchema, ok := ts.columns[col]
			if !ok {
				continue
			}
			row[col] = FromSQLiteType(values[i], colSchema.Type)
		}

		// Apply filter predicate
		if conn.FilterPredicate != nil && !conn.FilterPredicate(row) {
			continue
		}

		result = append(result, ivm.Node{Row: row})
	}

	// Apply overlay
	if ts.overlay != nil && conn.LastPushedEpoch < ts.overlay.Epoch {
		result = applyOverlay(result, ts.overlay.Change, conn.CompareRows, req.Constraint, ts.primaryKey)
	}

	// Apply start cursor (after overlay, since overlay may insert before cursor)
	if req.Start != nil && conn.Sort != nil {
		comparator := ivm.MakeComparator(conn.Sort, req.Reverse)
		result = applyStart(result, req.Start, comparator)
	}

	return result
}

// writeChange writes the change to the backing SQLite table.
func (ts *TableSource) writeChange(change ivm.SourceChange) {
	switch change.Type {
	case ivm.ChangeTypeAdd:
		params := ts.rowToInsertParams(change.Row)
		_, err := ts.db.Exec(ts.insertSQL, params...)
		if err != nil {
			panic(fmt.Sprintf("TableSource INSERT failed: %v", err))
		}

	case ivm.ChangeTypeRemove:
		params := ts.pkParams(change.Row)
		_, err := ts.db.Exec(ts.deleteSQL, params...)
		if err != nil {
			panic(fmt.Sprintf("TableSource DELETE failed: %v", err))
		}

	case ivm.ChangeTypeEdit:
		if ts.updateSQL != "" && canUseUpdate(change.OldRow, change.Row, ts.primaryKey) {
			// Merge old and new
			mergedRow := make(ivm.Row)
			for k, v := range change.OldRow {
				mergedRow[k] = v
			}
			for k, v := range change.Row {
				mergedRow[k] = v
			}
			nonPK := nonPrimaryKeys(ts.columns, ts.primaryKey)
			var params []interface{}
			for _, c := range nonPK {
				colSchema := ts.columns[c]
				params = append(params, ToSQLiteType(mergedRow[c], colSchema.Type))
			}
			params = append(params, ts.pkParams(mergedRow)...)
			_, err := ts.db.Exec(ts.updateSQL, params...)
			if err != nil {
				panic(fmt.Sprintf("TableSource UPDATE failed: %v", err))
			}
		} else {
			// DELETE old + INSERT new
			_, err := ts.db.Exec(ts.deleteSQL, ts.pkParams(change.OldRow)...)
			if err != nil {
				panic(fmt.Sprintf("TableSource DELETE (edit) failed: %v", err))
			}
			_, err = ts.db.Exec(ts.insertSQL, ts.rowToInsertParams(change.Row)...)
			if err != nil {
				panic(fmt.Sprintf("TableSource INSERT (edit) failed: %v", err))
			}
		}
	}
}

// Exists checks whether a row with the given PK exists in the table.
func (ts *TableSource) Exists(row ivm.Row) bool {
	params := ts.pkParams(row)
	var exists int
	err := ts.db.QueryRow(ts.checkExistSQL, params...).Scan(&exists)
	return err == nil
}

// --- tableSourceInput implements ivm.Input ---

type tableSourceInput struct {
	ts     *TableSource
	conn   *TableConnection
	schema *ivm.SourceSchema
}

func (i *tableSourceInput) GetSchema() *ivm.SourceSchema {
	return i.schema
}

func (i *tableSourceInput) SetOutput(output ivm.Output) {
	i.conn.Output = output
}

func (i *tableSourceInput) Fetch(req ivm.FetchRequest) []ivm.Node {
	return i.ts.Fetch(req, i.conn)
}

func (i *tableSourceInput) Destroy() {
	i.ts.mu.Lock()
	defer i.ts.mu.Unlock()
	for idx, c := range i.ts.connections {
		if c == i.conn {
			i.ts.connections = append(i.ts.connections[:idx], i.ts.connections[idx+1:]...)
			break
		}
	}
}

// --- Helpers ---

func (ts *TableSource) columnsAsStringMap() map[string]string {
	m := make(map[string]string, len(ts.columns))
	for k, v := range ts.columns {
		m[k] = v.Type
	}
	return m
}

func (ts *TableSource) rowToInsertParams(row ivm.Row) []interface{} {
	colNames := sortedColumnNames(ts.columns)
	params := make([]interface{}, len(colNames))
	for i, c := range colNames {
		params[i] = ToSQLiteType(row[c], ts.columns[c].Type)
	}
	return params
}

func (ts *TableSource) pkParams(row ivm.Row) []interface{} {
	params := make([]interface{}, len(ts.primaryKey))
	for i, k := range ts.primaryKey {
		params[i] = ToSQLiteType(row[k], ts.columns[k].Type)
	}
	return params
}

func nonPrimaryKeys(columns map[string]ColumnSchema, primaryKey []string) []string {
	pkSet := make(map[string]bool)
	for _, k := range primaryKey {
		pkSet[k] = true
	}
	var result []string
	for k := range columns {
		if !pkSet[k] {
			result = append(result, k)
		}
	}
	sortStrings(result)
	return result
}

func canUseUpdate(oldRow, newRow ivm.Row, primaryKey []string) bool {
	for _, pk := range primaryKey {
		if ivm.CompareValues(oldRow[pk], newRow[pk]) != 0 {
			return false
		}
	}
	return true
}

func editChangesSplitKeys(change ivm.SourceChange, splitKeys map[string]bool) bool {
	for k := range splitKeys {
		if ivm.CompareValues(change.Row[k], change.OldRow[k]) != 0 {
			return true
		}
	}
	return false
}

func sourceChangeToChange(sc ivm.SourceChange) *ivm.Change {
	switch sc.Type {
	case ivm.ChangeTypeAdd:
		c := ivm.MakeAddChange(ivm.Node{Row: sc.Row})
		return &c
	case ivm.ChangeTypeRemove:
		c := ivm.MakeRemoveChange(ivm.Node{Row: sc.Row})
		return &c
	case ivm.ChangeTypeEdit:
		c := ivm.MakeEditChange(ivm.Node{Row: sc.Row}, ivm.Node{Row: sc.OldRow})
		return &c
	}
	return nil
}

// applyOverlay applies the overlay change to fetched results.
func applyOverlay(nodes []ivm.Node, change ivm.SourceChange, comparator ivm.Comparator, constraint *ivm.Constraint, primaryKey []string) []ivm.Node {
	switch change.Type {
	case ivm.ChangeTypeAdd:
		if constraint != nil && !constraintMatchesRow(*constraint, change.Row) {
			return nodes
		}
		// Insert in sorted position
		return insertSorted(nodes, ivm.Node{Row: change.Row}, comparator)

	case ivm.ChangeTypeRemove:
		// Remove by PK match
		return removeByPK(nodes, change.Row, primaryKey)

	case ivm.ChangeTypeEdit:
		if constraint != nil && !constraintMatchesRow(*constraint, change.OldRow) {
			// Old row wasn't in this result set
			if constraintMatchesRow(*constraint, change.Row) {
				return insertSorted(nodes, ivm.Node{Row: change.Row}, comparator)
			}
			return nodes
		}
		// Remove old, insert new
		nodes = removeByPK(nodes, change.OldRow, primaryKey)
		if constraint == nil || constraintMatchesRow(*constraint, change.Row) {
			nodes = insertSorted(nodes, ivm.Node{Row: change.Row}, comparator)
		}
		return nodes
	}
	return nodes
}

func constraintMatchesRow(constraint ivm.Constraint, row ivm.Row) bool {
	for k, v := range constraint {
		if ivm.CompareValues(row[k], v) != 0 {
			return false
		}
	}
	return true
}

func insertSorted(nodes []ivm.Node, node ivm.Node, comparator ivm.Comparator) []ivm.Node {
	// Binary search for insert position
	lo, hi := 0, len(nodes)
	for lo < hi {
		mid := (lo + hi) / 2
		if comparator(node.Row, nodes[mid].Row) > 0 {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	// Insert at lo
	nodes = append(nodes, ivm.Node{})
	copy(nodes[lo+1:], nodes[lo:])
	nodes[lo] = node
	return nodes
}

func removeByPK(nodes []ivm.Node, row ivm.Row, primaryKey []string) []ivm.Node {
	for i, n := range nodes {
		match := true
		for _, pk := range primaryKey {
			if ivm.CompareValues(n.Row[pk], row[pk]) != 0 {
				match = false
				break
			}
		}
		if match {
			return append(nodes[:i], nodes[i+1:]...)
		}
	}
	return nodes
}

func applyStart(nodes []ivm.Node, start *ivm.Start, comparator ivm.Comparator) []ivm.Node {
	if start == nil {
		return nodes
	}
	startIdx := 0
	for i, n := range nodes {
		cmp := comparator(n.Row, start.Row)
		if start.Basis == "at" {
			if cmp >= 0 {
				startIdx = i
				break
			}
		} else { // "after"
			if cmp > 0 {
				startIdx = i
				break
			}
		}
		startIdx = i + 1
	}
	if startIdx >= len(nodes) {
		return nil
	}
	return nodes[startIdx:]
}

// createPredicate builds a filter function from a Condition.
func createPredicate(cond *Condition) func(ivm.Row) bool {
	if cond == nil {
		return nil
	}
	switch cond.Type {
	case "simple":
		return createSimplePredicate(cond)
	case "and":
		preds := make([]func(ivm.Row) bool, len(cond.Conditions))
		for i, c := range cond.Conditions {
			preds[i] = createPredicate(c)
		}
		return func(row ivm.Row) bool {
			for _, p := range preds {
				if !p(row) {
					return false
				}
			}
			return true
		}
	case "or":
		preds := make([]func(ivm.Row) bool, len(cond.Conditions))
		for i, c := range cond.Conditions {
			preds[i] = createPredicate(c)
		}
		return func(row ivm.Row) bool {
			for _, p := range preds {
				if p(row) {
					return true
				}
			}
			return false
		}
	}
	return func(ivm.Row) bool { return true }
}

func createSimplePredicate(cond *Condition) func(ivm.Row) bool {
	return func(row ivm.Row) bool {
		left := resolveValuePos(cond.Left, row)
		right := resolveValuePos(cond.Right, row)
		return evalOp(cond.Op, left, right)
	}
}

func resolveValuePos(vp ValuePos, row ivm.Row) ivm.Value {
	switch vp.Type {
	case "column":
		return row[vp.Name]
	case "literal":
		return vp.Value
	}
	return vp.Value
}

// evalOp mirrors filter.ts evaluator semantics including SQL three-valued
// logic around NULL. `=` and `!=` short-circuit on null operands (no row
// matches null via plain equality — that's what IS/IS NOT are for).
// REVIEW-final MED-PORT-2.
func evalOp(op string, left, right ivm.Value) bool {
	switch op {
	case "IS":
		return ivm.CompareValues(left, right) == 0
	case "IS NOT":
		return ivm.CompareValues(left, right) != 0
	}
	// For everything else, null on either side short-circuits to false.
	if left == nil || right == nil {
		return false
	}
	cmp := ivm.CompareValues(left, right)
	switch op {
	case "=":
		return cmp == 0
	case "!=":
		return cmp != 0
	case ">":
		return cmp > 0
	case "<":
		return cmp < 0
	case ">=":
		return cmp >= 0
	case "<=":
		return cmp <= 0
	}
	return false
}
