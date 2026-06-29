package testharness

import (
	"encoding/json"
	"fmt"
	"math/rand"

	"github.com/kartikparsoya-eng/go-ivm/builder"
	"github.com/kartikparsoya-eng/go-ivm/ivm"
)

// Fuzzer generates random valid test cases for differential testing.
type Fuzzer struct {
	rng *rand.Rand
}

func NewFuzzer(seed int64) *Fuzzer {
	return &Fuzzer{rng: rand.New(rand.NewSource(seed))}
}

// GenerateTestCase creates a random test case with configurable complexity.
func (f *Fuzzer) GenerateTestCase(opts FuzzOpts) []byte {
	tc := f.generateTestCaseStruct(opts)
	data, _ := json.MarshalIndent(tc, "", "  ")
	return data
}

type FuzzOpts struct {
	NumTables     int // 1-3
	NumColumns    int // 2-6 per table
	NumRows       int // 1-20 initial rows per table
	NumMutations  int // 1-30 mutations
	WithFilter    bool
	WithLimit     bool
	WithJoins     bool // generate Related subqueries (joins)
	WithExists    bool // generate EXISTS/NOT EXISTS conditions
	WithCompound  bool // generate AND/OR compound conditions
	WithDescSort  bool // use DESC ordering on some columns
	WithNested    bool // generate deeply nested AND/OR (3+ levels)
	WithMultiSort bool // order by 2-3 columns
	WithINList    bool // generate OR on same column (simulates IN)
	WithStart     bool // generate cursor/bound pagination
	WithNulls     bool // include null values in data
}

func DefaultFuzzOpts() FuzzOpts {
	return FuzzOpts{
		NumTables:    1,
		NumColumns:   3,
		NumRows:      5,
		NumMutations: 10,
		WithFilter:   false,
		WithLimit:    false,
		WithJoins:    false,
		WithExists:   false,
		WithCompound: false,
	}
}

func (f *Fuzzer) generateTestCaseStruct(opts FuzzOpts) TestCase {
	if opts.NumTables < 1 {
		opts.NumTables = 1
	}
	if opts.NumColumns < 2 {
		opts.NumColumns = 2
	}

	// Generate schema
	tables := make([]tableInfo, opts.NumTables)
	schema := make(map[string]TableSchema)
	for i := range tables {
		tables[i] = f.generateTable(i, opts.NumColumns)
		schema[tables[i].name] = tables[i].schema
	}

	// Generate initial data
	initialData := make(map[string][]ivm.Row)
	for _, tbl := range tables {
		rows := make([]ivm.Row, opts.NumRows)
		for j := range rows {
			if opts.WithNulls {
				rows[j] = f.generateRowWithNulls(tbl, j)
			} else {
				rows[j] = f.generateRow(tbl, j)
			}
		}
		initialData[tbl.name] = rows
	}

	// Pick query table (first table)
	queryTable := tables[0]

	// Generate AST
	ast := f.generateASTForTable(queryTable, tables, opts)

	// Generate mutations
	mutations := f.generateMutations(tables, initialData, opts.NumMutations)

	return TestCase{
		Schema:      schema,
		InitialData: initialData,
		AST:         ast,
		Mutations:   mutations,
	}
}

type tableInfo struct {
	name    string
	schema  TableSchema
	columns []colInfo
}

type colInfo struct {
	name  string
	ctype string // "string", "number", "boolean"
}

var tableNames = []string{"users", "items", "orders", "tasks", "events"}
var colNamePrefixes = []string{"name", "title", "status", "score", "count", "label", "value", "amount", "flag", "tag"}

func (f *Fuzzer) generateTable(idx int, numCols int) tableInfo {
	name := tableNames[idx%len(tableNames)]

	columns := make([]colInfo, 0, numCols+1)
	// Always have an "id" PK column (string)
	columns = append(columns, colInfo{name: "id", ctype: "string"})

	for i := 0; i < numCols-1; i++ {
		colName := colNamePrefixes[i%len(colNamePrefixes)]
		if i >= len(colNamePrefixes) {
			colName = fmt.Sprintf("col%d", i)
		}
		ctype := f.randomType()
		columns = append(columns, colInfo{name: colName, ctype: ctype})
	}

	colDefs := make(map[string]ColumnDef)
	for _, col := range columns {
		colDefs[col.name] = ColumnDef{Type: col.ctype}
	}

	return tableInfo{
		name: name,
		schema: TableSchema{
			Columns:    colDefs,
			PrimaryKey: []string{"id"},
		},
		columns: columns,
	}
}

func (f *Fuzzer) randomType() string {
	types := []string{"string", "number", "boolean"}
	return types[f.rng.Intn(len(types))]
}

func (f *Fuzzer) generateRow(tbl tableInfo, idx int) ivm.Row {
	row := make(ivm.Row)
	for _, col := range tbl.columns {
		if col.name == "id" {
			row["id"] = fmt.Sprintf("%d", idx+1)
		} else {
			row[col.name] = f.randomValue(col.ctype)
		}
	}
	return row
}

func (f *Fuzzer) generateRowWithNulls(tbl tableInfo, idx int) ivm.Row {
	row := make(ivm.Row)
	for _, col := range tbl.columns {
		if col.name == "id" {
			row["id"] = fmt.Sprintf("%d", idx+1)
		} else if f.rng.Intn(5) == 0 { // 20% chance of null
			row[col.name] = nil
		} else {
			row[col.name] = f.randomValue(col.ctype)
		}
	}
	return row
}

func (f *Fuzzer) randomValue(ctype string) interface{} {
	switch ctype {
	case "string":
		words := []string{"alpha", "beta", "gamma", "delta", "epsilon", "zeta", "eta", "theta", "iota", "kappa"}
		return words[f.rng.Intn(len(words))]
	case "number":
		return float64(f.rng.Intn(100))
	case "boolean":
		return f.rng.Intn(2) == 1
	default:
		return nil
	}
}

func (f *Fuzzer) generateAST(tbl tableInfo, opts FuzzOpts) builder.AST {
	return f.generateASTForTable(tbl, nil, opts)
}

func (f *Fuzzer) generateASTForTable(tbl tableInfo, allTables []tableInfo, opts FuzzOpts) builder.AST {
	// Generate ordering
	ordering := f.generateOrdering(tbl, opts)

	ast := builder.AST{
		Table:   tbl.name,
		OrderBy: ordering,
	}

	if opts.WithFilter || opts.WithCompound || opts.WithNested || opts.WithINList {
		if opts.WithNested {
			ast.Where = f.generateNestedCondition(tbl, 2)
		} else if opts.WithINList {
			ast.Where = f.generateINListCondition(tbl)
		} else if opts.WithCompound {
			ast.Where = f.generateCompoundCondition(tbl)
		} else {
			filterCol := f.pickFilterableColumn(tbl)
			if filterCol != nil {
				ast.Where = f.generateCondition(*filterCol)
			}
		}
	}

	if opts.WithLimit {
		limit := f.rng.Intn(5) + 1
		ast.Limit = &limit
	}

	if opts.WithStart && ast.Limit != nil {
		// Generate a cursor bound for pagination
		ast.Start = f.generateBound(tbl, ordering)
	}

	// Add Related subqueries (joins) if we have multiple tables
	if opts.WithJoins && allTables != nil && len(allTables) > 1 {
		ast.Related = f.generateRelated(tbl, allTables, opts)
	}

	// Add EXISTS conditions
	if opts.WithExists && allTables != nil && len(allTables) > 1 {
		existsCond := f.generateExistsCondition(tbl, allTables)
		if existsCond != nil {
			if ast.Where != nil {
				// Combine with existing where via AND
				ast.Where = &builder.Condition{
					Type:       "and",
					Conditions: []builder.Condition{*ast.Where, *existsCond},
				}
			} else if opts.WithJoins {
				// Wrap in AND so uniquifyCSQConditionAliases can rename it
				// (avoids alias collision with Related entries)
				ast.Where = &builder.Condition{
					Type:       "and",
					Conditions: []builder.Condition{*existsCond},
				}
			} else {
				ast.Where = existsCond
			}
		}
	}

	return ast
}

func (f *Fuzzer) generateOrdering(tbl tableInfo, opts FuzzOpts) ivm.Ordering {
	if opts.WithMultiSort && len(tbl.columns) > 3 {
		// Order by 2-3 non-PK columns + id
		numSortCols := f.rng.Intn(2) + 2 // 2 or 3
		if numSortCols > len(tbl.columns)-1 {
			numSortCols = len(tbl.columns) - 1
		}
		ordering := make(ivm.Ordering, 0, numSortCols+1)
		for i := 0; i < numSortCols; i++ {
			col := tbl.columns[i+1]
			dir := "asc"
			if opts.WithDescSort && f.rng.Intn(2) == 0 {
				dir = "desc"
			}
			ordering = append(ordering, [2]string{col.name, dir})
		}
		ordering = append(ordering, [2]string{"id", "asc"})
		return ordering
	}

	// Default: single sort column + id
	sortCol := tbl.columns[1]
	dir := "asc"
	if opts.WithDescSort && f.rng.Intn(2) == 0 {
		dir = "desc"
	}
	return ivm.Ordering{{sortCol.name, dir}, {"id", "asc"}}
}

func (f *Fuzzer) generateBound(tbl tableInfo, ordering ivm.Ordering) *builder.Bound {
	// Generate a bound row with values for each ordering column
	row := make(ivm.Row)
	for _, ord := range ordering {
		colName := ord[0]
		// Find column type
		for _, col := range tbl.columns {
			if col.name == colName {
				row[colName] = f.randomValue(col.ctype)
				break
			}
		}
	}
	return &builder.Bound{
		Row:       row,
		Exclusive: f.rng.Intn(2) == 0,
	}
}

func (f *Fuzzer) generateRelated(parentTbl tableInfo, allTables []tableInfo, opts FuzzOpts) []builder.CorrelatedSubquery {
	// Pick 1-2 other tables to join to
	var related []builder.CorrelatedSubquery
	for _, childTbl := range allTables {
		if childTbl.name == parentTbl.name {
			continue
		}
		// Find a shared column type for correlation (use "id" on both sides for simplicity)
		// The parent has an "id" column, so the child's correlation key is also "id"
		// This simulates a foreign key: parent.id = child.id (or use a generated FK column)
		childSortCol := childTbl.columns[1]
		childOrdering := ivm.Ordering{{childSortCol.name, "asc"}, {"id", "asc"}}

		childAST := builder.AST{
			Table:   childTbl.name,
			Alias:   childTbl.name,
			OrderBy: childOrdering,
		}

		// Optionally add limit on child
		if f.rng.Intn(2) == 0 {
			lim := f.rng.Intn(3) + 1
			childAST.Limit = &lim
		}

		related = append(related, builder.CorrelatedSubquery{
			Correlation: builder.Correlation{
				ParentField: []string{"id"},
				ChildField:  []string{"id"},
			},
			Subquery: childAST,
		})
	}
	return related
}

func (f *Fuzzer) generateExistsCondition(parentTbl tableInfo, allTables []tableInfo) *builder.Condition {
	// Pick a child table different from parent
	var childTbl *tableInfo
	for i := range allTables {
		if allTables[i].name != parentTbl.name {
			childTbl = &allTables[i]
			break
		}
	}
	if childTbl == nil {
		return nil
	}

	childSortCol := childTbl.columns[1]
	childOrdering := ivm.Ordering{{childSortCol.name, "asc"}, {"id", "asc"}}

	op := "EXISTS"
	// NOT EXISTS is not supported by the TS client builder (see bugs.rocicorp.dev/issue/3438)
	// so we only generate EXISTS conditions for differential testing.

	lim := 3
	return &builder.Condition{
		Type: "correlatedSubquery",
		Op:   op,
		Related: &builder.CorrelatedSubquery{
			Correlation: builder.Correlation{
				ParentField: []string{"id"},
				ChildField:  []string{"id"},
			},
			Subquery: builder.AST{
				Table:   childTbl.name,
				Alias:   childTbl.name,
				OrderBy: childOrdering,
				Limit:   &lim,
			},
		},
	}
}

func (f *Fuzzer) generateCompoundCondition(tbl tableInfo) *builder.Condition {
	// Generate an AND or OR of 2-3 simple conditions
	numConds := f.rng.Intn(2) + 2 // 2 or 3
	conditions := make([]builder.Condition, 0, numConds)
	for i := 0; i < numConds; i++ {
		col := f.pickFilterableColumn(tbl)
		if col == nil {
			continue
		}
		conditions = append(conditions, *f.generateCondition(*col))
	}
	if len(conditions) < 2 {
		if len(conditions) == 1 {
			return &conditions[0]
		}
		return nil
	}
	typ := "and"
	if f.rng.Intn(2) == 0 {
		typ = "or"
	}
	return &builder.Condition{
		Type:       typ,
		Conditions: conditions,
	}
}

func (f *Fuzzer) generateNestedCondition(tbl tableInfo, depth int) *builder.Condition {
	if depth <= 0 {
		col := f.pickFilterableColumn(tbl)
		if col == nil {
			return nil
		}
		return f.generateCondition(*col)
	}
	// Generate AND/OR with nested children
	numConds := f.rng.Intn(2) + 2
	conditions := make([]builder.Condition, 0, numConds)
	for i := 0; i < numConds; i++ {
		child := f.generateNestedCondition(tbl, depth-1)
		if child != nil {
			conditions = append(conditions, *child)
		}
	}
	if len(conditions) < 2 {
		if len(conditions) == 1 {
			return &conditions[0]
		}
		return nil
	}
	typ := "and"
	if f.rng.Intn(2) == 0 {
		typ = "or"
	}
	return &builder.Condition{
		Type:       typ,
		Conditions: conditions,
	}
}

func (f *Fuzzer) generateINListCondition(tbl tableInfo) *builder.Condition {
	// Simulate IN (val1, val2, ...) as OR of = conditions on same column
	col := f.pickFilterableColumn(tbl)
	if col == nil {
		return nil
	}
	numVals := f.rng.Intn(4) + 2 // 2-5 values
	conditions := make([]builder.Condition, 0, numVals)
	for i := 0; i < numVals; i++ {
		var value interface{}
		if col.ctype == "number" {
			value = float64(f.rng.Intn(50))
		} else {
			value = f.randomValue("string")
		}
		conditions = append(conditions, builder.Condition{
			Type:  "simple",
			Left:  &builder.ValuePos{Type: "column", Name: col.name},
			Op:    "=",
			Right: &builder.ValuePos{Type: "literal", Value: value},
		})
	}
	return &builder.Condition{
		Type:       "or",
		Conditions: conditions,
	}
}

func (f *Fuzzer) pickFilterableColumn(tbl tableInfo) *colInfo {
	// Pick a non-PK column that's string or number (easier to generate conditions for)
	candidates := make([]colInfo, 0)
	for _, col := range tbl.columns[1:] {
		if col.ctype == "string" || col.ctype == "number" {
			candidates = append(candidates, col)
		}
	}
	if len(candidates) == 0 {
		return nil
	}
	c := candidates[f.rng.Intn(len(candidates))]
	return &c
}

func (f *Fuzzer) generateCondition(col colInfo) *builder.Condition {
	ops := []string{">", "<", ">=", "<=", "=", "!="}
	op := ops[f.rng.Intn(len(ops))]

	var value interface{}
	if col.ctype == "number" {
		value = float64(f.rng.Intn(50))
	} else {
		value = f.randomValue("string")
	}

	return &builder.Condition{
		Type:  "simple",
		Left:  &builder.ValuePos{Type: "column", Name: col.name},
		Op:    op,
		Right: &builder.ValuePos{Type: "literal", Value: value},
	}
}

func (f *Fuzzer) generateMutations(tables []tableInfo, initialData map[string][]ivm.Row, numMutations int) []Mutation {
	mutations := make([]Mutation, 0, numMutations)

	// Track current state per table to generate valid mutations
	state := make(map[string]map[string]ivm.Row) // table -> id -> row
	for _, tbl := range tables {
		state[tbl.name] = make(map[string]ivm.Row)
		for _, row := range initialData[tbl.name] {
			id := fmt.Sprintf("%v", row["id"])
			state[tbl.name][id] = row
		}
	}

	nextID := 100 // start new IDs at 100 to avoid collisions

	for i := 0; i < numMutations; i++ {
		tbl := tables[f.rng.Intn(len(tables))]
		existingIDs := make([]string, 0)
		for id := range state[tbl.name] {
			existingIDs = append(existingIDs, id)
		}

		// Decide mutation type based on current state
		mutType := f.rng.Intn(3) // 0=add, 1=remove, 2=edit
		if len(existingIDs) == 0 {
			mutType = 0 // can only add if empty
		}

		switch mutType {
		case 0: // add
			newID := fmt.Sprintf("%d", nextID)
			nextID++
			row := f.generateRow(tbl, nextID)
			row["id"] = newID
			mutations = append(mutations, Mutation{Type: "add", Table: tbl.name, Row: row})
			state[tbl.name][newID] = row

		case 1: // remove
			id := existingIDs[f.rng.Intn(len(existingIDs))]
			row := state[tbl.name][id]
			mutations = append(mutations, Mutation{Type: "remove", Table: tbl.name, Row: row})
			delete(state[tbl.name], id)

		case 2: // edit
			id := existingIDs[f.rng.Intn(len(existingIDs))]
			oldRow := state[tbl.name][id]
			newRow := make(ivm.Row)
			for k, v := range oldRow {
				newRow[k] = v
			}
			// Mutate one non-PK column
			col := tbl.columns[1+f.rng.Intn(len(tbl.columns)-1)]
			newRow[col.name] = f.randomValue(col.ctype)
			mutations = append(mutations, Mutation{Type: "edit", Table: tbl.name, Row: newRow, OldRow: oldRow})
			state[tbl.name][id] = newRow
		}
	}

	return mutations
}
