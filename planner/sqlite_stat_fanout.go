package planner

import (
	"database/sql"
	"fmt"
	"sort"
	"strings"
	"sync"
)

// stat4Sample represents a row from sqlite_stat4.
type stat4Sample struct {
	neq    string
	nlt    string
	ndlt   string
	sample []byte
}

// indexInfo holds the name and column depth of a matching index.
type indexInfo struct {
	indexName string
	depth     int
}

// SQLiteStatFanout computes join fanout factors from SQLite statistics tables.
//
// It uses sqlite_stat4 (histogram, most accurate, excludes NULLs) with
// fallback to sqlite_stat1 (average, includes NULLs) and a default constant.
// Results are cached per (table, columns) pair.
//
// Fanout is the average number of child rows per distinct parent key value,
// used to estimate join cardinality in query planning.
type SQLiteStatFanout struct {
	db            *sql.DB
	defaultFanout float64
	cache         sync.Map // map[string]FanoutResult
}

// NewSQLiteStatFanout creates a new fanout calculator.
// defaultFanout is used when no statistics are available (0 → 3).
func NewSQLiteStatFanout(db *sql.DB, defaultFanout float64) *SQLiteStatFanout {
	if defaultFanout <= 0 {
		defaultFanout = 3
	}
	return &SQLiteStatFanout{
		db:            db,
		defaultFanout: defaultFanout,
	}
}

// GetFanout returns the fanout factor for join column(s).
//
// Strategy:
//  1. Try sqlite_stat4 (best): histogram with separate NULL/non-NULL samples
//  2. Fallback to sqlite_stat1: average across all rows (includes NULLs)
//  3. Fallback to default constant
func (f *SQLiteStatFanout) GetFanout(tableName string, columns []string) FanoutResult {
	cacheKey := f.cacheKey(tableName, columns)
	if cached, ok := f.cache.Load(cacheKey); ok {
		return cached.(FanoutResult)
	}

	if result, ok := f.getFanoutFromStat4(tableName, columns); ok {
		f.cache.Store(cacheKey, result)
		return result
	}

	if result, ok := f.getFanoutFromStat1(tableName, columns); ok {
		f.cache.Store(cacheKey, result)
		return result
	}

	result := FanoutResult{
		Fanout:     f.defaultFanout,
		Confidence: 0,
	}
	f.cache.Store(cacheKey, result)
	return result
}

// ClearCache clears the fanout cache. Call after running ANALYZE.
func (f *SQLiteStatFanout) ClearCache() {
	f.cache.Range(func(key, value any) bool {
		f.cache.Delete(key)
		return true
	})
}

func (f *SQLiteStatFanout) cacheKey(tableName string, columns []string) string {
	sorted := make([]string, len(columns))
	copy(sorted, columns)
	sort.Strings(sorted)
	return fmt.Sprintf("%s:%s", tableName, strings.Join(sorted, ","))
}

// getFanoutFromStat4 queries the sqlite_stat4 histogram, filters NULL samples,
// and returns the median fanout of non-NULL samples.
func (f *SQLiteStatFanout) getFanoutFromStat4(tableName string, columns []string) (FanoutResult, bool) {
	idxInfo, ok := f.findIndexForColumns(tableName, columns)
	if !ok {
		return FanoutResult{}, false
	}

	rows, err := f.db.Query(
		`SELECT neq, nlt, ndlt, sample FROM sqlite_stat4 WHERE tbl = ? AND idx = ? ORDER BY nlt`,
		tableName, idxInfo.indexName,
	)
	if err != nil {
		return FanoutResult{}, false
	}
	defer rows.Close()

	var samples []stat4Sample
	for rows.Next() {
		var s stat4Sample
		if err := rows.Scan(&s.neq, &s.nlt, &s.ndlt, &s.sample); err != nil {
			return FanoutResult{}, false
		}
		samples = append(samples, s)
	}
	if rows.Err() != nil {
		return FanoutResult{}, false
	}
	if len(samples) == 0 {
		return FanoutResult{}, false
	}

	neqIndex := idxInfo.depth - 1
	if neqIndex < 0 {
		neqIndex = 0
	}

	var fanouts []float64
	for _, s := range samples {
		neqParts := strings.Fields(s.neq)
		var fanout float64
		if neqIndex < len(neqParts) {
			fmt.Sscanf(neqParts[neqIndex], "%f", &fanout)
		} else if len(neqParts) > 0 {
			fmt.Sscanf(neqParts[0], "%f", &fanout)
		}
		if !decodeSampleIsNull(s.sample) {
			fanouts = append(fanouts, fanout)
		}
	}

	if len(fanouts) == 0 {
		return FanoutResult{Fanout: 0, Confidence: 1}, true
	}

	sort.Float64s(fanouts)
	var median float64
	n := len(fanouts)
	if n%2 == 0 {
		median = (fanouts[n/2-1] + fanouts[n/2]) / 2
	} else {
		median = fanouts[n/2]
	}

	return FanoutResult{Fanout: median, Confidence: 1}, true
}

// getFanoutFromStat1 queries sqlite_stat1 for the average fanout.
// Note: this includes NULL rows and may overestimate for sparse foreign keys.
func (f *SQLiteStatFanout) getFanoutFromStat1(tableName string, columns []string) (FanoutResult, bool) {
	idxInfo, ok := f.findIndexForColumns(tableName, columns)
	if !ok {
		return FanoutResult{}, false
	}

	var stat string
	err := f.db.QueryRow(
		`SELECT stat FROM sqlite_stat1 WHERE tbl = ? AND idx = ?`,
		tableName, idxInfo.indexName,
	).Scan(&stat)
	if err != nil {
		return FanoutResult{}, false
	}

	parts := strings.Fields(stat)
	if len(parts) < idxInfo.depth+1 {
		return FanoutResult{}, false
	}

	var fanout float64
	if _, err := fmt.Sscanf(parts[idxInfo.depth], "%f", &fanout); err != nil {
		return FanoutResult{}, false
	}

	return FanoutResult{Fanout: fanout, Confidence: 0.5}, true
}

// findIndexForColumns finds an index containing all columns as a prefix,
// using flexible (order-independent) matching. Queries pragma_index_list
// and pragma_index_info for reliable index column names.
func (f *SQLiteStatFanout) findIndexForColumns(tableName string, columns []string) (indexInfo, bool) {
	rows, err := f.db.Query(
		`SELECT il.name as index_name, ii.seqno, ii.name as column_name
		 FROM pragma_index_list(?) il
		 JOIN pragma_index_info(il.name) ii
		 ORDER BY il.seq, ii.seqno`,
		tableName,
	)
	if err != nil {
		return indexInfo{}, false
	}
	defer rows.Close()

	indexMap := make(map[string][]string)
	for rows.Next() {
		var indexName, columnName string
		var seqno int
		if err := rows.Scan(&indexName, &seqno, &columnName); err != nil {
			return indexInfo{}, false
		}
		indexMap[indexName] = append(indexMap[indexName], columnName)
	}
	if rows.Err() != nil {
		return indexInfo{}, false
	}

	for indexName, indexColumns := range indexMap {
		if isPrefixMatch(columns, indexColumns) {
			return indexInfo{indexName: indexName, depth: len(columns)}, true
		}
	}
	return indexInfo{}, false
}

// isPrefixMatch checks if all queryColumns exist in the first N positions
// of indexColumns, regardless of order. Gaps are not allowed: {a, c} does
// not match (a, b, c) because no depth represents just (a, c) without b.
func isPrefixMatch(queryColumns, indexColumns []string) bool {
	if len(queryColumns) > len(indexColumns) {
		return false
	}
	indexPrefix := make(map[string]bool, len(queryColumns))
	for i := 0; i < len(queryColumns); i++ {
		indexPrefix[strings.ToLower(indexColumns[i])] = true
	}
	for _, col := range queryColumns {
		if !indexPrefix[strings.ToLower(col)] {
			return false
		}
	}
	return true
}

// decodeSampleIsNull decodes a sqlite_stat4 sample value to check if it's
// NULL. SQLite record format: varint header size, serial types, data.
// Serial type 0 = NULL. We only check the first column's serial type.
func decodeSampleIsNull(sample []byte) bool {
	if len(sample) == 0 {
		return true
	}
	headerSize := sample[0]
	if headerSize == 0 || int(headerSize) >= len(sample) {
		return true
	}
	serialType := sample[1]
	return serialType == 0
}
