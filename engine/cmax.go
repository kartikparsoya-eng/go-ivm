package engine

// Cmax computation: the maximum concurrent-reader (cursor) demand across all
// queries in a hydrate batch. Used to size the reader pool K = P × Cmax so
// that P worker lanes can never deadlock waiting for a reader (§3d): the
// whole operator tree streams lazily (iter.Seq end-to-end), so a parent
// Join holds its cursor open while fetching each child — the pool must
// cover the deepest simultaneous-cursor chain of any query in the batch.

import (
	"github.com/kartikparsoya-eng/go-ivm/builder"
)

// ConservativeHydrateCmax returns a guaranteed UPPER BOUND on the
// concurrent-cursor demand (Cmax) for hydrating the given query ASTs. It
// works directly from the AST, so it is usable BEFORE the pipelines exist —
// which is exactly when
// the reader pool is sized (the sidecar builds the pool at connection setup, in
// refreshSnapForInitialHydrateLocked, ahead of engine.AddQueriesStream's
// pipeline build).
//
// The precise demand walks the built operator tree, where
// Join/FlippedJoin/UnionFanIn SUM their children and every other operator takes
// the MAX. Because sum ≥ max, the TOTAL number of table-sources (leaves) in a
// query upper-bounds its true Cmax for ANY tree shape. That leaf count is read
// straight off the AST: 1 (the main table) plus the recursive count of every
// related subquery (ast.Related) and every EXISTS correlatedSubquery in the
// WHERE tree.
//
// Erring high is SAFE: the pool gets a few extra pinned read connections it may
// not use. Erring low is NOT: a lazy hydrate that holds a parent reader while
// acquiring a child reader the pool can't supply blocks forever (deadlock, not
// failure). So this deliberately over-estimates. Returns at least 1.
func ConservativeHydrateCmax(asts []builder.AST) int {
	cmax := 1
	for i := range asts {
		if c := querySourceCount(asts[i]); c > cmax {
			cmax = c
		}
	}
	return cmax
}

// ConservativeHydrateCmaxForSpecs is ConservativeHydrateCmax over a batch of
// QuerySpecs (the shape the sidecar's addQueries handlers already hold).
func ConservativeHydrateCmaxForSpecs(specs []QuerySpec) int {
	cmax := 1
	for i := range specs {
		if c := querySourceCount(specs[i].AST); c > cmax {
			cmax = c
		}
	}
	return cmax
}

// querySourceCount counts the table-sources (leaves) reachable from a query
// AST: the main table, plus every related subquery and every EXISTS
// correlatedSubquery, recursively. This equals the worst-case (all-Join) Cmax
// and upper-bounds the true Cmax for any operator-tree shape. Double-counting a
// subquery that appears both as a relationship and as a WHERE-EXISTS only
// inflates the bound, which is safe.
func querySourceCount(ast builder.AST) int {
	n := 1 // the main table source
	for i := range ast.Related {
		n += querySourceCount(ast.Related[i].Subquery)
	}
	for _, csq := range existsSubqueriesInCondition(ast.Where) {
		n += querySourceCount(csq.Subquery)
	}
	return n
}

// existsSubqueriesInCondition collects every correlatedSubquery (EXISTS / NOT
// EXISTS) at any depth of an and/or condition tree. Each becomes a Join+Exists
// operator pair at build time, contributing its subquery's sources to the
// concurrent-cursor demand.
func existsSubqueriesInCondition(cond *builder.Condition) []builder.CorrelatedSubquery {
	if cond == nil {
		return nil
	}
	switch cond.Type {
	case "correlatedSubquery":
		if cond.Related != nil {
			return []builder.CorrelatedSubquery{*cond.Related}
		}
	case "and", "or":
		var out []builder.CorrelatedSubquery
		for i := range cond.Conditions {
			out = append(out, existsSubqueriesInCondition(&cond.Conditions[i])...)
		}
		return out
	}
	return nil
}
