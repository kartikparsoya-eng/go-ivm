package builder

// Converts AST Conditions into Go predicate functions (Row → bool)
// for use as FilterPredicate in pipeline operators.

import (
	"fmt"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/kartikparsoya-eng/go-ivm/ivm"
)

// Predicate is a function that tests whether a row matches a condition.
type Predicate func(ivm.Row) bool

// BuildPredicate converts an AST Condition into a Predicate function.
func BuildPredicate(cond *Condition) Predicate {
	if cond == nil {
		return func(ivm.Row) bool { return true }
	}
	return conditionToPredicate(cond)
}

func conditionToPredicate(cond *Condition) Predicate {
	switch cond.Type {
	case "simple":
		return simpleConditionPredicate(cond)
	case "and":
		preds := make([]Predicate, len(cond.Conditions))
		for i := range cond.Conditions {
			preds[i] = conditionToPredicate(&cond.Conditions[i])
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
		preds := make([]Predicate, len(cond.Conditions))
		for i := range cond.Conditions {
			preds[i] = conditionToPredicate(&cond.Conditions[i])
		}
		return func(row ivm.Row) bool {
			for _, p := range preds {
				if p(row) {
					return true
				}
			}
			return false
		}
	case "correlatedSubquery":
		// Correlated subqueries are handled by Join/Exists operators, not predicates.
		// Return true (passthrough) — the builder constructs operators for these.
		return func(ivm.Row) bool { return true }
	default:
		panic(fmt.Sprintf("unknown condition type: %s", cond.Type))
	}
}

func simpleConditionPredicate(cond *Condition) Predicate {
	return func(row ivm.Row) bool {
		left := resolveValue(cond.Left, row)
		right := resolveValue(cond.Right, row)
		return evalOp(cond.Op, left, right)
	}
}

func resolveValue(vp *ValuePos, row ivm.Row) ivm.Value {
	if vp == nil {
		return nil
	}
	switch vp.Type {
	case "column":
		return row[vp.Name]
	case "literal":
		return vp.Value
	case "static":
		// Static parameters should be resolved before building predicates
		panic("static parameters must be bound before predicate construction")
	default:
		return vp.Value
	}
}

// evalOp mirrors TS's createIsPredicate + createPredicateImpl semantics
// from packages/zql/src/builder/filter.ts.
//
// Key TS invariants we mirror here:
//
//  1. IS / IS NOT use JS strict-equality semantics: `null === null` is TRUE,
//     so `IS NULL` correctly matches null rows. All other types use Go ==
//     (which acts as strict-equality for interface{}-held values).
//
//  2. All other ops short-circuit on null: if EITHER side is nil, the
//     predicate returns false. This matches the TS top-of-createPredicate
//     short-circuit at filter.ts:74-93 — SQL `col = NULL` is always false,
//     never matches.
//
// ValuesEqual (the data.go function used at the IVM level) deliberately
// treats nil/nil as unequal for join semantics. That's WRONG for IS NULL
// so we don't use ValuesEqual here.
func evalOp(op string, left, right ivm.Value) bool {
	// IS / IS NOT: null-safe equality (null IS null → true).
	switch op {
	case "IS":
		return valuesIdentical(left, right)
	case "IS NOT":
		return !valuesIdentical(left, right)
	}

	// All other ops: short-circuit on null. SQL: any comparison with NULL is
	// not-true (treated as false here). Matches TS filter.ts:74-93.
	if left == nil || right == nil {
		return false
	}

	switch op {
	case "=":
		return valuesIdentical(left, right)
	case "!=":
		return !valuesIdentical(left, right)
	case "<":
		return compareForOrder(left, right) < 0
	case ">":
		return compareForOrder(left, right) > 0
	case "<=":
		return compareForOrder(left, right) <= 0
	case ">=":
		return compareForOrder(left, right) >= 0
	case "LIKE":
		return matchLike(fmt.Sprintf("%v", left), fmt.Sprintf("%v", right), false)
	case "NOT LIKE":
		return !matchLike(fmt.Sprintf("%v", left), fmt.Sprintf("%v", right), false)
	case "ILIKE":
		return matchLike(fmt.Sprintf("%v", left), fmt.Sprintf("%v", right), true)
	case "NOT ILIKE":
		return !matchLike(fmt.Sprintf("%v", left), fmt.Sprintf("%v", right), true)
	case "IN":
		return valueIn(left, right)
	case "NOT IN":
		return !valueIn(left, right)
	default:
		panic(fmt.Sprintf("unknown operator: %s", op))
	}
}

// valuesIdentical mirrors the equality semantics the TS path produces.
// The TS sqlite TableSource translates AST filters into SQL, so cross-
// type comparisons (e.g. `numeric_col = '5'`) go through SQLite's
// implicit cast — `'5'` becomes 5 and the row matches. Go's predicate
// runs in-process and never touches SQL, so it must replicate that
// coercion here to stay in parity with TS.
//
// Rules (in order):
//  1. null == null is TRUE; null vs anything else is FALSE.
//  2. Same-type equality (covers strings, identical numeric types, bool).
//  3. Both numeric → promote both to float64 and compare.
//  4. One side numeric, the other a string that parses as numeric →
//     coerce the string and compare. Matches SQLite's CAST behaviour
//     for `numeric_col = '5'` and inverse.
//  5. Otherwise FALSE.
//
// Rule 4 is the easy one to miss: without it a cross-type predicate like
// `participantCount = '5'` stops at rule 3 and rejects the string literal,
// diverging from SQLite (and therefore the TS path) which casts and matches.
func valuesIdentical(a, b ivm.Value) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	if a == b {
		return true
	}
	af, aNum := numericToFloat64(a)
	bf, bNum := numericToFloat64(b)
	if aNum && bNum {
		return af == bf
	}
	if aNum {
		if bs, ok := b.(string); ok {
			if bp, err := strconv.ParseFloat(bs, 64); err == nil {
				return af == bp
			}
		}
	}
	if bNum {
		if as, ok := a.(string); ok {
			if ap, err := strconv.ParseFloat(as, 64); err == nil {
				return ap == bf
			}
		}
	}
	return false
}

// numericToFloat64 promotes int/uint types to float64 for cross-type compare.
func numericToFloat64(v ivm.Value) (float64, bool) {
	switch n := v.(type) {
	case float64:
		return n, true
	case int64:
		return float64(n), true
	case uint64:
		return float64(n), true
	case int:
		return float64(n), true
	case int32:
		return float64(n), true
	case uint32:
		return float64(n), true
	case float32:
		return float64(n), true
	}
	return 0, false
}

// compareForOrder orders two values for </>/<=/>= predicates. It first applies
// the same cross-type numeric coercion valuesIdentical uses for =/!=, then
// falls back to ivm.CompareValues for same-type (string/bool) ordering.
//
// Operators MED-8: HIGH-2 taught =/!= to coerce a numeric column compared
// against a numeric-string literal (e.g. `count > '5'`) so Go matches the TS
// path, which evaluates filters through SQLite's implicit cast. The ordered
// operators were left calling ivm.CompareValues directly, which PANICS on the
// float-vs-numeric-string pair — an asymmetric divergence (=/!= returned a
// clean bool, </> crashed). This restores symmetry.
func compareForOrder(a, b ivm.Value) int {
	if c, ok := numericCmpCoerced(a, b); ok {
		return c
	}
	return ivm.CompareValues(a, b)
}

// numericCmpCoerced returns (cmp, true) when a numeric ordering applies after
// the valuesIdentical-style coercion (both numeric, or one numeric + the other
// a numeric-parseable string). Returns (0, false) when no numeric ordering
// applies, so compareForOrder can fall back to same-type comparison.
func numericCmpCoerced(a, b ivm.Value) (int, bool) {
	af, aNum := numericToFloat64(a)
	bf, bNum := numericToFloat64(b)
	if aNum && !bNum {
		if bs, ok := b.(string); ok {
			if bp, err := strconv.ParseFloat(bs, 64); err == nil {
				bf, bNum = bp, true
			}
		}
	} else if bNum && !aNum {
		if as, ok := a.(string); ok {
			if ap, err := strconv.ParseFloat(as, 64); err == nil {
				af, aNum = ap, true
			}
		}
	}
	if aNum && bNum {
		switch {
		case af < bf:
			return -1, true
		case af > bf:
			return 1, true
		default:
			return 0, true
		}
	}
	return 0, false
}

// matchLike implements SQL LIKE pattern matching (% and _ wildcards).
// likeRegexCache memoizes compiled LIKE patterns. The pattern (RHS) is
// constant per condition but matchLike runs per row, so compile once and reuse.
//
// Bounded: a hostile or high-cardinality query stream (e.g. parametrized LIKE
// with ever-changing RHS) could grow this map without limit (D3). We keep
// sync.Map's lock-free Load fastpath (the per-row hot path) and cap the entry
// count; on overflow we do a one-shot clear-and-reset (generational reset —
// the working set re-populates from the live queries). This avoids a
// heavyweight LRU with per-access locking that would slow the hot path. The
// cap is generous (typical apps have a small fixed set of LIKE patterns); a
// clear costs O(n) but fires only on the rare miss-after-overflow path.
var (
	likeRegexCache    sync.Map        // key string -> *regexp.Regexp (typed nil = bad pattern)
	likeRegexCacheLen atomic.Int64    // approximate entry count; accurate except mid-overflow
	likeRegexCacheCap = int64(1 << 14) // 16k compiled patterns; tune via GO_IVM_LIKE_CACHE_CAP
)

// matchLike reports whether s matches a SQL LIKE/ILIKE pattern, using SQLite's
// default LIKE semantics (the engine both TS and Go ultimately read through):
// `%` -> .*, `_` -> ., and EVERY other character — including `\` — is a literal
// (no escape character; SQLite requires an explicit `ESCAPE` clause for that,
// which Zero never emits). The pattern is compiled to an anchored, multiline,
// Unicode-aware regexp. caseInsensitive maps to (?i) (ILIKE).
//
// Earlier this treated `\` as an escape (mirroring TS's in-memory patternToRegExp).
// But TS's TableSource hydrates by pushing `col LIKE ?` into SQLite with no
// ESCAPE clause, so `\` is literal there — and a `%\%%` pattern matched
// backslash-content in TS but percent-content in Go, a deterministic hydrate
// divergence. Matching SQLite is the faithful behavior.
func matchLike(s, pattern string, caseInsensitive bool) bool {
	re := likeRegexpFor(pattern, caseInsensitive)
	if re == nil {
		// Unparsable pattern (e.g. trailing escape — TS throws). Fail the match
		// rather than crash the predicate.
		return false
	}
	return re.MatchString(s)
}

func likeRegexpFor(pattern string, caseInsensitive bool) *regexp.Regexp {
	key := "s\x00" + pattern
	if caseInsensitive {
		key = "i\x00" + pattern
	}
	if v, ok := likeRegexCache.Load(key); ok {
		re, _ := v.(*regexp.Regexp)
		return re
	}
	re := compileLikePattern(pattern, caseInsensitive)
	// Bound the cache (D3): if we're at/over cap, clear before inserting so a
	// high-cardinality pattern stream can't grow the map without limit. The
	// clear is racy by design (Range+Delete under no lock) but safe —
	// concurrent Store/Load on sync.Map is fine; at worst two goroutines both
	// clear and both reset the counter to a small number, which is
	// self-correcting on the next overflow. The fastpath Load above is
	// unaffected. applyCacheCap() reads the env override once at init.
	if n := likeRegexCacheLen.Add(1); n > applyLikeCacheCap() {
		// Over cap: drop everything and start fresh. The live queries' patterns
		// recompile on their next miss (a few regexp.Compile calls, cheap
		// relative to a hydrate).
		likeRegexCache.Range(func(k, _ any) bool {
			likeRegexCache.Delete(k)
			return true
		})
		likeRegexCacheLen.Store(1) // we're about to add this entry
	}
	likeRegexCache.Store(key, re) // store nil too, so bad patterns aren't recompiled
	return re
}

// applyLikeCacheCap returns the effective cache cap, honoring a one-time
// GO_IVM_LIKE_CACHE_CAP override (parsed at first use, then cached). A
// non-positive override disables the cap (unbounded — only for tests/diagnostics).
var applyLikeCacheCap = sync.OnceValue(func() int64 {
	if v := os.Getenv("GO_IVM_LIKE_CACHE_CAP"); v != "" {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil {
			return n
		}
	}
	return likeRegexCacheCap
})

// compileLikePattern translates a SQL LIKE pattern to a Go regexp, mirroring TS
// patternToRegExp. Returns nil on an invalid pattern (TS throws).
func compileLikePattern(source string, caseInsensitive bool) *regexp.Regexp {
	var b strings.Builder
	// (?m): multiline so ^/$ anchor at line boundaries, matching JS's 'm' flag.
	// Go RE2 '.' already excludes \n (like JS without 's'), so % (-> .*) won't
	// cross newlines.
	b.WriteString("(?m)")
	if caseInsensitive {
		b.WriteString("(?i)")
	}
	b.WriteByte('^')
	runes := []rune(source)
	for i := 0; i < len(runes); i++ {
		switch c := runes[i]; c {
		case '%':
			b.WriteString(".*")
		case '_':
			b.WriteString(".")
		default:
			// NOTE: backslash is NOT an escape character here. TS's TableSource
			// pushes the LIKE filter into SQLite SQL (`col LIKE ?`) with NO
			// `ESCAPE` clause (zqlite/query-builder.ts), so SQLite — the engine
			// both sides share — treats `\` (and every non-`%`/`_` char) as a
			// LITERAL. We MUST match that, or a pattern like `%\%%` diverges:
			// SQLite/TS reads it as "contains a backslash" while a backslash-as-
			// escape reading is "contains a percent". Treating `\` as a metachar
			// to quote keeps it literal — exactly SQLite's default.
			b.WriteString(regexp.QuoteMeta(string(c)))
		}
	}
	b.WriteByte('$')
	re, err := regexp.Compile(b.String())
	if err != nil {
		return nil
	}
	return re
}

// valueIn checks if left is contained in right (which should be a slice).
//
// Types MED-8: `x IN (a, b)` is sugar for `x = a OR x = b`, so each element
// test must use the SAME equality as the `=` operator — valuesIdentical, which
// applies the cross-type numeric↔numeric-string coercion the TS path gets from
// SQLite (HIGH-2). The old code used ivm.ValuesEqual (strict, no coercion) for
// []interface{}/[]float64 and a fragile fmt.Sprintf string compare for
// []string, so `count IN ('5')` with count=5 returned false while `count = '5'`
// returned true — an asymmetric divergence from TS. Routing every element
// through valuesIdentical makes IN/NOT IN agree with =/!=.
func valueIn(left, right ivm.Value) bool {
	switch arr := right.(type) {
	case []interface{}:
		for _, v := range arr {
			if valuesIdentical(left, v) {
				return true
			}
		}
	case []string:
		for _, v := range arr {
			if valuesIdentical(left, v) {
				return true
			}
		}
	case []float64:
		for _, v := range arr {
			if valuesIdentical(left, v) {
				return true
			}
		}
	}
	return false
}
