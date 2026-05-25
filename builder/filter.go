package builder

// Converts AST Conditions into Go predicate functions (Row → bool)
// for use as FilterPredicate in pipeline operators.

import (
	"fmt"
	"strconv"
	"strings"

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
		return ivm.CompareValues(left, right) < 0
	case ">":
		return ivm.CompareValues(left, right) > 0
	case "<=":
		return ivm.CompareValues(left, right) <= 0
	case ">=":
		return ivm.CompareValues(left, right) >= 0
	case "LIKE":
		return matchLike(fmt.Sprintf("%v", left), fmt.Sprintf("%v", right), true)
	case "NOT LIKE":
		return !matchLike(fmt.Sprintf("%v", left), fmt.Sprintf("%v", right), true)
	case "ILIKE":
		return matchLike(
			strings.ToLower(fmt.Sprintf("%v", left)),
			strings.ToLower(fmt.Sprintf("%v", right)),
			false,
		)
	case "NOT ILIKE":
		return !matchLike(
			strings.ToLower(fmt.Sprintf("%v", left)),
			strings.ToLower(fmt.Sprintf("%v", right)),
			false,
		)
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
// Discovered via gap-cross-type-num-eq-str in the soak (2026-05-25):
// `participantCount = '5'` produced TS=1/Go=0 mismatches because Go
// stopped at rule 3 and rejected the string literal.
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

// matchLike implements SQL LIKE pattern matching (% and _ wildcards).
func matchLike(s, pattern string, caseSensitive bool) bool {
	if !caseSensitive {
		s = strings.ToLower(s)
		pattern = strings.ToLower(pattern)
	}
	return likeMatch(s, pattern, 0, 0)
}

func likeMatch(s, p string, si, pi int) bool {
	for pi < len(p) {
		switch p[pi] {
		case '%':
			pi++
			// % matches any sequence
			for i := si; i <= len(s); i++ {
				if likeMatch(s, p, i, pi) {
					return true
				}
			}
			return false
		case '_':
			if si >= len(s) {
				return false
			}
			si++
			pi++
		default:
			if si >= len(s) || s[si] != p[pi] {
				return false
			}
			si++
			pi++
		}
	}
	return si == len(s)
}

// valueIn checks if left is contained in right (which should be a slice).
func valueIn(left, right ivm.Value) bool {
	switch arr := right.(type) {
	case []interface{}:
		for _, v := range arr {
			if ivm.ValuesEqual(left, v) {
				return true
			}
		}
	case []string:
		ls := fmt.Sprintf("%v", left)
		for _, v := range arr {
			if ls == v {
				return true
			}
		}
	case []float64:
		for _, v := range arr {
			if ivm.ValuesEqual(left, v) {
				return true
			}
		}
	}
	return false
}
