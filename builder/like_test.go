package builder

import (
	"strings"
	"testing"

	"github.com/kartikparsoya-eng/go-ivm/ivm"
)

// TestMatchLike pins matchLike to TS's in-memory matcher contract
// (zql/src/builder/like.ts @ v1.7.0), which zero 1.7.0 made authoritative on
// BOTH paths (the SQL side now runs case_sensitive_like=ON with ESCAPE '\'
// and lower()ed ILIKE operands):
//   - backslash escapes the next character (Postgres default)
//   - wildcards are DOTALL (cross newlines) and ^/$ anchor the whole string
//   - LIKE is case-sensitive; ILIKE lowercases with full Unicode mapping
//   - a trailing backslash is invalid (TS throws; Go panics a DataError)
//
// Also keeps the HIGH-8 cases the old byte-by-byte matcher got wrong
// (multi-byte UTF-8 under `_`).
func TestMatchLike(t *testing.T) {
	cases := []struct {
		name            string
		s, pattern      string
		caseInsensitive bool
		want            bool
	}{
		// Multi-byte UTF-8: `_` matches one CODE POINT, not one byte.
		{"underscore matches multibyte rune", "café", "caf_", false, true},
		{"underscore one rune not two", "café", "ca_", false, false},

		// Backslash ESCAPES the next char (like.ts / Postgres — the 1.7.0
		// contract; pre-1.7.0 both TS SQL and this matcher treated it as a
		// literal). `\%` = literal percent, `\_` = literal underscore.
		{"escaped percent is literal percent", "a%", `a\%`, false, true},
		{"escaped percent does not match backslash", `a\b`, `a\%`, false, false},
		{"escaped underscore is literal underscore", "a_", `a\_`, false, true},
		{"escaped underscore not a wildcard", "ab", `a\_`, false, false},
		// The flipped regression: %\%% now matches content containing a
		// PERCENT (escape), not content containing a backslash.
		{"escaped-percent pattern matches percent", "50% off", `%\%%`, false, true},
		{"escaped-percent pattern not backslash", `path\to`, `%\%%`, false, false},
		// Escaped backslash is a literal backslash.
		{"escaped backslash literal", `a\b`, `a\\b`, false, true},
		// `_` not preceded by escape is still a wildcard.
		{"underscore still wildcard", "10%", `10_`, false, true},

		// Newlines: dotall (like.ts 's' flag) — wildcards cross newlines,
		// and ^/$ anchor the WHOLE string, not line boundaries.
		{"underscore crosses newline", "foo\nbar", "foo_bar", false, true},
		{"percent crosses newline", "a\nb", "a%b", false, true},
		{"no interior line-boundary anchoring", "fooa\nbar", "foo_", false, false},
		{"percent matches within a line", "bar\nfoo", "%foo%", false, true},

		// Case sensitivity: LIKE is case-SENSITIVE (Postgres semantics, needs
		// case_sensitive_like=ON on the SQL side); ILIKE folds case.
		{"like is case sensitive", "CAFE", "cafe", false, false},
		{"ilike folds ascii (fast path)", "CAFE", "cafe", true, true},
		{"ilike folds unicode (fast path)", "CAFÉ", "café", true, true},
		// Full Unicode case mapping on the non-wildcard fast path: Greek
		// final sigma. toLowerCase("ΟΔΟΣ") = "οδος"; a simple fold or
		// strings.ToLower gives "οδοσ" and would NOT match.
		{"ilike final sigma full mapping", "ΟΔΟΣ", "οδος", true, true},
		{"ilike wildcard path folds", "CAFÉ!", "caf_!", true, true},

		// Plain wildcards still work.
		{"percent prefix", "hello world", "%world", false, true},
		{"percent both ends", "hello world", "%lo wo%", false, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := matchLike(c.s, c.pattern, c.caseInsensitive); got != c.want {
				t.Fatalf("matchLike(%q, %q, ci=%v) = %v, want %v",
					c.s, c.pattern, c.caseInsensitive, got, c.want)
			}
		})
	}
}

// TestMatchLikeTrailingEscapePanics: TS throws "LIKE pattern must not end
// with escape character" at predicate build; Go must panic a DataError (the
// engine recovers it per-query) rather than silently matching nothing.
func TestMatchLikeTrailingEscapePanics(t *testing.T) {
	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("matchLike with trailing escape did not panic; TS throws")
		}
	}()
	matchLike(`x\`, `x\`, false)
}

// TestBuildPredicateTrailingEscapeFailsAtBuild pins H1 from the napi-path
// hostile review: TS compiles the LIKE pattern at predicate BUILD
// (filter.ts:79 createPredicateImpl → like.ts patternToRegExp) and throws
// "LIKE pattern must not end with escape character" there — surfacing as a
// clean per-query error from addQuery. Pre-fix, Go deferred compilation to
// the first matchLike call, so a bad pattern installed a LIVE pipeline that
// panicked per-row mid-advance — tearing down the whole CG, and the
// orphaned pipeline re-panicked on every subsequent push. BuildPredicate
// must panic a DataError WITHOUT the predicate ever being invoked.
func TestBuildPredicateTrailingEscapeFailsAtBuild(t *testing.T) {
	for _, op := range []string{"LIKE", "NOT LIKE", "ILIKE", "NOT ILIKE"} {
		t.Run(op, func(t *testing.T) {
			defer func() {
				r := recover()
				if r == nil {
					t.Fatalf("BuildPredicate(%s, trailing escape) returned; "+
						"want DataError panic at build (pre-H1 it panicked "+
						"per-row mid-advance instead)", op)
				}
				de, ok := r.(*ivm.DataError)
				if !ok {
					t.Fatalf("panic value = %T (%v); want *ivm.DataError", r, r)
				}
				if !strings.Contains(de.Error(), "must not end with escape character") {
					t.Fatalf("DataError message = %q; want the like.ts message", de.Error())
				}
			}()
			BuildPredicate(&Condition{
				Type:  "simple",
				Op:    op,
				Left:  &ValuePos{Type: "column", Name: "s"},
				Right: &ValuePos{Type: "literal", Value: `x\`},
			})
		})
	}
}

// TestBuildPredicateLikeEagerSemantics: the eager-compile path must
// reproduce the lazy evalOp semantics exactly — including the null
// short-circuits that fire BEFORE negation (NULL NOT LIKE p is false, not
// true), the nil-RHS constant-false without compiling (TS filter.ts:75),
// and the Unicode fast path.
func TestBuildPredicateLikeEagerSemantics(t *testing.T) {
	col := func(name string) *ValuePos { return &ValuePos{Type: "column", Name: name} }
	lit := func(v any) *ValuePos { return &ValuePos{Type: "literal", Value: v} }
	cases := []struct {
		name string
		op   string
		rhs  *ValuePos
		row  ivm.Row
		want bool
	}{
		{"LIKE wildcard match", "LIKE", lit("Al%"), ivm.Row{"s": "Alice"}, true},
		{"LIKE wildcard non-match", "LIKE", lit("Al%"), ivm.Row{"s": "Bob"}, false},
		{"NOT LIKE non-match negates", "NOT LIKE", lit("Al%"), ivm.Row{"s": "Bob"}, true},
		{"NOT LIKE match negates", "NOT LIKE", lit("Al%"), ivm.Row{"s": "Alice"}, false},
		{"NULL lhs LIKE is false", "LIKE", lit("Al%"), ivm.Row{"s": nil}, false},
		{"NULL lhs NOT LIKE is false too", "NOT LIKE", lit("Al%"), ivm.Row{"s": nil}, false},
		{"nil literal rhs LIKE is false", "LIKE", lit(nil), ivm.Row{"s": "x"}, false},
		{"nil literal rhs NOT LIKE is false", "NOT LIKE", lit(nil), ivm.Row{"s": "x"}, false},
		{"ILIKE fast path unicode", "ILIKE", lit("οδος"), ivm.Row{"s": "ΟΔΟΣ"}, true},
		{"ILIKE wildcard folds", "ILIKE", lit("caf_"), ivm.Row{"s": "CAFÉ"}, true},
		{"NOT ILIKE non-match", "NOT ILIKE", lit("caf_"), ivm.Row{"s": "tea"}, true},
		{"LIKE fast path equality", "LIKE", lit("plain"), ivm.Row{"s": "plain"}, true},
		{"LIKE fast path case-sensitive", "LIKE", lit("plain"), ivm.Row{"s": "PLAIN"}, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			pred := BuildPredicate(&Condition{
				Type: "simple", Op: c.op, Left: col("s"), Right: c.rhs,
			})
			if got := pred(c.row); got != c.want {
				t.Fatalf("%s %v on %v = %v, want %v", c.op, c.rhs.Value, c.row, got, c.want)
			}
		})
	}
}
