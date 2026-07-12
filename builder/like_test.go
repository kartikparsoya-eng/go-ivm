package builder

import (
	"strings"
	"testing"

	"github.com/kartikparsoya-eng/go-ivm/ivm"
)

// TestMatchLike pins matchLike to the in-memory matcher contract:
//   - backslash escapes the next character (Postgres default)
//   - wildcards are DOTALL (cross newlines) and ^/$ anchor the whole string
//   - LIKE is case-sensitive; ILIKE lowercases with full Unicode mapping
//   - a trailing backslash is invalid (panics a DataError)
//
// Also covers multi-byte UTF-8 cases where `_` must match one code point,
// not one byte.
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

// TestMatchLikeUpstreamCases ports the entire upstream corpus from
// the reference test cases 1:1. Every input/expectation pair is verified
// against SQLite upstream, so this is the strongest parity pin without
// executing the reference implementation.
func TestMatchLikeUpstreamCases(t *testing.T) {
	type block struct {
		pattern string
		ci      bool // upstream flags: 'i' | ''
		inputs  []struct {
			s    string
			want bool
		}
	}
	in := func(pairs ...any) (out []struct {
		s    string
		want bool
	}) {
		for i := 0; i < len(pairs); i += 2 {
			out = append(out, struct {
				s    string
				want bool
			}{pairs[i].(string), pairs[i+1].(bool)})
		}
		return
	}
	blocks := []block{
		{"foo", false, in("foo", true, "bar", false, "Foo", false, "FOO", false,
			"fo", false, "fooa", false, "afoo", false, "afoob", false)},
		{"foo", true, in("foo", true, "bar", false, "Foo", true, "FOO", true,
			"fo", false, "fooa", false, "afoo", false, "afoob", false)},
		{"foo%", false, in("foo", true, "foobar", true, "bar", false, "Foo", false,
			"FOO", false, "fo", false, "fooa", true, "afoo", false, "afoob", false)},
		{"foo%", true, in("foo", true, "foobar", true, "bar", false, "Foo", true,
			"FOO", true, "fo", false, "fooa", true, "afoo", false, "afoob", false,
			"foo\nbar", true, "foobar\nbaz", true)},
		{"foo_", false, in("foo", false, "foobar", false, "foob", true, "bar", false,
			"Foo", false, "FOO", false, "fo", false, "afoo", false, "afoob", false,
			// 8 chars vs exactly-4 pattern; the buggy 'm' flag matched "fooa".
			"fooa\nbar", false)},
		{"a%b", false, in("a\nb", true, "axb", true, "ab", true, "z\nab", false, "a\nbz", false)},
		{"a_b", false, in("a\nb", true, "axb", true, "ab", false, "a\nbc", false)},
		{`foo\%`, false, in("foo%", true, "foobar", false, "bar", false, "Foo", false,
			"FOO", false, "fo", false, "fooa", false, "afoo", false, "afoob", false)},
		{`foo\%`, true, in("foo%", true, "FOO%", true, "foobar", false, "bar", false,
			"Foo", false, "FOO", false, "fo", false, "fooa", false, "afoo", false, "afoob", false)},
		{`foo\_`, false, in("foo_", true, "FOO_", false, "foobar", false, "bar", false,
			"Foo", false, "FOO", false, "fo", false, "fooa", false, "afoo", false, "afoob", false)},
		{"%foo", false, in("foo", true, "foobar", false, "bar", false, "Foo", false,
			"FOO", false, "fo", false, "fooa", false, "afoo", true, "afoob", false,
			"monkey\nfoo", true, "mon\nkeyfoo", true)},
		{"%foo%", false, in("foo", true, "foobar", true, "bar", false, "Foo", false,
			"FOO", false, "fo", false, "fooa", true, "afoo", true, "afoob", true,
			"mon\nfoo\nkey", true, "m\nonfooke\ny", true)},
		{"%foo\nbar%", false, in("foo\nbar", true, "foo\nbar\n", true,
			"foo\nbar\nbaz", true, "monkey\nfoo\nbar", true)},
	}
	for _, blk := range blocks {
		flag := ""
		if blk.ci {
			flag = "i"
		}
		for _, c := range blk.inputs {
			if got := matchLike(c.s, blk.pattern, blk.ci); got != c.want {
				t.Errorf("matchLike(%q, %q, %q) = %v, want %v (upstream like-test-cases.ts)",
					c.s, blk.pattern, flag, got, c.want)
			}
		}
	}
}

// TestMatchLikeJSCanonicalizeFold verifies that the ILIKE case-folding
// matches ECMA-262 Canonicalize semantics: it never folds a non-ASCII
// character into an ASCII one and never folds KELVIN SIGN into k. RE2's
// (?i) applies Unicode simple case folding, which conflates these and
// would over-match.
func TestMatchLikeJSCanonicalizeFold(t *testing.T) {
	cases := []struct {
		name       string
		s, pattern string
		want       bool
	}{
		// ſ (U+017F): RE2 (?i) folds ſ~s~S (pre-fix: true); JS does not.
		{"long s does not fold into s (wildcard)", "\u017Ftra\u00DFe", "s%", false},
		{"s does not fold into long s (wildcard)", "strasse", "\u017F%", false},
		// KELVIN SIGN (U+212A): RE2 (?i) folds K~k~K; JS does not.
		{"kelvin does not fold into k (wildcard)", "\u212Aelvin", "k%", false},
		{"k does not fold into kelvin (wildcard)", "kelvin", "\u212A%", false},
		// Positive controls: ordinary folds still work through canonicalize.
		{"ascii fold still matches", "Kelvin", "k%", true},
		{"accents fold still matches", "\u00C9cole", "\u00E9%", true},
		{"greek fold still matches", "\u03A3\u03BF\u03C6", "\u03C3%", true},
		// Final sigma: Canonicalize(ς)=Σ=Canonicalize(σ) — JS matches these.
		{"final sigma folds via uppercase", "\u03BF\u03B4\u03BF\u03C2!", "\u03BF\u03B4\u03BF\u03C3_", true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := matchLike(c.s, c.pattern, true); got != c.want {
				t.Fatalf("ILIKE matchLike(%q, %q) = %v, want %v (JS 'i' Canonicalize)",
					c.s, c.pattern, got, c.want)
			}
		})
	}
}

// TestLikeNonStringLHSPanics verifies that a non-string LIKE LHS
// panics a DataError, matching the reference implementation's assertString
// guard. Both the eager (literal-RHS) and lazy (evalOp) paths must panic.
func TestLikeNonStringLHSPanics(t *testing.T) {
	mustPanic := func(t *testing.T, f func()) {
		t.Helper()
		defer func() {
			r := recover()
			if r == nil {
				t.Fatal("non-string LIKE LHS did not panic; TS assertString throws")
			}
			if _, ok := r.(*ivm.DataError); !ok {
				t.Fatalf("panic value = %T (%v); want *ivm.DataError", r, r)
			}
		}()
		f()
	}
	t.Run("eager literal-RHS path", func(t *testing.T) {
		pred := BuildPredicate(&Condition{
			Type: "simple", Op: "LIKE",
			Left:  &ValuePos{Type: "column", Name: "j"},
			Right: &ValuePos{Type: "literal", Value: "5%"},
		})
		mustPanic(t, func() { pred(ivm.Row{"j": float64(55)}) })
	})
	t.Run("lazy evalOp path", func(t *testing.T) {
		mustPanic(t, func() { evalOp("ILIKE", float64(5), "5") })
	})
	// Null LHS stays FALSE (short-circuits before the assert), matching
	// filter.ts:88-93.
	pred := BuildPredicate(&Condition{
		Type: "simple", Op: "LIKE",
		Left:  &ValuePos{Type: "column", Name: "j"},
		Right: &ValuePos{Type: "literal", Value: "5%"},
	})
	if pred(ivm.Row{"j": nil}) {
		t.Fatal("null LHS must be false, not a panic or a match")
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

// TestBuildPredicateTrailingEscapeFailsAtBuild verifies that a LIKE
// pattern ending with a trailing escape character panics a DataError at
// predicate build time, not at match time. BuildPredicate must panic
// without the predicate ever being invoked.
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
