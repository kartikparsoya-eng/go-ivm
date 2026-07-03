package builder

import "testing"

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
