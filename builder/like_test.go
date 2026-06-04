package builder

import "testing"

// TestMatchLike covers the HIGH-8 cases the old byte-by-byte matcher got
// wrong: multi-byte UTF-8 under `_` and `%`/`_` vs embedded newlines — plus
// ILIKE case folding. Backslash is a LITERAL (SQLite default LIKE, which TS's
// SQL pushdown uses — no ESCAPE clause).
func TestMatchLike(t *testing.T) {
	cases := []struct {
		name            string
		s, pattern      string
		caseInsensitive bool
		want            bool
	}{
		// Multi-byte UTF-8: `_` matches one CODE POINT, not one byte. The old
		// matcher matched a single byte of 'é' (0xC3) and left 0xA9 dangling.
		{"underscore matches multibyte rune", "café", "caf_", false, true},
		{"underscore one rune not two", "café", "ca_", false, false},

		// Backslash is LITERAL (no escape — SQLite default; TS pushes LIKE to
		// SQLite without ESCAPE). So `\_` = literal backslash + any char, and
		// `\%` = literal backslash + zero-or-more chars.
		{"backslash is literal not escape", `a\b`, `a\%`, false, true},
		{"backslash underscore one char after slash", `a\b`, `a\_`, false, true},
		{"no backslash no match", "ab", `a\_`, false, false},
		// The exact regression: %\%% matches content containing a backslash,
		// NOT content containing a percent (matches TS's SQLite fetch).
		{"escaped-percent pattern matches backslash", `path\to`, `%\%%`, false, true},
		{"escaped-percent pattern not percent", "50% off", `%\%%`, false, false},
		// `_` after the literal backslash is still a single-char wildcard.
		{"underscore still wildcard after literal", "10%", `10_`, false, true},

		// Newlines: `.` (from _/%) does not cross \n (Go RE2 default, like JS).
		{"underscore does not cross newline", "foo\nbar", "foo_bar", false, false},
		{"percent matches within a line", "bar\nfoo", "%foo%", false, true},

		// ILIKE: Unicode-aware case folding via (?i).
		{"ilike folds unicode", "CAFÉ", "café", true, true},
		{"like is case sensitive", "CAFE", "cafe", false, false},

		// Trailing backslash is now just a literal backslash (no longer invalid).
		{"trailing backslash literal", `x\`, `x\`, false, true},

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
