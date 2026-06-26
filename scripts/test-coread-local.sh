#!/bin/bash
# test-coread-local.sh — Run wal2 coread Go tests locally on macOS.
#
# The blocker: mattn/go-sqlite3 hardcodes -L/opt/homebrew/opt/sqlite/lib -lsqlite3
# in its cgo directives, linking against Homebrew's stock SQLite which lacks the
# sqlite3_wal2_coread_* symbols. This script works around it by:
#   1. Building a patched libsqlite3.dylib from our vendored sqlite3.c
#   2. Creating a local copy of mattn with edited LDFLAGS pointing to our dylib
#   3. Adding a temporary `replace` directive to go.mod
#   4. Running the tests
#   5. Removing the replace directive (always, via trap)
#
# Usage:  bash scripts/test-coread-local.sh
#         bash scripts/test-coread-local.sh -race   # with race detector
set -uo pipefail

GO_IVM="$(cd "$(dirname "$0")/.." && pwd)"
SQLITE_SRC="$GO_IVM/c/sqlite3/sqlite3.c"
SQLITE_HDR="$GO_IVM/c/sqlite3/sqlite3.h"
SQLITE_EXT_HDR="$GO_IVM/c/sqlite3/sqlite3ext.h"
TMP_LIB="/tmp/coread-lib"
TMP_MATTN="/tmp/mattn-local"
RACE_FLAG="${1:-}"

# ── Step 1: Build patched dylib ────────────────────────────────────────
echo "=== Building patched libsqlite3.dylib ==="
mkdir -p "$TMP_LIB"
cc -dynamiclib -O2 -fPIC \
  -DSQLITE_THREADSAFE=2 \
  -DSQLITE_ENABLE_FTS5 \
  -DSQLITE_ENABLE_JSON1 \
  -DSQLITE_ENABLE_RTREE \
  -DSQLITE_OMIT_LOAD_EXTENSION \
  -DSQLITE_ENABLE_SNAPSHOT \
  -DSQLITE_ENABLE_WAL2_COREAD \
  -o "$TMP_LIB/libsqlite3.dylib" \
  "$SQLITE_SRC" \
  -lpthread
echo "  done"

# ── Step 2: Set up local mattn fork with edited LDFLAGS ────────────────
echo "=== Setting up local mattn fork ==="
MATTN_CACHE="$(go env GOMODCACHE)/github.com/mattn/go-sqlite3@v1.14.44"
if [ ! -d "$TMP_MATTN" ]; then
  cp -R "$MATTN_CACHE/" "$TMP_MATTN/"
  chmod -R u+w "$TMP_MATTN/"
fi
# Edit LDFLAGS to point to our patched dylib + headers
sed -i.bak 's|#cgo darwin,arm64 LDFLAGS:.*|#cgo darwin,arm64 LDFLAGS: -L'"$TMP_LIB"' -lsqlite3|' "$TMP_MATTN/sqlite3_libsqlite3.go"
sed -i.bak2 's|#cgo darwin,arm64 CFLAGS:.*|#cgo darwin,arm64 CFLAGS:  -I'"$GO_IVM"'/c/sqlite3|' "$TMP_MATTN/sqlite3_libsqlite3.go"
rm -f "$TMP_MATTN/sqlite3_libsqlite3.go.bak" "$TMP_MATTN/sqlite3_libsqlite3.go.bak2"
echo "  done"

# ── Step 3: Add replace directive ──────────────────────────────────────
echo "=== Adding replace directive to go.mod ==="
cd "$GO_IVM"
if ! grep -q "replace github.com/mattn/go-sqlite3" go.mod; then
  echo "" >> go.mod
  echo "replace github.com/mattn/go-sqlite3 v1.14.44 => $TMP_MATTN" >> go.mod
fi

# ── Cleanup trap (always remove replace) ──────────────────────────────
cleanup() {
  echo "=== Removing replace directive from go.mod ==="
  sed -i.tmp '/^replace github.com\/mattn\/go-sqlite3/d' go.mod
  # Remove trailing blank line if any
  sed -i.tmp2 -e :a -e '/^\n*$/{$d;N;ba' -e '}' go.mod
  rm -f go.mod.tmp go.mod.tmp2
}
trap cleanup EXIT

# ── Step 4: Run tests ──────────────────────────────────────────────────
echo "=== Running coread tests (tablesource primitives) ==="
go test -tags "libsqlite3 sqlite_omit_load_extension" $RACE_FLAG -count=1 -v \
  ./internal/tablesource/ \
  -run 'TestCoRead' -timeout 60s 2>&1

echo ""
echo "=== Decision-level fallback test (cmd/sidecar buildReaderPoolLocked) ==="
# Proves the coread-fast / converge-fallback wiring: on a plain-wal (non-wal2)
# anchor the co-read capture errors and buildReaderPoolLocked falls through to
# NewReaderPool (coread==nil, pool!=nil). Companion to TestCoRead_NonWal2Errors.
go test -tags "libsqlite3 sqlite_omit_load_extension" $RACE_FLAG -count=1 -v \
  ./cmd/sidecar/ \
  -run 'TestBuildReaderPool_FallsBackOnNonWal2' -timeout 60s 2>&1

echo ""
echo "=== Regression check: existing reader pool tests ==="
go test -tags "libsqlite3 sqlite_omit_load_extension" -count=1 -v \
  ./internal/tablesource/ \
  -run 'TestReaderPool|TestSnapshot|TestDB' -timeout 60s 2>&1

echo ""
echo "=== DONE ==="
