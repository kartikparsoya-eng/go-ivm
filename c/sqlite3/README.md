# Vendored SQLite (rocicorp/zero-sqlite3 patched)

Source: <https://github.com/rocicorp/zero-sqlite3>
NPM package: `@rocicorp/zero-sqlite3@1.0.17`
Vendored from: `node_modules/@rocicorp/zero-sqlite3/deps/sqlite3/`
SQLite base version: **3.51.0**

## Why a custom SQLite?

The TS side of zero-cache (`@rocicorp/zero-sqlite3`) writes the replica
in `journal_mode=wal2` — a rocicorp-specific patch to SQLite that uses
TWO WAL files alternately so writers never stall during checkpoint.

Upstream SQLite (and therefore upstream Go drivers like
`modernc.org/sqlite`) doesn't understand the wal2 header flag and
errors with `SQLITE_NOTADB` (code 26) when opening the file.

To read the replica from Go, we must use the same SQLite library the
writer uses. Vendoring the amalgamation here lets the Go sidecar's
`mattn/go-sqlite3` link against rocicorp's patched build via CGO,
producing a binary that opens the file natively.

## What's here

| File | Purpose |
|---|---|
| `sqlite3.c` | SQLite amalgamation (~269K LOC of C) with rocicorp's wal2 patch |
| `sqlite3.h` | Public header |
| `sqlite3ext.h` | Loadable-extension API |

`shell.c` (the `sqlite3` CLI) from the upstream package is intentionally
NOT vendored — we don't ship a CLI.

## Updating

To bump the SQLite version, update `@rocicorp/zero-sqlite3` in
`mono/packages/zero-cache/package.json`, then re-copy:

```sh
cp mono/node_modules/@rocicorp/zero-sqlite3/deps/sqlite3/{sqlite3.c,sqlite3.h,sqlite3ext.h} \
   go-ivm/c/sqlite3/
```

Then re-run `go test ./internal/tablesource/` and the soak to confirm
nothing regressed.

## License

SQLite itself is public domain. Rocicorp's patches inherit the SQLite
license (public domain) per their repo.
