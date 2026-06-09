# Multi-stage build for the Go IVM sidecar binary.
#
# The published image (ghcr.io/kartikparsoya-eng/go-ivm-:<tag>) is
# consumed by the zero-cache Dockerfile via `COPY --from=...`; it is
# not normally run on its own. The binary itself lives at
# /usr/local/bin/go-ivm-sidecar and takes one positional arg: the
# Unix socket path to listen on.

FROM golang:1.25-alpine AS builder

# Build deps: musl-dev + gcc for CGO; the vendored sqlite3.c compiles
# into the binary so it understands rocicorp's wal2 journal mode (which
# upstream SQLite — and therefore modernc.org/sqlite — does not).
RUN apk add --no-cache build-base

WORKDIR /src

# Cache module deps separately so source-only edits don't re-download.
COPY go.mod go.sum ./
RUN go mod download

COPY . .

# CGO_ENABLED=1: we now use mattn/go-sqlite3 (CGO) so internal/tablesource
# can link against rocicorp's patched SQLite.
#
# Build tags:
#   sqlite_omit_load_extension — drop the runtime extension loader
#                                (we don't load extensions; smaller binary)
#   libsqlite3                 — mattn skips its own vendored sqlite3.c
#                                and links against system libsqlite3. We
#                                make that "system" SQLite be rocicorp's
#                                amalgamation by compiling c/sqlite3/
#                                into a static library installed below.
#                                KEEP THIS: it is what gives the binary
#                                wal2-journal awareness. The manual amd64
#                                cross-compile (sandbox) links mattn's
#                                vendored sqlite instead — a deliberate
#                                local convenience, NOT the production path.
#   osusergo netgo             — force the pure-Go os/user + net resolvers
#                                instead of cgo getpwnam/getaddrinfo. On a
#                                fully static (-extldflags '-static') musl
#                                build the cgo resolvers can fail at runtime;
#                                the pure-Go ones don't. Matches the manual
#                                amd64 cross-compile so the CI image and the
#                                locally-tested binary share net/user
#                                resolution semantics.
#
# Static linking: -extldflags '-static' produces a self-contained binary
# that runs in alpine or scratch without dragging libsqlite3.so along.
# This matters because the consuming zero-cache Dockerfile copies our
# binary over via COPY --from=...; any dynamic .so would have to be
# copied separately and put on the loader path.
RUN gcc -O2 -fPIC -c c/sqlite3/sqlite3.c -o /tmp/sqlite3.o \
        -DSQLITE_THREADSAFE=2 \
        -DSQLITE_ENABLE_FTS5 \
        -DSQLITE_ENABLE_JSON1 \
        -DSQLITE_ENABLE_RTREE \
        -DSQLITE_OMIT_LOAD_EXTENSION \
        -DSQLITE_ENABLE_SNAPSHOT \
    && ar rcs /usr/lib/libsqlite3.a /tmp/sqlite3.o \
    && cp c/sqlite3/sqlite3.h /usr/include/sqlite3.h \
    && cp c/sqlite3/sqlite3ext.h /usr/include/sqlite3ext.h

RUN CGO_ENABLED=1 GOOS=linux go build \
    -tags "libsqlite3 sqlite_omit_load_extension osusergo netgo" \
    -ldflags="-s -w -extldflags '-static'" \
    -trimpath \
    -o /go-ivm-sidecar \
    ./cmd/sidecar

# Minimal runtime image. alpine gives us a shell + apk for debugging
# and ca-certificates for OTLP/HTTPS when tracing is enabled.
FROM alpine:3.20

RUN apk add --no-cache ca-certificates

COPY --from=builder /go-ivm-sidecar /usr/local/bin/go-ivm-sidecar

ENTRYPOINT ["/usr/local/bin/go-ivm-sidecar"]
