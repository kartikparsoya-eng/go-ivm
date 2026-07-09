# Build for the Go IVM in-process engine library (libgoivm.so).
#
# The published image (ghcr.io/kartikparsoya-eng/go-ivm-:<tag>) is an
# ARTIFACT CARRIER consumed by mono's Dockerfile.go-ivm via
# `COPY --from=...` / bind-mount: it carries /usr/local/lib/libgoivm.so,
# the c-shared library each zero-cache syncer worker dlopens (the NAPI
# in-process transport). The socket-transport sidecar binary was removed
# in the RPC-surface removal sweep (protocolRev 10) — cmd/sidecar's main()
# is a stub and no runnable binary ships here.
ARG GO_IVM_BUILD_SHA=unknown
ARG GO_IVM_BUILD_REF=unknown

# Stage: libgoivm.so — the c-shared library for the in-process (NAPI)
# transport. Built on BOOKWORM (glibc), NOT alpine: the consumer is the
# zero-cache image (node:22-slim, Debian/glibc) whose goivm_napi addon
# dlopen()s this .so — a musl-linked shared object will not load there.
# c-shared also cannot be fully static (-extldflags '-static' conflicts
# with -buildmode=c-shared), so glibc-matching the consumer is the whole
# game. Recipe smoke-tested 2026-07-02 against the exact node:22-slim
# runtime (dlopen + goivm_start + ping round-trip).
#
# Build tags:
#   sqlite_omit_load_extension — drop the runtime extension loader
#                                (we don't load extensions; smaller binary)
#   libsqlite3                 — mattn skips its own vendored sqlite3.c
#                                and links against system libsqlite3. We
#                                make that "system" SQLite be rocicorp's
#                                amalgamation by compiling c/sqlite3/
#                                into a static library installed below.
#                                KEEP THIS: it is what gives the library
#                                wal2-journal awareness.
#   osusergo netgo             — force the pure-Go os/user + net resolvers
#                                instead of cgo getpwnam/getaddrinfo, so the
#                                CI image and locally-tested builds share
#                                net/user resolution semantics.
#   napilib                    — compile the cgo //export ABI shims
#                                (cmd/sidecar/napi_lib.go).
FROM golang:1.25-bookworm AS libgoivm-builder
ARG GO_IVM_BUILD_SHA
ARG GO_IVM_BUILD_REF

WORKDIR /src

# Cache module deps separately so source-only edits don't re-download.
COPY go.mod go.sum ./
RUN go mod download

COPY . .

# -ffp-contract=off: the vendored fork (≤3.51) renders REAL→TEXT through
# dekkerMul2 double-double arithmetic whose results shift at the last ulp
# if the compiler fuses mul+add into FMA (baseline ISA on arm64, so the
# amd64 and arm64 images would even disagree with EACH OTHER under
# -ffp-contract=fast, gcc's -O2 default). internal/tablesource/realtext.go
# ports the unfused arithmetic bit-for-bit and TestLowerCoercionParity
# pins Go == linked-library; the flag makes that contract hold on every
# arch. Keep in lockstep with the test-wal2 CI job and build-wal2.sh.
RUN gcc -O2 -ffp-contract=off -fPIC -c c/sqlite3/sqlite3.c -o /tmp/sqlite3.o \
        -DSQLITE_THREADSAFE=2 \
        -DSQLITE_ENABLE_FTS5 \
        -DSQLITE_ENABLE_JSON1 \
        -DSQLITE_ENABLE_RTREE \
        -DSQLITE_OMIT_LOAD_EXTENSION \
        -DSQLITE_ENABLE_SNAPSHOT \
        -DSQLITE_ENABLE_WAL2_COREAD \
    && ar rcs /usr/lib/libsqlite3.a /tmp/sqlite3.o \
    && cp c/sqlite3/sqlite3.h /usr/include/sqlite3.h \
    && cp c/sqlite3/sqlite3ext.h /usr/include/sqlite3ext.h

RUN CGO_ENABLED=1 GOOS=linux go build \
    -tags "libsqlite3 sqlite_omit_load_extension osusergo netgo napilib" \
    -ldflags="-s -w" \
    -trimpath \
    -buildmode=c-shared \
    -o /libgoivm.so \
    ./cmd/sidecar

RUN printf 'go_ivm_build_sha=%s\ngo_ivm_build_ref=%s\n' \
    "$GO_IVM_BUILD_SHA" "$GO_IVM_BUILD_REF" > /libgoivm.buildinfo

# Minimal artifact-carrier image. alpine keeps a shell for inspection;
# nothing here is meant to run — the .so is glibc-linked for the
# node:22-slim consumer, which pulls it via COPY --from / bind-mount in
# mono's Dockerfile.go-ivm.
FROM alpine:3.20
ARG GO_IVM_BUILD_SHA
ARG GO_IVM_BUILD_REF

LABEL org.opencontainers.image.revision="${GO_IVM_BUILD_SHA}"
LABEL org.opencontainers.image.source-ref="${GO_IVM_BUILD_REF}"

COPY --from=libgoivm-builder /libgoivm.so /usr/local/lib/libgoivm.so
COPY --from=libgoivm-builder /libgoivm.buildinfo /usr/local/lib/libgoivm.buildinfo
