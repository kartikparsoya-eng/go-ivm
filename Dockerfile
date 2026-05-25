# Multi-stage build for the Go IVM sidecar binary.
#
# The published image (ghcr.io/kartikparsoya-eng/go-ivm-:<tag>) is
# consumed by the zero-cache Dockerfile via `COPY --from=...`; it is
# not normally run on its own. The binary itself lives at
# /usr/local/bin/go-ivm-sidecar and takes one positional arg: the
# Unix socket path to listen on.

FROM golang:1.25-alpine AS builder

WORKDIR /src

# Cache module deps separately so source-only edits don't re-download.
COPY go.mod go.sum ./
RUN go mod download

COPY . .

# modernc.org/sqlite is pure-Go, so CGO_ENABLED=0 → fully static
# binary that runs in scratch / distroless. -ldflags trim symbol
# tables and DWARF for a smaller artifact; -trimpath removes
# local build paths from the binary.
RUN CGO_ENABLED=0 GOOS=linux go build \
    -ldflags="-s -w" \
    -trimpath \
    -o /go-ivm-sidecar \
    ./cmd/sidecar

# Minimal runtime image. alpine gives us a shell + apk for debugging
# and ca-certificates for OTLP/HTTPS when tracing is enabled.
FROM alpine:3.20

RUN apk add --no-cache ca-certificates

COPY --from=builder /go-ivm-sidecar /usr/local/bin/go-ivm-sidecar

ENTRYPOINT ["/usr/local/bin/go-ivm-sidecar"]
