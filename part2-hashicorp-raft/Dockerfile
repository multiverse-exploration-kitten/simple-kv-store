# syntax=docker/dockerfile:1

# Build a static binary. The golang image sets GOTOOLCHAIN=local, so the build
# never downloads another Go version.
FROM golang:1.25.14-bookworm AS builder

WORKDIR /src

# Modules first: this layer changes only when go.mod or go.sum changes.
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod \
    go mod download

COPY cmd ./cmd
COPY internal ./internal
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/kvstore ./cmd/kvstore

# Empty mount points. A new named volume copies the owner of its mount point
# from the image, so the volumes belong to nonroot (uid 65532) too.
RUN mkdir -p /out/data /out/certs

# No shell and no package manager. Runs as nonroot (uid 65532).
FROM gcr.io/distroless/static-debian12:nonroot

COPY --from=builder /out/kvstore /kvstore
COPY --from=builder --chown=65532:65532 /out/data /data
COPY --from=builder --chown=65532:65532 /out/certs /certs

# 7000: Raft traffic between the nodes (mutual TLS). 8000: HTTP API.
EXPOSE 7000 8000

ENTRYPOINT ["/kvstore"]
CMD ["serve"]
