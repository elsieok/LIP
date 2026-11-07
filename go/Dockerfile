# syntax=docker/dockerfile:1
# One build stage, two final images:  --target router  /  --target worker
FROM golang:1 AS build
WORKDIR /src
COPY . .
# `go mod tidy` resolves dependencies on first build. After running it once
# locally, commit go.mod + go.sum so builds are reproducible.
RUN --mount=type=cache,target=/go/pkg/mod --mount=type=cache,target=/root/.cache/go-build \
    go mod tidy && \
    CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/router ./cmd/router && \
    CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/worker ./cmd/worker

FROM alpine:3 AS router
COPY --from=build /out/router /router
EXPOSE 8000
HEALTHCHECK --interval=10s --timeout=5s --start-period=10s --retries=3 \
    CMD wget -qO- http://localhost:8000/health || exit 1
ENTRYPOINT ["/router"]

FROM alpine:3 AS worker
COPY --from=build /out/worker /worker
EXPOSE 8000
HEALTHCHECK --interval=10s --timeout=5s --start-period=30s --retries=3 \
    CMD wget -qO- http://localhost:8000/health || exit 1
ENTRYPOINT ["/worker"]
