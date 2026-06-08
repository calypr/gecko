# syntax=docker/dockerfile:1.7
FROM golang:1.26.3-alpine3.22 AS builder
RUN apk add --no-cache git ca-certificates tzdata

ENV CGO_ENABLED=0

WORKDIR /src

COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    go mod download

COPY . .

ARG TARGETOS=linux
ARG TARGETARCH=amd64
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    GOOS="$TARGETOS" GOARCH="$TARGETARCH" \
    go build \
      -trimpath \
      -ldflags="-s -w" \
      -o /out/gecko

FROM alpine:3.22
RUN apk add --no-cache ca-certificates tzdata && \
    addgroup -S gecko && \
    adduser -S -G gecko -h /app gecko

WORKDIR /app

COPY --from=builder /out/gecko /app/gecko
COPY --from=builder /src/docs/swagger.json /app/docs/swagger.json

USER gecko
EXPOSE 8080
ENTRYPOINT ["/app/gecko"]
