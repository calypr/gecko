FROM golang:1.26.2-alpine AS builder
RUN apk add --no-cache git build-base ca-certificates tzdata

ENV CGO_ENABLED=0 \
    GOOS=linux \
    GOARCH=amd64

WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .

ARG GITCOMMIT=unknown
ARG GITVERSION=unknown
RUN resolved_commit="$GITCOMMIT"; \
    resolved_version="$GITVERSION"; \
    if [ "$resolved_commit" = "unknown" ] && git rev-parse HEAD >/dev/null 2>&1; then \
      resolved_commit="$(git rev-parse HEAD)"; \
    fi; \
    if [ "$resolved_version" = "unknown" ] && git describe --always --tags >/dev/null 2>&1; then \
      resolved_version="$(git describe --always --tags)"; \
    fi; \
    go build \
      -trimpath \
      -ldflags="-s -w -X 'github.com/calypr/gecko/internal/server/version.GitCommit=${resolved_commit}' -X 'github.com/calypr/gecko/internal/server/version.GitVersion=${resolved_version}'" \
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
