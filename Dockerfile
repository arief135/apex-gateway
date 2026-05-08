# ── Stage 1: Build ────────────────────────────────────────────────────────
FROM golang:1.22-alpine AS builder

RUN apk add --no-cache git ca-certificates tzdata

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download

COPY . .

# Build a statically-linked binary
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
    go build -ldflags="-w -s -X main.buildTime=$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
    -o /bin/gateway ./cmd/gateway

# ── Stage 2: Runtime ──────────────────────────────────────────────────────
FROM scratch

# Pull in CA certs and timezone data from builder
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=builder /usr/share/zoneinfo /usr/share/zoneinfo

# Copy the binary and default config
COPY --from=builder /bin/gateway /gateway
COPY config.yaml /config.yaml

EXPOSE 8080 8443 9090

ENTRYPOINT ["/gateway", "-config", "/config.yaml"]
