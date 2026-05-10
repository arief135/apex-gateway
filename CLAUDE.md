# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```bash
# Run locally (requires Postgres + Redis running)
go run ./cmd/gateway -config config.yaml      # or ./start.sh

# Build static binary
go build -o bin/gateway ./cmd/gateway

# Build Docker image
docker build -t apex-gateway .

# Module hygiene / static analysis (no test suite or Makefile exists yet)
go build ./...
go vet ./...
```

The repo ships `example.docker-compose.yml` but no `docker-compose.yml` — to use compose locally, copy/rename it first. Likewise `example.config.yaml` is the canonical template; `config.yaml` is the active config.

`go.mod` declares `go 1.23`, but the `Dockerfile` pins `golang:1.22-alpine`. If you bump language features that need 1.23, update the Dockerfile too.

## Architecture

Single Go binary (`cmd/gateway/main.go`) that wires four independent subsystems behind two HTTP servers:

- **Data plane** (`:8080` HTTP, optionally `:8443` HTTPS) — `internal/gateway/proxy.go` is the sole `http.Handler`. It calls `Router.Match` → applies the middleware chain → dispatches to the per-protocol handler.
- **Control plane** (`:9090`) — `internal/admin` exposes the JWT-protected REST API for routes/logs/stats/health. Mutations go through `invalidateAndReload`, which busts the Redis cache and forces an immediate `Router.Reload`.

### Route lifecycle (important invariant)

Routes have **two sources of truth that must stay in sync**: PostgreSQL (durable) and Redis (hot cache, 30s TTL). The in-memory `Router.routes` slice is a third copy refreshed on a ticker (`route_config_ttl`, default 30s) — so a stale gateway pod can serve traffic against an out-of-date route table for up to one TTL window unless an admin mutation triggers `invalidateAndReload`. **Any new admin handler that mutates routes must call `h.invalidateAndReload(ctx)`** or the change won't propagate until the next tick.

Reload order is: Redis cache → fall back to Postgres → repopulate Redis. See `gateway/router.go:reload`.

### Protocol dispatch

`Proxy.dispatchHandler` switches on `route.Protocol`:
- `HTTP`/`HTTPS` → `httputil.NewSingleHostReverseProxy` with a custom `Director` that handles `StripPrefix`, header injection, and standard `X-Forwarded-*` headers.
- `WebSocket` → `gorilla/websocket` upgrade + bidirectional goroutine relay. Only `Authorization`, `Cookie`, `X-Request-ID` are propagated to upstream by default.
- `gRPC` → reverse proxy with `ForceAttemptHTTP2` and `FlushInterval: -1` for streaming. `grpc://`/`grpcs://` schemes are rewritten to `http://`/`https://`.
- `MQTT` is declared as a constant but **has no handler** — falls through to HTTP.

### Middleware chain

`MiddlewareChain.Wrap` composes (outer → inner): `requestID` → `rateLimit` → `circuitBreaker` → `logging` → handler.

- **Rate limit**: Redis sliding-window via sorted sets, keyed by `routeID:clientIP`, 1-second window. Fails open on Redis errors.
- **Circuit breaker**: simple counter in Redis with a 30s TTL; opens at 10 5xx failures (`circuitBreakerThreshold` is hardcoded in `middleware.go`). Reset by any non-5xx response.
- **Logging**: captures status/size always; captures full request/response bodies (capped at 64KB) only when `route.LogPayload=true`. Always pushes to the in-memory `RingBuffer` (10k entries) — the admin `/logs` endpoint serves from this buffer, **not** Postgres.

### Traffic logger

`logger.TrafficLogger` is dual-sink: every entry goes to the ring buffer; if `kafka.enabled=true` it also async-writes to Kafka. There is **no Kafka consumer in this repo** — Kafka is fire-and-forget; durable storage is whatever consumes that topic externally. ClickHouse appears in `example.docker-compose.yml` but has no integration code yet.

`RouteStore.InsertTrafficLog` exists but is **not called from the hot path**; it's a fallback hook for whoever wires Postgres-as-sink.

## Configuration

YAML-first with `GATEWAY_*` env overrides applied last (see `applyEnv` in `internal/config/config.go`). Defaults are also defined there — the YAML file can be partial. Important env vars: `GATEWAY_POSTGRES_DSN`, `GATEWAY_REDIS_ADDR`, `GATEWAY_JWT_SECRET`, `GATEWAY_KAFKA_ENABLED`.

JWT auth on `/admin/*` (except `/admin/health`) uses HMAC; the secret comes from `admin.jwt_secret` / `GATEWAY_JWT_SECRET`. CORS is currently `*` — tighten before shipping.

## Schema

`RouteStore.Migrate` runs the inline `schema` constant in `internal/store/postgres.go` on startup (idempotent `CREATE TABLE IF NOT EXISTS`). There is no migration tool — schema changes mean editing that constant. `methods` and `headers` are stored as JSONB; route handlers marshal/unmarshal them in `scanRoute`.
