# TradePulse

> A high-throughput, event-driven crypto trade-analytics pipeline in Go —
> six independent microservices over Kafka, RabbitMQ, Redis and ClickHouse.

TradePulse ingests live trades from a crypto exchange, streams them through Kafka
for processing and analytics, serves real-time data to clients over REST and
WebSocket, and dispatches whale/liquidation alerts through RabbitMQ —
effectively once (at-least-once delivery made safe by idempotent, Redis-deduped
consumers). It's built to demonstrate production-grade Go: worker pools, fan-out,
graceful shutdown, circuit breakers, idempotent consumers, and broker-based
decoupling.

> **Work in progress — the paragraph above is the target design, not today's
> state.** Three services are built and running (`ingestion`, `processor`,
> `api`), one is partial (`fx-rate`), and two are boot-only scaffolds that serve
> `/health` and nothing else (`analytics`, `notification`). The WebSocket push,
> ClickHouse writes, RabbitMQ alert path and circuit breaker are designed but
> not built yet. Per-service state is in the table below; per-task state in
> [SPRINT_PLAN.md](SPRINT_PLAN.md).

- **Design source of truth:** [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md)
- **Delivery plan (status per sprint):** [SPRINT_PLAN.md](SPRINT_PLAN.md)

---

## Architecture at a glance

```
Binance WS ─▶ ingestion ─▶ Kafka(trades.raw) ─┬─▶ processor ─▶ Redis ─▶ api-service ─▶ REST clients
                                              ╎        ╎                     ╎╌▶ WebSocket clients
                                              ╎        ╎╌▶ RabbitMQ ╌▶ notification ╌▶ Telegram/Webhook/Email
                                              ╎                              ╎
                                              ╎╌▶ analytics ╌▶ ClickHouse ╌╌╌╯

              fx-rate ╌▶ (poll FX provider 60s) ╌▶ Redis(fx:rates) ╌▶ api /convert

  ─▶  built and running today          ╌▶  designed, not built yet
```

**The one decision to understand first — Kafka *and* RabbitMQ:**
trade **events** go on Kafka because *every* consumer must see them (processor
and analytics each consume every trade, independently, via separate consumer
groups — fan-out). Alert **commands** go on RabbitMQ because *exactly one*
notifier must act on them (consumed-once). Right tool, right job — see
[§ Why Kafka AND RabbitMQ](docs/ARCHITECTURE.md#why-kafka-and-rabbitmq).

*Reads/Writes describe the target design. **State** is what exists on this
branch today.*

| # | Service | Responsibility | Reads | Writes | Ops port (host) | State |
|---|---------|----------------|-------|--------|-----------------|-------|
| 1 | `ingestion-service` | Exchange WS → normalize → Kafka | Binance WS | Kafka `trades.raw` | 8081 | **Built** |
| 2 | `processor-service` | Consume, enrich, order book, whale detect | Kafka | Redis, RabbitMQ | 8082 | **Built**, minus whale detect → RabbitMQ |
| 3 | `analytics-service` | Candles/VWAP (independent consumer group) | Kafka | ClickHouse, RabbitMQ | 8083 | **Scaffold** — boots, `/health`, no consumer |
| 4 | `api-service` | REST + WebSocket to clients | Redis, ClickHouse | WS clients | 8080 | **Built** for REST-from-Redis; no WebSocket, no ClickHouse |
| 5 | `notification-service` | Consume alerts, send once | RabbitMQ, Redis | Telegram/Webhook/Email | 8085 | **Scaffold** — boots, `/health`, no consumer |
| 6 | `fx-rate-service` | Poll FX provider, cache fiat rates | FX HTTP API | Redis `fx:rates` | 8086 | **Partial** — poll loop + backoff live; provider call is a no-op stub |

Every service exposes `GET /health` and `GET /metrics` on its ops port — the
scaffolds included. Endpoints that exist today: `GET /api/v1/health`,
`/api/v1/trades/{symbol}`, `/api/v1/orderbook/{symbol}` on api-service.

---

## Repository layout

```
.
├── services/                 # one Go module per deployable service
│   ├── ingestion-service/    #   cmd/main.go (bootstrap) + internal/ (logic) + Dockerfile + go.mod
│   ├── processor-service/
│   ├── analytics-service/
│   ├── api-service/          #   + internal/{rest,ws,middleware}
│   ├── notification-service/
│   └── fx-rate-service/
├── shared/                   # one module imported by all services (the contract)
│   ├── domain/               #   TradeEvent, Candle, alerts, FXRates + topic/queue/key names
│   ├── config/               #   Viper loader (env + YAML)
│   ├── log/                  #   zerolog setup
│   ├── httpserver/           #   /health + /metrics + graceful shutdown
│   ├── runtime/              #   signal-aware ctx + errgroup process lifecycle
│   └── version/              #   build metadata (ldflags)
├── developments/             # docker-compose (profiled), prometheus, grafana
├── docs/                     # ARCHITECTURE.md + CONTRIBUTING.md
├── SPRINT_PLAN.md            # delivery plan, sprint by sprint
├── go.work                   # ties the modules together for local dev
└── Makefile                  # ci / build-all / dev (live reload) / up / down
```

**Why a multi-module monorepo + `go.work`?** Each service is its own module so it
versions and builds independently (and its Docker image only pulls its own deps).
`go.work` stitches them together so local edits to `shared/` are picked up
instantly without publishing. CI deliberately runs with `GOWORK=off` so it
validates each module exactly as `docker build` does —
through its `go.mod` + `replace`, not the workspace. See
[docs/CONTRIBUTING.md](docs/CONTRIBUTING.md).

---

## Quickstart

Requires **Go 1.24+**, **Docker**, **golangci-lint v2**, and (for live reload)
**[air](https://github.com/air-verse/air)**.

```bash
# 1. Register all modules in the go.work workspace
make sync

# 2. Run the full local quality gate
make ci            # fmt-check + vet + lint + race tests + build-all (into ./bin)

# 3. Bring up the core backbone (Kafka + Zookeeper + Redis)
make up            # == docker compose -f developments/docker-compose.yml up -d

# 4. Run a single service locally against that backbone
make run s=fx-rate-service      # uses localhost defaults; override with TRADEPULSE_* env
curl localhost:8086/health

# 5. Or develop one service with live reload
make dev s=api-service

make logs          # tail the compose stack
make down          # tear it down
```

Infra is staged behind compose **profiles** so the default `up` stays light:

| Command | Brings up |
|---|---|
| `make up` | kafka, zookeeper, redis (Sprint 0/1 backbone) |
| `docker compose --profile analytics up -d` | + ClickHouse (Sprint 3) |
| `docker compose --profile alerts up -d` | + RabbitMQ (Sprint 6) |
| `docker compose --profile observability up -d` | + Prometheus + Grafana (Sprint 4/6) |
| `make up-full` | everything, services built from source |

`make help` lists every target and the auto-discovered services.

---

## Configuration

All config is 12-factor: env vars (prefix `TRADEPULSE_`) override an optional
`config.yaml` override built-in defaults. Examples:

```bash
TRADEPULSE_ENV=prod              # prod => JSON logs; dev => console logs
TRADEPULSE_LOG_LEVEL=debug
TRADEPULSE_HTTP_ADDR=:8080
TRADEPULSE_KAFKA_BROKERS=kafka:29093
TRADEPULSE_REDIS_ADDR=redis:6379
TRADEPULSE_FX_PROVIDER=exchangerate.host
```

The full schema lives in [shared/config/config.go](shared/config/config.go).

---

## Status

Built sprint-by-sprint per [SPRINT_PLAN.md](SPRINT_PLAN.md), which tracks it
task by task. Where the code stands on this branch:

| Sprint | Scope | State |
|---|---|---|
| 0 | Foundation: multi-module monorepo + `go.work`, `shared/` (domain, config, log, httpserver, runtime, retry), uniform bootstrap, compose backbone, `make ci` gate, CI + release workflows | **Done** |
| 1 | `ingestion-service`: Binance WS → normalize → Kafka, per-symbol workers, backoff reconnect, `/health` | **Code complete** (live-deliverable verification pending) |
| 2 | `processor-service`: consumer group → worker pool → fan-out → enrich → order book → Redis; api-service REST reads + consumer-lag health | **Done** |
| 3 | `analytics-service`: candles/VWAP → ClickHouse | **Not started** — boot-only scaffold |
| 4 | `api-service` WebSocket push + Prometheus/Grafana | **Not started** |
| 5 | `fx-rate-service` provider HTTP + circuit breaker + `/convert` | **Partial** — poll loop and jittered backoff live; provider request, Redis write and `/convert` not wired |
| 6 | Whale/liquidation detection → RabbitMQ → `notification-service` dispatch | **Not started** — boot-only scaffold |
| 7 | Hardening + load proof | **Not started** |

Every service boots, serves `/health` and `/metrics`, and shuts down cleanly on
SIGTERM — including the two scaffolds, which log
`skeleton — no consumer wired yet` and idle until shutdown. Each service's
`internal/service.go` documents which files arrive in which sprint.

---

## License

TBD.
