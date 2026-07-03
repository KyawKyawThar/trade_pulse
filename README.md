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

- **Design source of truth:** [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md)
- **Delivery plan (status per sprint):** [SPRINT_PLAN.md](SPRINT_PLAN.md)

---

## Architecture at a glance

```
Binance WS ─▶ ingestion ─▶ Kafka(trades.raw) ─┬─▶ processor ─▶ Redis ─┐
                                               │       │                ├─▶ api-service ─▶ REST + WebSocket clients
                                               │       └─▶ RabbitMQ ─▶ notification ─▶ Telegram/Webhook/Email
                                               └─▶ analytics ─▶ ClickHouse ┘
                              fx-rate ─▶ (poll FX provider 60s) ─▶ Redis(fx:rates) ─▶ api /convert
```

**The one decision to understand first — Kafka *and* RabbitMQ:**
trade **events** go on Kafka because *every* consumer must see them (processor
and analytics each consume every trade, independently, via separate consumer
groups — fan-out). Alert **commands** go on RabbitMQ because *exactly one*
notifier must act on them (consumed-once). Right tool, right job — see
[§ Why Kafka AND RabbitMQ](docs/ARCHITECTURE.md#why-kafka-and-rabbitmq).

| # | Service | Responsibility | Reads | Writes | Ops port (host) |
|---|---------|----------------|-------|--------|-----------------|
| 1 | `ingestion-service` | Exchange WS → normalize → Kafka | Binance WS | Kafka `trades.raw` | 8081 |
| 2 | `processor-service` | Consume, enrich, order book, whale detect | Kafka | Redis, RabbitMQ | 8082 |
| 3 | `analytics-service` | Candles/VWAP (independent consumer group) | Kafka | ClickHouse, RabbitMQ | 8083 |
| 4 | `api-service` | REST + WebSocket to clients | Redis, ClickHouse | WS clients | 8080 |
| 5 | `notification-service` | Consume alerts, send once | RabbitMQ, Redis | Telegram/Webhook/Email | 8085 |
| 6 | `fx-rate-service` | Poll FX provider, cache fiat rates | FX HTTP API | Redis `fx:rates` | 8086 |

Every service exposes `GET /health` and `GET /metrics` on its ops port.

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
├── docs/                     # ARCHITECTURE.md + CONTRIBUTING.md
├── SPRINT_PLAN.md            # delivery plan, sprint by sprint
├── go.work                   # ties the modules together for local dev
└── Makefile                  # build-all / dev (live reload) / run / tidy
```

**Why a multi-module monorepo + `go.work`?** Each service is its own module so it
versions and builds independently (and its Docker image only pulls its own deps).
`go.work` stitches them together so local edits to `shared/` are picked up
instantly without publishing. CI (landing in Sprint 0) deliberately runs with
`GOWORK=off` so it validates each module exactly as `docker build` does —
through its `go.mod` + `replace`, not the workspace. See
[docs/CONTRIBUTING.md](docs/CONTRIBUTING.md).

---

## Quickstart

Requires **Go 1.24+** and (for live reload) **[air](https://github.com/air-verse/air)**.

```bash
# 1. Register all modules in the go.work workspace
make sync

# 2. Build every service binary (into services/<name>/tmp/)
make build-all

# 3. Run a single service locally
make run s=fx-rate-service      # uses localhost defaults; override with TRADEPULSE_* env
curl localhost:8086/health

# 4. Or develop one service with live reload
make dev s=api-service

# 5. Per-module hygiene
make tidy s=analytics-service
```

`make help` lists every target and the auto-discovered services. The
docker-compose infra stack (Kafka + Zookeeper + Redis, staged behind compose
profiles for ClickHouse / RabbitMQ / Prometheus + Grafana) and the CI pipeline
land as the remaining Sprint 0 tasks — see [SPRINT_PLAN.md](SPRINT_PLAN.md).

---

## Configuration

All config is 12-factor: env vars (prefix `TRADEPULSE_`) override an optional
`config.yaml` override built-in defaults. Examples:

```bash
TRADEPULSE_ENV=prod              # prod => JSON logs; dev => console logs
TRADEPULSE_LOG_LEVEL=debug
TRADEPULSE_HTTP_ADDR=:8080
TRADEPULSE_KAFKA_BROKERS=kafka:29092
TRADEPULSE_REDIS_ADDR=redis:6379
TRADEPULSE_FX_PROVIDER=exchangerate.host
```

The full schema lives in [shared/config/config.go](shared/config/config.go).

---

## Status

This repository is being built sprint-by-sprint per [SPRINT_PLAN.md](SPRINT_PLAN.md).

- **Sprint 0 — Foundation: in progress.** Done so far: multi-module monorepo +
  `go.work`, shared domain contract (`shared/domain`), config/logging/runtime
  packages, uniform service bootstrap (config → logging → health/metrics →
  graceful shutdown), Makefile with per-service build/run/live-reload. Every
  service boots, serves `/health` and `/metrics`, and shuts down cleanly on
  SIGTERM. Remaining: docker-compose infra stack and CI/CD workflows.
- **Sprint 1+ — upcoming.** Domain logic lands per the plan; each service's
  `internal/service.go` documents exactly which files arrive in which sprint.

---

## License

TBD.
