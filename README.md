# TradePulse

> A high-throughput, event-driven crypto trade-analytics pipeline in Go —
> six independent microservices over Kafka, RabbitMQ, Redis and ClickHouse.

TradePulse ingests live trades from a crypto exchange, streams them through Kafka
for processing and analytics, serves real-time data to clients over REST and
WebSocket, and dispatches whale/liquidation alerts through RabbitMQ — exactly
once. It's built to demonstrate production-grade Go: worker pools, fan-out,
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
├── deployments/              # docker-compose (profiled), prometheus, grafana, clickhouse schema
├── docs/                     # ARCHITECTURE.md + CONTRIBUTING.md
├── .github/workflows/        # CI (PR gate) + Release (tag → GHCR)
├── go.work                   # ties the modules together for local dev
└── Makefile                  # build-all / test / lint / run / ...
```

**Why a multi-module monorepo + `go.work`?** Each service is its own module so it
versions and builds independently (and its Docker image only pulls its own deps).
`go.work` stitches them together so local edits to `shared/` are picked up
instantly without publishing. CI deliberately runs with `GOWORK=off` so it
validates each module exactly as `docker build` does — through its `go.mod` +
`replace`, not the workspace. See [docs/CONTRIBUTING.md](docs/CONTRIBUTING.md).

---

## Quickstart

Requires **Go 1.24+**, **Docker**, and (for linting) **golangci-lint v2**.

```bash
# 1. Build every service binary into ./bin
make build-all

# 2. Run the full local quality gate (what CI runs)
make ci            # gofmt + vet + race tests + build

# 3. Bring up the core backbone (Kafka + Zookeeper + Redis)
make run           # == docker compose -f deployments/docker-compose.yml up -d

# 4. Run a single service locally against that backbone
./bin/fx-rate-service           # uses localhost defaults; override with TRADEPULSE_* env
curl localhost:8086/health

# 5. Or run the entire stack (infra + all six services) in containers
make up-full       # docker compose --profile full up -d --build
make logs
make down
```

Infra is staged behind compose **profiles** so the default `up` stays light:

| Command | Brings up |
|---|---|
| `make run` | kafka, zookeeper, redis (Sprint 0/1 backbone) |
| `docker compose --profile analytics up -d` | + ClickHouse (Sprint 3) |
| `docker compose --profile alerts up -d` | + RabbitMQ (Sprint 6) |
| `docker compose --profile observability up -d` | + Prometheus + Grafana (Sprint 4/6) |
| `make up-full` | everything, services built from source |

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

- **Sprint 0 — Foundation: ✅ done.** Monorepo, shared domain contract, uniform
  service bootstrap (config → logging → health/metrics → graceful shutdown),
  docker-compose, Makefile, CI/CD. Every service boots, serves `/health` and
  `/metrics`, and shuts down cleanly on SIGTERM.
- **Sprint 1+ — in progress.** Domain logic lands per the plan; each service's
  `internal/service.go` documents exactly which files arrive in which sprint.

---

## License

TBD.
