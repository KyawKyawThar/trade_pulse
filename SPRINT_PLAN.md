# Sprint Plan — TradePulse: Real-Time Crypto Trade Analytics Pipeline

**Mode:** Solo developer · 1-week sprints · MVP-first
**MVP target (Definition of Done):** the *first end-to-end demonstrable pipeline* — a live Binance trade flows ingestion → Kafka → processor → Redis → real-time WebSocket push; a whale-sized trade flows processor → RabbitMQ → notification → Telegram, exactly once; and a client can convert any symbol's live price into a fiat quote (`/convert?quote=EUR`) off the cached FX rate. The analytics (candles/VWAP in ClickHouse), alert path, and conversion are all queryable/observable. All **6 services** participate.
**Source of truth:** [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) — section references (§) below point into it (e.g. § *Why Kafka AND RabbitMQ*, *Decision 4*, *Pattern 5*, *Service 6*, *Phase 1*).
**Path to GA:** the *Production Hardening* items (§ Phase 4) are baked in as non-negotiables from the first event-emitting sprint — graceful shutdown, reconnection/backoff, idempotent dedup, drop-slow-clients, serve-stale on upstream failure — not retrofitted. Full hardening + load proof runs as a gated epic (Sprint 7) after the MVP.

> **Sequencing principle.** The architecture is broker-decoupled (§ *Inter-Service Communication*: "No direct HTTP calls between services"). So the *shared domain types and Kafka topic contract come first* — every later service is a producer/consumer on a backbone that already exists. Build the unglamorous ingestion → store → serve spine before the WebSocket and alert demos. Don't skip the foundation to show the dashboard.

---

## Velocity & scope assumptions (solo)

- ~1 service reaches "vertical slice working" per sprint; the two heaviest (processor, api) each get a dedicated sprint; the lightweight, independent fx-rate-service gets a short sprint of its own.
- Each sprint ends with: green CI (`go vet` + `golangci-lint` + `go test ./...`), the slice runnable via `docker-compose up`, a green `/api/v1/health`, and updated docs.
- Infra (Kafka, Zookeeper, RabbitMQ, Redis, ClickHouse, Prometheus, Grafana) runs locally via `docker-compose` from Sprint 0 — no cloud/K8s until post-MVP.
- Reconnection and graceful-shutdown semantics are baked into the first sprint that owns a long-lived connection, not retrofitted (§ Phase 4, *Pattern 1* context-cancellation).

**MVP = Sprints 0–6 (≈7 weeks).** § Phase 4 (production hardening) is mapped as a post-MVP gated sprint at the end.

---

## Datastore & broker roles (why each one exists)

Stores and brokers are split by **access pattern**, not by service. Don't reach for the wrong one when a sprint adds state (§ *Why Kafka AND RabbitMQ*, *Key Engineering Decisions*).

| System | Type | Holds | Why it, not the others |
|---|---|---|---|
| **Kafka** | Event log / transport | High-volume trade + order-book events in flight; one topic per type (`trades.raw`, `orderbook.raw`, `candles`), partitioned by symbol | Fan-out to *independent* consumer groups (processor **and** analytics both see every trade) + replayable by offset for backtests. Days/weeks retention, millions/sec (§ *Why Kafka AND RabbitMQ*, *Decision 1*). |
| **RabbitMQ** | Work queue | One-time alert **commands**: `whale.alerts`, `liquidations`, `price.alerts` on the `alerts` exchange | Consumed-once semantics — exactly **one** notification instance sends the email/Telegram. Per-message ack/nack + dead-letter; a task queue, not a log (§ *Decision 1*, *Pattern 6*). |
| **Redis** | Cache | Live order-book snapshots, latest trade + live price per symbol (`price:<symbol>`), cached fiat FX rates (`fx:rates`, TTL 5m), whale-alert dedup keys, rate-limiter token buckets | Sub-ms reads for the API hot path; isolates the slow external FX provider from the request path (serve last-good under TTL); dedup TTL keys; never the source of truth (§ *Decision 4*, *Pattern 4*, *Service 6*). |
| **ClickHouse** | OLAP / columnar | Append-only historical candles + trades; analytics projections | Columnar — VWAP over 100M rows reads only `price`/`qty` columns, 10–100× faster than row-oriented PG, 80% compression (§ *Decision 3*). |

**Rule of thumb:** *should every consumer see this message? → Kafka. Should exactly one? → RabbitMQ. Hot read / dedup / rate-limit → Redis. Wide historical scan → ClickHouse.* No direct service-to-service HTTP; share via brokers or read APIs (§ *Inter-Service Communication*).

---

### Sprint 0 — Foundation & scaffolding (Week 0, half-sprint) — IN PROGRESS

**Goal:** A workspace that builds, with local infra and CI, before any domain logic.

| # | Task | Est | Status |
|---|---|---|---|
| 1 | Monorepo layout: `services/{ingestion,processor,analytics,api,notification,fx-rate}`, `shared/`, `developments/`, `docs/` (§ *Directory Structure*) | 3h | ✅ DONE |
| 2 | `shared/domain` module: `TradeEvent`, `OrderBook`, `Candle`, `WhaleAlert`, `LiquidationAlert`, `FXRates` — defined once, imported by all 6 services (§ *Decision 6*) | 5h | ✅ DONE |
| 3 | `developments/docker-compose.yml`: Kafka + Zookeeper + Redis (the Sprint-1 backbone); RabbitMQ/ClickHouse/Prometheus/Grafana staged behind compose profiles for their sprints | 4h | ✅ DONE |
| 4 | Root `Makefile`: `make build-all`, `make test`, `make run`, `make lint` (+ `fmt`, `vet`, `ci`, `up`/`down`/`logs`) | 2h | ✅ DONE |
| 5 | CI: `go vet` + `golangci-lint` + `go test ./...` on PR; pin Go 1.24+ and all module versions | 4h | TODO |
| 6 | Config + logging skeleton: Viper (env + YAML) and zerolog wired into a stub `main.go` per service (§ *Tech Stack*) | 3h | ✅ DONE |

**Done so far:** `make up` brings Kafka + Zookeeper + Redis online (healthy);
`make build-all` / `test` / `lint` / `vet` green across all 6 services + shared;
every service imports `shared/domain` without drift and boots the uniform
skeleton (config → logging → `/health` + `/metrics` → graceful shutdown on
SIGTERM), with version/commit build metadata injected via ldflags.
**Remaining for the deliverable:** CI green on PR (task 5).

---

### Sprint 1 — ingestion-service: Binance → Kafka (Week 1) — CODE COMPLETE (deliverable verification pending)

**Goal:** Live trades land on the Kafka backbone, reconnection-safe (§ *Service 1*, *Pattern 1*).

| # | Task | Est | Status |
|---|---|---|---|
| 1 | `service.go`: start one goroutine per symbol (BTC, ETH, SOL) via `errgroup` + context cancellation (§ *Pattern 1*) | 5h | ✅ DONE |
| 2 | `worker.go`: manage one Binance WebSocket connection per symbol (`wss://stream.binance.com:9443/ws/<sym>@trade`) | 6h | ✅ DONE |
| 3 | `normalizer.go`: Binance JSON → `shared/domain.TradeEvent` with validation (§ *Data Flow* step 2) | 4h | ✅ DONE |
| 4 | `publisher.go`: Kafka producer (franz-go, pure-Go — swapped from confluent-kafka-go to keep the `CGO_ENABLED=0`/distroless build), batch + compression, **partition by symbol** → `trades.raw` (§ *Service 1*) | 6h | ✅ DONE |
| 5 | `reconnect.go`: exponential backoff on WS disconnect — baked in now, not Sprint 6 (§ Phase 4) | 4h | ✅ DONE |
| 6 | `GET /health`: WS connection state + Kafka producer health | 2h | ✅ DONE |

**Done so far:** all 6 tasks complete — per-symbol WS workers under `errgroup`,
normalization with injected ingest time (pure/testable), franz-go producer
(batch + lz4 + acks-all idempotent) keyed by symbol → `trades.raw`, jittered
exponential backoff reconnect with uptime-based reset, and `/health` reporting
per-symbol WS connection state (`websocket` checker) + broker reachability
(`kafka_producer` checker: `client.Ping` for reachability **plus** a
consecutive-delivery-failure streak, so /health reflects trades actually
landing, not just a reachable broker). Symbols are config-driven
(`ingestion.symbols`, default btc/eth/sol), normalized (lowercase/trim/dedupe)
so a mixed-case env value can't silently subscribe to a dead stream. Shared
httpserver now splits probes: `/health/live` = liveness (process up, no
dependency checks — safe for a k8s livenessProbe), `/health` = readiness/
dependency report (a symbol in reconnect backoff degrades it by design and
must never trigger a restart). Worker loop publishes through a consumer-side
`tradePublisher` interface, so it's testable without Kafka.
**Remaining for the deliverable:** end-to-end verification — observe a Binance
trade landing on `trades.raw` < 1s and a killed WS connection reconnecting
with backoff without crashing the process.

**Deliverable:** A trade observed on Binance is published to `trades.raw` within < 1s; killing the WS connection auto-reconnects with backoff and resumes without crashing the process.

---

### Sprint 2 — processor-service: Kafka → Redis + order book + REST read (Week 2) — TODO

**Goal:** Trades are consumed, enriched, and the live snapshot is queryable (§ *Service 2*, *Service 4*).

| # | Task | Est | Status |
|---|---|---|---|
| 1 | `consumer.go`: Kafka consumer group on `trades.raw` (processor's own group) | 5h | ✅ DONE |
| 2 | `pool.go`: worker pool (configurable size, ~100) via `errgroup` (§ *Pattern 1*) | 5h | ✅ DONE |
| 3 | `fanout.go`: fan-out one trade to N downstream channels (order-book updater, Redis writer, broadcaster) (§ *Pattern 2*) | 4h | TODO |
| 4 | `enricher.go`: add notional (`price × qty`), market metadata (§ *Data Flow* step 4) | 3h | TODO |
| 5 | `orderbook.go`: in-memory order book with `sync.RWMutex` — concurrent reads, single writer (§ *Pattern 3*) | 6h | TODO |
| 6 | `redis_writer.go`: latest-trade, **live `price:<symbol>` (USD)**, and order-book snapshot writes to Redis — `price:<symbol>` is the source the Sprint-5 `/convert` endpoint reads (§ *Decision 4* store, *Service 6* data flow) | 4h | TODO |
| 7 | api-service `server.go` (Chi) + `rest/trades.go`, `rest/orderbook.go` reading from Redis (§ *Service 4*) | 6h | TODO |
| 8 | `GET /api/v1/health`: Kafka consumer lag + Redis ping (§ *API Design*) | 2h | TODO |

**Deliverable:** `GET /api/v1/trades/:symbol` and `/orderbook/:symbol` return correct, fresh data sourced from Redis; trade appears in Redis < 1s after Binance. **Phase 1 complete.**

---

### Sprint 3 — analytics-service: candles + VWAP → ClickHouse (Week 3) — TODO

**Goal:** A second, independent Kafka consumer builds historical analytics (§ *Service 3*, *Decision 1* fan-out).

| # | Task | Est | Status |
|---|---|---|---|
| 1 | `docker-compose`: add ClickHouse; init schema (trades, candles `MergeTree` tables) (§ *Tech Stack*) | 4h | TODO |
| 2 | `consumer.go`: **separate** Kafka consumer group on `trades.raw` — proves fan-out (§ *Decision 1*) | 4h | TODO |
| 3 | `candle.go`: OHLCV aggregation per symbol per interval (1m/5m/15m/1h) via `time.Ticker` window close (§ *Pattern* time.Ticker) | 7h | TODO |
| 4 | `vwap.go`: rolling VWAP per symbol; `volume.go`: volume profile per price level | 5h | TODO |
| 5 | `clickhouse.go`: batch writer flushing on window close or batch size (§ *Service 3*) | 5h | TODO |
| 6 | api-service `rest/candles.go`, `rest/analytics.go` reading ClickHouse + Redis (§ *API Design*) | 5h | TODO |

**Deliverable:** `GET /api/v1/candles/:symbol?interval=1m` returns OHLCV from ClickHouse that matches a manual recompute from raw trades; analytics consumer runs independently of processor (one can be down without affecting the other).

---

### Sprint 4 — api-service WebSocket + observability (Week 4) — TODO

**Goal:** Real-time push to clients without slow clients degrading the rest (§ *Service 4*, *Pattern 5*, *Decision 5*).

| # | Task | Est | Status |
|---|---|---|---|
| 1 | `ws/hub.go`: connection hub on `sync.Map` + broadcast/register/unregister loop with **drop-slow-client** policy (§ *Pattern 5*, *Decision 5*) | 7h | TODO |
| 2 | `ws/client.go`: per-client read/write pumps with buffered `send` channel (§ *Service 4*) | 5h | TODO |
| 3 | `ws/broadcaster.go`: fan-out trade/candle messages to subscribed clients; `ws/trades/:symbol`, `ws/orderbook/:symbol`, `ws/candles/:symbol` (§ *API Design* WebSocket) | 5h | TODO |
| 4 | `middleware/ratelimit.go`: token-bucket limiter on REST (§ *Pattern 4*) — baked in now | 4h | TODO |
| 5 | `middleware/logger.go`: structured request logging (zerolog) | 2h | TODO |
| 6 | `docker-compose`: add Prometheus; `deployments/prometheus.yml` scrape config targeting every service's `/metrics` endpoint (§ *Directory Structure*) | 3h | TODO |
| 7 | Prometheus metrics on all services: goroutine count, Kafka consumer lag, latency histograms; expose `/metrics` per service (§ *Tech Stack* — add as built) | 5h | TODO |

**Deliverable:** A browser on `ws://localhost:8080/ws/trades/BTCUSDT` sees live ticks within < 100ms of the Redis write; a deliberately slow client is dropped without stalling the other 9,999; Prometheus scrapes all 4 services. **Phase 2 complete.**

---

### Sprint 5 — fx-rate-service + currency conversion (Week 5) — TODO

**Goal:** Fiat conversion served O(1) off a cached rate, with the external FX provider fully isolated from the request and tick paths (§ *Service 6*).

> **Decision — separate service, for fault isolation (not scale).** FX rates change on a scale of minutes, so this service will *never* need a second instance for load — "independently scalable" is **not** the justification. It's a separate process so a flaky third-party FX provider (connection-pool leak, memory spike, panic in the HTTP client) can never share a process with — and degrade — the latency-critical api-service (10k WS clients, <10ms p99). It also keeps the single-responsibility / fails-independently model (§ *Decision 2*) uniform across all 6 services. **Considered alternative (Option B):** a background goroutine inside api-service with the same Redis cache — a legitimate YAGNI choice, rejected only to contain the external dependency in its own process and avoid one service breaking the architectural pattern. Cost accepted: one more service to deploy and monitor.

| # | Task | Est | Status |
|---|---|---|---|
| 1 | fx-rate-service `service.go`: start the poll ticker via `errgroup` + context cancellation (§ *Service 6*, *Pattern 1*) | 3h | TODO |
| 2 | `provider.go`: external FX API client behind a **swappable interface** (openexchangerates / exchangerate.host / ECB) — mockable, so no live HTTP in tests (§ *Service 6*) | 5h | TODO |
| 3 | `poller.go` + `cache.go`: `time.Ticker` (60s) fetch → Redis `SET fx:rates` hash `{USD:1, EUR:0.92, …}` TTL 5m (§ *Service 6* data flow) | 5h | TODO |
| 4 | `breaker.go`: a **real circuit breaker** (closed → open → half-open) wrapping the provider call — `closed`: calls pass through, a counter tracks *consecutive* failures; after **N** consecutive failures the breaker trips **open** and provider calls are skipped entirely (fail-fast, no HTTP attempted) for a `cooldown` window; on cooldown expiry → **half-open**, allowing exactly **one** probe request — success closes the breaker and resets the counter, failure re-opens it for another cooldown. State + transitions are unit-tested with no live HTTP (§ *Service 6*) — baked in now, not Sprint 7 | 6h | TODO |
| 5 | `staleness.go`: serve-stale-on-error — when the breaker is open or a poll fails, keep returning the last-good rates held in Redis under TTL; jittered retry/backoff on the poll loop (distinct from the breaker: the breaker decides *whether to call*, serve-stale decides *what to return*) (§ *Service 6*) | 4h | TODO |
| 6 | `health.go`: expose last-successful-poll timestamp **and current breaker state** for `/health` | 2h | TODO |
| 7 | api-service `rest/convert.go`: `GET /api/v1/convert/:symbol?quote=EUR` — read `price:<symbol>` + `fx:rates` from Redis, multiply, return `{price, quote, rate, asOf}`; on-exchange quote assets (USDT/USDC) served as-is, no rate lookup (§ *Service 4* quote note, *API Design*) | 5h | TODO |

**Deliverable:** `GET /api/v1/convert/BTCUSDT?quote=EUR` returns the live USD price multiplied by the cached EUR rate in < 10ms (fully cached, no external call on the request path). With the FX provider taken down: after N consecutive failed polls the breaker trips **open** and stops attempting the call, conversion keeps serving last-good rates under TTL, and `/health` reports breaker state `open`; when the provider recovers, the next poll after cooldown goes **half-open**, one probe succeeds, and the breaker returns to **closed** — all observable via `/health`.

---

### Sprint 6 — RabbitMQ alerts + notification-service (Week 6) — MVP DONE — TODO

**Goal:** Whale detection dispatches one-time alerts via RabbitMQ to notifiers, exactly once (§ *Service 5*, *Pattern 6*, *Decision 4*).

| # | Task | Est | Status |
|---|---|---|---|
| 1 | `docker-compose`: add RabbitMQ; declare `alerts` exchange + `whale.alerts` / `liquidations` / `price.alerts` queues + dead-letter exchange (§ *Service 5*, Phase 4 DLQ) | 4h | TODO |
| 2 | processor `whale_detector.go`: notional threshold check (> $500K) → publish to RabbitMQ (§ *Pattern* whale, *Data Flow* whale journey) | 4h | TODO |
| 3 | analytics `liquidation.go`: liquidation tracker → publish liquidation alerts | 4h | TODO |
| 4 | notification `consumer.go`: RabbitMQ consumer, **manual ack/nack**, goroutine per queue, nack→requeue→DLQ on failure (§ *Pattern 6*) | 6h | TODO |
| 5 | notification `router.go` + `telegram.go` / `webhook.go` / `email.go` senders (§ *Service 5*) | 7h | TODO |
| 6 | notification `dedup.go`: Redis dedup key `alert:whale:<sym>:<price>:<ts>` with 60s TTL — survives Kafka rebalance duplicates (§ *Decision 4*) | 4h | TODO |
| 7 | api-service `ws/alerts`: push whale + liquidation events to subscribed clients (§ *API Design*) | 4h | TODO |
| 8 | `docker-compose`: add Grafana; provision the Prometheus datasource + auto-load dashboards from `deployments/grafana/dashboards/` (§ *Directory Structure*) | 3h | TODO |
| 9 | Grafana dashboard (`deployments/grafana/dashboards/tradepulse.json`): throughput, Kafka lag, alert counts from Prometheus metrics | 4h | TODO |

**Deliverable (MVP DONE):** A simulated $2.5M BTC trade produces **exactly one** Telegram message even under Kafka rebalance (dedup verified); only one of N notification instances handles a given alert; failed sends nack/requeue then dead-letter. The full path — live trade → WebSocket tick → whale → alert → notification, plus fiat conversion — is demoable end to end across all 6 services. **Phase 3 complete.**

---

## Post-MVP backlog (Phase 4) — **not part of the MVP**

The MVP closes with Sprint 6. The hardening below runs as a gated epic; re-estimate after MVP velocity is known.

### Sprint 7 — Production hardening + load proof (Week 7) — TODO

**Goal:** Resilient, observable, and load-proven against the § *Performance Targets* table.

| # | Task | Est | Status |
|---|---|---|---|
| 1 | Graceful shutdown (SIGTERM + context cancellation, drain in-flight) on all 6 services (§ Phase 4) | 6h | TODO |
| 2 | Exponential backoff reconnection audit: Binance WS, Kafka, RabbitMQ, FX provider (consolidate Sprint 1/5/6 work) | 4h | TODO |
| 3 | **Real circuit breaker** on Kafka producer (§ Phase 4) — same closed/open/half-open machine as fx-rate-service's `breaker.go` (Sprint 5): N consecutive publish failures trip it open, the producer fails fast / buffers to local disk for the cooldown instead of blocking the worker pool, then half-open probes one publish before closing. Extract the breaker into `shared/` so both services use one implementation | 4h | TODO |
| 4 | k6 load test harness: ingestion throughput + concurrent WS clients | 6h | TODO |
| 5 | Validate § *Performance Targets* (table below) and record measured results | 5h | TODO |
| 6 | `README.md`: architecture diagram, one-command setup, run guide | 4h | TODO |

**Acceptance — § Performance Targets**

| Metric | Target |
|---|---|
| Ingestion throughput | 50,000 msg/sec |
| Kafka end-to-end latency | < 50ms p99 |
| Processor worker pool | 100 goroutines |
| WebSocket concurrent clients | 10,000+ |
| Redis read latency | < 1ms p99 |
| ClickHouse write throughput | 100,000 rows/s |
| RabbitMQ notification latency | < 500ms p99 |
| API REST response time | < 10ms p99 |

**Deliverable:** SIGTERM drains in-flight work with no message loss; reconnection recovers from broker/exchange blips; k6 hits the throughput/client targets; a new engineer runs the full stack in one command.

---

## Cross-cutting tracks (every sprint, not a separate phase)

- **Observability (§ *Tech Stack*):** add Prometheus metrics + structured logs for each new service as it's built; don't backfill.
- **Reorg/dedup safety (§ *Decision 4*):** any sprint introducing an at-least-once consumer dedups replayed events from day one (Kafka rebalance is the trigger, not an edge case).
- **Broker discipline (§ *Why Kafka AND RabbitMQ*):** trade *events* stay on Kafka (fan-out); one-time alert *commands* stay on RabbitMQ (consumed once). Guard this at code review.
- **No service-to-service HTTP (§ *Inter-Service Communication*):** services communicate only via brokers or read APIs over Redis/ClickHouse.
- **File-size discipline (§ *Go File Size Rules*):** "if you need to scroll to find a function — split the file." Limits below are *guidance, not a gate*; comments and tests don't count.

| File type | Comfortable | Warning zone |
|---|---|---|
| Handler / controller | ~200 | 400+ |
| Service layer | ~300 | 500+ |
| Repository / DB layer | ~300 | 500+ |
| Model / types | ~400 | — |
| Main / bootstrap | ~150 | 300+ |

## Key risks to watch

1. **Binance WS rate limits / bans** — ingestion stalls. Backoff + one shared connection per symbol group; baked into Sprint 1, not deferred.
2. **Kafka rebalance → duplicate whale alerts** (§ *Decision 4*) — the most likely correctness bug. Redis dedup is not optional; verify it under a forced rebalance.
3. **Slow WebSocket client blocks the broadcaster** (§ *Decision 5*) — freezes *all* clients. Drop policy must work before trusting concurrent-client numbers.
4. **~~confluent-kafka-go cgo build friction~~ (RESOLVED, Sprint 1)** — this materialized as predicted: `confluent-kafka-go` is a cgo/librdkafka wrapper and broke the `CGO_ENABLED=0` build and the `distroless/static` image every service uses. Resolved by switching the producer to **franz-go** (pure Go) — no cgo, no build deps, keeps the static/distroless design. No longer a risk to watch.
5. **External FX provider down / rate-limited** (§ *Service 6*) — must never block a `/convert` request or the tick path. fx-rate-service serves last-good rates under TTL with a circuit breaker; the api-service only ever reads Redis, never the provider. Verify the stale-serve path before trusting the endpoint.
6. **Solo bus factor** — keep each sprint's slice independently demoable so progress is never blocked on a half-finished service.
