# TradePulse — Real-Time Crypto Trade Analytics Pipeline

> A high-throughput, event-driven microservices pipeline built in Go.  
> Demonstrates advanced Go concurrency, distributed systems, and real-time streaming at scale.

---

## Table of Contents

1. [What Is TradePulse](#what-is-tradepulse)
2. [Microservices Overview](#microservices-overview)
3. [Full System Architecture](#full-system-architecture)
4. [Why Kafka AND RabbitMQ](#why-kafka-and-rabbitmq)
5. [Service Breakdown](#service-breakdown)
6. [Inter-Service Communication](#inter-service-communication)
7. [Go Concurrency Patterns](#go-concurrency-patterns)
8. [Data Flow](#data-flow)
9. [API Design](#api-design)
10. [Tech Stack](#tech-stack)
11. [Directory Structure](#directory-structure)
12. [Key Engineering Decisions](#key-engineering-decisions)
13. [Performance Targets](#performance-targets)
14. [Build Phases](#build-phases)
15. [Go File Size Rules](#go-file-size-rules)

---

## What Is TradePulse

TradePulse is a real-time market data pipeline built as independent microservices that:

- **Ingests** live trade and order book events from crypto exchanges via WebSocket
- **Streams** events through Kafka for high-throughput, replayable event processing
- **Dispatches** one-time tasks (alerts, notifications) through RabbitMQ
- **Processes** events concurrently using Go worker pools
- **Stores** real-time snapshots in Redis and historical data in ClickHouse
- **Serves** data to clients via REST API and WebSocket push
- **Notifies** users of whale alerts and liquidations via Email / Telegram / Webhook

### Real World Equivalent

| Company     | Their Version                        |
|-------------|--------------------------------------|
| Binance     | Market data WebSocket infrastructure |
| TradingView | Real-time chart data engine          |
| Coinalyze   | Futures analytics pipeline           |
| Pi42        | Internal trade data engine           |

---

## Microservices Overview

TradePulse is split into **6 independent services**, each with a single responsibility:

```
┌─────────────────────────────────────────────────────────┐
│                    TRADEPULSE SERVICES                  │
│                                                         │
│  1. ingestion-service    Connect to exchange WebSocket  │
│                          Publish raw events to Kafka    │
│                                                         │
│  2. processor-service    Consume Kafka trade events     │
│                          Worker pool, fan-out, enrich   │
│                          Detect whales → RabbitMQ       │
│                                                         │
│  3. analytics-service    Consume Kafka trade events     │
│                          Build candles, VWAP, volume    │
│                          Write to ClickHouse            │
│                                                         │
│  4. api-service          REST + WebSocket server        │
│                          Read from Redis                │
│                          Push real-time to clients      │
│                                                         │
│  5. notification-service Consume RabbitMQ queue         │
│                          Send Email/Telegram/Webhook    │
│                                                         │
│  6. fx-rate-service      Poll external FX rate provider │
│                          Cache fiat rates in Redis      │
│                          (USD→EUR/GBP/INR/JPY…)         │
└─────────────────────────────────────────────────────────┘
```

Each service:
- Has its **own** `main.go` entrypoint
- Can be **deployed independently**
- Can be **scaled independently**
- **Fails independently** — one service down does not crash others

---

## Full System Architecture

```
┌──────────────────────────────────────────────────────────────────────────┐
│                          EXTERNAL DATA SOURCE                            │
│                                                                          │
│           Binance Public WebSocket (no API key required)                 │
│           wss://stream.binance.com:9443/ws/btcusdt@trade                 │
└─────────────────────────────┬────────────────────────────────────────────┘
                              │ raw market events
                              ▼
┌──────────────────────────────────────────────────────────────────────────┐
│                     SERVICE 1: ingestion-service                         │
│                                                                          │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐                  │
│  │  BTC Worker  │  │  ETH Worker  │  │  SOL Worker  │  ← goroutines    │
│  └──────┬───────┘  └──────┬───────┘  └──────┬───────┘                  │
│         └─────────────────┼─────────────────┘                           │
│                           │ normalize & validate                         │
│                           ▼                                              │
│                   ┌───────────────┐                                      │
│                   │ Kafka Producer│  batch publishing                   │
│                   └───────────────┘                                      │
└─────────────────────────────┬────────────────────────────────────────────┘
                              │
                              ▼
┌──────────────────────────────────────────────────────────────────────────┐
│                              KAFKA                                       │
│                                                                          │
│  Topic: trades.raw        Topic: orderbook.raw      Topic: candles      │
│  Partitions: 12           Partitions: 12            Partitions: 4       │
│  Retention: 7 days        Retention: 1 day          Retention: 30 days  │
│                                                                          │
│  → processor-service consumes trades.raw                                │
│  → analytics-service consumes trades.raw (independent consumer group)   │
└──────────┬──────────────────────────────┬───────────────────────────────┘
           │                              │
           ▼                              ▼
┌──────────────────────┐    ┌─────────────────────────────────────────────┐
│ SERVICE 2:           │    │ SERVICE 3: analytics-service                │
│ processor-service    │    │                                             │
│                      │    │  ┌─────────────────┐  ┌──────────────────┐ │
│  Worker Pool         │    │  │ Candle Aggregator│  │ VWAP Calculator  │ │
│  - Fan-out trades    │    │  │ 1m/5m/15m/1h    │  │ per symbol       │ │
│  - Enrich events     │    │  └─────────────────┘  └──────────────────┘ │
│  - Detect whales     │    │                                             │
│  - Update orderbook  │    │  ┌─────────────────┐  ┌──────────────────┐ │
│         │            │    │  │ Volume Profiler  │  │ Liquidation Track│ │
│         │ whale!     │    │  └─────────────────┘  └──────────────────┘ │
│         ▼            │    │              │                              │
│  ┌─────────────┐     │    │              ▼                              │
│  │  RabbitMQ   │     │    │       ┌─────────────┐                      │
│  │  Producer   │     │    │       │  ClickHouse │                      │
│  └──────┬──────┘     │    │       │  Writer     │                      │
│         │            │    │       └─────────────┘                      │
│         │            │    └─────────────────────────────────────────────┘
│         │  Redis     │
│         │  writes    │
└─────────┼────────────┘
          │                            Redis
          │  ┌─────────────────────────────────────────────────────┐
          │  │  - Live order book snapshots per symbol             │
          │  │  - Latest trade per symbol                          │
          │  │  - Whale alert cache (dedup)                        │
          │  │  - Rate limiter token buckets                       │
          │  └───────────────────────┬─────────────────────────────┘
          │                          │
          ▼                          │
┌────────────────────────┐           │
│  RABBITMQ              │           │
│                        │           │
│  Exchange: alerts      │           │
│  Queue: whale.alerts   │           │
│  Queue: liquidations   │           │
│  Queue: price.alerts   │           │
└───────────┬────────────┘           │
            │                        │
            ▼                        ▼
┌────────────────────────┐  ┌────────────────────────────────────────────┐
│ SERVICE 5:             │  │ SERVICE 4: api-service                     │
│ notification-service   │  │                                            │
│                        │  │  REST API (Chi router)                     │
│  ┌──────────────────┐  │  │  GET /trades/:symbol                       │
│  │ RabbitMQ Consumer│  │  │  GET /orderbook/:symbol                    │
│  └────────┬─────────┘  │  │  GET /candles/:symbol                      │
│           │            │  │  GET /analytics/:symbol                    │
│  ┌────────▼─────────┐  │  │  GET /convert/:symbol?quote=EUR  ◄── new   │
│  │  Alert Router    │  │  │                                            │
│  └────────┬─────────┘  │  │  WebSocket Server                          │
│           │            │  │  ws://.../ws/trades/BTCUSDT                │
│  ┌────────▼─────────┐  │  │  ws://.../ws/orderbook/BTCUSDT             │
│  │ Email│TG│Webhook │  │  └───────────────────────┬────────────────────┘
│  └──────────────────┘  │                          │ Redis GET fx:rates
└────────────────────────┘                          │
                                                     │
┌─────────────────────────────────────┐             │
│ SERVICE 6: fx-rate-service          │             │
│                                     │             │
│  ┌───────────────────────────────┐  │  external HTTPS call,
│  │ Ticker (poll every 60s)       │  │  NOT on the tick hot path
│  │ → external FX rates provider  │  │             │
│  └─────────────┬─────────────────┘  │             │
│  ┌─────────────▼─────────────────┐  │  Redis SET  │
│  │ Redis  fx:rates  (TTL 5m)     │──┼──fx:rates───┘
│  └───────────────────────────────┘  │
└─────────────────────────────────────┘
                                        │
                                        ▼
                               ┌─────────────────┐
                               │    CLIENTS      │
                               │  Dashboard /    │
                               │  Trading Bots / │
                               │  Alert Systems  │
                               └─────────────────┘
```

---

## Why Kafka AND RabbitMQ

This is the most important architectural decision in TradePulse.

### They solve DIFFERENT problems

| | Kafka | RabbitMQ |
|---|---|---|
| Pattern | Event log | Task queue |
| Message retention | Days/weeks | Until consumed |
| Consumer model | All consumers see all messages | One consumer gets one message |
| Replay | Yes — seek to any offset | No |
| Best for | High-volume streaming | One-time job dispatch |
| Throughput | Millions/sec | Thousands/sec |

### Rule of Thumb

```
Ask: "Should every consumer see this message?"

YES → Kafka
  Trade events: processor-service AND analytics-service
  both need to see every trade independently

NO → RabbitMQ
  Whale alert notification: only ONE notification-service
  instance should send the email — not all of them
```

### Concrete Example

```
Trade event flow (Kafka):
  BTC trade happens
  → processor-service sees it    (consumer group A)
  → analytics-service sees it    (consumer group B)
  Both get the SAME event independently ✓

Whale alert flow (RabbitMQ):
  processor-service detects whale order
  → publishes to RabbitMQ queue
  → notification-service instance #1 picks it up
  → sends ONE email to user ✓
  (if 3 notification instances running, only ONE processes it)
```

### Same Pattern as MEVWatch

```
MEVWatch:
  Kafka    → domain events (block detected, tx seen)
  RabbitMQ → simulation jobs (run this simulation once)

TradePulse:
  Kafka    → trade events (process and analyze)
  RabbitMQ → notification tasks (send this alert once)
```

Consistent architectural thinking across both projects.

---

## Service Breakdown

### Service 1 — ingestion-service

**Single responsibility:** Connect to exchange WebSocket, normalize, publish to Kafka.

```
ingestion-service/
├── cmd/main.go
└── internal/
    ├── service.go          — starts all symbol goroutines via errgroup
    ├── worker.go           — one goroutine per symbol, manages WebSocket
    ├── normalizer.go       — Binance format → internal TradeEvent
    ├── publisher.go        — Kafka producer, batching, compression
    └── reconnect.go        — exponential backoff on disconnect
```

**Key Go patterns:** errgroup, goroutine per symbol, context cancellation

---

### Service 2 — processor-service

**Single responsibility:** Consume Kafka trades, enrich, detect patterns, fan-out to Redis and RabbitMQ.

```
processor-service/
├── cmd/main.go
└── internal/
    ├── service.go          — wires consumer, pool, fanout together
    ├── consumer.go         — Kafka consumer group
    ├── pool.go             — worker pool (configurable size)
    ├── fanout.go           — fan-out one event to N downstream channels
    ├── enricher.go         — add notional value, market metadata
    ├── whale_detector.go   — detect large orders, publish to RabbitMQ
    ├── orderbook.go        — maintain in-memory order book with RWMutex
    └── redis_writer.go     — write live snapshots to Redis
```

**Key Go patterns:** worker pool, fan-out channels, sync.RWMutex, sync/atomic

---

### Service 3 — analytics-service

**Single responsibility:** Consume Kafka trades, aggregate into candles and analytics, write to ClickHouse.

```
analytics-service/
├── cmd/main.go
└── internal/
    ├── service.go          — wires consumer and aggregators
    ├── consumer.go         — Kafka consumer group (separate from processor)
    ├── candle.go           — OHLCV aggregation per symbol per interval
    ├── vwap.go             — rolling VWAP calculation
    ├── volume.go           — volume profile per price level
    ├── liquidation.go      — liquidation event tracker
    └── clickhouse.go       — batch writer to ClickHouse
```

**Key Go patterns:** time.Ticker for window closing, concurrent map with mutex, batch writes

---

### Service 4 — api-service

**Single responsibility:** Serve REST and WebSocket endpoints to external clients.

```
api-service/
├── cmd/main.go
└── internal/
    ├── server.go           — HTTP server, routing, middleware
    ├── rest/
    │   ├── trades.go       — GET /api/v1/trades/:symbol
    │   ├── orderbook.go    — GET /api/v1/orderbook/:symbol
    │   ├── candles.go      — GET /api/v1/candles/:symbol
    │   ├── analytics.go    — GET /api/v1/analytics/:symbol
    │   └── convert.go      — GET /api/v1/convert/:symbol?quote=EUR (reads fx:rates)
    ├── ws/
    │   ├── hub.go          — WebSocket connection hub (sync.Map)
    │   ├── client.go       — individual client with send channel
    │   └── broadcaster.go  — fan-out to all subscribed clients
    └── middleware/
        ├── ratelimit.go    — token bucket rate limiter
        └── logger.go       — structured request logging
```

**Key Go patterns:** sync.Map for client registry, buffered channels, drop policy for slow clients

> **Quote-currency conversion lives here.** The new `GET /api/v1/convert/:symbol?quote=EUR`
> endpoint reads the live USD-denominated price from Redis and multiplies it by the cached
> fiat rate that **fx-rate-service** writes to `fx:rates`. On-exchange quote assets (USDT,
> USDC) need no conversion and are served as-is; only fiat (EUR/GBP/INR/JPY…) and
> cross-crypto denomination hit the rate cache. The api-service never calls the external FX
> provider directly — it only reads Redis, so a slow or down provider can never block a
> client request.

---

### Service 5 — notification-service

**Single responsibility:** Consume RabbitMQ alert queues, route and send notifications.

```
notification-service/
├── cmd/main.go
└── internal/
    ├── service.go          — starts consumers for each queue
    ├── consumer.go         — RabbitMQ consumer with ack/nack
    ├── router.go           — route alert type to correct notifier
    ├── email.go            — SMTP email sender
    ├── telegram.go         — Telegram Bot API sender
    ├── webhook.go          — HTTP webhook POST sender
    └── dedup.go            — Redis-based deduplication (prevent duplicate alerts)
```

**Key Go patterns:** goroutine per queue, manual ack/nack, retry with dead letter queue

---

### Service 6 — fx-rate-service

**Single responsibility:** Periodically fetch fiat exchange rates from an external provider and cache them in Redis for the api-service to read.

**Why a separate service?** Fiat rates (USD→EUR/GBP/INR/JPY…) change on a scale of *minutes*, not milliseconds — they must **never** ride the trade tick path. Isolating them as their own polling service means: the external HTTP dependency is contained to one place, its failure can't touch ingestion or the API, and the rate cache survives even if the provider is briefly down (last-good values held under TTL).

```
fx-rate-service/
├── cmd/main.go
└── internal/
    ├── service.go          — starts the poll ticker via errgroup
    ├── poller.go           — time.Ticker (60s); fetch rates, write to Redis
    ├── provider.go         — external FX API client (interface: swappable provider)
    │                         e.g. openexchangerates / exchangerate.host / ECB
    ├── cache.go            — Redis SET fx:rates {USD:1, EUR:0.92, GBP:0.79, …} TTL 5m
    ├── staleness.go        — serve-stale-on-error: keep last-good rates if a poll fails
    └── health.go           — exposes last-successful-poll timestamp for /health
```

**Key Go patterns:** single `time.Ticker` loop, `context` cancellation, provider behind an
interface (so the rate source is swappable / mockable), a **real circuit breaker** around the
provider call, serve-stale fallback, jittered retry with backoff.

> **Circuit breaker — the actual state machine, not a try/catch.** The breaker has three
> states: **closed** (calls pass through; a counter tracks *consecutive* failures), **open**
> (after N consecutive failures the breaker trips — provider calls are skipped entirely,
> *no HTTP is attempted*, for a cooldown window), and **half-open** (on cooldown expiry, exactly
> *one* probe request is allowed — success closes the breaker and resets the counter, failure
> re-opens it for another cooldown). This is distinct from the **serve-stale fallback**: the
> breaker decides *whether to call the provider*, serve-stale decides *what to return* (the
> last-good rates held in Redis under TTL) when there's no fresh value. `/health` exposes the
> current breaker state and the last-successful-poll timestamp.

**Data flow:**

```
fx-rate-service  →  (every 60s) GET external FX provider over HTTPS
                 →  Redis SET fx:rates (hash, TTL 5m)

api-service      →  GET /api/v1/convert/BTCUSDT?quote=EUR
                 →  Redis GET price:BTCUSDT  (live USD price)
                 →  Redis GET fx:rates       (cached rate, 0.92)
                 →  return { price: 57060.23, quote: "EUR", rate: 0.92, asOf: <ts> }
```

This keeps the conversion **O(1) and fully cached** on the request path — exactly the
“fetch once a minute and multiply” model, never streamed at tick rate.

---

## Inter-Service Communication

```
SERVICE               SENDS TO              PROTOCOL       WHY
─────────────────────────────────────────────────────────────────────
ingestion-service  →  Kafka trades.raw      Kafka          high volume, replayable
ingestion-service  →  Kafka orderbook.raw   Kafka          high volume, replayable

processor-service  →  Redis                 Redis SET      real-time snapshot cache
processor-service  →  RabbitMQ alerts       RabbitMQ       consumed once, task dispatch

analytics-service  →  ClickHouse            TCP batch      time-series storage

api-service        →  Redis                 Redis GET      read live snapshots
api-service        →  ClickHouse            TCP query      read historical data
api-service        →  Redis fx:rates        Redis GET      read cached fiat rates (convert)
api-service        →  WebSocket clients     WS             real-time push

notification-svc   ←  RabbitMQ alerts       RabbitMQ       consume whale/liq alerts

fx-rate-service    →  External FX API       HTTPS poll     fetch fiat rates every 60s
fx-rate-service    →  Redis fx:rates        Redis SET      cache rates (TTL 5m)
```

No direct HTTP calls between services — all communication is via message brokers or shared storage. This means:

```
✓ Services are fully decoupled
✓ No service-to-service network failures cascade
✓ Each service can be scaled independently
✓ Easy to add a new service that consumes existing Kafka topics
```

---

## Go Concurrency Patterns

### Pattern 1 — Worker Pool with errgroup

```go
// processor-service/internal/pool.go

type WorkerPool struct {
    numWorkers int
    jobs       <-chan TradeEvent
    results    chan<- ProcessedEvent
}

func (p *WorkerPool) Start(ctx context.Context) error {
    g, ctx := errgroup.WithContext(ctx)

    for i := 0; i < p.numWorkers; i++ {
        workerID := i
        g.Go(func() error {
            return p.runWorker(ctx, workerID)
        })
    }

    return g.Wait()
}

func (p *WorkerPool) runWorker(ctx context.Context, id int) error {
    for {
        select {
        case job, ok := <-p.jobs:
            if !ok {
                return nil
            }
            result := processEvent(job)
            p.results <- result
        case <-ctx.Done():
            return ctx.Err()
        }
    }
}
```

---

### Pattern 2 — Fan-Out with Channels

```go
// processor-service/internal/fanout.go

// One trade event fans out to N downstream processors
func FanOut(source <-chan TradeEvent, numConsumers int) []<-chan TradeEvent {
    outputs := make([]chan TradeEvent, numConsumers)
    for i := range outputs {
        outputs[i] = make(chan TradeEvent, 256) // buffered
    }

    go func() {
        defer func() {
            for _, ch := range outputs {
                close(ch)
            }
        }()
        for event := range source {
            for _, ch := range outputs {
                ch <- event
            }
        }
    }()

    result := make([]<-chan TradeEvent, numConsumers)
    for i, ch := range outputs {
        result[i] = ch
    }
    return result
}
```

---

### Pattern 3 — Concurrent Order Book with RWMutex

```go
// processor-service/internal/orderbook.go

type OrderBookStore struct {
    mu   sync.RWMutex
    data map[string]*OrderBook
}

// Multiple goroutines can read concurrently
func (s *OrderBookStore) Get(symbol string) *OrderBook {
    s.mu.RLock()
    defer s.mu.RUnlock()
    return s.data[symbol]
}

// Only one goroutine can write at a time
func (s *OrderBookStore) Update(symbol string, book *OrderBook) {
    s.mu.Lock()
    defer s.mu.Unlock()
    s.data[symbol] = book
}
```

---

### Pattern 4 — Token Bucket Rate Limiter

```go
// api-service/internal/middleware/ratelimit.go

type TokenBucket struct {
    tokens chan struct{}
    ticker *time.Ticker
    quit   chan struct{}
}

func NewTokenBucket(rps int) *TokenBucket {
    tb := &TokenBucket{
        tokens: make(chan struct{}, rps),
        ticker: time.NewTicker(time.Second / time.Duration(rps)),
        quit:   make(chan struct{}),
    }
    go tb.refill()
    return tb
}

func (tb *TokenBucket) refill() {
    for {
        select {
        case <-tb.ticker.C:
            select {
            case tb.tokens <- struct{}{}:
            default: // bucket full, discard
            }
        case <-tb.quit:
            tb.ticker.Stop()
            return
        }
    }
}

func (tb *TokenBucket) Allow() bool {
    select {
    case <-tb.tokens:
        return true
    default:
        return false
    }
}
```

---

### Pattern 5 — WebSocket Hub with Drop Policy

```go
// api-service/internal/ws/hub.go

type Hub struct {
    clients    sync.Map
    broadcast  chan []byte
    register   chan *Client
    unregister chan *Client
}

func (h *Hub) Run(ctx context.Context) {
    for {
        select {
        case client := <-h.register:
            h.clients.Store(client, struct{}{})

        case client := <-h.unregister:
            h.clients.Delete(client)
            close(client.send)

        case message := <-h.broadcast:
            h.clients.Range(func(key, _ any) bool {
                client := key.(*Client)
                select {
                case client.send <- message:
                default:
                    // slow client — drop and disconnect
                    // never block the broadcaster
                    h.clients.Delete(client)
                    close(client.send)
                }
                return true
            })

        case <-ctx.Done():
            return
        }
    }
}
```

---

### Pattern 6 — RabbitMQ Consumer with Manual Ack

```go
// notification-service/internal/consumer.go

func (c *AlertConsumer) Consume(ctx context.Context) error {
    msgs, err := c.channel.Consume(
        "whale.alerts",
        "",    // consumer tag
        false, // auto-ack OFF — we ack manually
        false, // exclusive
        false, // no-local
        false, // no-wait
        nil,
    )
    if err != nil {
        return err
    }

    for {
        select {
        case msg, ok := <-msgs:
            if !ok {
                return fmt.Errorf("channel closed")
            }

            if err := c.router.Route(msg.Body); err != nil {
                // failed — nack and requeue
                msg.Nack(false, true)
                continue
            }

            // success — ack to remove from queue
            msg.Ack(false)

        case <-ctx.Done():
            return nil
        }
    }
}
```

---

## Data Flow

### Trade Event — Full Journey

```
1. Binance WebSocket emits:
   {"e":"trade","s":"BTCUSDT","p":"65000.50","q":"0.5","m":false}

2. ingestion-service normalizes:
   TradeEvent{Symbol:"BTC", Price:65000.50, Qty:0.5, Side:BUY, TS:1719158400000}

3. Kafka producer publishes to "trades.raw" partition by symbol

4. processor-service worker pool consumes:
   - Enriches: Notional = 65000.50 × 0.5 = $32,500
   - Checks: Notional > $500,000? → NO → not a whale
   - Fan-out to: orderbook updater, Redis writer, WebSocket broadcaster

5. analytics-service (separate consumer group) also consumes same event:
   - Updates 1m candle: High/Low/Close/Volume
   - Recalculates rolling VWAP
   - On minute close → batch write to ClickHouse

6. api-service WebSocket hub pushes to subscribed clients:
   {"type":"trade","symbol":"BTC","price":65000.50,"side":"BUY","ts":1719158400000}
```

---

### Whale Alert — Full Journey

```
1. processor-service detects:
   TradeEvent{Notional: $2,500,000}  ← exceeds $500K threshold

2. Publishes to RabbitMQ:
   Exchange: alerts
   Routing key: whale.alert
   Body: {"symbol":"BTC","price":65000,"notional":2500000,"side":"BUY"}

3. notification-service consumes from "whale.alerts" queue:
   - Checks Redis dedup key: "alert:whale:BTC:65000:1719158400"
   - Already sent? → ack and skip
   - Not sent yet? → proceed

4. Routes to notifiers:
   - Telegram: sends message to configured channel
   - Webhook: POST to user's configured URL
   - Sets Redis dedup key with 60s TTL

5. Acks RabbitMQ message → removed from queue
```

---

## API Design

### REST Endpoints

```
GET  /api/v1/trades/:symbol
     Query: limit=50&since=1719158400000
     Source: Redis (recent) + ClickHouse (historical)
     Returns: array of trade events

GET  /api/v1/orderbook/:symbol
     Source: Redis
     Returns: top 20 bids/asks, best bid/ask

GET  /api/v1/candles/:symbol
     Query: interval=1m&limit=200
     Source: ClickHouse
     Returns: OHLCV array

GET  /api/v1/analytics/:symbol
     Source: Redis + ClickHouse
     Returns: VWAP, 24h volume, 24h change %, liquidation count

GET  /api/v1/health
     Returns: Kafka consumer lag, Redis ping, ClickHouse ping
```

### WebSocket Subscriptions

```
ws://localhost:8080/ws/trades/BTCUSDT
→ real-time trade stream

ws://localhost:8080/ws/orderbook/BTCUSDT
→ order book delta updates

ws://localhost:8080/ws/candles/BTCUSDT?interval=1m
→ completed candle events

ws://localhost:8080/ws/alerts
→ whale alerts and liquidation events
```

### WebSocket Message Format

```json
{
  "type": "trade",
  "symbol": "BTC",
  "price": 65000.50,
  "qty": 0.5,
  "side": "BUY",
  "notional": 32500.25,
  "is_whale": false,
  "timestamp": 1719158400000
}
```

```json
{
  "type": "whale_alert",
  "symbol": "BTC",
  "price": 65000.00,
  "notional": 2500000.00,
  "side": "BUY",
  "timestamp": 1719158400000
}
```

---

## Tech Stack

| Layer            | Technology          | Why                                             |
|------------------|---------------------|-------------------------------------------------|
| Language         | Go 1.22+            | Native concurrency, low latency, strong stdlib  |
| Event Streaming  | Kafka               | High-volume trades, replayable, multi-consumer  |
| Task Queue       | RabbitMQ            | One-time alert dispatch, ack/nack semantics     |
| Cache            | Redis 7             | Sub-millisecond reads, dedup, rate limiting     |
| Analytics DB     | ClickHouse          | Columnar OLAP, blazing fast time-series queries |
| HTTP Router      | Chi                 | Lightweight, idiomatic Go                       |
| WebSocket        | gorilla/websocket   | Production-grade WebSocket in Go                |
| Kafka Client     | confluent-kafka-go  | Official Confluent client, high performance     |
| RabbitMQ Client  | amqp091-go          | Official Go AMQP client                         |
| Metrics          | Prometheus          | Goroutine count, Kafka lag, latency histograms  |
| Dashboard        | Grafana             | Visualize all Prometheus metrics                |
| Container        | Docker Compose      | Full local stack in one command                 |
| Config           | Viper               | Env vars + YAML config                          |
| Logging          | zerolog             | Structured JSON logging, zero allocation        |

---

## Directory Structure

```
tradepulse/
│
├── services/
│   ├── ingestion-service/
│   │   ├── cmd/main.go
│   │   ├── internal/
│   │   │   ├── service.go
│   │   │   ├── worker.go
│   │   │   ├── normalizer.go
│   │   │   ├── publisher.go
│   │   │   └── reconnect.go
│   │   ├── Dockerfile
│   │   └── go.mod
│   │
│   ├── processor-service/
│   │   ├── cmd/main.go
│   │   ├── internal/
│   │   │   ├── service.go
│   │   │   ├── consumer.go
│   │   │   ├── pool.go
│   │   │   ├── fanout.go
│   │   │   ├── enricher.go
│   │   │   ├── whale_detector.go
│   │   │   ├── orderbook.go
│   │   │   └── redis_writer.go
│   │   ├── Dockerfile
│   │   └── go.mod
│   │
│   ├── analytics-service/
│   │   ├── cmd/main.go
│   │   ├── internal/
│   │   │   ├── service.go
│   │   │   ├── consumer.go
│   │   │   ├── candle.go
│   │   │   ├── vwap.go
│   │   │   ├── volume.go
│   │   │   ├── liquidation.go
│   │   │   └── clickhouse.go
│   │   ├── Dockerfile
│   │   └── go.mod
│   │
│   ├── api-service/
│   │   ├── cmd/main.go
│   │   ├── internal/
│   │   │   ├── server.go
│   │   │   ├── rest/
│   │   │   │   ├── trades.go
│   │   │   │   ├── orderbook.go
│   │   │   │   ├── candles.go
│   │   │   │   └── analytics.go
│   │   │   ├── ws/
│   │   │   │   ├── hub.go
│   │   │   │   ├── client.go
│   │   │   │   └── broadcaster.go
│   │   │   └── middleware/
│   │   │       ├── ratelimit.go
│   │   │       └── logger.go
│   │   ├── Dockerfile
│   │   └── go.mod
│   │
│   └── notification-service/
│       ├── cmd/main.go
│       ├── internal/
│       │   ├── service.go
│       │   ├── consumer.go
│       │   ├── router.go
│       │   ├── email.go
│       │   ├── telegram.go
│       │   ├── webhook.go
│       │   └── dedup.go
│       ├── Dockerfile
│       └── go.mod
│
├── shared/                         ← shared domain types across services
│   ├── domain/
│   │   ├── trade.go               — TradeEvent, OrderBook structs
│   │   ├── candle.go              — Candle OHLCV struct
│   │   └── alert.go               — WhaleAlert, LiquidationAlert structs
│   └── go.mod
│
├── deployments/
│   ├── docker-compose.yml         ← Kafka, Zookeeper, RabbitMQ, Redis,
│   │                                 ClickHouse, Prometheus, Grafana
│   ├── prometheus.yml
│   └── grafana/
│       └── dashboards/
│           └── tradepulse.json
│
├── docs/
│   └── ARCHITECTURE.md            ← this file
│
├── Makefile                       ← make run, make test, make build-all
└── README.md
```

---

## Key Engineering Decisions

### Decision 1 — Kafka for trade events, RabbitMQ for alerts

```
Trade events need fan-out: processor-service AND analytics-service
both need to independently consume every trade.
Kafka's consumer group model makes this trivial.

Whale alert notifications need consumed-once semantics:
only ONE notification-service instance should send the email.
RabbitMQ's queue model guarantees exactly this.

Using Kafka for notifications would require extra dedup logic.
Using RabbitMQ for trade events would break fan-out.
Right tool for right job.
```

---

### Decision 2 — 6 independent services, not 1 monolith

```
Each service can be scaled independently:
  - ingestion-service: scale with number of symbols
  - processor-service: scale with trade volume
  - analytics-service: scale with analytics queries
  - api-service: scale with number of API clients
  - notification-service: scale with alert volume

One service crashing does not crash the others.
Deploy notification-service update without touching api-service.
```

---

### Decision 3 — ClickHouse over PostgreSQL for analytics

```
PostgreSQL is row-oriented.
Scanning 100M trade rows for VWAP = full table scan = slow.

ClickHouse is columnar.
VWAP query reads only the "price" and "qty" columns.
10-100x faster for time-series aggregations.
Native compression on numeric columns saves 80% disk space.
```

---

### Decision 4 — Redis dedup for notifications

```
Race condition: processor-service may emit duplicate whale alerts
if Kafka rebalances (at-least-once delivery).

notification-service sets a Redis key:
  "alert:whale:BTC:65000:1719158400" with 60s TTL

If key exists → already sent → skip
This prevents duplicate emails/Telegrams to users.
```

---

### Decision 5 — Drop slow WebSocket clients

```
If a client's send channel is full, the broadcaster must not block.
Blocking the broadcaster freezes ALL connected clients.

Policy: if channel full → client is too slow → disconnect.
Client reconnects and gets fresh state from Redis.
1 slow client never degrades 9,999 fast clients.
```

---

### Decision 6 — Shared domain package

```
TradeEvent, Candle, Alert structs are used by all 6 services.
Defining them once in /shared/domain prevents drift.
If TradeEvent gets a new field, update once, rebuild all services.
```

---

### Decision 7 — fx-rate-service as a separate service (fault isolation, not scale)

```
FX rates change on a scale of minutes, so this service will never
need a second instance for load. "Independently scalable" is NOT
the reason it's separate.

It's a separate PROCESS to isolate a third-party failure:
  api-service is on the hot path (10k WS clients, <10ms p99 REST).
  An outbound HTTP client to a flaky FX provider — connection-pool
  leak, memory spike, panic — must never share that process and
  degrade the latency-critical API.

Everyone reads Redis, never the provider, so the request path is
already isolated. Putting the poller in its own process isolates
the process too, and keeps the single-responsibility model (Decision 2)
uniform across all 6 services.

Considered alternative: a background goroutine inside api-service
with the same Redis cache — a legitimate YAGNI choice. Rejected only
to contain the external dependency and avoid one service breaking the
pattern. Cost accepted: one more service to deploy and monitor.
```

---

## Performance Targets

| Metric                          | Target          |
|---------------------------------|-----------------|
| Ingestion throughput            | 50,000 msg/sec  |
| Kafka end-to-end latency        | < 50ms p99      |
| Processor worker pool size      | 100 goroutines  |
| WebSocket concurrent clients    | 10,000+         |
| Redis read latency              | < 1ms p99       |
| ClickHouse write throughput     | 100,000 rows/s  |
| RabbitMQ notification latency   | < 500ms p99     |
| API REST response time          | < 10ms p99      |
| Goroutines at peak (all svc)    | ~1,000 total    |

---

## Build Phases

### Phase 1 — Core Pipeline (Week 1)

```
□ ingestion-service: Binance WebSocket → Kafka (BTC, ETH, SOL)
□ processor-service: Kafka consumer → worker pool → Redis
□ api-service: GET /trades, GET /orderbook (reads from Redis)
□ Docker Compose: Kafka + Zookeeper + Redis
□ Basic health check endpoints
```

### Phase 2 — Analytics + WebSocket (Week 2)

```
□ analytics-service: candle aggregation → ClickHouse
□ api-service WebSocket: real-time trade push to clients
□ VWAP and volume endpoints
□ Docker Compose: add ClickHouse
□ Prometheus metrics on all services
```

### Phase 3 — RabbitMQ + Notifications (Week 3)

```
□ processor-service: whale detection → RabbitMQ
□ notification-service: RabbitMQ consumer → Telegram
□ Redis dedup for notifications
□ Docker Compose: add RabbitMQ
□ Grafana dashboard
```

### Phase 4 — Production Hardening (Week 4)

```
□ Graceful shutdown (SIGTERM) on all services
□ Exponential backoff reconnection (Binance WS, Kafka, RabbitMQ)
□ Circuit breaker on Kafka producer
□ Rate limiter on REST API
□ Structured logging with zerolog
□ Load testing with k6
□ README with architecture diagram and setup guide
```

---

## What This Demonstrates to a Senior Interviewer

```
Go Concurrency:
  → Worker pools, fan-out channels, sync primitives,
    errgroup, context cancellation, atomic operations

Distributed Systems:
  → Kafka consumer groups, at-least-once delivery,
    idempotency via Redis dedup, partition strategy

Microservices:
  → Single responsibility, independent deployment,
    broker-based decoupling, no direct service calls

Message Broker Expertise:
  → Knows WHEN to use Kafka vs RabbitMQ
    (streaming vs task queue — not just "use both")

Production Readiness:
  → Graceful shutdown, circuit breakers, rate limiting,
    structured logging, observability, health checks

Domain Knowledge:
  → Understands crypto order books, VWAP, liquidations,
    whale detection — speaks the language of the industry
```

---

## Go File Size Rules

> **Golden Rule: If you need to scroll to find a function — split the file.**

---

### Practical Line Limits Per File Type

| File Type              | Comfortable | Warning Zone | Action |
|------------------------|-------------|--------------|--------|
| Handler / Controller   | ~200        | 400+         | Split by route group |
| Service layer          | ~300        | 500+         | Split by responsibility |
| Repository / DB layer  | ~300        | 500+         | Split by entity/table |
| Model / types          | ~400        | —            | Fine to keep together |
| Main / bootstrap       | ~150        | 300+         | Extract setup functions |

---

### Why This Matters in TradePulse

Each service in TradePulse follows these limits strictly.

**Example — processor-service**

Wrong — everything in one file:

```
processor-service/internal/
└── service.go   ← 800 lines, does everything
```

Correct — split by responsibility:

```
processor-service/internal/
├── service.go          ~150 lines  bootstrap, wires components together
├── consumer.go         ~200 lines  Kafka consumer logic only
├── pool.go             ~150 lines  worker pool only
├── fanout.go           ~100 lines  fan-out channel logic only
├── enricher.go         ~120 lines  trade enrichment only
├── whale_detector.go   ~100 lines  whale detection only
├── orderbook.go        ~200 lines  order book state only
└── redis_writer.go     ~150 lines  Redis write logic only
```

Every file fits on one screen. Any engineer can open a file and understand it immediately.

---

### Split Signals — When to Break a File

```
1. You have more than 2 structs with methods in one file
   → each struct gets its own file

2. You find yourself writing comments like:
   // ===== KAFKA SECTION =====
   // ===== REDIS SECTION =====
   → those sections are separate files

3. A file has more than 3 imports from different domains
   (e.g. kafka + redis + websocket all in one file)
   → split by dependency

4. You cannot describe the file in one sentence
   → it is doing too much

5. A colleague asks "where is the whale detection logic?"
   and the answer is "in service.go, around line 340"
   → split immediately
```

---

### How This Maps to Each TradePulse Service

**ingestion-service**

```
service.go          starts symbol workers                    ~150 lines
worker.go           manages one WebSocket connection         ~200 lines
normalizer.go       converts Binance format → TradeEvent     ~120 lines
publisher.go        Kafka producer + batching                ~180 lines
reconnect.go        exponential backoff logic                ~80 lines
```

**processor-service**

```
service.go          wires everything together                ~150 lines
consumer.go         Kafka consumer group                     ~200 lines
pool.go             worker pool                              ~150 lines
fanout.go           fan-out to N channels                    ~100 lines
enricher.go         add notional, metadata                   ~120 lines
whale_detector.go   threshold check + RabbitMQ publish       ~100 lines
orderbook.go        in-memory book with RWMutex              ~200 lines
redis_writer.go     snapshot writes to Redis                 ~150 lines
```

**api-service**

```
server.go           HTTP setup, middleware, routing          ~150 lines
rest/trades.go      GET /trades/:symbol handler             ~150 lines
rest/orderbook.go   GET /orderbook/:symbol handler          ~120 lines
rest/candles.go     GET /candles/:symbol handler            ~120 lines
rest/analytics.go   GET /analytics/:symbol handler          ~150 lines
ws/hub.go           connection hub + broadcast loop         ~200 lines
ws/client.go        individual client read/write pumps      ~150 lines
ws/broadcaster.go   fan-out message to subscribed clients   ~100 lines
middleware/ratelimit.go  token bucket                       ~100 lines
middleware/logger.go     structured request logging         ~80 lines
```

**notification-service**

```
service.go          starts consumers per queue              ~120 lines
consumer.go         RabbitMQ consumer + ack/nack            ~200 lines
router.go           routes alert type to notifier           ~100 lines
email.go            SMTP send logic                         ~150 lines
telegram.go         Telegram Bot API                        ~120 lines
webhook.go          HTTP POST to user webhook               ~100 lines
dedup.go            Redis-based dedup with TTL              ~80 lines
```

---

### The Test

Before committing any file, ask yourself:

```
1. Can I describe this file in one sentence?       YES → good
2. Does every function relate to that sentence?    YES → good
3. Can I find any function without searching?      YES → good
4. Is the file under its comfortable limit?        YES → good

Any NO → split the file.
```

---

*TradePulse — Production-grade Go microservices demonstrating real-world distributed systems engineering.*
