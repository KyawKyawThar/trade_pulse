package domain

import "fmt"

// This file is the wire contract: the exact Kafka topics, RabbitMQ
// exchange/queues/routing-keys, and Redis keys that services agree on. A
// producer and a consumer that disagree on a topic string fail silently (the
// consumer just never sees messages), so these live in one place and are
// referenced by name — never typed as literals at call sites.

// Kafka topics. One topic per event type; partitioned by symbol so all trades
// for a symbol keep their relative order on a single partition
// (Architecture § Why Kafka AND RabbitMQ).
const (
	TopicTradesRaw    = "trades.raw"    // ingestion -> processor, analytics (fan-out)
	TopicOrderbookRaw = "orderbook.raw" // ingestion -> processor
	TopicCandles      = "candles"       // analytics -> (future consumers)
)

// Kafka consumer groups. Distinct groups are what give Kafka its fan-out:
// processor and analytics each see every trade because they are in different
// groups (Architecture § Decision 1).
const (
	ConsumerGroupProcessor = "processor-service"
	ConsumerGroupAnalytics = "analytics-service"
)

// RabbitMQ topology for one-time alert commands (consumed once).
const (
	ExchangeAlerts        = "alerts"       // topic exchange
	ExchangeAlertsDLX     = "alerts.dlx"   // dead-letter exchange
	QueueWhaleAlerts      = "whale.alerts" //
	QueueLiquidations     = "liquidations" //
	QueuePriceAlerts      = "price.alerts" //
	RoutingKeyWhale       = "whale.alert"  //
	RoutingKeyLiquidation = "liquidation.alert"
	RoutingKeyPrice       = "price.alert"
)

// Redis keys. Helpers (not constants) where the key is per-symbol, so the
// symbol is interpolated in exactly one place.
const (
	KeyFXRates = "fx:rates" // hash of fiat rates, written by fx-rate-service (TTL 5m)
)

// KeyPrice is the live USD price per symbol, written by processor-service and
// read by api-service's /convert handler .
func KeyPrice(symbol string) string { return fmt.Sprintf("price:%s", symbol) }

// KeyLatestTrade is the most recent full trade event per symbol.
func KeyLatestTrade(symbol string) string { return fmt.Sprintf("trade:latest:%s", symbol) }

// KeyOrderBook is the live order-book snapshot per symbol.
func KeyOrderBook(symbol string) string { return fmt.Sprintf("orderbook:%s", symbol) }
