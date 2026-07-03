package domain

import (
	"fmt"
	"time"
)

// AlertType discriminates the alert payloads carried on the RabbitMQ `alerts`
// exchange. It maps 1:1 to a routing key (see RoutingKey* in broker.go).

type AlertType string

// Alert types, one per RabbitMQ routing key.
const (
	AlertWhale     AlertType = "whale"
	AlertLiquation AlertType = "liquidation"
	AlertPrice     AlertType = "price"
)

// WhaleAlert is a one-time command emitted by processor-service when a trade's
// notional crosses the whale threshold. It rides RabbitMQ (consumed-once), not
// Kafka (fan-out) — see Architecture § Why Kafka AND RabbitMQ. Exactly one
// notification-service instance must send the resulting message.
type WhaleAlert struct {
	Symbol    string    `json:"symbol"`
	Price     float64   `json:"price"`
	Quantity  float64   `json:"quantity"`
	Notional  float64   `json:"notional"`
	Side      Side      `json:"side"`
	Threshold float64   `json:"threshold"`
	EventTime time.Time `json:"event_time"`
}

// LiquidationAlert is emitted by analytics-service when it detects a forced
// liquidation. Also one-time and RabbitMQ.
type LiquidationAlert struct {
	Symbol    string    `json:"symbol"`
	Price     float64   `json:"price"`
	Quantity  float64   `json:"quantity"`
	Side      Side      `json:"side"`
	EventTime time.Time `json:"event_time"`
}

// DedupKey returns the Redis key notification-service uses to guarantee a whale
// alert is sent at most once, even when Kafka rebalances cause processor-service
// to replay the trade  The key is intentionally
// derived from stable trade attributes — not a random id — so that two physical
// copies of the same logical alert collapse to one key. Set with a short TTL.
func (a WhaleAlert) DedupKey() string {
	return fmt.Sprintf("alert:whale:%s:%.2f:%d", a.Symbol, a.Price, a.EventTime.Unix())
}
