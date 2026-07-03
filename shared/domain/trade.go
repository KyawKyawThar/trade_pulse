package domain

import "time"

// Side is the aggressor side of a trade — whether the market order that
// crossed the spread was a buy or a sell.
type Side string

// Trade aggressor sides.
const (
	SideBuy  Side = "BUY"
	SideSell Side = "SELL"
)

// TradeEvent is the normalized representation of a single executed trade,
// produced by ingestion-service from a raw exchange message and published to
// the Kafka topic TopicTradesRaw. Every downstream consumer (processor,
// analytics) reads this exact shape.
//
// Prices and quantities are carried as float64 for throughput. This is a
// deliberate trade-off: market-data fan-out favors speed over the exact-decimal
// guarantees you would want in a settlement/ledger system. If this pipeline ever
// drove balances, these would become integer minor-units or a decimal type.

type TradeEvent struct {
	// Symbol is the normalized instrument, e.g. "BTCUSDT".
	Symbol string `json:"symbol"`
	// Price is the execution price in the quote asset (USDT for *USDT pairs).
	Price float64 `json:"price"`
	// Quantity is the executed size in the base asset.
	Quantity float64 `json:"quantity"`
	// Side is the aggressor side (BUY/SELL).
	Side Side `json:"side"`
	// TradeID is the exchange-assigned id, used for dedup and ordering.
	TradeID int64 `json:"trade_id"`
	// Notional is Price*Quantity in the quote asset. Set by processor-service's
	// enricher; zero on the raw event straight off ingestion.
	Notional float64 `json:"notional,omitempty"`
	// IsWhale is set by processor-service when Notional crosses the threshold.
	IsWhale bool `json:"is_whale,omitempty"`
	// EventTime is the exchange's trade timestamp (when it happened upstream).
	EventTime time.Time `json:"event_time"`
	// IngestTime is when ingestion-service received it (for end-to-end latency).
	IngestTime time.Time `json:"ingest_time"`
}

// PriceLevel is one rung of an order book: an aggregated resting quantity at a
// single price.
type PriceLevel struct {
	Price    float64 `json:"price"`
	Quantity float64 `json:"quantity"`
}

// OrderBook is a point-in-time snapshot of resting liquidity for one symbol.
// processor-service maintains the live book in memory and writes snapshots to
// Redis for api-service to serve. Bids are sorted highest-first, asks
// lowest-first, so index 0 of each is the best (top of book).
type OrderBook struct {
	Symbol     string       `json:"symbol"`
	Bids       []PriceLevel `json:"bids"`
	Asks       []PriceLevel `json:"asks"`
	LastUpdate time.Time    `json:"last_update"`
}

// BestBid returns the highest bid, or false if the book has no bids.
func (b OrderBook) BestBid() (PriceLevel, bool) {
	if len(b.Bids) == 0 {
		return PriceLevel{}, false
	}
	return b.Bids[0], true
}

// BestAsk returns the lowest ask, or false if the book has no asks.
func (b OrderBook) BestAsk() (PriceLevel, bool) {
	if len(b.Asks) == 0 {
		return PriceLevel{}, false
	}
	return b.Asks[0], true
}
