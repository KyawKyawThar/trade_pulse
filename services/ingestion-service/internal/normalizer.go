package internal

import (
	"encoding/json"
	"fmt"
	"strconv"
	"time"
	"trade_pulse/shared/domain"
)

// binanceTradeMessage is the wire shape of a Binance raw @trade stream event:
//
//	{"e":"trade","E":123456789,"s":"BTCUSDT","t":12345,"p":"0.001","q":"100","b":88,"a":50,"T":123456785,"m":true,"M":true}

type binanceTradeMessage struct {
	EventType string `json:"e"`
	EventTime int64  `json:"E"`
	Symbol    string `json:"s"`
	TradeID   int64  `json:"t"`
	Price     string `json:"p"`
	Quantity  string `json:"q"`
	TradeTime int64  `json:"T"`

	// BuyerIsMaker is Binance's "m" field. true means the buy order was
	// resting on the book (maker) and the sell order crossed the spread, so
	// the aggressor — the side that took liquidity — was the seller.
	BuyerIsMaker bool `json:"m"`
	Ignore       bool `json:"M"`
}

// normalizeTrade parses and validates one raw Binance @trade message into a
// domain.TradeEvent. It rejects messages that aren't trade events or have
// non-positive price/quantity, since those can't have come from a real fill.
// ingestTime is injected (not read from the clock here) so the function stays
// pure and its output is fully assertable in tests.
func normalizeTrade(raw []byte, ingestTime time.Time) (domain.TradeEvent, error) {
	var msg binanceTradeMessage
	if err := json.Unmarshal(raw, &msg); err != nil {
		return domain.TradeEvent{}, fmt.Errorf("unmarshal trade message: %w", err)
	}

	if msg.EventType != "trade" {
		return domain.TradeEvent{}, fmt.Errorf("unexpected event type %q", msg.EventType)
	}
	if msg.Symbol == "" {
		return domain.TradeEvent{}, fmt.Errorf("missing symbol")
	}

	price, err := strconv.ParseFloat(msg.Price, 64)
	if err != nil || price <= 0 {
		return domain.TradeEvent{}, fmt.Errorf("invalid price %q", msg.Price)
	}

	quantity, err := strconv.ParseFloat(msg.Quantity, 64)
	if err != nil || quantity <= 0 {
		return domain.TradeEvent{}, fmt.Errorf("invalid quantity %q", msg.Quantity)
	}

	if msg.TradeTime <= 0 {
		return domain.TradeEvent{}, fmt.Errorf("invalid trade time %d", msg.TradeTime)
	}

	side := domain.SideBuy
	if msg.BuyerIsMaker {
		side = domain.SideSell
	}

	return domain.TradeEvent{
		Symbol:     msg.Symbol,
		Price:      price,
		Quantity:   quantity,
		Side:       side,
		TradeID:    msg.TradeID,
		EventTime:  time.UnixMilli(msg.TradeTime).UTC(),
		IngestTime: ingestTime,
	}, nil
}
