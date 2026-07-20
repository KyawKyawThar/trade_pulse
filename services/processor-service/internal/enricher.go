package internal

import (
	"context"
	"trade_pulse/shared/domain"
)

type Enricher struct {
	next     TradeHandler
	metadata MetadataProvider
}

// NewEnricher builds an Enricher that hands enriched events to next (in
// service.go, fanOut.Publish), looking up each event's market metadata via
// metadata (metadata.go).
func NewEnricher(next TradeHandler, metadata MetadataProvider) *Enricher {
	return &Enricher{
		next:     next,
		metadata: metadata,
	}
}

// Handle computes event's notional value (price * quantity, in the quote
// asset), attaches its market metadata, and forwards the enriched copy to
// next. event is passed by value, so the caller's copy (e.g. pool.go's job
// queue) is never mutated.
//
// A metadata miss isn't treated as an error: Notional still feeds the whale
// detector regardless, so a trade is forwarded with zero-value Market rather
// than dropped over a symbol the metadata table hasn't caught up with.
func (e *Enricher) Handle(ctx context.Context, event domain.TradeEvent) error {

	event.Notional = event.Price * event.Quantity
	event.Market, _ = e.metadata.Lookup(event.Symbol)

	return e.next(ctx, event)
}
