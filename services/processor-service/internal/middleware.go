package internal

import (
	"context"
	"trade_pulse/shared/domain"

	"github.com/rs/zerolog"
)

// TradeMiddleware wraps a TradeHandler with a cross-cutting concern (logging,
// metrics, retry, ...) and returns a new TradeHandler. Composing a chain of
// these keeps the terminal handler free of concerns unrelated to what a trade
// event actually means, mirroring the net/http middleware idiom.
type TradeMiddleware func(next TradeHandler) TradeHandler

// Chain wraps base with mws, in order: mws[0] is outermost and runs first.
func Chain(base TradeHandler, mws ...TradeMiddleware) TradeHandler {
	h := base
	for i := len(mws) - 1; i >= 0; i-- {
		h = mws[i](h)
	}
	return h
}

// withLogging logs each trade at debug level before handing it to next. It
// never alters next's return value — logging must not change dispatch's
// commit decision.
func withLogging(log zerolog.Logger) TradeMiddleware {
	return func(next TradeHandler) TradeHandler {
		return func(ctx context.Context, event domain.TradeEvent) error {
			log.Debug().
				Str("symbol", event.Symbol).
				Float64("price", event.Price).
				Float64("quantity", event.Quantity).
				Str("side", string(event.Side)).
				Int64("trade_id", event.TradeID).
				Msg("consumed trade")

			return next(ctx, event)
		}
	}
}
