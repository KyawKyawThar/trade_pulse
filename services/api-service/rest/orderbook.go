package rest

import (
	"encoding/json"
	"errors"
	"net/http"
	"time"
	"trade_pulse/shared/domain"

	"github.com/go-chi/chi"
	"github.com/redis/go-redis/v9"
)

type orderBookResponse struct {
	Symbol     string              `json:"symbol"`
	Bids       []domain.PriceLevel `json:"bids"`
	Asks       []domain.PriceLevel `json:"asks"`
	BestBid    *domain.PriceLevel  `json:"best_bid,omitempty"`
	BestAsk    *domain.PriceLevel  `json:"best_ask,omitempty"`
	LastUpdate time.Time           `json:"last_update"`
	Source     string              `json:"source"`
}

func (r *RedisReader) OrderBook(w http.ResponseWriter, req *http.Request) {

	symbol := normalizeSymbol(chi.URLParam(req, "symbol"))

	if symbol == "" {
		writeError(w, http.StatusBadRequest, "symbol is required")
		return
	}

	raw, err := r.client.Get(req.Context(), domain.KeyOrderBook(symbol)).Bytes()

	if errors.Is(err, redis.Nil) {
		writeError(w, http.StatusNotFound, "order book not found")
		return
	}
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, "redis unavailable")
		return
	}

	var book domain.OrderBook
	if err := json.Unmarshal(raw, &book); err != nil {
		writeError(w, http.StatusInternalServerError, "cached order book is invalid")
		return
	}

	resp := orderBookResponse{
		Symbol:     book.Symbol,
		Bids:       limitLevels(book.Bids, 20),
		Asks:       limitLevels(book.Asks, 20),
		LastUpdate: book.LastUpdate,
		Source:     "redis",
	}
	if bid, ok := book.BestBid(); ok {
		resp.BestBid = &bid
	}
	if ask, ok := book.BestAsk(); ok {
		resp.BestAsk = &ask
	}
	writeJSON(w, http.StatusOK, resp)
}

func limitLevels(levels []domain.PriceLevel, limit int) []domain.PriceLevel {
	if len(levels) <= limit {
		return levels
	}

	return levels[:limit]
}
