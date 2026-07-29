package rest

import (
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"trade_pulse/shared/domain"

	"github.com/go-chi/chi"
	"github.com/redis/go-redis/v9"
)

type tradeResponse struct {
	Symbol string              `json:"symbol"`
	Trades []domain.TradeEvent `json:"trades"`
	Source string              `json:"source"`
}

func (r *RedisReader) LatestTrade(w http.ResponseWriter, req *http.Request) {

	symbol := normalizeSymbol(chi.URLParam(req, "symbol"))

	if symbol == "" {
		writeError(w, http.StatusBadRequest, "symbol is required")
		return
	}

	raw, err := r.client.Get(req.Context(), domain.KeyLatestTrade(symbol)).Bytes()

	if errors.Is(err, redis.Nil) {
		writeError(w, http.StatusNotFound, "trade not found")
		return
	}

	if err != nil {
		writeError(w, http.StatusServiceUnavailable, "redis unavailable")
		return
	}

	var trade domain.TradeEvent

	if err := json.Unmarshal(raw, &trade); err != nil {
		writeError(w, http.StatusInternalServerError, "cached trade is invalid")
		return
	}

	writeJSON(w, http.StatusOK, tradeResponse{
		Symbol: symbol,
		Trades: []domain.TradeEvent{trade},
		Source: "redis",
	})
}

func normalizeSymbol(symbol string) string {
	return strings.ToUpper(strings.TrimSpace(symbol))
}
