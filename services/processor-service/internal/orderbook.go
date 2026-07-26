package internal

import (
	"context"
	"sync"
	"trade_pulse/shared/domain"
)

type OrderBookStore struct {
	mu   sync.RWMutex
	data map[string]domain.OrderBook
}

func NewOrderBookStore() *OrderBookStore {
	return &OrderBookStore{
		data: make(map[string]domain.OrderBook),
	}

}

func (s *OrderBookStore) Get(symbol string) (domain.OrderBook, bool) {

	s.mu.Lock()
	defer s.mu.Unlock()
	book, ok := s.data[symbol]

	return book, ok
}

func (s *OrderBookStore) Update(symbol string, book domain.OrderBook) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data[symbol] = book
}

func (s *OrderBookStore) Apply(event domain.TradeEvent) {
	book, _ := s.Get(event.Symbol)
	book.Symbol = event.Symbol
	book.LastUpdate = event.EventTime

	level := domain.PriceLevel{
		Price:    event.Price,
		Quantity: event.Quantity,
	}

	switch event.Side {
	case domain.SideBuy:
		book.Asks = []domain.PriceLevel{level}
	case domain.SideSell:
		book.Bids = []domain.PriceLevel{level}
	}
	s.Update(event.Symbol, book)
}

func (s *OrderBookStore) Run(ctx context.Context, update <-chan domain.TradeEvent) {
	for {
		select {
		case event := <-update:
			s.Apply(event)
		case <-ctx.Done():
			return
		}
	}
}
