package internal

import (
	"context"
	"sync"
	"testing"
	"time"
	"trade_pulse/shared/domain"
)

func TestOrderBookStoreGetMissingSymbol(t *testing.T) {

	s := NewOrderBookStore()

	if _, ok := s.Get("BTCUSDT"); ok {
		t.Fatal("Get() ok = true for a symbol never updated, want false")
	}
}

func TestOrderBookStoreUpdateThenGet(t *testing.T) {

	s := NewOrderBookStore()

	want := domain.OrderBook{
		Symbol: "BTCUSDT",
		Bids:   []domain.PriceLevel{{Price: 100, Quantity: 1}},
		Asks:   []domain.PriceLevel{{Price: 101, Quantity: 2}},
	}

	s.Update("BTCUSDT", want)

	got, ok := s.Get("BTCUSDT")

	if !ok {
		t.Fatal("Get() ok = false, want true after Update")
	}

	if got.Symbol != want.Symbol || len(got.Bids) != 1 || len(got.Asks) != 1 ||
		got.Bids[0] != want.Bids[0] || got.Asks[0] != want.Asks[0] {
		t.Errorf("Get() = %+v, want %+v", got, want)
	}
}

func TestOrderBookStoreApplyBuySetsBestAsk(t *testing.T) {
	s := NewOrderBookStore()

	now := time.Now()

	s.Apply(domain.TradeEvent{
		Symbol:    "BTCUSDT",
		Price:     65000.50,
		Quantity:  0.5,
		Side:      domain.SideBuy,
		EventTime: now,
	})

	book, ok := s.Get("BTCUSDT")

	if !ok {
		t.Fatal("Get() ok = false after Apply")
	}

	ask, ok := book.BestAsk()

	if !ok {
		t.Fatal("BestAsk() ok = false, want an ask set by the BUY print")
	}

	if want := (domain.PriceLevel{Price: 65000.50, Quantity: 0.5}); want != ask {
		t.Errorf("BestAsk() = %+v, want %+v", ask, want)
	}

	if _, ok := book.BestBid(); ok {
		t.Error("BestBid() ok = true, want false: a BUY print must not set the bid side")
	}

	if !book.LastUpdate.Equal(now) {
		t.Errorf("LastUpdate = %v, want %v", book.LastUpdate, now)
	}
}

func TestOrderBookStoreApplySellSetsBestBid(t *testing.T) {
	s := NewOrderBookStore()

	s.Apply(domain.TradeEvent{Symbol: "ETHUSDT", Price: 3000, Quantity: 2, Side: domain.SideSell})

	book, ok := s.Get("ETHUSDT")

	if !ok {
		t.Fatal("Get() ok = false after Apply")
	}

	bid, ok := book.BestBid()
	if !ok {
		t.Fatal("BestBid() ok = false, want a bid set by the SELL print")
	}
	if want := (domain.PriceLevel{Price: 3000, Quantity: 2}); bid != want {
		t.Errorf("BestBid() = %+v, want %+v", bid, want)
	}
}

func TestOrderBookStoreApplyReplacesPreviousLevel(t *testing.T) {
	s := NewOrderBookStore()

	s.Apply(domain.TradeEvent{Symbol: "BTCUSDT", Price: 100, Quantity: 1, Side: domain.SideBuy})
	s.Apply(domain.TradeEvent{Symbol: "BTCUSDT", Price: 200, Quantity: 3, Side: domain.SideBuy})

	book, _ := s.Get("BTCUSDT")

	if len(book.Asks) != 1 {
		t.Fatalf("len(Asks) = %d, want 1 (top-of-book only)", len(book.Asks))
	}

	if want := (domain.PriceLevel{Price: 200, Quantity: 3}); book.Asks[0] != want {
		t.Errorf("Asks[0] = %+v, want %+v", book.Asks[0], want)
	}
}

func TestOrderBookStoreApplyKeepsOtherSideOfSameSymbol(t *testing.T) {

	s := NewOrderBookStore()

	s.Apply(domain.TradeEvent{Symbol: "BTCUSDT", Price: 100, Quantity: 1, Side: domain.SideSell})
	s.Apply(domain.TradeEvent{Symbol: "BTCUSDT", Price: 101, Quantity: 2, Side: domain.SideBuy})

	book, _ := s.Get("BTCUSDT")

	if _, ok := book.BestBid(); !ok {
		t.Error("BestBid() ok = false, want the earlier SELL print's bid to survive")
	}

	if _, ok := book.BestAsk(); !ok {
		t.Error("BestAsk() ok = false, want the BUY print's ask")
	}

}

func TestOrderBookStoreRunAppliesUntilCancelled(t *testing.T) {
	s := NewOrderBookStore()
	update := make(chan domain.TradeEvent)

	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan struct{})

	go func() {
		s.Run(ctx, update)
		close(done)
	}()

	update <- domain.TradeEvent{Symbol: "BTCUSDT", Price: 50, Quantity: 1, Side: domain.SideBuy}

	deadline := time.Now().Add(2 * time.Second)

	for {
		if _, ok := s.Get("BTCUSDT"); ok {
			break
		}

		if time.Now().After(deadline) {
			t.Fatal("timed out waiting for Run to apply the update")
		}
		time.Sleep(time.Microsecond)
	}

	cancel()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return after context cancellation")
	}
}

func TestOrderBookStoreConcurrentAccess(t *testing.T) {

	s := NewOrderBookStore()

	const symbol = "BTCUSDT"

	const iterations = 200

	var wg sync.WaitGroup

	wg.Add(2)

	go func() {
		defer wg.Done()
		for i := range iterations {
			s.Apply(domain.TradeEvent{Symbol: symbol, Price: float64(i), Quantity: 1, Side: domain.SideBuy})
		}
	}()

	go func() {
		defer wg.Done()
		for range iterations {
			s.Get(symbol)
		}
	}()

	wg.Wait()

	if _, ok := s.Get(symbol); !ok {
		t.Fatal("Get() ok = false after concurrent Apply calls, want the last write to be visible")
	}
}
