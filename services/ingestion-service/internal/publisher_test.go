package internal

import (
	"errors"
	"strings"
	"testing"
	"trade_pulse/shared/domain"

	"github.com/rs/zerolog"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestPublisherDeliveryStatus(t *testing.T) {
	rec := &kgo.Record{Topic: domain.TopicTradesRaw, Key: []byte("btcusdt")}

	t.Run("healthy with no deliveries yet", func(t *testing.T) {
		p := &Publisher{log: zerolog.Nop()}

		if err := p.deliveryStatus(); err != nil {
			t.Errorf("deliveryStatus() = %v, want nil", err)
		}
	})

	t.Run("failure streak is reported with count and cause", func(t *testing.T) {
		p := &Publisher{log: zerolog.Nop()}
		cause := errors.New("broker unreachable")

		p.onDelivery(rec, cause)
		p.onDelivery(rec, cause)

		err := p.deliveryStatus()
		if err == nil {
			t.Fatal("deliveryStatus() = nil, want error after failed deliveries")
		}
		if !strings.Contains(err.Error(), "last 2 deliveries failed") {
			t.Errorf("deliveryStatus() = %q, want failure count of 2", err)
		}
		if !errors.Is(err, cause) {
			t.Errorf("deliveryStatus() = %q, want wrapped cause %q", err, cause)
		}
	})

	t.Run("one successful delivery clears the streak", func(t *testing.T) {
		p := &Publisher{log: zerolog.Nop()}

		p.onDelivery(rec, errors.New("transient"))
		p.onDelivery(rec, nil)

		if err := p.deliveryStatus(); err != nil {
			t.Errorf("deliveryStatus() = %v, want nil after a success", err)
		}
	})
}
