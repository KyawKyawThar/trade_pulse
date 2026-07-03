package domain

import "time"

// Interval is an OHLCV aggregation window. (Open, High, Low, Close, and Volume)
type Interval string

// Supported candle intervals.
const (
	Interval1m  Interval = "1m"
	Interval5m  Interval = "5m"
	Interval15m Interval = "15m"
	Interval1h  Interval = "1h"
)

func (i Interval) Duration() time.Duration {

	switch i {
	case Interval1m:
		return time.Minute
	case Interval5m:
		return 5 * time.Minute
	case Interval15m:
		return 15 * time.Minute
	case Interval1h:
		return time.Hour
	default:
		return 0
	}
}
