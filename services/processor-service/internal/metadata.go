package internal

import "trade_pulse/shared/domain"

// MetadataProvider resolves a symbol's static market reference data. It's an
// interface — not a bare map field on Enricher — so Enricher doesn't care
// whether the table is built in-memory, loaded from config, or eventually
// served from Redis, and tests can substitute a fake instead of the real
// table.
type MetadataProvider interface {
	Lookup(symbol string) (domain.MarketMetadata, bool)
}

// defaultMarketMetadata seeds the symbols shared/config/config.go's
// ingestion.symbols defaults to. Every symbol ingestion-service streams today
// comes from Binance (see services/ingestion-service/internal/worker.go). Add
// an entry here when a new symbol or exchange is onboarded.
var defaultMarketMetadata = map[string]domain.MarketMetadata{
	"BTCUSDT": {BaseAsset: "BTC", QuoteAsset: "USDT", Exchange: "Binance"},
	"ETHUSDT": {BaseAsset: "ETH", QuoteAsset: "USDT", Exchange: "Binance"},
	"SOLUSDT": {BaseAsset: "SOL", QuoteAsset: "USDT", Exchange: "Binance"},
}

// StaticMetadataProvider looks symbols up in an in-memory table built once at
// construction. The table is read-only after New, so — unlike orderbook.go's
// live book — concurrent Lookup calls from every pool worker need no mutex.
type StaticMetadataProvider struct {
	table map[string]domain.MarketMetadata
}

// NewStaticMetadataProvider builds a provider from table. Tests pass their
// own fixture; NewDefaultMetadataProvider passes defaultMarketMetadata.
func NewStaticMetadataProvider(table map[string]domain.MarketMetadata) *StaticMetadataProvider {
	return &StaticMetadataProvider{table: table}
}

// NewDefaultMetadataProvider builds the provider service.go wires into
// Enricher for production use.
func NewDefaultMetadataProvider() *StaticMetadataProvider {
	return NewStaticMetadataProvider(defaultMarketMetadata)
}

// Lookup returns symbol's market metadata, or ok=false if symbol isn't in the
// table — e.g. ingestion.symbols was configured with a symbol this table
// hasn't been updated for yet. Callers should fail open on a miss (forward
// the trade with zero-value metadata) rather than drop it.
func (p *StaticMetadataProvider) Lookup(symbol string) (domain.MarketMetadata, bool) {
	meta, ok := p.table[symbol]
	return meta, ok
}
