package config

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/spf13/viper"
)

type Config struct {
	// ServiceName is set by the caller (Load argument), not from config.
	ServiceName string `mapstructure:"-"`

	Env      string `mapstructure:"env"`       // "dev" | "prod" — switches log format
	LogLevel string `mapstructure:"log_level"` // zerolog level: debug/info/warn/error
	HTTPAddr string `mapstructure:"http_addr"` // health + /metrics listen address

	Kafka      KafkaConfig      `mapstructure:"kafka"`
	Redis      RedisConfig      `mapstructure:"redis"`
	RabbitMQ   RabbitMQConfig   `mapstructure:"rabbitmq"`
	ClickHouse ClickHouseConfig `mapstructure:"clickhouse"`
	FX         FXConfig         `mapstructure:"fx"`
	Ingestion  IngestionConfig  `mapstructure:"ingestion"`
	Processor  ProcessorConfig  `mapstructure:"processor"`
}

// IngestionConfig tunes ingestion-service: the set of symbols to stream from
// the exchange. Config-driven so the symbol set changes without a rebuild.
type IngestionConfig struct {
	Symbols []string `mapstructure:"symbols"`
}

type ProcessorConfig struct {
	PoolSize     int `mapstructure:"pool_size"`
	FanOutBuffer int `mapstructure:"fanout_buffer"` // per-sink channel capacity in fanout.go
}

// KafkaConfig is the event-streaming backbone connection.
type KafkaConfig struct {
	Brokers []string `mapstructure:"brokers"`
}

// RedisConfig is the cache connection (live snapshots, dedup, rate limits).
type RedisConfig struct {
	Addr     string `mapstructure:"addr"`
	Password string `mapstructure:"password"`
	DB       int    `mapstructure:"db"`
}

// RabbitMQConfig is the alert task-queue connection.
type RabbitMQConfig struct {
	URL string `mapstructure:"url"`
}

// ClickHouseConfig is the analytics OLAP store connection.
type ClickHouseConfig struct {
	Addr     string `mapstructure:"addr"`
	Database string `mapstructure:"database"`
	Username string `mapstructure:"username"`
	Password string `mapstructure:"password"`
}

// FXConfig tunes fx-rate-service: which provider, poll cadence, cache TTL, and
// the circuit-breaker thresholds.
type FXConfig struct {
	Provider        string        `mapstructure:"provider"`         // openexchangerates | exchangerate.host | ecb
	APIKey          string        `mapstructure:"api_key"`          //
	PollInterval    time.Duration `mapstructure:"poll_interval"`    // default 60s
	CacheTTL        time.Duration `mapstructure:"cache_ttl"`        // default 5m
	BreakerMaxFail  int           `mapstructure:"breaker_max_fail"` // consecutive failures to trip open
	BreakerCooldown time.Duration `mapstructure:"breaker_cooldown"`
}

// Load reads configuration for the named service. It looks for an optional YAML
// file at $TRADEPULSE_CONFIG (or ./config.yaml), overlays any TRADEPULSE_*
// environment variables, and falls back to defaults. Env wins over file wins
// over default — the standard 12-factor precedence.
func Load(serviceName string) (Config, error) {
	v := viper.New()

	// Defaults sensible for local `docker-compose up`. Production overrides via env.
	v.SetDefault("env", "dev")
	v.SetDefault("log_level", "info")
	v.SetDefault("http_addr", ":8080")
	v.SetDefault("kafka.brokers", []string{"localhost:9093"})
	v.SetDefault("redis.addr", "localhost:6379")
	v.SetDefault("redis.db", 0)
	v.SetDefault("rabbitmq.url", "amqp://guest:guest@localhost:5672/")
	v.SetDefault("clickhouse.addr", "localhost:9000")
	v.SetDefault("clickhouse.database", "tradepulse")
	v.SetDefault("clickhouse.username", "default")
	v.SetDefault("fx.provider", "exchangerate.host")
	v.SetDefault("fx.poll_interval", time.Minute)
	v.SetDefault("fx.cache_ttl", 5*time.Minute)
	v.SetDefault("fx.breaker_max_fail", 5)
	v.SetDefault("fx.breaker_cooldown", 30*time.Second)
	v.SetDefault("ingestion.symbols", []string{"btcusdt", "ethusdt", "solusdt"})
	v.SetDefault("processor.pool_size", 100)
	v.SetDefault("processor.fanout_buffer", 256)

	// Environment: TRADEPULSE_REDIS_ADDR -> redis.addr, etc.
	v.SetEnvPrefix("TRADEPULSE")
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	v.AutomaticEnv()

	// Optional YAML file. Absence is fine; a malformed file is not.
	if path := v.GetString("config"); path != "" {
		v.SetConfigFile(path)
	} else {
		v.SetConfigName("config")
		v.SetConfigType("yaml")
		v.AddConfigPath(".")
		v.AddConfigPath("/etc/tradepulse")
	}
	var notFound viper.ConfigFileNotFoundError
	if err := v.ReadInConfig(); err != nil && !errors.As(err, &notFound) {
		return Config{}, fmt.Errorf("read config: %w", err)
	}

	var cfg Config
	if err := v.Unmarshal(&cfg); err != nil {
		return Config{}, fmt.Errorf("unmarshal config: %w", err)
	}
	cfg.ServiceName = serviceName
	return cfg, nil
}
