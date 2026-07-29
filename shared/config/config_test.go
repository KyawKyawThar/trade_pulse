package config

import (
	"fmt"
	"testing"
)

func TestLoadDefaults(t *testing.T) {
	clearTradePulseEnv(t)

	cfg, err := Load("ingestion-service")

	fmt.Println("cfg is:", cfg)
	if err != nil {
		t.Fatalf("Load returned error: %v", err)
	}

	if cfg.ServiceName != "ingestion-service" {
		t.Errorf("ServiceName = %q, want %q", cfg.ServiceName, "ingestion-service")

	}

	if cfg.Env != "dev" {
		t.Errorf("Env = %q, want %q", cfg.Env, "dev")
	}
	if cfg.LogLevel != "info" {
		t.Errorf("LogLevel = %q, want %q", cfg.LogLevel, "info")
	}
	if cfg.HTTPAddr != ":8080" {
		t.Errorf("HTTPAddr = %q, want %q", cfg.HTTPAddr, ":8080")
	}
	if cfg.Redis.Addr != "localhost:6379" {
		t.Errorf("Redis.Addr = %q, want %q", cfg.Redis.Addr, "localhost:6379")
	}
}

// clearTradePulseEnv unsets every TRADEPULSE_* var for the duration of the test
// so a developer's shell (or a previous test) can't leak into these assertions.
// t.Setenv with an empty value, paired with AutomaticEnv treating empty as
// unset for our scalar fields, keeps each test hermetic.
func clearTradePulseEnv(t *testing.T) {

	t.Helper()

	for _, k := range []string{
		"TRADEPULSE_CONFIG",
		"TRADEPULSE_ENV",
		"TRADEPULSE_LOG_LEVEL",
		"TRADEPULSE_HTTP_ADDR",
		"TRADEPULSE_REDIS_ADDR",
		"TRADEPULSE_REDIS_DB",
	} {
		t.Setenv(k, "")
	}
}
