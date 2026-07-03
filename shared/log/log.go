package log

import (
	"os"
	"time"

	"github.com/rs/zerolog"
)

func New(serviceName, env, level string) zerolog.Logger {
	lvl, err := zerolog.ParseLevel(level)

	if err != nil {
		lvl = zerolog.InfoLevel
	}

	var logger zerolog.Logger

	if env == "prod" {
		logger = zerolog.New(os.Stdout)
	} else {
		logger = zerolog.New(zerolog.ConsoleWriter{Out: os.Stderr, TimeFormat: time.RFC3339})
	}

	return logger.Level(lvl).With().Timestamp().Str("service", serviceName).Logger()

}
