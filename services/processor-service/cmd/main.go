package main

import (
	"os"
	"trade_pulse/services/processor-service/internal"
	"trade_pulse/shared/config"
	"trade_pulse/shared/httpserver"
	"trade_pulse/shared/runtime"

	applog "trade_pulse/shared/log"
)

const serviceName = "processor-service"

func main() {
	cfg, err := config.Load(serviceName)

	if err != nil {
		println("config load failed:", err.Error())
		os.Exit(1)
	}

	log := applog.New(serviceName, cfg.Env, cfg.LogLevel)

	log.Info().Str("env", cfg.Env).Str("http-addr", cfg.HTTPAddr).Msg("starting")

	ops := httpserver.New(cfg.HTTPAddr, log)
	svc := internal.New(cfg, log)

	ctx, cancel := runtime.SignalContext()
	defer cancel()

	if err := runtime.Run(ctx, log, ops.Start, svc.Run); err != nil {
		log.Fatal().Err(err).Msg("service exited with error")
	}
}
