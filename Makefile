# TradePulse — microservice dev Makefile
#
# Usage:
#   make sync                       # add all service modules to go.work
#   make dev   s=ingestion-service  # live-reload a service with air
#   make run   s=analytics-service  # build + run once (no reload)
#   make build s=ingestion-service  # build binary into <service>/tmp
#   make tidy  s=analytics-service  # go mod tidy for one service
#   make build-all                  # build every service
#   make clean                      # remove all tmp/ build dirs
#   make help                       # list targets + discovered services

SERVICES_DIR := services
# Auto-discover service dirs (each holds its own go.mod + .air.*.toml)
SERVICES     := $(notdir $(wildcard $(SERVICES_DIR)/*))

# Per-service air config, e.g. services/ingestion-service/.air.ingestion.toml
AIR_CFG       = $(notdir $(firstword $(wildcard $(SERVICES_DIR)/$(s)/.air.*.toml)))

.DEFAULT_GOAL := help

.PHONY: help
help:
	@echo "TradePulse microservice targets:"
	@echo "  make sync                  - go work use -r .  (register all modules)"
	@echo "  make dev   s=<service>     - live reload with air"
	@echo "  make run   s=<service>     - build + run once"
	@echo "  make build s=<service>     - build binary into <service>/tmp"
	@echo "  make tidy  s=<service>     - go mod tidy for one service"
	@echo "  make build-all             - build every service"
	@echo "  make clean                 - remove all tmp/ dirs"
	@echo ""
	@echo "Available services: $(SERVICES)"

# Fail fast when a target needs s= but none was given.
.PHONY: guard-s
guard-s:
	@if [ -z "$(s)" ]; then \
		echo "Error: missing service. Usage: make $(firstword $(MAKECMDGOALS)) s=<service>"; \
		echo "Available services: $(SERVICES)"; \
		exit 1; \
	fi

# ---- go workspace -----------------------------------------------------------
.PHONY: sync
sync:
	go work use -r .
	@echo "go.work synced with all modules under ./$(SERVICES_DIR)"

# ---- live reload (air) ------------------------------------------------------
.PHONY: dev
dev: guard-s
	@test -n "$(AIR_CFG)" || { echo "No .air.*.toml found in $(SERVICES_DIR)/$(s)"; exit 1; }
	cd $(SERVICES_DIR)/$(s) && air -c $(AIR_CFG)

# ---- build / run ------------------------------------------------------------
.PHONY: build
build: guard-s
	cd $(SERVICES_DIR)/$(s) && go build -o ./tmp/$(s) ./cmd

.PHONY: run
run: build
	./$(SERVICES_DIR)/$(s)/tmp/$(s)

.PHONY: build-all
build-all:
	@for svc in $(SERVICES); do \
		echo ">> building $$svc"; \
		( cd $(SERVICES_DIR)/$$svc && go build -o ./tmp/$$svc ./cmd ) || exit 1; \
	done

# ---- housekeeping -----------------------------------------------------------
.PHONY: tidy
tidy: guard-s
	cd $(SERVICES_DIR)/$(s) && go mod tidy

.PHONY: clean
clean:
	rm -rf $(SERVICES_DIR)/*/tmp
	@echo "removed all tmp/ build dirs"
