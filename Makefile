# ============================================================================
# TradePulse Makefile
# ============================================================================

SERVICES_DIR := services
BIN_DIR := bin
COMPOSE := docker compose -f developments/docker-compose.yml

# ---------------------------------------------------------------------------
# Discover services automatically
# ---------------------------------------------------------------------------

SERVICES := $(notdir $(wildcard $(SERVICES_DIR)/*))

MODULES := shared $(wildcard $(SERVICES_DIR)/*)

AIR_CFG = $(notdir $(firstword $(wildcard $(SERVICES_DIR)/$(s)/.air.*.toml)))

# ---------------------------------------------------------------------------
# Build metadata
# ---------------------------------------------------------------------------

VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
COMMIT  ?= $(shell git rev-parse --short HEAD 2>/dev/null || echo none)
DATE    ?= $(shell date -u +%Y-%m-%dT%H:%M:%SZ)

LDFLAGS := -s -w \
	-X trade_pulse/shared/version.Version=$(VERSION) \
	-X trade_pulse/shared/version.Commit=$(COMMIT) \
	-X trade_pulse/shared/version.Date=$(DATE)

.DEFAULT_GOAL := help

# ============================================================================
# Help
# ============================================================================

.PHONY: help
help:
	@echo ""
	@echo "TradePulse Make Targets"
	@echo ""
	@echo "Development"
	@echo "  make sync"
	@echo "  make dev s=<service>"
	@echo "  make run s=<service>"
	@echo "  make build s=<service>"
	@echo "  make build-all"
	@echo "  make tidy s=<service>"
	@echo "  make clean"
	@echo ""
	@echo "Quality"
	@echo "  make fmt"
	@echo "  make fmt-check"
	@echo "  make vet"
	@echo "  make lint"
	@echo "  make test"
	@echo "  make ci"
	@echo ""
	@echo "Docker"
	@echo "  make up"
	@echo "  make up-full"
	@echo "  make down"
	@echo "  make logs"
	@echo "  make docker-build"
	@echo ""
	@echo "Available services:"
	@echo "  $(SERVICES)"

# ============================================================================
# Guard
# ============================================================================

.PHONY: guard-s
guard-s:
	@if [ -z "$(s)" ]; then \
		echo "Usage: make <target> s=<service>"; \
		echo "Available: $(SERVICES)"; \
		exit 1; \
	fi

# ============================================================================
# Workspace
# ============================================================================

.PHONY: sync
sync:
	go work use -r .
	@echo "Workspace synced."

# ============================================================================
# Development
# ============================================================================

.PHONY: dev
dev: guard-s
	@test -n "$(AIR_CFG)" || { echo "No air config found."; exit 1; }
	cd $(SERVICES_DIR)/$(s) && air -c $(AIR_CFG)

.PHONY: build
build: guard-s
	@mkdir -p $(SERVICES_DIR)/$(s)/tmp
	cd $(SERVICES_DIR)/$(s) && \
	go build -trimpath \
		-ldflags "$(LDFLAGS)" \
		-o ./tmp/$(s) ./cmd

.PHONY: run run-service
run run-service: build
	./$(SERVICES_DIR)/$(s)/tmp/$(s)

.PHONY: build-all
build-all:
	@mkdir -p $(BIN_DIR)
	@for svc in $(SERVICES); do \
		echo ">> $$svc"; \
		cd $(SERVICES_DIR)/$$svc && \
		go build -trimpath \
			-ldflags "$(LDFLAGS)" \
			-o ../../$(BIN_DIR)/$$svc ./cmd || exit 1; \
		cd ../..; \
	done

.PHONY: tidy
tidy: guard-s
	cd $(SERVICES_DIR)/$(s) && go mod tidy

.PHONY: tidy-all
tidy-all:
	@for m in $(MODULES); do \
		echo ">> $$m"; \
		( cd $$m && go mod tidy ) || exit 1; \
	done

# Mirrors the CI "go mod tidy is clean" step: tidy with GOWORK=off (workspace
# mode hides missing/misclassified requires that a hermetic module build or
# docker build would fail on) and fail if that changes go.mod/go.sum.
.PHONY: tidy-check
tidy-check:
	@for m in $(MODULES); do \
		echo ">> $$m"; \
		( cd $$m && GOWORK=off go mod tidy && git diff --exit-code -- go.mod go.sum ) \
			|| { echo "$$m: go.mod/go.sum not tidy — run 'make tidy s=<service>' (or 'make tidy-all') and commit"; exit 1; }; \
	done

.PHONY: clean
clean:
	rm -rf $(SERVICES_DIR)/*/tmp
	rm -rf $(BIN_DIR)

# ============================================================================
# Formatting
# ============================================================================

.PHONY: fmt
fmt:
	@gofmt -w $(shell find . -name '*.go')

.PHONY: fmt-check
fmt-check:
	@out=$$(gofmt -l $(shell find . -name '*.go')); \
	if [ -n "$$out" ]; then \
		echo "$$out"; \
		exit 1; \
	fi

# ============================================================================
# Static analysis
# ============================================================================

.PHONY: vet
vet:
	@for m in $(MODULES); do \
		echo ">> $$m"; \
		( cd $$m && go vet ./... ) || exit 1; \
	done

.PHONY: lint
lint:
	@for m in $(MODULES); do \
		echo ">> $$m"; \
		( cd $$m && golangci-lint run ./... ) || exit 1; \
	done

# ============================================================================
# Tests
# ============================================================================

.PHONY: test
test:
	@for m in $(MODULES); do \
		echo ">> $$m"; \
		( cd $$m && go test -race -count=1 ./... ) || exit 1; \
	done

.PHONY: ci
ci: fmt-check tidy-check vet lint test build-all

# ============================================================================
# Docker
# ============================================================================

.PHONY: docker-build
docker-build:
	@for svc in $(SERVICES); do \
		echo ">> $$svc"; \
		docker build \
			-f $(SERVICES_DIR)/$$svc/Dockerfile \
			--build-arg VERSION=$(VERSION) \
			--build-arg COMMIT=$(COMMIT) \
			--build-arg DATE=$(DATE) \
			-t tradepulse/$$svc:$(VERSION) \
			-t tradepulse/$$svc:latest . || exit 1; \
	done

.PHONY: up
up:
	$(COMPOSE) up -d

.PHONY: up-full
up-full:
	$(COMPOSE) --profile full up -d --build

.PHONY: down
down:
	$(COMPOSE) --profile full down

.PHONY: logs
logs:
	$(COMPOSE) logs -f --tail=100
