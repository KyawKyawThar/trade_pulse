# Contributing to TradePulse

This is a multi-engineer monorepo. The rules below keep six independently-built
services coherent. Read [ARCHITECTURE.md](ARCHITECTURE.md) for *why* the system
is shaped this way; this doc is *how to work in it*.

## Local setup

```bash
go version        # need 1.24+
golangci-lint --version   # need v2.x
make build-all && make ci
```

`go.work` is checked in, so `go build ./...` inside any module resolves `shared/`
from your working tree automatically — no publish step. Don't add a service to
the tree without adding it to `go.work` (`go work use ./services/<name>`), the
CI matrices in `.github/workflows/`, and `.github/dependabot.yml`.

## Branching & PR flow

1. Branch off `main`: `git switch -c sprintN/<service>-<short-desc>`.
2. Keep PRs small — **one sprint task ≈ one PR**. A reviewer should be able to
   hold the whole change in their head.
3. Push and open a PR. The template's architecture checklist is not decoration —
   tick it honestly.
4. CI (`.github/workflows/ci.yml`) must be green. It runs, per module:
   gofmt, `go mod tidy` cleanliness, golangci-lint, `go vet`, `go test -race`,
   `go build`, and a no-push Docker build. Point branch protection at the
   `ci-success` check.
5. `CODEOWNERS` auto-requests review. Changes under `shared/` touch every
   service — expect (and require) an extra reviewer there.

## Commit messages

Conventional-commits style, scoped by service:

```
feat(ingestion): reconnect Binance WS with exponential backoff
fix(processor): dedup whale alerts across Kafka rebalance
chore(ci): pin golangci-lint version
```

## The non-negotiables (guarded at review)

These come straight from the architecture and the sprint plan's cross-cutting
tracks. A PR that violates one gets sent back regardless of green CI:

- **Broker discipline.** Trade *events* → Kafka (fan-out). One-time alert
  *commands* → RabbitMQ (consumed once). Never cross them.
- **No service-to-service HTTP.** Services talk only through brokers or shared
  storage (Redis/ClickHouse). The one allowed outbound HTTP is fx-rate-service →
  the FX provider, and even that result reaches everyone else via Redis.
- **Dedup at-least-once consumers from day one.** Kafka *will* rebalance and
  redeliver. Any new consumer that can act twice must have a Redis dedup gate.
- **Hardening is baked in, not bolted on.** A new long-lived connection ships
  with reconnection/backoff and participates in graceful shutdown in the same PR
  that introduces it.
- **File-size discipline.** "If you need to scroll to find a function, split the
  file." Guidance, not a gate — see ARCHITECTURE.md § Go File Size Rules.

## The shared contract (`shared/domain`)

`TradeEvent`, `Candle`, the alert types, `FXRates`, and all Kafka/RabbitMQ/Redis
names live in `shared/domain` and are imported by every service. Change them in
one place; every service rebuilds against the change (that's the point — drift
becomes a compile error, not a wire mismatch). Never hard-code a topic, queue,
routing key, or Redis key string at a call site — reference the constant/helper.

## Adding a new service

1. `mkdir -p services/<name>/{cmd,internal}` and copy an existing service's
   `cmd/main.go` (the bootstrap is uniform — only the name and imports change).
2. `internal/service.go` with a `Service` exposing `Run(ctx) error`.
3. `go.mod` with `replace github.com/tradepulse/shared => ../../shared`, plus a
   `Dockerfile` (copy an existing one, swap the path).
4. Register it: `go work use ./services/<name>`, add it to the CI/release/
   dependabot matrices and the Makefile `SERVICES` list, and add a compose
   service block.

## Running the quality gate

`make ci` mirrors the PR gate. Run it before pushing. If `go mod tidy` changed
something, commit it — CI fails on a dirty `go.mod`/`go.sum`.
