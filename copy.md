<!-- Keep PRs small and reviewable. One sprint task ≈ one PR. -->

## What & why

<!-- One or two sentences. Link the sprint task: SPRINT_PLAN.md § Sprint N task X. -->

## Changes

-

## Architecture discipline (check what applies)

- [ ] Trade **events** stay on Kafka (fan-out); one-time **commands** stay on RabbitMQ (consumed once)
- [ ] No direct service-to-service HTTP — communication via brokers or Redis/ClickHouse only
- [ ] Any new at-least-once consumer dedups replayed events (Kafka rebalance is expected, not an edge case)
- [ ] New long-lived connections handle reconnection/backoff and graceful shutdown
- [ ] Shared types changed? Updated `shared/domain` once; all services rebuild against it

## Testing

<!-- How did you verify? `make ci` green? Manual demo per the sprint deliverable? -->

- [ ] `make ci` passes locally
