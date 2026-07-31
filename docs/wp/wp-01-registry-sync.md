# WP-01 — `registry-sync`

## Summary
WP-01 delivers the registry projection path that keeps sensor/key authorization state locally available for low-latency lookups. It consumes finalized substrate/Robonomics registry updates and projects them into Redis. This removes blockchain RPC from the Authorizer hot path and enables predictable ingress latency.

## Depends on
- WP-00

## Scope / Goal
Implement `registry-sync` so it continuously ingests finalized registry events and maintains a Redis projection keyed for Authorizer reads (sensor/key enabled/disabled and related authorization status required by ingress checks).

## Out of scope / Deferred
- Authorizer HTTP ingress implementation.
- Direct sensor telemetry validation/signature verification.
- Non-finalized-chain speculative projection, finality/reorg advanced handling beyond minimal finalized-event processing.

## Inputs & Outputs
### Inputs
- Upstream external system: substrate-based Robonomics blockchain registry events (finalized events only).
- Shared contracts/runtime from WP-00.

### Outputs
- Redis projection used by `authorizer` for sensor/key status lookups.
- Operational telemetry/logging/metrics for sync lag, projection updates, and failures.

## Detailed tasks / Implementation checklist
- [ ] Replace scaffold/TODO logic in `registry-sync` with finalized registry event consumption.
- [ ] Define normalized Redis keyspace/schema for sensor and key status lookups required by Authorizer.
- [ ] Implement event-to-projection mapping for create/update/disable flows needed by authorization checks.
- [ ] Ensure projection updates are idempotent (safe to replay same chain event without inconsistent state).
- [ ] Track processed chain cursor/checkpoint to support restart recovery.
- [ ] Add bounded retry handling for transient chain/Redis failures.
- [ ] Route exhausted failures to module DLQ path using shared runtime patterns from WP-00 where applicable.
- [ ] Expose health and metrics endpoints (sync height/lag, update count, failure count, retry count).
- [ ] Add structured logs with correlation fields for chain event identifiers.

## Idempotency & error handling
- Dedup/idempotency key for projection updates should be chain-event identity (block height + event index, or equivalent finalized-event unique key).
- Redis writes must be upsert/idempotent so replay cannot corrupt authorization state.
- Retries must be bounded; exhausted failures must be surfaced to DLQ/operations without blocking all forward progress.

## Testing
- Unit tests:
  - chain event parsing,
  - projection mapping,
  - Redis key computation,
  - idempotent re-application behavior.
- Integration tests:
  - local `docker-compose` Redis + chain event fixture/replayer,
  - restart/replay behavior from stored cursor,
  - transient failure retry and exhausted-failure handling.
- Contract tests:
  - Authorizer read contract against projected Redis schema.

## Definition of Done
- All `TODO` markers in `registry-sync` are replaced with real logic.
- Unit tests + integration test against local `docker-compose` infra are green.
- `pnpm build`, `pnpm typecheck`, `pnpm lint`, `pnpm test` are green.
- Bounded retry + DLQ behavior is wired per shared runtime policy.
- Baseline structured logging and health/metrics endpoint are implemented.
- Authorizer can read required sensor/key status from Redis with no blockchain RPC in ingress hot path.
