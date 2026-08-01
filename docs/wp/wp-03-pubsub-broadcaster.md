# WP-03 — `pubsub-broadcaster`

## Summary
WP-03 delivers GossipSub fan-out for authorized telemetry. The service consumes trusted events from Kafka and publishes them to configured libp2p/GossipSub topics. It then emits publish result events and commits offsets only after publish confirmation policy is satisfied.

## Depends on
- WP-00
- WP-02

## Scope / Goal
Implement `pubsub-broadcaster` as an idempotent Kafka consumer that relays `telemetry.authorized.v1` events to GossipSub and records publish outcomes via Kafka result events.

## Out of scope / Deferred
- Sensor authentication/authorization logic.
- IPFS batching/publishing.
- Blockchain anchoring logic.

## Inputs & Outputs
### Inputs
- Kafka topic consumed: `telemetry.authorized.v1`.
- Envelope/payload contract from WP-00.
- External system: libp2p/GossipSub network.

### Outputs
- External side effect: publish authorized telemetry to configured GossipSub topics.
- Kafka result topic produced: `telemetry.pubsub.result.v1`.
- Kafka DLQ topic produced on exhausted failures: `telemetry.dlq.v1`.

## Detailed tasks / Implementation checklist
- [ ] Replace service stubs/TODOs with a real consumer wired to `telemetry.authorized.v1`.
- [ ] Validate consumed envelope/payload against WP-00 schemas.
- [ ] Map telemetry events to configured GossipSub topic(s) deterministically.
- [ ] Implement publish confirmation policy (success/failure signal from pubsub client).
- [ ] Emit `telemetry.pubsub.result.v1` outcome event after publish attempt using canonical envelope.
- [ ] Implement bounded retry behavior for transient publish failures.
- [ ] Route exhausted failures to `telemetry.dlq.v1` using shared consumer runtime policy.
- [ ] Commit Kafka offset only after external publish handling and result event emission are successful.
- [ ] Add structured logging and health/metrics endpoint (consumer lag, publish success/failure, retry/DLQ counts).
- [ ] Add permanent reserved-peer support via `PUBSUB_RESERVED_PEERS` (comma-separated multiaddrs), with startup dial and continuous re-dial.

## Idempotency & error handling
- Dedup key: `event_id` from consumed envelope.
- Reprocessing same `event_id` must not cause unbounded duplicate side effects; apply idempotency tracking around publish attempts.
- Retry transient publish errors with bounded policy.
- On exhausted retries, emit DLQ record to `telemetry.dlq.v1` and avoid premature offset commit.

## Configuration notes

- `PUBSUB_TOPIC` controls the deterministic GossipSub fan-out topic (default `telemetry/authorized/v1`).
- `PUBSUB_RESERVED_PEERS` accepts a comma-separated list of libp2p multiaddrs that should remain permanently connected; the service dials these peers at startup and periodically re-dials to maintain peering.

## Testing
- Unit tests:
  - topic mapping,
  - publish result handling,
  - idempotency behavior on repeated `event_id`,
  - retry classification (transient vs terminal).
- Integration tests:
  - local `docker-compose` Kafka with a GossipSub test harness/mock,
  - verify consume → publish → result event → commit order,
  - verify DLQ path for exhausted failures.
- Contract tests:
  - schema compatibility for consumed `telemetry.authorized.v1` and produced `telemetry.pubsub.result.v1`.

## Definition of Done
- All `TODO` markers in `pubsub-broadcaster` are replaced with real logic.
- Unit tests + integration test against local `docker-compose` infra are green.
- `pnpm build`, `pnpm typecheck`, `pnpm lint`, `pnpm test` are green.
- Bounded retry + DLQ via shared consumer runtime is implemented.
- Baseline structured logging and health/metrics endpoint are implemented.
- Offset commits happen only after publish confirmation policy and result event emission.
