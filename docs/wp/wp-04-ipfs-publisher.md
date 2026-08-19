# WP-04 — `ipfs-publisher`

## Summary
WP-04 implements deterministic batching and IPFS publication for authorized telemetry events. It consumes `telemetry.authorized.v1`, builds stable batch artifacts, publishes/pins to IPFS, and emits CID result events back to Kafka. The module is responsible for reliable side-effect sequencing with idempotent batch handling.

## Depends on
- WP-00
- WP-02

## Scope / Goal
Deliver `ipfs-publisher` as an idempotent Kafka consumer that deterministically batches authorized telemetry, publishes/pins to IPFS, and emits `ipfs.published.v1` events containing the CID and batch event count.

## Out of scope / Deferred
- Blockchain anchoring submission logic (handled by WP-05).
- Sensor ingress authorization.
- Multi-format archival strategy beyond the selected deterministic object/CAR output.

## Inputs & Outputs
### Inputs
- Kafka topic consumed: `telemetry.authorized.v1`.
- Binary protobuf envelope and payload schemas from `@scp/core`.
- External system: IPFS node/gateway/pinning service.

### Outputs
- External side effect: deterministic batch object/CAR publication and pin success.
- Kafka topic produced: `ipfs.published.v1` payload with:
  - `cid`
  - `event_count`

## Detailed tasks / Implementation checklist
- [x] Replace service stubs/TODOs with a real `telemetry.authorized.v1` consumer.
- [x] Validate consumed events with WP-00 schemas.
- [x] Implement deterministic batching strategy and `batch_id` derivation.
- [x] Build deterministic IPFS object/CAR bytes from batch contents.
- [x] Publish and pin batch artifact to IPFS; capture resulting CID.
- [x] Deduplicate batch processing by `batch_id` to avoid duplicate IPFS publication.
- [x] Emit `ipfs.published.v1` with canonical envelope and payload (`cid`, `event_count`).
- [x] Apply bounded retry for transient IPFS/pinning failures.
- [x] Route exhausted failures to `telemetry.dlq.v1`.
- [x] Commit Kafka offset only after pin success and result-event emission succeed.
- [x] Add structured logging and health/metrics endpoint (batch count, pin latency, retry/DLQ counts).

## Idempotency & error handling
- Primary dedup key: `batch_id`.
- Replayed inputs that map to same `batch_id` must not create inconsistent duplicate publication records.
- Only commit offset after successful pin + `ipfs.published.v1` emission.
- Use bounded retry and DLQ for exhausted transient failures.

## Testing
- Unit tests:
  - deterministic batch formation,
  - `batch_id` derivation stability,
  - `event_count` calculation,
  - retry/error classification.
- Integration tests:
  - local `docker-compose` Kafka + IPFS,
  - validate consume → batch → publish/pin → emit result → commit order,
  - verify DLQ behavior on exhausted failures.
- Contract tests:
  - consumed `telemetry.authorized.v1` schema and produced `ipfs.published.v1` payload compatibility.

## Definition of Done
- All `TODO` markers in `ipfs-publisher` are replaced with real logic.
- Unit tests + integration test against local `docker-compose` infra are green.
- `pnpm build`, `pnpm typecheck`, `pnpm lint`, `pnpm test` are green.
- Bounded retry and DLQ handling are implemented locally against the current consumer API.
- Baseline structured logging and health/metrics endpoint are implemented.
- Published `ipfs.published.v1` events consistently contain valid `cid` and `event_count` after successful pin.
