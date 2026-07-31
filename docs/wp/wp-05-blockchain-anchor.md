# WP-05 — `blockchain-anchor`

## Summary
WP-05 anchors IPFS CIDs into the substrate-based Robonomics blockchain as the first minimal blockchain phase. It consumes IPFS publication events, deduplicates by CID, submits anchoring transactions, and publishes result events. The implementation must preserve at-least-once processing guarantees without committing offsets before submission confirmation.

## Depends on
- WP-00
- WP-04

## Scope / Goal
Implement `blockchain-anchor` to consume `telemetry.ipfs.result.v1`, submit CID-only anchoring transactions to Robonomics, emit `telemetry.blockchain.result.v1`, and commit offsets only after submission confirmation and result emission.

## Out of scope / Deferred
- Advanced finality/reorg handling.
- Attestation/proof frameworks.
- Multi-chain adapters/abstraction.
- Advanced fee/relay optimization strategies.

## Inputs & Outputs
### Inputs
- Kafka topic consumed: `telemetry.ipfs.result.v1` payload fields:
  - `cid`
  - `event_count`
- Shared envelope/contracts from WP-00.
- External system: substrate-based Robonomics blockchain.

### Outputs
- External side effect: CID-only blockchain anchoring transaction submission.
- Kafka topic produced: `telemetry.blockchain.result.v1` payload fields:
  - `target`
  - `status` (`submitted` | `failed`)
  - `cid`
  - `tx_hash`
  - `error_code`
  - `error_message`

## Detailed tasks / Implementation checklist
- [ ] Replace service stubs/TODOs with a real consumer for `telemetry.ipfs.result.v1`.
- [ ] Validate consumed events against WP-00 schemas.
- [ ] Implement CID deduplication guard before blockchain submission.
- [ ] Implement substrate/Robonomics client submission for CID-only anchoring.
- [ ] Wait for submission confirmation per minimal phase policy.
- [ ] Emit `telemetry.blockchain.result.v1` with success/failure status and transaction/error details.
- [ ] Apply bounded retry for transient submission failures.
- [ ] Route exhausted failures to `telemetry.dlq.v1`.
- [ ] Commit Kafka offset only after submission confirmation and result-event emission.
- [ ] Add structured logging and health/metrics endpoint (submission latency, success/failure, retry/DLQ counts).

## Idempotency & error handling
- Dedup key: `cid`.
- Replayed messages with same `cid` must not create inconsistent duplicate anchoring behavior.
- Consumer rule must remain: consume → submit CID → wait confirmation → emit result event → commit offset.
- Bounded retries for transient blockchain/RPC failures; exhausted failures must go to `telemetry.dlq.v1`.

## Testing
- Unit tests:
  - CID dedup logic,
  - result payload mapping (`submitted` vs `failed`),
  - retry policy classification.
- Integration tests:
  - local `docker-compose` Kafka + substrate/Robonomics test environment/mock,
  - verify consume → submit → result event → commit order,
  - verify DLQ behavior on exhausted failures.
- Contract tests:
  - consumed `telemetry.ipfs.result.v1` and produced `telemetry.blockchain.result.v1` compatibility.

## Definition of Done
- All `TODO` markers in `blockchain-anchor` are replaced with real logic.
- Unit tests + integration test against local `docker-compose` infra are green.
- `pnpm build`, `pnpm typecheck`, `pnpm lint`, `pnpm test` are green.
- Bounded retry + DLQ via shared consumer runtime is implemented.
- Baseline structured logging and health/metrics endpoint are implemented.
- CID-only anchoring works end-to-end with result events and offset-commit-after-confirmation policy.
- Deferred items (finality/reorg, attestation, multi-chain) are explicitly documented as not in this phase.
