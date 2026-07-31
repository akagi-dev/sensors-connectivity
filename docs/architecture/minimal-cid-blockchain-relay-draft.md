# Architecture Draft: Minimal CID Blockchain Relay

## Status
Draft for architecture review (docs-only, no service implementation in this phase).

## Context
Current high-level pipeline keeps **Kafka** as the central durable bus and decoupling point:

`Sensor -> Authorizer -> Kafka -> {PubSub Broadcaster, IPFS Aggregator, Network Relay}`

The IPFS Aggregator publishes `telemetry.ipfs.published.v1` events with CID metadata.  
This draft narrows scope to the smallest relay path needed now.

## Scope of this phase (minimal)
Implement a **Network Relay** that:
1. Consumes `telemetry.ipfs.published.v1` from Kafka.
2. Sends only the IPFS CID to blockchain (anchor transaction).
3. Emits delivery result to `telemetry.relay.result.v1`.

No other payload transformation or cross-module orchestration is included.

## Minimal data contract for relay input
Expected fields from `telemetry.ipfs.published.v1` used by relay:

- `cid` (required, string)
- `trace_id` (optional, string)
- `event_id` (optional, string)

Relay output (`telemetry.relay.result.v1`) minimal fields:

- `cid`
- `status` (`submitted` | `failed`)
- `tx_hash` (when submitted)
- `error_code` / `error_message` (when failed)
- `trace_id` (if available)

## Minimal processing sequence
1. Consume one message from `telemetry.ipfs.published.v1`.
2. Deduplicate by `cid` (idempotency guard).
3. Submit blockchain transaction with CID.
4. Emit result event to `telemetry.relay.result.v1`.
5. Commit Kafka offset only after step 4 succeeds.

## Error handling (minimal policy)
- Retry transient submission/publish failures with bounded retry policy.
- If retries are exhausted, route to DLQ and emit failed result.
- Do not commit source offset before successful result handling policy is satisfied.

## Idempotency requirement
At-least-once Kafka delivery is accepted.  
Relay must enforce effective-once external action by deduplicating on `cid` before blockchain submission.

## Explicitly out of scope for this phase
- Advanced blockchain finality/reorg handling.
- Complex cryptographic attestation/proof models.
- Multi-chain abstraction and pluggable chain adapters.
- Batch anchoring optimization, fee strategy tuning, and MEV mitigation.
- Rich delivery SLA workflows beyond basic retry + DLQ.

## ADR-style decision note
### ADR: Anchor CID first, defer full relay complexity
**Decision:** Start with minimal CID anchoring from `telemetry.ipfs.published.v1` to blockchain via Network Relay.

**Rationale:**
- Delivers the core trust anchor quickly with smallest operational surface.
- Reuses existing Kafka decoupling and IPFS publication flow.
- Keeps failure domain narrow and reviewable before adding chain-specific complexity.
- Creates a clear base for future phases (finality/reorg logic, multi-chain support).

**Consequences:**
- Faster implementation and architecture validation now.
- Some reliability/compliance features are intentionally deferred and must be addressed in next phases.

## Next implementation steps (lightweight checklist)
- [ ] Define/confirm `telemetry.ipfs.published.v1` and `telemetry.relay.result.v1` minimal schemas (`cid` + optional trace/event IDs).
- [ ] Scaffold Network Relay consumer group for `telemetry.ipfs.published.v1`.
- [ ] Implement CID dedup store and idempotency checks.
- [ ] Implement blockchain submit path for CID-only transaction.
- [ ] Implement result event emission and commit-after-result flow.
- [ ] Add retry + DLQ wiring and basic operational metrics/logging.
