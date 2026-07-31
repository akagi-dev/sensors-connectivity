# Project Architecture Draft

## Status
Draft for architecture review (docs-only, no service implementation in this phase).

## Purpose
The system accepts signed weather sensor telemetry, verifies authenticity and authorization, and distributes trusted events to downstream channels with Kafka as the central durable bus.

## High-level architecture

`Sensor -> Telemetry Authorizer -> Kafka -> {PubSub Broadcaster, IPFS Aggregator, Network Relay}`

Supporting path for authorization data:

`Blockchain -> Registry Sync -> Local Registry / Redis -> Telemetry Authorizer`

## Module responsibilities

### 1) Sensor
- Out of scope for this project phase (designed and implemented by third party).
- Sends telemetry via `POST /v1/telemetry`.
- Includes signature and anti-replay fields.
- Retries same payload safely when delivery fails.

### 2) Telemetry Authorizer
- Validates request schema and limits.
- Verifies hash, timestamp window, nonce, and Ed25519 signature.
- Checks sensor/key status from local registry projection (no blockchain RPC in hot path).
- Publishes authorized events to Kafka.
- Returns `202` only after Kafka ACK.

### 3) Registry Sync + Local Registry
- Consumes finalized blockchain registry events.
- Maintains local projection of sensor/key status.
- Serves low-latency reads to Authorizer (optionally via Redis cache).

### 4) Kafka (central bus)
- Durable event log and decoupling point for all processing modules.
- Enables replay, independent scaling, and fault isolation per consumer group.

### 5) PubSub Broadcaster
- Consumes authorized events from Kafka.
- Publishes to libp2p/GossipSub topics.
- Commits offset only after publish confirmation policy.

### 6) IPFS Aggregator
- Consumes authorized events from Kafka.
- Batches, produces IPFS object/CAR, publishes CID.
- Emits `telemetry.ipfs.published.v1`.
- Commits offset only after publish/pin success policy.

### 7) Network Relay
- Consumes either authorized events or IPFS-published events (by relay type).
- Sends data to external networks.
- Emits delivery result events.
- **Current minimal phase:** CID-only blockchain anchoring from `telemetry.ipfs.published.v1`.

## Core Kafka topics
- `telemetry.authorized.v1`
- `telemetry.rejected.v1`
- `telemetry.pubsub.result.v1`
- `telemetry.ipfs.published.v1`
- `telemetry.relay.result.v1`
- `telemetry.retry.v1`
- `telemetry.dlq.v1`

## Delivery semantics and processing rule
- Target semantics: at-least-once delivery + idempotent consumers (effectively-once behavior).
- Consumer rule:
  1. Consume event.
  2. Execute external action.
  3. Wait for confirmation.
  4. Emit result event.
  5. Commit offset.
- Offsets must not be committed before external action/result handling succeeds.

## Idempotency and deduplication
- All downstream consumers must be idempotent.
- Dedup keys depend on module:
  - `event_id` for event-level operations.
  - `batch_id` for IPFS batch operations.
  - `delivery_id` or `cid` for relay operations.

## Error handling baseline
- Bounded retries for transient failures.
- Route exhausted failures to DLQ.
- Keep retries/DLQ isolated per module to prevent cross-module blocking.

## Explicit architectural constraints
- No synchronous dependency between processing modules.
- Allowed flow: `Authorizer -> Kafka -> Consumers`.
- Disallowed direct couplings:
  - Authorizer -> PubSub
  - Authorizer -> IPFS
  - PubSub -> IPFS
  - IPFS -> Relay

## ADR note: why minimal CID anchoring first
Decision: deliver the first Network Relay phase as CID-only blockchain anchoring.  
Reason: smallest safe slice to validate integration and operations while preserving future extensibility for finality/reorg, attestation, and multi-chain support.

## Deferred items (not in first implementation phase)
- Advanced finality/reorg handling.
- Complex attestation/proof models.
- Multi-chain abstraction/adapters.
- Advanced fee strategy and relay optimization.
- Rich SLA/workflow logic beyond basic retry + DLQ.

## Next implementation checklist
- [ ] Freeze minimal event contracts for `telemetry.authorized.v1`, `telemetry.ipfs.published.v1`, `telemetry.relay.result.v1`.
- [ ] Implement Telemetry Authorizer MVP with Kafka ACK-gated `202`.
- [ ] Implement Registry Sync + local projection read path.
- [ ] Implement PubSub Broadcaster consumer with commit-after-publish policy.
- [ ] Implement IPFS Aggregator consumer with deterministic batching and CID emission.
- [ ] Implement Network Relay phase 1 (CID-only anchoring) with CID dedup.
- [ ] Add per-module retry + DLQ and baseline observability.
- [ ] Add integration tests for end-to-end pipeline behavior.

## Related documents
- Integration contracts draft: `docs/architecture/integration-contracts-draft.md`
