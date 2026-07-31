# General Architecture

## Purpose

The system accepts Ed25519-signed environmental sensor telemetry (Altruist-series sensors), verifies authenticity and authorization, and distributes trusted events to downstream channels with Kafka as the central durable bus.

## High-level architecture

`Sensor -> Authorize -> Message Bus (Kafka) -> Services (IPFS, PubSub, Blochain)`

> Authorization source: `Robonomics Blockchain -> Registry Sync -> Redis -> Authorize`

### PubSub Broadcast

`Trusted Messages (Kafka) -> PubSub Broadcaster -> IPFS PubSub -> Web UI (sensors.social)`

### Blockchain anchoring

`Trusted Messages (Kafka) -> IPFS Publisher -> Message Bus (Kafka) -> Robonomics Blockchain`

## Module responsibilities

### Sensor
- Out of scope for this project phase (designed and implemented by third party).
- Sends telemetry via `POST /v1/telemetry`.
- Includes signature and anti-replay fields.
- Retries same payload safely when delivery fails.

### Authorize
- Validates request schema and limits.
- Verifies hash, timestamp window, nonce, and Ed25519 signature.
- Checks sensor/key status from local registry projection (no blockchain RPC in hot path).
- Publishes authorized events to Kafka.
- Returns `202` only after Kafka ACK.

### Registry Sync
- Consumes finalized Robonomics blockchain events.
- Maintains local projection of sensor/key status.
- Serves low-latency reads to Authorize module (optionally via Redis cache).

### Kafka (central bus)
- Durable event log and decoupling point for all processing modules.
- Enables replay, independent scaling, and fault isolation per consumer group.

### PubSub Broadcaster
- Consumes authorized events from Kafka.
- Publishes to libp2p/GossipSub topics.
- Commits offset only after publish confirmation policy.

### IPFS Publisher
- Consumes authorized events from Kafka.
- Batches, produces IPFS object/CAR, publishes CID.
- Emits `telemetry.ipfs.published.v1`.
- Commits offset only after publish/pin success policy.

### Robonomics Blockchain
- Consumes IPFS-published events (`telemetry.ipfs.published.v1`) from Kafka.
- Publishes the CID into the substrate-based Robonomics blockchain to make the CID immutable.
- Deduplicates by CID before submission.
- Emits anchoring result events (`telemetry.blockchain.result.v1`).
- Commits offset only after blockchain submission confirmation.

## Core Kafka topics
- `telemetry.authorized.v1`
- `telemetry.rejected.v1`
- `telemetry.pubsub.result.v1`
- `telemetry.ipfs.published.v1`
- `telemetry.blockchain.result.v1`
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
  - `cid` for blockchain anchoring operations.

## Error handling baseline
- Bounded retries for transient failures.
- Route exhausted failures to DLQ.
- Keep retries/DLQ isolated per module to prevent cross-module blocking.

## Explicit architectural constraints
- No synchronous dependency between processing modules.
- Allowed flow: `Authorizer -> Kafka -> Consumers`.
- Disallowed direct couplings:
  - Authorize -> PubSub
  - Authorize -> IPFS
  - PubSub -> IPFS
  - IPFS -> Blockchain (must flow through Kafka)
