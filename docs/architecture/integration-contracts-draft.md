# Sensors Integration Draft (Data Structures and APIs First)

## Status
Draft for architecture review (integration-first, docs-only).

## Goal
Define **sensor integration** boundaries before service implementation:
1. Sensor data structures.
2. Sensor-facing API contract.
3. Kafka event structures produced from sensor ingest.

## Integration principles
- Kafka is the central integration backbone.
- Contracts are versioned (`*.v1`) and backward-compatible within major version.
- Producers/consumers must preserve idempotency keys.
- Offsets are committed only after external side effects and result publication.

## Sensor request data structure
`POST /v1/telemetry` request body:

- `event_id` (string, required, UUID/ULID recommended)
- `observed_at` (RFC3339 UTC timestamp, required)
- `sequence_number` (integer >= 0, required)
- `measurements` (object, required)
- `sensor_address` (string, required)
- `key_id` (string, required)
- `timestamp` (RFC3339 UTC timestamp, required)
- `nonce` (string, required)
- `body_hash` (string, required, SHA-256 hex)
- `signature` (string, required, Ed25519 signature)

## Canonical event envelope (Kafka)
All telemetry Kafka topics use a common envelope:

- `event_id` (string, required)
- `event_type` (string, required)
- `event_version` (string, required, e.g. `v1`)
- `occurred_at` (RFC3339 timestamp, required)
- `trace_id` (string, optional)
- `source` (string, required)
- `payload` (object, required)

## Sensor-related topic contracts

### `telemetry.authorized.v1`
Payload:
- `sensor_address` (string, required)
- `observed_at` (timestamp, required)
- `sequence_number` (integer, required)
- `measurements` (object, required)
- `signature` (string, required, original sensor signature)
- `attestation` (object, optional)

### `telemetry.rejected.v1`
Payload:
- `sensor_address` (string, optional)
- `reason_code` (string, required)
- `reason_message` (string, optional)

### `telemetry.ipfs.published.v1`
Payload:
- `cid` (string, required)
- `batch_id` (string, required)
- `event_count` (integer, required)
- `merkle_root` (string, optional)

### `telemetry.relay.result.v1`
Payload:
- `target` (string, required, e.g. blockchain relay name)
- `status` (`submitted` | `failed`, required)
- `cid` (string, optional)
- `tx_hash` (string, optional)
- `error_code` (string, optional)
- `error_message` (string, optional)

## Sensor API contract (define first)

### 1) Sensor Ingest API
`POST /v1/telemetry`

Request body: see **Sensor request data structure** section.

Responses:
- `202 Accepted` (only after Kafka ACK on `telemetry.authorized.v1`)
- `401 Unauthorized` (signature/hash invalid)
- `403 Forbidden` (sensor/key disabled or not permitted)
- `409 Conflict` (replay: duplicate nonce/event)
- `503 Service Unavailable` (Kafka not available)

### Response body (minimal)
- `status` (`accepted` | `rejected`)
- `event_id` (echo)
- `trace_id` (optional)
- `error_code` (for non-202 responses)

## Validation and compatibility rules
- Required fields must be validated at boundaries.
- Unknown fields are allowed but ignored unless explicitly adopted by `v1` contract update.
- Breaking changes require `v2` topic/api version.
- Time fields must be RFC3339 UTC.

## Error handling and idempotency at sensor integration layer
- Retries: bounded retry policy for transient failures.
- DLQ: route exhausted failures to `telemetry.dlq.v1`.
- Idempotency keys:
  - ingest path: `event_id` + `nonce`
  - IPFS path: `batch_id`
  - relay path: `cid` (and optional `delivery_id`)

## Implementation checklist
- [ ] Freeze JSON schemas for all `telemetry.*.v1` contracts listed above.
- [ ] Add contract validation library shared by producers/consumers.
- [ ] Publish API/OpenAPI draft for `POST /v1/telemetry`.
- [ ] Add contract tests for envelope and payload compatibility.
- [ ] Add replay/idempotency conformance tests for `event_id`, `nonce`, and `cid`.
