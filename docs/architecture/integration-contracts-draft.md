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

- `measurements` (object, required)
- `sensor_address` (string, required)
- `timestamp` (RFC3339 UTC timestamp, required)
- `nonce` (string, required)
- `signature` (string, required, Ed25519 signature)

## Signing sensor data
Sensors sign a reproducible hash so the backend can verify authenticity independently of JSON formatting.

Canonical hash input is built from the measurements, nonce, and sensor address:

1. Canonicalize `measurements` deterministically (sorted keys, no insignificant whitespace, UTF-8).
2. Concatenate the canonical fields in fixed order:
   `canonical_measurements || nonce || sensor_address`.
3. Compute `data_hash = SHA-256(concatenated_bytes)`.
4. `signature = Ed25519_sign(sensor_private_key, data_hash)`.

Verification recomputes `data_hash` from the received `measurements`, `nonce`, and `sensor_address`, then checks `signature` against the sensor public key resolved from `sensor_address`. The same canonicalization rules must be used by sensor and backend to keep the hash reproducible.

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
- `timestamp` (RFC3339 UTC timestamp, required)
- `nonce` (string, required)
- `measurements` (object, required)
- `signature` (string, required, original sensor signature)

### `telemetry.rejected.v1`
Payload:
- `sensor_address` (string, optional)
- `reason_code` (string, required)
- `reason_message` (string, optional)

### `telemetry.ipfs.published.v1`
Payload:
- `cid` (string, required)
- `event_count` (integer, required)

### `telemetry.blockchain.result.v1`
Payload:
- `target` (string, required, e.g. Robonomics blockchain name)
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
  - ingest path: `sensor_address` + `nonce`
  - IPFS path: `cid`
  - blockchain path: `cid`

## Implementation checklist
- [ ] Freeze JSON schemas for all `telemetry.*.v1` contracts listed above.
- [ ] Add contract validation library shared by producers/consumers.
- [ ] Publish API/OpenAPI draft for `POST /v1/telemetry`.
- [ ] Add contract tests for envelope and payload compatibility.
- [ ] Add replay/idempotency conformance tests for `sensor_address`, `nonce`, and `cid`.
