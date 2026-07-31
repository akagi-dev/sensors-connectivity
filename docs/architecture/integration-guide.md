# Sensors Integration (Data Structures and APIs)

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

## Implementation checklist
- [ ] Freeze JSON schemas for all `telemetry.*.v1` contracts listed above.
- [ ] Add contract validation library shared by producers/consumers.
- [ ] Publish API/OpenAPI draft for `POST /v1/telemetry`.
- [ ] Add contract tests for envelope and payload compatibility.
- [ ] Add replay/idempotency conformance tests for `sensor_address`, `nonce`, and `cid`.
