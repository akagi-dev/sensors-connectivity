# Sensors Integration Guide

## Purpose and scope

This guide defines the wire contract for sensor ingestion through `POST /v1/telemetry`, including signing, replay protection, routing, and response behavior.

It is implementation-oriented and aligned with the architecture baseline in `/docs/architecture/project-architecture.md`:

- The Authorize module verifies requests.
- `202 Accepted` is returned only after Kafka ACK.
- Downstream processing is decoupled via Kafka topics (no direct module coupling).

## `POST /v1/telemetry`

### Endpoint

- Path: `POST /v1/telemetry`
- Content type: `application/json; charset=utf-8`
- Transport: HTTPS (TLS 1.2+)

### Endpoint limits and protection

- Max clock skew policy example: `±300s` (see replay section for details).
- Nonce replay protection scope: `(sensor_address, nonce)`.
- Per-sensor request rate limiting applies; clients MUST handle `429 Too Many Requests`.

### Required request headers

- `Content-Type: application/json; charset=utf-8`
- `X-Request-Id: <uuid>` (recommended for tracing; SHOULD be unique per attempt)
- `X-Sensor-Zone: eu-west|us-east|ap-southeast` (required for global endpoint routing)

### Optional request headers

- None.

### Request body schema (normative)

| Field            | Type                 | Required | Description                                                                                                    |
| ---------------- | -------------------- | -------- | -------------------------------------------------------------------------------------------------------------- |
| `measurements`   | object               | yes      | Sensor readings to be signed.                                                                                  |
| `sensor_address` | string               | yes      | Sensor identity used to resolve public key/authorization state.                                                |
| `timestamp`      | string (RFC3339 UTC) | yes      | Measurement creation time used for skew/replay checks.                                                         |
| `nonce`          | string               | yes      | Unique request nonce for replay protection (for example monotonic counter or random hex string).               |
| `signature`      | string               | yes      | Hex-encoded 64-byte Ed25519 signature (`0x`-prefixed) compatible with Substrate `sp_core::ed25519::Signature`. |

### Minimal request example

```http
POST /v1/telemetry HTTP/1.1
Host: eu-west.ingest.sensors.social
Content-Type: application/json; charset=utf-8
X-Request-Id: 95ef04de-4de8-409e-b807-156460698514
X-Sensor-Zone: eu-west

{
  "measurements": {
    "temperature_c": 21.4
  },
  "sensor_address": "5F3sa2TJAWMqDhXG6jhV4N8ko9V7zj8R8v7q8xM3A1Q2abcd",
  "timestamp": "2026-07-31T14:20:18Z",
  "nonce": "0000017a",
  "signature": "0x3ab10c...64-byte-ed25519-signature...9f21"
}
```

### Full request example (multiple measurements)

```json
{
  "measurements": {
    "temperature_c": 21.4,
    "humidity_pct": 53.2,
    "pressure_hpa": 1008.7,
    "pm25_ug_m3": 8.1,
    "battery_v": 3.78,
    "location": {
      "lat": 51.5074,
      "lon": -0.1278
    },
    "tags": ["indoor", "lab-2"]
  },
  "sensor_address": "5F3sa2TJAWMqDhXG6jhV4N8ko9V7zj8R8v7q8xM3A1Q2abcd",
  "timestamp": "2026-07-31T14:20:18Z",
  "nonce": "2a5b7c0d-44d1-4b2c-84a1-df2cb14d1f14",
  "signature": "0xf2e3a7...64-byte-ed25519-signature...11bc"
}
```

## Signing and verification (normative)

### Canonicalization rules for `measurements`

`measurements` MUST be canonicalized deterministically before hashing.

1. Encode as UTF-8 bytes.
2. Object keys MUST be sorted lexicographically at every nesting level.
3. No insignificant whitespace.
4. Arrays preserve original order (no sorting).
5. Strings are JSON-escaped deterministically.
6. Numbers use a deterministic JSON numeric form (no `+`, no leading zeros, no locale formatting).
7. Booleans and `null` use JSON literals.

### Hash input concatenation order

The hash input bytes MUST be built in this exact order:

`canonical_measurements || nonce || sensor_address`

Then:

1. `data_hash = SHA-256(hash_input_bytes)`
2. `signature = Ed25519_sign(sensor_private_key, data_hash)`
3. Send `signature` as lowercase hex with `0x` prefix (64 bytes total), compatible with Substrate `sp_core::ed25519::Signature`.

### Signing pseudocode

```text
function signTelemetry(measurements, nonce, sensorAddress, privateKey):
  canonical = canonicalJson(measurements)      // sorted keys, deterministic encoding
  bytesToHash = utf8(canonical) + utf8(nonce) + utf8(sensorAddress)
  digest = sha256(bytesToHash)
  sig = substrate_ed25519_sign(privateKey, digest) // 64-byte signature
  return "0x" + hex(sig)
```

### Verification pseudocode

```text
function verifyTelemetry(request, publicKey):
  canonical = canonicalJson(request.measurements)
  bytesToHash = utf8(canonical) + utf8(request.nonce) + utf8(request.sensor_address)
  digest = sha256(bytesToHash)
  signatureBytes = hex_decode_0x(request.signature)
  return substrate_ed25519_verify(publicKey, digest, signatureBytes)
```

### Common pitfalls (signature mismatch)

- Key order differs between signer and verifier.
- Different numeric serialization (for example `21.40` vs `21.4`).
- Non-UTF-8 encoding.
- Hashing full request JSON instead of required tuple.
- Trailing spaces/newlines in `nonce` or `sensor_address`.
- Wrong signature encoding (must be `0x`-prefixed 64-byte hex).

## Replay and timestamp protection

### Timestamp skew policy

- `timestamp` MUST be RFC3339 UTC.
- Example policy: accept if `abs(server_time - timestamp) <= 300s`.
- Production value MAY be adjusted by configuration, but MUST be documented and consistent across zones.

### Nonce uniqueness and retention

- Uniqueness scope: `(sensor_address, nonce)`.
- A nonce accepted once for a sensor MUST be rejected on reuse.
- Retention guidance: keep nonce records for at least the max timestamp window plus retry horizon (example: `15 minutes`).

### Expected replay/timestamp failures

- Stale or future timestamp outside skew window: reject.
- Duplicate nonce for same `sensor_address`: reject as replay.

## Regional routing and ingestion endpoints

Zone-aware ingestion reduces latency and contains regional failures.

### Supported zones

- `eu-west`
- `us-east`
- `ap-southeast`

### Endpoint matrix

| Environment | Zone          | Base URL                                             |
| ----------- | ------------- | ---------------------------------------------------- |
| production  | eu-west       | `https://eu-west.ingest.sensors.social`              |
| production  | us-east       | `https://us-east.ingest.sensors.social`              |
| production  | ap-southeast  | `https://ap-southeast.ingest.sensors.social`         |
| production  | global router | `https://ingest.sensors.social`                      |
| staging     | eu-west       | `https://eu-west.ingest.staging.sensors.social`      |
| staging     | us-east       | `https://us-east.ingest.staging.sensors.social`      |
| staging     | ap-southeast  | `https://ap-southeast.ingest.staging.sensors.social` |
| staging     | global router | `https://ingest.staging.sensors.social`              |

Primary path in every zone: `POST /v1/telemetry`.

### Routing policy

1. Sensor SHOULD send to its provisioned home zone.
2. On timeout/network failure, sensor MAY retry in the same zone first with exponential backoff.
3. If configured, sensor MAY fail over to another zone.
4. During retries/failover, sensor MUST preserve the exact same `nonce` and payload bytes to keep signatures and replay behavior correct.
5. Backend replay enforcement remains scoped to `(sensor_address, nonce)` and should be synchronized across zones with bounded replication lag.

### Global endpoint behavior (required)

Global ingestion endpoints are required:

- Production: `https://ingest.sensors.social/v1/telemetry`
- Staging: `https://ingest.staging.sensors.social/v1/telemetry`

Router behavior:

1. Route by `X-Sensor-Zone` when present and valid.
2. If header is absent, route by provisioned sensor home zone.
3. Return `307 Temporary Redirect` to the zone endpoint so clients preserve HTTP method and body.

### Metadata headers (routing/observability)

- Required: `Content-Type`
- Required for global routing: `X-Sensor-Zone`
- Recommended: `X-Request-Id`

`X-Request-Id` SHOULD be propagated into Kafka event metadata for cross-system correlation.

## Response and error behavior

### Success: `202 Accepted`

`202` means request validation, signature verification, authorization, replay/timestamp checks, and enqueue to Kafka succeeded.

Normative guarantee: response is returned only after Kafka ACK for publish to `telemetry.authorized.v1`.

Example:

```json
{
  "status": "accepted",
  "request_id": "95ef04de-4de8-409e-b807-156460698514",
  "zone": "eu-west"
}
```

### Error format

```json
{
  "error": {
    "code": "invalid_signature",
    "message": "Signature verification failed",
    "request_id": "95ef04de-4de8-409e-b807-156460698514",
    "zone": "eu-west"
  }
}
```

### Error examples

#### `401 Unauthorized` — invalid signature

```json
{
  "error": {
    "code": "invalid_signature",
    "message": "Signature verification failed",
    "request_id": "95ef04de-4de8-409e-b807-156460698514",
    "zone": "eu-west"
  }
}
```

#### `401 Unauthorized` — stale timestamp

```json
{
  "error": {
    "code": "stale_timestamp",
    "message": "Timestamp outside allowed skew window",
    "request_id": "95ef04de-4de8-409e-b807-156460698514",
    "zone": "eu-west"
  }
}
```

#### `409 Conflict` — duplicate nonce (replay)

```json
{
  "error": {
    "code": "duplicate_nonce",
    "message": "Nonce already used for this sensor",
    "request_id": "95ef04de-4de8-409e-b807-156460698514",
    "zone": "eu-west"
  }
}
```

#### `400 Bad Request` — schema validation error

```json
{
  "error": {
    "code": "schema_validation_error",
    "message": "measurements.temperature_c must be a number",
    "request_id": "95ef04de-4de8-409e-b807-156460698514",
    "zone": "eu-west"
  }
}
```

#### `429 Too Many Requests` — rate limit exceeded

```json
{
  "error": {
    "code": "rate_limit_exceeded",
    "message": "Request rate exceeded for sensor",
    "request_id": "95ef04de-4de8-409e-b807-156460698514",
    "zone": "eu-west"
  }
}
```

### Retry and backoff guidance for sensor clients

- Retry on network errors/timeouts and `429`.
- Retry on `5xx` using exponential backoff with jitter.
- Do not retry `400`/`401`/`409` without changing payload or credentials.
- Preserve payload, `timestamp`, and `nonce` on retries for a single logical send attempt.
- Generate a new nonce only when creating a new logical telemetry event.

## Appendix: HTTP-to-Kafka outcome mapping

| HTTP outcome                         | Kafka topic                                                                                   | Notes                                                             |
| ------------------------------------ | --------------------------------------------------------------------------------------------- | ----------------------------------------------------------------- |
| `202 Accepted`                       | `telemetry.authorized.v1`                                                                     | Published after successful verification and authorization.        |
| `4xx` rejection                      | `telemetry.rejected.v1`                                                                       | Rejection reason in event payload (for observability/audit).      |
| Downstream publish/processing status | `telemetry.pubsub.result.v1`, `telemetry.ipfs.published.v1`, `telemetry.blockchain.result.v1` | Produced by decoupled consumers, not by synchronous API response. |

Recommended event correlation metadata fields:

- `event_id`
- `sensor_address`
- `request_id`
- `zone`
