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
- Nonce replay protection scope: `(sensor_id, nonce)`.
- Default per-sensor rate limit: `1 request / 10 seconds`.
- Per-sensor request rate limiting applies; clients MUST handle `429 Too Many Requests`.

### Required request headers

- `Content-Type: application/json; charset=utf-8`

### Optional request headers

- `X-Sensor-Zone: ru|eu-west|us-east|ap-southeast` (recommended for routing)
- `X-Request-Id: <uuid>` (recommended for tracing)

### Request body schema (normative)

| Field                    | Type                 | Required | Description                                                                                                             |
| ------------------------ | -------------------- | -------- | ----------------------------------------------------------------------------------------------------------------------- |
| `data`                   | object               | yes      | Canonical telemetry data object to be signed. No fixed nested schema is required by this contract.                    |
| `sensor_id`              | string               | yes      | Robonomics SS58 sensor identity used to resolve public key/authorization state.                                        |
| `timestamp`              | string (RFC3339 UTC) | yes      | Measurement creation time used for skew/replay checks.                                                                  |
| `nonce`                  | string               | yes      | Unique request nonce for replay protection (for example monotonic counter or random hex string).                      |
| `signature`              | string               | yes      | Base64 Ed25519 signature (64 raw bytes) generated via Substrate-compatible signing flow over the documented message bytes. |

Compatibility note: this is a breaking wire-contract change. Top-level `measurements` and later `payload` are replaced by top-level `data`.

### Minimal request example

```http
POST /v1/telemetry HTTP/1.1
Host: eu-west.ingest.sensors.social
Content-Type: application/json; charset=utf-8
X-Request-Id: 95ef04de-4de8-409e-b807-156460698514
X-Sensor-Zone: eu-west

{
  "data": {
    "meta": {
      "device_type": "sensor_v2"
    },
    "payload": {
      "temperature_c": 21.4
    }
  },
  "sensor_id": "4CvP46mxFm54eBbTMFayHK7n38MaXo7gCbq7KCHSd28xrWSJ",
  "timestamp": "2026-07-31T14:20:18Z",
  "nonce": "0000017a",
  "signature": "Q5cvaM...base64-ed25519-signature...P8="
}
```

### Full request example (multiple telemetry values)

```json
{
  "data": {
    "meta": {
      "device_type": "sensor_v2",
      "firmware_version": "1.2.3"
    },
    "payload": {
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
    }
  },
  "sensor_id": "4CvP46mxFm54eBbTMFayHK7n38MaXo7gCbq7KCHSd28xrWSJ",
  "timestamp": "2026-07-31T14:20:18Z",
  "nonce": "2a5b7c0d-44d1-4b2c-84a1-df2cb14d1f14",
  "signature": "d9j0z2...base64-ed25519-signature...qk="
}
```

`data` content is producer-defined and not constrained by this API contract. The examples above show one common shape with `meta` and `payload` objects.

## Signing and verification (normative)

### Canonicalization rules for `data`

`data` MUST be canonicalized deterministically before hashing.

1. Encode as UTF-8 bytes.
2. Object keys MUST be sorted lexicographically at every nesting level.
3. No insignificant whitespace.
4. Arrays preserve original order (no sorting).
5. Strings are JSON-escaped deterministically.
6. Numbers use a deterministic JSON numeric form (no `+`, no leading zeros, no locale formatting).
7. Booleans and `null` use JSON literals.

### Message input concatenation order

The message bytes to sign MUST be built in this exact order:

`canonical_data || timestamp || nonce || sensor_id`

Then:

1. `message` is the byte sequence from the concatenation rule above.
2. `signature = substrate_ed25519_sign(sensor_private_key, message)`
3. Send `signature` as base64 text of the raw 64-byte Ed25519 signature.

### Signing pseudocode

```text
function signTelemetry(data, timestamp, nonce, sensorId, privateKey):
  canonical = canonicalJson(data)              // sorted keys, deterministic encoding
  message = utf8(canonical) + utf8(timestamp) + utf8(nonce) + utf8(sensorId)
  sig = substrate_ed25519_sign(privateKey, message) // 64-byte signature
  return base64(sig)
```

### Verification pseudocode

```text
function verifyTelemetry(request, publicKey):
  canonical = canonicalJson(request.data)
  message = utf8(canonical) + utf8(request.timestamp) + utf8(request.nonce) + utf8(request.sensor_id)
  signatureBytes = base64_decode(request.signature)
  return substrate_ed25519_verify(publicKey, message, signatureBytes)
```

### Common pitfalls (signature mismatch)

- Key order differs between signer and verifier.
- Different numeric serialization (for example `21.40` vs `21.4`).
- Non-UTF-8 encoding.
- Address is not encoded as Robonomics Network address (general substrate encoding).
- Hashing externally before signing/verifying instead of passing message bytes directly to the Substrate-compatible Ed25519 library.
- Canonicalizing only `data.payload` instead of canonicalizing the entire `data`.
- Omitting `timestamp` from the signed message input.
- Trailing spaces/newlines in `nonce` or `sensor_id`.
- Wrong signature encoding (must be base64 of raw 64-byte signature).

## Replay and timestamp protection

### Timestamp skew policy

- `timestamp` MUST be RFC3339 UTC.
- Example policy: accept if `abs(server_time - timestamp) <= 300s`.
- Production value MAY be adjusted by configuration, but MUST be documented and consistent across zones.

### Nonce uniqueness and retention

- Uniqueness scope: `(sensor_id, nonce)`.
- A nonce accepted once for a sensor MUST be rejected on reuse.
- Retention guidance: keep nonce records for at least the max timestamp window plus retry horizon (example: `15 minutes`).

### Expected replay/timestamp failures

- Stale or future timestamp outside skew window: reject.
- Duplicate nonce for same `sensor_id`: reject as replay.

## Regional routing and ingestion endpoints

Zone-aware ingestion reduces latency and contains regional failures.

### Supported zones

- `ru`
- `eu-west`
- `us-east`
- `ap-southeast`

### Endpoint matrix

| Environment | Zone          | Base URL                                             |
| ----------- | ------------- | ---------------------------------------------------- |
| production  | global        | `https://ingest.sensors.social`                      |
| production  | ru            | `https://ru.ingest.sensors.social`                   |
| production  | eu-west       | `https://eu-west.ingest.sensors.social`              |
| production  | us-east       | `https://us-east.ingest.sensors.social`              |
| production  | ap-southeast  | `https://ap-southeast.ingest.sensors.social`         |
| staging     | global        | `https://ingest.staging.sensors.social`              |
| staging     | ru            | `https://ru.ingest.staging.sensors.social`           |
| staging     | eu-west       | `https://eu-west.ingest.staging.sensors.social`      |
| staging     | us-east       | `https://us-east.ingest.staging.sensors.social`      |
| staging     | ap-southeast  | `https://ap-southeast.ingest.staging.sensors.social` |

Primary path in every zone: `POST /v1/telemetry`.

### Routing policy

1. Sensor SHOULD send to its provisioned home zone.
2. On timeout/network failure, sensor MAY retry in the same zone first with exponential backoff.
3. If configured, sensor MAY fail over to another zone.
4. During retries/failover, sensor MUST preserve the exact same `nonce` and data bytes to keep signatures and replay behavior correct.
5. Backend replay enforcement remains scoped to `(sensor_id, nonce)` and should be synchronized across zones with bounded replication lag.

### Global endpoint behavior

Global ingestion endpoints:

- Production: `https://ingest.sensors.social/v1/telemetry`
- Staging: `https://ingest.staging.sensors.social/v1/telemetry`

Router behavior:

1. Route by `X-Sensor-Zone` when present and valid.
2. If header is absent, route by sender IP geolocation policy.
3. Return `307 Temporary Redirect` to the zone endpoint so clients preserve HTTP method and body.

### Metadata headers (routing/observability)

- Required: `Content-Type`
- Optional for routing hints: `X-Sensor-Zone`
- Recommended for tracing: `X-Request-Id`

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
    "message": "data must be a JSON object",
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
- Do not retry `400`/`401`/`409` without changing data or credentials.
- Preserve data, `timestamp`, and `nonce` on retries for a single logical send attempt.
- Generate a new nonce only when creating a new logical telemetry event.

## Appendix: HTTP-to-Kafka outcome mapping

| HTTP outcome                         | Kafka topic                                                                                   | Notes                                                             |
| ------------------------------------ | --------------------------------------------------------------------------------------------- | ----------------------------------------------------------------- |
| `202 Accepted`                       | `telemetry.authorized.v1`                                                                     | Published after successful verification and authorization.        |
| `4xx` rejection                      | `telemetry.rejected.v1`                                                                       | Rejection reason in event payload (for observability/audit).      |
| Downstream publish/processing status | `telemetry.pubsub.result.v1`, `telemetry.ipfs.result.v1`, `telemetry.blockchain.result.v1` | Produced by decoupled consumers, not by synchronous API response. |

Recommended event correlation metadata fields:

- `event_id`
- `sensor_id`
- `request_id`
- `zone`
