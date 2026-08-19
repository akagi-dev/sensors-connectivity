# Sensors Integration Guide (Protobuf)

## `POST /v1/telemetry`

- Primary transport: `Content-Type: application/protobuf`

## Wire format

Request body is serialized `crypto.v1.SignedEnvelope`:

```protobuf
message SignedEnvelope {
  bytes sensor_id = 1;   // 32-byte Ed25519 public key
  uint64 timestamp = 2;  // unix ms UTC
  bytes nonce = 3;       // 16-32 bytes
  bytes message = 4;     // serialized core.v1.Message
  bytes signature = 5;   // 64-byte Ed25519 signature
}
```

Protocol and schema references:

- Buf Schema Registry docs: https://buf.build/docs
- Sensors Social module: https://buf.build/airalab/sensors-social-proto

`message` contains:

```protobuf
message Message {
  Meta metadata = 1;
  oneof payload {
    device.v1.Urban urban = 2;
    device.v1.Insight insight = 3;
  }
}
```

## Signature (normative)

```text
timestamp_le = uint64(timestamp) encoded as 8 bytes little-endian
signing_bytes = sensor_id || timestamp_le || nonce || message
signature = Ed25519.sign(private_key, signing_bytes)
verify    = Ed25519.verify(public_key, signing_bytes, signature)
```

## Routing and zones

- `X-Request-Id` is passed through for tracing/auditing.
- `X-Sensor-Zone` remains available for upstream routing policies.

Supported zones:

- `ru`
- `eu-west`
- `us-east`
- `ap-southeast`

Endpoint matrix:

| Environment | Zone         | Base URL                                     |
| ----------- | ------------ | -------------------------------------------- |
| production  | global       | `https://ingest.sensors.social`              |
| production  | ru           | `https://ru.ingest.sensors.social`           |
| production  | eu-west      | `https://eu-west.ingest.sensors.social`      |
| production  | us-east      | `https://us-east.ingest.sensors.social`      |
| production  | ap-southeast | `https://ap-southeast.ingest.sensors.social` |
| staging     | global       | `https://ingest.staging.sensors.social`      |
| staging     | ru           | `https://ru.ingest.staging.sensors.social`   |
| staging     | eu-west      | `https://eu-west.ingest.staging.sensors.social` |
| staging     | us-east      | `https://us-east.ingest.staging.sensors.social` |
| staging     | ap-southeast | `https://ap-southeast.ingest.staging.sensors.social` |

Primary path in every zone: `POST /v1/telemetry`.

Global endpoint routing behavior:

1. Route by `X-Sensor-Zone` when present and valid.
2. If header is absent, route by sender IP geolocation policy.
3. Return `307 Temporary Redirect` to a zone endpoint so clients preserve method and body.

Connectivity validates signature and envelope constraints; it does not decode inner measurements.

## Validation rules

- `sensor_id` MUST be 32 bytes
- `signature` MUST be 64 bytes
- `nonce` MUST be 16..32 bytes
- `message` MUST be non-empty
- timestamp skew policy: reject outside configured window (default `±300s`)
- replay scope: `(hex(sensor_id), hex(nonce))`

## Responses

- `202` accepted and published to Kafka (`telemetry.authorized.v1`)
- `401` invalid signature or stale timestamp
- `403` unknown/disabled sensor
- `409` duplicate nonce
- `503` Kafka/infra unavailable

## Kafka authorized payload

`telemetry.authorized.v1` payload uses binary-safe strings:

```json
{
  "sensor_id": "<base64 32-byte pubkey>",
  "timestamp": 1723727295123,
  "nonce": "<base64 nonce>",
  "message": "<base64 serialized core.v1.Message>",
  "signature": "<base64 64-byte signature>",
  "envelope": "<base64 serialized SignedEnvelope>"
}
```

## Proto source of truth

- Buf module: `buf.build/airalab/sensors-social-proto`
- SDK package target: `@buf/airalab_sensors-social-proto.bufbuild_es` (when registry/network access is available)
