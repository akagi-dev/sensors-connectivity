# `registry-sync`

Consumes finalized Robonomics/Substrate registry events and projects authorization state to Redis.

## Redis projection keyspace (`REGISTRY_REDIS_PREFIX`, default `registry-sync:v1`)

- `${prefix}:sensor:{sensor_address}` hash
  - `sensor_address`, `public_key`, `enabled`, `updated_at_block`, `updated_at_event`
- `${prefix}:key:{public_key}` hash
  - `public_key`, `sensor_address`, `enabled`, `updated_at_block`, `updated_at_event`
- `${prefix}:events:processed` set (idempotency keys: `blockHeight:eventIndex`)
- `${prefix}:cursor:finalized-height` string checkpoint
- `${prefix}:dlq:events` list (exhausted failures)

Authorizer reads sensor status/public key from the sensor hash and uses nonce keys:

- `${prefix}:nonce:{sensor_address}:{nonce}`

## Endpoints

- `GET /health`
- `GET /metrics`

## Event shape assumptions

Registry events are parsed from finalized chain event records. Projection requires resolved fields:

- `sensorAddress`/`sensor_address`
- `publicKey`/`public_key`
- optional `enabled` boolean (otherwise inferred from method name: create/update/enable => true, disable/revoke/remove => false)
