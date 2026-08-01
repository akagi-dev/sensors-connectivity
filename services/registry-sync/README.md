# `registry-sync`

Consumes finalized Robonomics/Substrate registry events and projects authorization state to Redis.

## Redis projection keyspace (`REGISTRY_REDIS_PREFIX`, default `registry-sync:v1`)

- `${prefix}:sensor:{sensor_id}` hash
  - `sensor_id`, `enabled`, `updated_at_block`, `updated_at_event`
- `${prefix}:events:processed` set (idempotency keys: `blockHeight:eventIndex`)
- `${prefix}:cursor:finalized-height` string checkpoint
- `${prefix}:dlq:events` list (exhausted failures)

Authorizer reads sensor status from the sensor hash and uses nonce keys:

- `${prefix}:nonce:{sensor_id}:{nonce}`

## Endpoints

- `GET /health`
- `GET /metrics`

## Event shape assumptions

Registry events are parsed from finalized chain event records. Projection requires resolved fields:

- `rws.NewDevices`: every address in `devices` becomes eligible (`enabled=true`) for Authorizer.
- For other registry-shaped events: use resolved `sensorId`/`sensor_id`; optional `enabled` is inferred from method names when omitted.
