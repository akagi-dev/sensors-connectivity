# `@scp/heartbeat-tracker`

Observability consumer for trusted `telemetry.authorized.v1` events. It tracks sensor liveness and uptime in-memory and exposes metrics over HTTP.

## Online and uptime definitions

- **Online**: `now - lastSeen <= HEARTBEAT_TRACKER_ONLINE_WINDOW_MS` (default: `30000` ms / 30s).
- **firstSeen**: first time a sensor is observed.
- **lastSeen**: most recent authorized message time.
- **onlineSince**: start of the current continuous online streak. If the gap between two messages is greater than the window, streak uptime resets at the new message.

## Environment

- `KAFKA_BROKERS` (default: `localhost:9092`)
- `HEARTBEAT_TRACKER_GROUP_ID` (default: `heartbeat-tracker-v1`)
- `HEARTBEAT_TRACKER_SOURCE` (default: `heartbeat-tracker`)
- `HEARTBEAT_TRACKER_HEALTH_PORT` (default: `3030`)
- `HEARTBEAT_TRACKER_ONLINE_WINDOW_MS` (default: `30000`)

## Endpoints

### `GET /health`

```json
{ "status": "ok" }
```

### `GET /metrics`

Example:

```json
{
  "sensors_online": 2,
  "sensors_total_tracked": 3,
  "online_window_ms": 30000,
  "consumed": 125,
  "sensor_uptime_seconds": {
    "sensor-a": 42,
    "sensor-b": 7
  },
  "sensors_uptime": [
    {
      "sensor_id": "sensor-a",
      "online": true,
      "first_seen": "2026-01-01T00:00:00.000Z",
      "last_seen": "2026-01-01T00:01:02.000Z",
      "uptime_seconds": 42,
      "seconds_since_last_seen": 1
    },
    {
      "sensor_id": "sensor-c",
      "online": false,
      "first_seen": "2026-01-01T00:00:10.000Z",
      "last_seen": "2026-01-01T00:00:20.000Z",
      "uptime_seconds": 0,
      "seconds_since_last_seen": 120
    }
  ],
  "max_uptime_seconds": 42,
  "avg_uptime_seconds": 24.5
}
```
