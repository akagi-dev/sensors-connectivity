# `@scp/pubsub-broadcaster`

Consumes `telemetry.authorized.v1`, publishes authorized payloads to GossipSub, and emits `telemetry.pubsub.result.v1` outcomes.

## Environment

- `KAFKA_BROKERS` (default: `localhost:9092`)
- `PUBSUB_BROADCASTER_GROUP_ID` (default: `pubsub-broadcaster-v1`)
- `PUBSUB_BROADCASTER_SOURCE` (default: `pubsub-broadcaster`)
- `PUBSUB_BROADCASTER_HEALTH_PORT` (default: `3020`)
- `PUBSUB_BROADCASTER_MAX_RETRIES` (default: `3`)
- `PUBSUB_BROADCASTER_RETRY_BACKOFF_MS` (default: `250`)
- `PUBSUB_TOPIC` (default: `telemetry/authorized/v1`)
- `PUBSUB_RESERVED_PEERS` (default: empty; comma-separated libp2p multiaddrs to keep permanently connected)
