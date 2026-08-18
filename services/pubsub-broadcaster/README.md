# `@scp/pubsub-broadcaster`

Simple consumer that publishes authorized telemetry to IPFS GossipSub for real-time web UI updates.

## Architecture

- **Pattern**: Simple consumer (observability-only, no retry/DLQ/result events)
- **Input**: `telemetry.authorized.v1` from Kafka
- **Output**: SignedEnvelope bytes published to IPFS PubSub topic
- **Autocommit**: Enabled (best-effort delivery)
- **Failure handling**: Logs failures but does not retry or emit result events

This service forwards telemetry to real-time subscribers. Failures are acceptable since telemetry is also archived via `ipfs-publisher` and `blockchain-anchor` services.

## Environment

- `KAFKA_BROKERS` (default: `localhost:9092`)
- `PUBSUB_BROADCASTER_GROUP_ID` (default: `pubsub-broadcaster-v1`)
- `PUBSUB_BROADCASTER_SOURCE` (default: `pubsub-broadcaster`)
- `PUBSUB_BROADCASTER_HEALTH_PORT` (default: `3020`)
- `PUBSUB_TOPIC` (default: `sensors.social/telemetry/v1`)
- `IPFS_API_URL` (default: `http://localhost:5001`)

## Metrics

Available at `http://localhost:3020/metrics`:

- `consumed`: Total messages consumed
- `publishSuccess`: Successful PubSub publishes
- `publishFailure`: Failed PubSub publishes (not retried)
