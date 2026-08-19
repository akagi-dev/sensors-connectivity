# IPFS Publisher

`ipfs-publisher` consumes binary protobuf `Envelope` records from `telemetry.authorized.v1`, builds deterministic batches, and publishes and pins them to Kubo IPFS. Contracts and generated protobuf schemas come from `@scp/core`.

## Configuration

- `KAFKA_BROKERS`: comma-separated brokers; default `localhost:9092`. Empty lists are rejected.
- `IPFS_PUBLISHER_GROUP_ID`: consumer group; default `ipfs-publisher-v1`. Empty values are rejected.
- `IPFS_PUBLISHER_BATCH_MAX_EVENTS`: events per partition batch; default `100`.
- `IPFS_PUBLISHER_BATCH_MAX_WAIT_MS`: maximum batching delay; default `1000`.
- `IPFS_PUBLISHER_MAX_RETRIES`: Kubo attempts per publication or pin check; default `3`.
- `IPFS_PUBLISHER_RETRY_BACKOFF_MS`: retry delay; default `250`.
- `IPFS_PUBLISHER_HEALTH_PORT`: health/metrics port; default `3040`.
- `IPFS_API_URL`: Kubo RPC endpoint; default `http://localhost:5001`.

Batches never mix Kafka partitions. A stable `batch_id` is derived from the topic, partition, ordered offsets, and event IDs. The canonical JSON IPFS artifact contains the validated current payload fields as base64 strings: `sensor_id` and `signed_envelope`.

After `ipfs.add`, explicit pinning, and pin confirmation, the service publishes a binary protobuf result envelope to `ipfs.published.v1`. Its `TelemetryIpfsPublishedPayload` contains CID bytes and `event_count`; the Kafka key is `batch_id`.

Transient Kubo failures are retried with a bounded delay. Exhausted or terminal failures are published to `telemetry.dlq.v1` in a protobuf envelope with detailed canonical JSON failure context. The result/DLQ producer waits for the broker acknowledgement.

Platformatic Kafka auto-commit is disabled. Successful and already-deduplicated batches invoke the manual commit callback of the last message only after result publication. Failed and DLQ batches stay uncommitted for replay. A process-local deduplication store prevents repeated IPFS publication during the lifetime of one service instance.

## Verification

```bash
pnpm --filter @scp/ipfs-publisher typecheck
pnpm --filter @scp/ipfs-publisher lint
pnpm --filter @scp/ipfs-publisher test
pnpm --filter @scp/ipfs-publisher build
```

Run the optional Kubo integration test against Compose infrastructure:

```bash
docker compose up -d ipfs
IPFS_PUBLISHER_IPFS_INTEGRATION=1 pnpm --filter @scp/ipfs-publisher test -- tests/ipfs.integration.test.ts
```

The service exposes `GET /health` and Prometheus metrics at `GET /metrics`, including batch, pin-latency, retry, and DLQ counters.
