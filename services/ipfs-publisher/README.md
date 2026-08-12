# IPFS Publisher

`ipfs-publisher` consumes `telemetry.authorized.v1` from Kafka and publishes deterministic, pinned telemetry batches to Kubo IPFS.

Consumed messages must match the WP-00 envelope and `telemetry.authorized.v1` payload schemas exported by `@scp/contracts`; invalid messages are rejected before IPFS publication.

## Configuration

- `KAFKA_BROKERS` — comma-separated broker endpoints; default `localhost:9092`.
- `IPFS_PUBLISHER_GROUP_ID` — Kafka consumer group; default `ipfs-publisher-v1`.
- `IPFS_PUBLISHER_BATCH_MAX_EVENTS` — maximum events per partition batch; default `100`.
- `IPFS_PUBLISHER_BATCH_MAX_WAIT_MS` — maximum time before sealing a non-empty batch; default `1000`.
- `IPFS_PUBLISHER_MAX_RETRIES` — maximum Kubo attempts per publication or pin-confirmation stage; default `3`.
- `IPFS_PUBLISHER_RETRY_BACKOFF_MS` — delay between transient Kubo attempts; default `250`.
- `IPFS_PUBLISHER_HEALTH_PORT` — HTTP port for health and Prometheus metrics; default `3040`.
- `IPFS_API_URL` — Kubo RPC endpoint; default `http://localhost:5001`.

Batches never mix Kafka partitions and preserve offset order. Their IDs are SHA-256 hashes of a canonical descriptor containing topic, partition, offset range, and ordered event IDs.

The v1 IPFS object is canonical JSON containing `schema_version`, `batch_id`, `event_count`, and the validated `telemetry.authorized.v1` payloads in batch order. Identical batch contents produce identical bytes before Kubo CID calculation.

The publisher adds an artifact with fixed CIDv1/SHA-256 options, explicitly pins the returned CID, and confirms the pin before processing can continue. Verify this path against the Compose Kubo node with:

```bash
docker compose up -d ipfs
IPFS_PUBLISHER_IPFS_INTEGRATION=1 pnpm --filter @scp/ipfs-publisher test -- tests/ipfs.integration.test.ts
```

Completed `batch_id` values are recorded only after the processing sequence succeeds. A replay of the same batch is then classified as a duplicate before `ipfs.add` is called. The current deduplication store is process-local; it does not preserve processed IDs across service restarts.

After pin confirmation, the service publishes a WP-00 envelope to `telemetry.ipfs.result.v1`. The Kafka message key is `batch_id`; its payload contains the pinned `cid` and `event_count`. Processing continues only after `producer.send` resolves with the broker acknowledgement.

Kubo network failures, timeouts, HTTP `408`/`425`/`429`/`5xx`, and temporarily missing pin confirmation are retried with a bounded delay. Contract and CID validation failures are terminal and are not retried.

After Kubo attempts are exhausted, the service publishes an ACK-gated WP-00 envelope to `telemetry.dlq.v1` with `batch_id` as the Kafka key. The payload records the failed batch, failure stage, reason, actual attempt count, configured limit, and failure timestamp.

KafkaJS auto-commit is disabled. After pin confirmation and the `telemetry.ipfs.result.v1` broker ACK, the consumer explicitly commits `last_batch_offset + 1`. Duplicate batches may also commit because their successful result was previously recorded. Failed and DLQ batches remain uncommitted for replay.

## Observability

Pino writes structured JSON logs with batch, Kafka offset, CID, retry, DLQ, and pin-latency fields. While the service is running, `GET /health` returns a JSON liveness response and `GET /metrics` exposes Prometheus text metrics:

- `ipfs_publisher_batches_total`
- `ipfs_publisher_pin_latency_ms`, `_sum`, and `_count`
- `ipfs_publisher_retries_total`
- `ipfs_publisher_dlq_total`
