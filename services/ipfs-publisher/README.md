# `@scp/ipfs-publisher`

Batches authorized telemetry events and publishes them to IPFS for permanent archival storage.

## Architecture

- **Pattern**: Standard consumer with manual commit (durable archival)
- **Input**: `telemetry.authorized.v1` from Kafka
- **Output**: Batches published to IPFS + result events to `telemetry.ipfs.published.v1`
- **Autocommit**: Disabled (commit only after IPFS confirmation)
- **Failure handling**: Transient failures retry on next message; persistent failures logged

This service provides durable archival by batching telemetry into `SignedEnvelopeBatch` protobuf messages, optionally compressing with XZ, and publishing to IPFS. Each batch returns a Content-ID (CID) that is emitted as a result event for downstream blockchain anchoring.

## Batching Strategy

The service uses adaptive batching to optimize throughput and latency:

- **Size-based flush**: Batch publishes when `IPFS_PUBLISHER_BATCH_SIZE` messages are accumulated
- **Time-based flush**: Batch publishes after `IPFS_PUBLISHER_BATCH_TIMEOUT_MS` when under load threshold
- **Lag-based flush**: When consumer lag is high (>= batch size), flushes early to catch up
- **Lag-based skip timer**: When consumer lag is low (< batch size), uses timeout to avoid publishing tiny batches

On service shutdown, any remaining messages in the batch are flushed to avoid data loss.

## Environment

- `KAFKA_BROKERS` (default: `localhost:9092`)
- `IPFS_PUBLISHER_GROUP_ID` (default: `ipfs-publisher-v1`)
- `IPFS_PUBLISHER_SOURCE` (default: `ipfs-publisher`)
- `IPFS_PUBLISHER_HEALTH_PORT` (default: `3040`)
- `IPFS_API_URL` (default: `http://localhost:5001`) - Kubo RPC endpoint
- `IPFS_PUBLISHER_BATCH_SIZE` (default: `10`) - Maximum messages per batch
- `IPFS_PUBLISHER_BATCH_TIMEOUT_MS` (default: `30000`) - Timeout for partial batches under low load
- `IPFS_PUBLISHER_ENABLE_COMPRESSION` (default: `true`) - XZ compression before IPFS upload

## Metrics

Available at `http://localhost:3040/metrics`:

- `consumed`: Total authorized telemetry messages consumed
- `batchesPublished`: Total batches successfully published to IPFS
- `eventsPublished`: Total individual telemetry events archived to IPFS
- `publishFailure`: Failed IPFS publish attempts (retried on next message)

## Data Flow

1. Consume `telemetry.authorized.v1` event from Kafka
2. Extract `SignedEnvelope` from payload
3. Add to current batch buffer
4. When batch is full, timeout reached, or lag-based flush triggered:
   - Serialize batch as `SignedEnvelopeBatch` protobuf
   - Optionally compress with XZ (LZMA2)
   - Upload to IPFS via Kubo RPC
   - Receive CID from IPFS
   - Emit `telemetry.ipfs.published.v1` result event to Kafka
   - Commit Kafka offsets (only after successful upload + publish)

## Result Event Schema

Published to `telemetry.ipfs.published.v1`:

```typescript
{
  cid: Uint8Array;          // CID bytes (use multiformats/cid to parse)
  eventCount: number;        // Number of telemetry events in batch
  compression: Compression;  // NONE or XZ
}
```

## Development

```bash
# Start ipfs-publisher
pnpm --filter @scp/ipfs-publisher dev

# Build
pnpm --filter @scp/ipfs-publisher build

# Run tests
pnpm --filter @scp/ipfs-publisher test

# Type check
pnpm --filter @scp/ipfs-publisher typecheck

# Lint
pnpm --filter @scp/ipfs-publisher lint
```

## Dependencies

- **Kafka**: For consuming authorized telemetry and publishing results
- **IPFS Kubo**: For permanent content-addressed storage (RPC API on port 5001)

## Commit Safety

The service uses manual offset commits to ensure at-least-once delivery semantics:

1. Batch is published to IPFS
2. CID is received from IPFS
3. Result event is published to Kafka
4. Kafka offsets are committed

If any step fails, the batch remains uncommitted and will be reprocessed. This ensures no telemetry is lost, though duplicate CIDs may occur (idempotent).

## Compression

XZ compression (enabled by default) significantly reduces IPFS storage and bandwidth:

- **Compression level**: Default (level 6, LZMA2 algorithm)
- **Typical savings**: 60-80% for telemetry batches
- **Trade-off**: Slight CPU overhead during batch flush

Disable with `IPFS_PUBLISHER_ENABLE_COMPRESSION=false` for low-latency scenarios where storage is not a constraint.
