# Distributed Tracing

## Overview

Every telemetry event can be traced through the entire pipeline using `trace_id` and `sensor_id` in structured logs.

## Trace Flow

```
┌─────────┐  trace_id   ┌────────┐  trace_id   ┌────────────────┐
│ Sensor  │────────────▶│Endpoint│────────────▶│ Kafka Topics   │
│         │  sensor_id  │        │  sensor_id  │                │
└─────────┘             └────────┘             └────────────────┘
                            │                        │
                            │                        │
                            ▼                        ▼
                    ┌──────────────┐      ┌──────────────────┐
                    │ Redis Nonce  │      │  Consumer Group  │
                    │  Projection  │      │                  │
                    └──────────────┘      └──────────────────┘
                                                    │
                         ┌──────────────────────────┼──────────────────────────┐
                         │                          │                          │
                         ▼                          ▼                          ▼
              ┌─────────────────────┐  ┌─────────────────────┐  ┌─────────────────────┐
              │ pubsub-broadcaster  │  │  ipfs-publisher     │  │  heartbeat-tracker  │
              │  trace_id ✓         │  │  trace_id ✓         │  │  trace_id ✓         │
              │  sensor_id ✓        │  │  sensor_id ✓        │  │  sensor_id ✓        │
              └─────────────────────┘  └─────────────────────┘  └─────────────────────┘
                         │                          │
                         ▼                          ▼
              ┌─────────────────────┐  ┌─────────────────────┐
              │  IPFS GossipSub     │  │  IPFS Storage       │
              │  (real-time)        │  │  + CID              │
              └─────────────────────┘  └─────────────────────┘
                                                    │
                                                    ▼
                                       ┌─────────────────────┐
                                       │ Blockchain Anchor   │
                                       │ (future)            │
                                       └─────────────────────┘
```

## Log Context

### Endpoint
```json
{
  "trace_id": "550e8400-e29b-41d4-a716-446655440000",
  "sensor_id": "5GrwvaEF5zXb26Fz9rcQpDWS57CtERHpNehXCPcNoHGKutQY",
  "event_id": "evt_123",
  "msg": "telemetry accepted"
}
```

### IPFS Publisher (Batch)
```json
{
  "trace_ids": ["550e8400-...", "660f9511-..."],
  "sensor_ids": ["5GrwvaEF...", "5HGjWAe..."],
  "unique_sensors": 2,
  "batch_size": 10,
  "cid": "QmYwAPJzv5CZsnA625s3Xf2nemtYgPpHdWEz79ojWnPbdG",
  "msg": "batch published to IPFS"
}
```

### PubSub Broadcaster
```json
{
  "trace_id": "550e8400-e29b-41d4-a716-446655440000",
  "sensor_id": "5GrwvaEF5zXb26Fz9rcQpDWS57CtERHpNehXCPcNoHGKutQY",
  "pubsub_topic": "sensors.social/telemetry/v1",
  "msg": "telemetry published to PubSub"
}
```

### Heartbeat Tracker
```json
{
  "trace_id": "550e8400-e29b-41d4-a716-446655440000",
  "sensor_id": "5GrwvaEF5zXb26Fz9rcQpDWS57CtERHpNehXCPcNoHGKutQY",
  "eventId": "evt_123",
  "msg": "authorized envelope received"
}
```

### Blockchain Anchor
```json
{
  "event_id": "evt_789",
  "cid": "QmYwAPJzv5CZsnA625s3Xf2nemtYgPpHdWEz79ojWnPbdG",
  "event_count": 10,
  "node_id": 0,
  "msg": "anchoring IPFS CID to blockchain"
}
```

**Design Note**: `blockchain-anchor` operates on IPFS batches (identified by CID), not individual telemetry events. CID is the natural correlation key for tracing batches to blockchain. Individual sensor trace_ids are available via IPFS publisher logs (see correlation strategy below).

## Querying Logs

### Find all events for a specific sensor
```bash
grep '5GrwvaEF5zXb26Fz9rcQpDWS57CtERHpNehXCPcNoHGKutQY' logs/*.log | jq
```

### Trace a specific request end-to-end
```bash
grep '550e8400-e29b-41d4-a716-446655440000' logs/*.log | jq
```

### Find which sensors are in a specific IPFS batch
```bash
grep 'QmYwAPJzv5CZsnA625s3Xf2nemtYgPpHdWEz79ojWnPbdG' logs/ipfs-publisher.log | jq '.sensor_ids'
```

### Trace from sensor to blockchain
```bash
# 1. Find sensor's telemetry in endpoint logs
grep '5GrwvaEF...' logs/endpoint.log | jq '.trace_id'
# Returns: 550e8400-e29b-41d4-a716-446655440000

# 2. Find batch containing that sensor in ipfs-publisher
grep '550e8400-e29b-41d4-a716-446655440000' logs/ipfs-publisher.log | jq '.cid'
# Returns: QmYwAPJzv5CZsnA625s3Xf2nemtYgPpHdWEz79ojWnPbdG

# 3. Find blockchain anchoring of that CID
grep 'QmYwAPJzv5CZsnA625s3Xf2nemtYgPpHdWEz79ojWnPbdG' logs/blockchain-anchor.log | jq
```

## Trace ID Lifecycle

1. **Generated**: At HTTP ingress in `endpoint` service
2. **Propagated**: Through Kafka envelope (`envelope.traceId`)
3. **Extracted**: By each consumer service from envelope
4. **Logged**: In all processing steps
5. **Batched**: Multiple trace IDs logged together in IPFS publisher batches

## Sensor ID Format

- **Storage**: Hex string (e.g., in Redis, internal data structures)
- **Logging**: SS58 address format (e.g., `5GrwvaEF...`) for human readability
- **Wire**: Raw bytes in protobuf messages

## Implementation Notes

- Telemetry consumers (`pubsub-broadcaster`, `ipfs-publisher`, `heartbeat-tracker`) extract `trace_id` from `Envelope.traceId` field
- **Batch-level tracing**: `blockchain-anchor` uses CID as the correlation key (cleaner separation of concerns)
- Batch operations log arrays of affected sensors and traces
- Log level `debug` for per-message tracing, `info` for batch summaries
- Structured logging via Pino for easy parsing/filtering

## Correlation Strategy

Two-tier tracing approach:

### Individual Sensor Tracing
```
sensor_id → trace_id → endpoint → consumer logs
```
Use `trace_id` to track individual sensor telemetry through the pipeline.

### Batch-to-Blockchain Tracing
```
sensor_id → trace_id → batch CID → blockchain transaction
```
Use CID as the correlation key:
1. Find sensor's `trace_id` in endpoint/consumer logs
2. Find batch CID containing that `trace_id` in ipfs-publisher logs (includes full `sensor_ids` and `trace_ids` arrays)
3. Find blockchain anchoring for that CID in blockchain-anchor logs
