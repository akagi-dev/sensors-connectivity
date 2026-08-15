# Sensors.social Connectivity monorepo

TypeScript monorepo for the event-driven telemetry pipeline described in:

- [`project-architecture.md`](./docs/architecture/project-architecture.md)
- [`integration-guide.md`](./docs/architecture/integration-guide.md)

> Current phase: WP-00 through WP-03 are fully implemented (`contracts`, `registry-sync`, `endpoint`, `pubsub-broadcaster`, `heartbeat-tracker`). WP-04 (`ipfs-publisher`) and WP-05 (`blockchain-anchor`) remain scaffolded.

## Stack

- Node.js 22+ LTS + TypeScript (strict mode with extra-strict flags)
- pnpm workspaces + Turborepo
- Fastify, kafkajs, zod
- Protobuf-es (`@bufbuild/protobuf`), `@polkadot/util-crypto` for Ed25519
- `kubo-rpc-client`, libp2p + GossipSub, `@polkadot/api`, ioredis
- tsup, vitest, eslint, prettier

## Prerequisites

- Node.js 22+ (`.nvmrc`)
- pnpm 11+
- Docker + Docker Compose

## Install and run

```bash
pnpm install
cp .env.example .env
docker compose up -d
```

### Monorepo commands

```bash
pnpm build
pnpm typecheck
pnpm lint
pnpm test
pnpm dev
```

## Workspace layout

```text
packages/contracts         # @scp/contracts - shared schemas, types, validation, consumer runtime
services/endpoint          # POST /v1/telemetry ingress - protobuf validation, signature verification
services/registry-sync     # Robonomics blockchain→Redis projection sync service
services/whitelist         # Whitelist-based sensor authentication provider
services/pubsub-broadcaster # Kafka→libp2p GossipSub bridge for real-time web UI
services/heartbeat-tracker  # Observability: sensor liveness & uptime metrics
services/ipfs-publisher    # Kafka→IPFS publisher (batches, produces CIDs)
services/blockchain-anchor # IPFS CID→blockchain anchoring (stubbed)
tools/fake-sensor-cli      # Generate test telemetry with Ed25519 signatures
```

## Per-service development

```bash
pnpm --filter @scp/endpoint dev
pnpm --filter @scp/registry-sync dev
pnpm --filter @scp/whitelist dev
pnpm --filter @scp/pubsub-broadcaster dev
pnpm --filter @scp/heartbeat-tracker dev
pnpm --filter @scp/ipfs-publisher dev
pnpm --filter @scp/blockchain-anchor dev
```

## Fake sensor telemetry CLI

Use this helper CLI to post fake telemetry messages for local debugging and integration testing.

Run from repo root:

```bash
pnpm fake-sensor -- \
  --endpoint http://localhost:3000/v1/telemetry \
  --signer-seed-hex 0x0101010101010101010101010101010101010101010101010101010101010101 \
  --count 5 \
  --interval-ms 1000 \
  --sensor-zone eu-west
```

Run directly in tools workspace:

```bash
pnpm --filter @scp/fake-sensor-cli fake-sensor -- --count 3
```

Available options:

- `--endpoint <url>` (env: `SENSOR_FAKE_ENDPOINT_URL`, default: `http://localhost:3000/v1/telemetry`)
- `--sensor-id <ss58>` (env: `SENSOR_FAKE_SENSOR_ID`; defaults to address derived from signer seed)
- `--signer-seed-hex <hex>` (env: `SENSOR_FAKE_SIGNER_SEED_HEX`, default: deterministic debug seed `0x00...01`)
- `--sensor-zone <zone>` (env: `SENSOR_FAKE_SENSOR_ZONE`, optional; sent as `X-Sensor-Zone`)
- `--count <n>` (env: `SENSOR_FAKE_COUNT`, default: `1`)
- `--interval-ms <ms>` (env: `SENSOR_FAKE_INTERVAL_MS`, default: `1000`)

The CLI now sends binary protobuf `crypto.v1.SignedEnvelope` (`Content-Type: application/protobuf`) and includes `X-Request-Id` on every request. It exits with a non-zero code on invalid options, request failures, or non-2xx responses.

## Protocol assets

- Buf module: `buf.build/airalab/sensors-social-proto`
- Generated SDK package: `@buf/airalab_sensors-social-proto.bufbuild_es`

## Service overview

- **endpoint**: Validates `POST /v1/telemetry` (protobuf `crypto.v1.SignedEnvelope`), verifies Ed25519 signatures, checks sensor authorization via Redis projection, publishes `telemetry.authorized.v1` and `telemetry.rejected.v1`, returns `202` only after Kafka ACK. Supports pluggable authentication strategies (registry-sync or whitelist).
- **registry-sync**: Consumes finalized Robonomics blockchain events, projects sensor/key authorization state to Redis for endpoint lookups.
- **whitelist**: Alternative authentication provider - maintains static sensor whitelist in Redis, bypassing blockchain dependency for simpler deployments.
- **pubsub-broadcaster**: Consumes `telemetry.authorized.v1`, publishes to libp2p/GossipSub for real-time web UI, emits `telemetry.pubsub.result.v1`, routes exhausted failures to DLQ.
- **heartbeat-tracker**: Observability-only consumer of `telemetry.authorized.v1`, tracks sensor liveness (`firstSeen`, `lastSeen`, `onlineSince`) in Redis, exposes `sensors_online` count and per-sensor/aggregate uptime metrics over configurable online window (default 30s). Does not emit result events or participate in retry/DLQ.
- **ipfs-publisher**: Consumes `telemetry.authorized.v1`, batches and publishes to IPFS (stubbed), emits `telemetry.ipfs.result.v1`.
- **blockchain-anchor**: Consumes `telemetry.ipfs.result.v1`, deduplicates by CID, emits `telemetry.blockchain.result.v1`; phase-1 scope is CID-only anchoring (stubbed).

## Local infrastructure

`docker-compose.yml` provides:

- Kafka + ZooKeeper
- Redis
- IPFS (Kubo RPC)
- Robonomics substrate dev node
