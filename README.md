# Sensors.social Connectivity monorepo

TypeScript monorepo scaffold for the telemetry pipeline described in:

- [`project-architecture.md`](./docs/architecture/project-architecture.md)
- [`integration-guide.md`](./docs/architecture/integration-guide.md)

> Current phase: WP-00/WP-01 are implemented and WP-02 (`authorizer`) is fully implemented. WP-03 to WP-05 remain scaffolded.

## Stack

- Node.js 20 LTS + TypeScript (strict)
- pnpm workspaces + Turborepo
- Fastify, kafkajs, zod
- `@noble/ed25519`, `json-canonicalize`, node `crypto`
- `kubo-rpc-client`, libp2p + GossipSub, `@polkadot/api`, ioredis
- tsup, vitest, eslint, prettier

## Prerequisites

- Node.js 20 (`.nvmrc`)
- pnpm 9+
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
packages/contracts         # @scp/contracts shared schemas/types/helpers
services/authorizer        # POST /v1/telemetry ingress stub
services/registry-sync     # substrate->redis projection sync service
services/pubsub-broadcaster
services/ipfs-publisher
services/blockchain-anchor # phase-1 CID-only anchoring stub
tools/fake-sensor-cli      # fake telemetry sender for debug/tests
```

## Per-service development

```bash
pnpm --filter @scp/authorizer dev
pnpm --filter @scp/registry-sync dev
pnpm --filter @scp/pubsub-broadcaster dev
pnpm --filter @scp/ipfs-publisher dev
pnpm --filter @scp/blockchain-anchor dev
```

## Fake sensor telemetry CLI

Use this helper CLI to post fake telemetry messages for local debugging and integration testing.

Run from repo root:

```bash
pnpm fake-sensor -- --endpoint http://localhost:3000/v1/telemetry --sensor-id debug-01 --count 5 --interval-ms 1000
```

Run directly in tools workspace:

```bash
pnpm --filter @scp/fake-sensor-cli fake-sensor -- --count 3
```

Available options:

- `--endpoint <url>` (env: `SENSOR_FAKE_ENDPOINT_URL`, default: `http://localhost:3000/v1/telemetry`)
- `--sensor-id <id>` (env: `SENSOR_FAKE_SENSOR_ID`, default: `debug-sensor-001`)
- `--count <n>` (env: `SENSOR_FAKE_COUNT`, default: `1`)
- `--interval-ms <ms>` (env: `SENSOR_FAKE_INTERVAL_MS`, default: `1000`)
- `--auth-token <token>` (env: `SENSOR_FAKE_AUTH_TOKEN`, optional)
- `--auth-header <name>` (env: `SENSOR_FAKE_AUTH_HEADER`, default: `authorization`)

The CLI logs each outgoing payload and the HTTP response status. It exits with a non-zero code on invalid options or request failures.

## Service overview

- **authorizer**: validates `POST /v1/telemetry`, applies timestamp/nonce/signature/registry checks, publishes `telemetry.authorized.v1` and `telemetry.rejected.v1`, returns `202` only after Kafka ACK.
- **registry-sync**: consumes finalized Robonomics/substrate registry events and projects sensor/key authorization state into Redis for Authorizer reads.
- **pubsub-broadcaster**: consumes `telemetry.authorized.v1` and publishes to GossipSub (stubbed), commit-after-success flow.
- **ipfs-publisher**: consumes `telemetry.authorized.v1`, batches and publishes to IPFS (stubbed), emits `telemetry.ipfs.result.v1`.
- **blockchain-anchor**: consumes `telemetry.ipfs.result.v1`, dedups by CID, emits `telemetry.blockchain.result.v1`; phase-1 scope is CID-only anchoring.

## Local infrastructure

`docker-compose.yml` provides:

- Kafka + ZooKeeper
- Redis
- IPFS (Kubo RPC)
- Robonomics substrate dev node
