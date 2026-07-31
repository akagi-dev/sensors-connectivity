# Sensors.social Connectivity monorepo

TypeScript monorepo scaffold for the telemetry pipeline described in:

- [`project-architecture.md`](./docs/architecture/project-architecture.md)
- [`integration-guide.md`](./docs/architecture/integration-guide.md)

> This repository is intentionally scaffold-only in this phase. Service implementations are stubs with clear TODO markers.

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
```

## Per-service development

```bash
pnpm --filter @scp/authorizer dev
pnpm --filter @scp/registry-sync dev
pnpm --filter @scp/pubsub-broadcaster dev
pnpm --filter @scp/ipfs-publisher dev
pnpm --filter @scp/blockchain-anchor dev
```

## Service overview

- **authorizer**: validates `POST /v1/telemetry`, applies registry/signature checks, returns `202` after Kafka publish ACK (stubbed producer).
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
