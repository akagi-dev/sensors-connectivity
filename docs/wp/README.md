# Work Packages (WPs)

Implementation work packages for the Sensors.social Connectivity telemetry pipeline.
Each WP takes a scaffold/stub service to a working, tested implementation.

## Source of truth

- [`../architecture/project-architecture-draft.md`](../architecture/project-architecture-draft.md)
- [`../architecture/integration-contracts-draft.md`](../architecture/integration-contracts-draft.md)

## Pipeline

```text
Sensor -> Authorizer -> Kafka -> {PubSub Broadcaster, IPFS Publisher} -> Kafka -> Blockchain Anchor
                          ^
Robonomics chain -> Registry Sync -> Redis projection
```

## Work packages

| WP | Service / Package | Depends on | Status |
|----|-------------------|------------|--------|
| [WP-00](./wp-00-contracts.md) | `@scp/contracts` (shared schemas, envelope, topics, consumer runtime) | — | Not started |
| [WP-01](./wp-01-registry-sync.md) | `registry-sync` (substrate → Redis projection) | WP-00 | Not started |
| [WP-02](./wp-02-authorizer.md) | `authorizer` (`POST /v1/telemetry` ingress) | WP-00, WP-01 | Not started |
| [WP-03](./wp-03-pubsub-broadcaster.md) | `pubsub-broadcaster` (GossipSub fan-out) | WP-00, WP-02 | Not started |
| [WP-04](./wp-04-ipfs-publisher.md) | `ipfs-publisher` (batch + IPFS CID) | WP-00, WP-02 | Not started |
| [WP-05](./wp-05-blockchain-anchor.md) | `blockchain-anchor` (CID-only anchoring) | WP-00, WP-04 | Not started |

## Recommended sequencing

1. **WP-00** first — all services import shared contracts, so freeze schemas/envelope/topics before wiring services.
2. **WP-01** — the authorizer depends on the registry read path.
3. **WP-02** — enables end-to-end producing onto Kafka.
4. **WP-03 / WP-04** in parallel — both consume `telemetry.authorized.v1`.
5. **WP-05** last — consumes `telemetry.ipfs.result.v1` from WP-04.

## Definition of done (applies to every WP)

- All `TODO` markers in the service replaced with real logic.
- Unit tests + integration test against local `docker-compose` infra.
- `pnpm build`, `pnpm typecheck`, `pnpm lint`, `pnpm test` green.
- Bounded retry + DLQ wired via the shared consumer runtime.
- Baseline structured logging and health/metrics endpoint.
