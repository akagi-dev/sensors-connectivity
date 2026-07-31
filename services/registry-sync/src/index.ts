import { runConsumerProcessingRule, TELEMETRY_TOPICS } from '@scp/contracts';
import Redis from 'ioredis';
import { createServer, type Server } from 'node:http';
import { fileURLToPath } from 'node:url';
import { SubstrateFinalizedRegistryEventSource, type FinalizedRegistryEventSource } from './chain-source.js';
import { loadRegistrySyncConfig, type RegistrySyncConfig } from './config.js';
import {
  createRedisKeyspace,
  mapRegistryEventToUpdate,
  toChainEventId,
  type RegistryEvent
} from './keyspace.js';
import { RedisProjectionStore, RedisRetryCounterStore, type RedisLike } from './projection-store.js';

export * from './chain-source.js';
export * from './config.js';
export * from './keyspace.js';
export * from './projection-store.js';
export * from './reader.js';

interface RegistrySyncMetrics {
  syncHeight: number;
  latestFinalizedHeight: number;
  updateCount: number;
  failureCount: number;
  retryCount: number;
  dlqCount: number;
}

export interface RegistrySyncService {
  start(): Promise<void>;
  stop(): Promise<void>;
  getMetrics(): Readonly<RegistrySyncMetrics>;
}

interface RegistrySyncDependencies {
  config?: RegistrySyncConfig;
  redis?: RedisLike;
  eventSource?: FinalizedRegistryEventSource;
  enableHealthServer?: boolean;
  sleep?: (ms: number) => Promise<void>;
}

type RedisConstructor = new (url: string) => RedisLike;

export function createRegistrySyncService(
  deps: RegistrySyncDependencies = {}
): RegistrySyncService {
  const config = deps.config ?? loadRegistrySyncConfig();
  const RedisClient = Redis as unknown as RedisConstructor;
  const redis = deps.redis ?? new RedisClient(config.redisUrl);
  const keyspace = createRedisKeyspace(config.redisKeyPrefix);
  const projectionStore = new RedisProjectionStore(redis, keyspace);
  const retryStore = new RedisRetryCounterStore(redis, keyspace);
  const eventSource = deps.eventSource ?? new SubstrateFinalizedRegistryEventSource(config.substrateWsUrl);
  const sleep = deps.sleep ?? defaultSleep;

  let started = false;
  let healthServer: Server | null = null;
  const metrics: RegistrySyncMetrics = {
    syncHeight: 0,
    latestFinalizedHeight: 0,
    updateCount: 0,
    failureCount: 0,
    retryCount: 0,
    dlqCount: 0
  };

  async function processEvent(event: RegistryEvent): Promise<void> {
    const eventId = toChainEventId(event);
    metrics.latestFinalizedHeight = Math.max(metrics.latestFinalizedHeight, event.blockHeight);

    let pending = true;
    while (pending) {
      const result = await runConsumerProcessingRule(event, {
        retryPolicy: {
          maxAttempts: config.maxRetries,
          getEventId: (current) => toChainEventId(current),
          store: retryStore
        },
        idempotency: {
          getEventId: (current) => toChainEventId(current),
          hasProcessed: (candidate) => projectionStore.hasProcessed(candidate),
          markProcessed: (candidate) => projectionStore.markProcessed(candidate)
        },
        performExternalAction: async (current) => {
          const projection = mapRegistryEventToUpdate(current);
          if (!projection) {
            throw new Error('Registry event missing sensorAddress/publicKey for projection');
          }

          await projectionStore.applyProjection(projection, {
            updatedAtBlock: current.blockHeight,
            updatedAtEvent: eventId
          });

          metrics.updateCount += 1;
          logStructured('projection_updated', {
            eventId,
            blockHeight: current.blockHeight,
            eventIndex: current.eventIndex,
            section: current.section,
            method: current.method,
            sensorAddress: projection.sensorAddress,
            publicKey: projection.publicKey,
            enabled: projection.enabled
          });
        },
        waitForConfirmation: async () => {},
        emitResultEvent: async (current) => ({
          type: 'registry-sync.result',
          eventId,
          blockHeight: current.blockHeight
        }),
        publishResultEvent: async () => {},
        commitOffset: async () => {
          await projectionStore.commitCursorHeight(event.blockHeight);
          metrics.syncHeight = Math.max(metrics.syncHeight, event.blockHeight);
        },
        retryDlqPublisher: {
          publishRetry: async (_event, reason, context) => {
            metrics.retryCount += 1;
            metrics.failureCount += 1;
            logStructured('projection_retry', {
              eventId,
              reason,
              topic: context?.topic ?? TELEMETRY_TOPICS.RETRY,
              attempt: context?.attempt,
              maxAttempts: context?.maxAttempts,
              blockHeight: event.blockHeight,
              eventIndex: event.eventIndex
            });
          },
          publishDlq: async (failedEvent, reason, context) => {
            metrics.dlqCount += 1;
            metrics.failureCount += 1;
            await projectionStore.publishDlq({
              eventId,
              blockHeight: failedEvent.blockHeight,
              eventIndex: failedEvent.eventIndex,
              reason,
              context,
              section: failedEvent.section,
              method: failedEvent.method,
              rawData: failedEvent.rawData
            });
            logStructured('projection_dlq', {
              eventId,
              reason,
              topic: context?.topic ?? TELEMETRY_TOPICS.DLQ,
              attempt: context?.attempt,
              maxAttempts: context?.maxAttempts,
              blockHeight: event.blockHeight,
              eventIndex: event.eventIndex
            });
          }
        }
      });

      if (result === 'retried') {
        await sleep(config.retryBackoffMs);
        continue;
      }

      if (result === 'dlq') {
        await projectionStore.commitCursorHeight(event.blockHeight);
        metrics.syncHeight = Math.max(metrics.syncHeight, event.blockHeight);
      }

      pending = false;
    }
  }

  return {
    async start(): Promise<void> {
      if (started) {
        return;
      }

      started = true;
      await eventSource.connect();
      metrics.syncHeight = await projectionStore.loadCursorHeight();
      metrics.latestFinalizedHeight = await eventSource.getLatestFinalizedHeight();

      if (deps.enableHealthServer ?? true) {
        healthServer = startHealthAndMetricsServer(config.healthPort, metrics);
      }

      await eventSource.startFrom(metrics.syncHeight + 1, async (event) => {
        await processEvent(event);
      });

      logStructured('registry_sync_started', {
        substrateWsUrl: config.substrateWsUrl,
        redisUrl: config.redisUrl,
        redisKeyPrefix: config.redisKeyPrefix,
        syncHeight: metrics.syncHeight
      });
    },
    async stop(): Promise<void> {
      if (!started) {
        return;
      }

      started = false;
      await eventSource.stop();
      await eventSource.disconnect();

      if (healthServer) {
        await new Promise<void>((resolve, reject) => {
          healthServer?.close((error) => {
            if (error) {
              reject(error);
              return;
            }
            resolve();
          });
        });
        healthServer = null;
      }

      if (typeof redis.quit === 'function') {
        await redis.quit();
      } else if (typeof redis.disconnect === 'function') {
        redis.disconnect();
      }

      logStructured('registry_sync_stopped', {
        syncHeight: metrics.syncHeight,
        updateCount: metrics.updateCount,
        failureCount: metrics.failureCount,
        retryCount: metrics.retryCount,
        dlqCount: metrics.dlqCount
      });
    },
    getMetrics(): Readonly<RegistrySyncMetrics> {
      return metrics;
    }
  };
}

function startHealthAndMetricsServer(port: number, metrics: RegistrySyncMetrics): Server {
  const server = createServer((request, response) => {
    if (request.url === '/health') {
      response.statusCode = 200;
      response.setHeader('content-type', 'application/json; charset=utf-8');
      response.end(
        JSON.stringify({
          status: 'ok',
          sync_height: metrics.syncHeight,
          latest_finalized_height: metrics.latestFinalizedHeight,
          lag: Math.max(metrics.latestFinalizedHeight - metrics.syncHeight, 0)
        })
      );
      return;
    }

    if (request.url === '/metrics') {
      response.statusCode = 200;
      response.setHeader('content-type', 'text/plain; version=0.0.4');
      response.end([
        '# HELP registry_sync_height Current synced finalized block height.',
        '# TYPE registry_sync_height gauge',
        `registry_sync_height ${metrics.syncHeight}`,
        '# HELP registry_sync_lag Finalized height lag.',
        '# TYPE registry_sync_lag gauge',
        `registry_sync_lag ${Math.max(metrics.latestFinalizedHeight - metrics.syncHeight, 0)}`,
        '# HELP registry_sync_updates_total Projection updates applied.',
        '# TYPE registry_sync_updates_total counter',
        `registry_sync_updates_total ${metrics.updateCount}`,
        '# HELP registry_sync_failures_total Processing failures.',
        '# TYPE registry_sync_failures_total counter',
        `registry_sync_failures_total ${metrics.failureCount}`,
        '# HELP registry_sync_retries_total Processing retries.',
        '# TYPE registry_sync_retries_total counter',
        `registry_sync_retries_total ${metrics.retryCount}`,
        '# HELP registry_sync_dlq_total DLQ events emitted.',
        '# TYPE registry_sync_dlq_total counter',
        `registry_sync_dlq_total ${metrics.dlqCount}`
      ].join('\n'));
      return;
    }

    response.statusCode = 404;
    response.end('not found');
  });

  server.listen({ host: '0.0.0.0', port });
  return server;
}

function logStructured(message: string, fields: Record<string, unknown>): void {
  console.log(JSON.stringify({ service: 'registry-sync', message, ...fields }));
}

async function defaultSleep(ms: number): Promise<void> {
  await new Promise<void>((resolve) => {
    setTimeout(resolve, ms);
  });
}

const isDirectRun = process.argv[1] === fileURLToPath(import.meta.url);
if (isDirectRun) {
  const service = createRegistrySyncService();
  service.start().catch((error: unknown) => {
    console.error('[registry-sync] failed to start', error);
    process.exitCode = 1;
  });
}
