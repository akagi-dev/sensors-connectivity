import Redis from 'ioredis';
import { describe, expect, it } from 'vitest';
import { createRegistrySyncService } from '../src/index.js';
import type { RegistrySyncConfig } from '../src/config.js';
import { createRedisKeyspace } from '../src/keyspace.js';
import type { RedisLike } from '../src/projection-store.js';
import { FixtureEventSource } from './test-helpers.js';

const runRedisIntegration = process.env.REGISTRY_SYNC_REDIS_INTEGRATION === '1';
type RedisConstructor = new (url: string) => {
  hgetall(key: string): Promise<Record<string, string>>;
  get(key: string): Promise<string | null>;
  quit(): Promise<unknown>;
} & RedisLike;

describe.runIf(runRedisIntegration)('registry-sync redis integration', () => {
  it('replays fixture events into redis projection and checkpoint', async () => {
    const redisUrl = process.env.REDIS_URL ?? 'redis://127.0.0.1:6379';
    const redisPrefix = `registry-sync:int:${Date.now()}`;
    const keyspace = createRedisKeyspace(redisPrefix);
    const RedisClient = Redis as unknown as RedisConstructor;
    const redis = new RedisClient(redisUrl);

    const config: RegistrySyncConfig = {
      substrateWsUrl: 'ws://unused',
      redisUrl,
      logLevel: 'info',
      redisKeyPrefix: redisPrefix,
      healthPort: 3021,
      maxRetries: 3,
      retryBackoffMs: 1,
      nonceTtlSeconds: 900
    };

    const source = new FixtureEventSource([
      {
        blockHeight: 401,
        eventIndex: 0,
        section: 'registry',
        method: 'AuthorizationCreated',
        sensorId: 'sensor-int'
      },
      {
        blockHeight: 402,
        eventIndex: 0,
        section: 'registry',
        method: 'AuthorizationDisabled',
        sensorId: 'sensor-int'
      }
    ]);

    const service = createRegistrySyncService({
      config,
      redis,
      eventSource: source,
      enableHealthServer: false
    });

    await service.start();
    await service.stop();

    const sensor = await redis.hgetall(keyspace.sensorState('sensor-int'));

    expect(sensor.enabled).toBe('false');
    expect(await redis.get(keyspace.cursorHeight)).toBe('402');

    await redis.quit();
  });
});
