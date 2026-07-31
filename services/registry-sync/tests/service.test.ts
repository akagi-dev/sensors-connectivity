import { describe, expect, it } from 'vitest';
import { createRegistrySyncService } from '../src/index.js';
import type { RegistrySyncConfig } from '../src/config.js';
import { createRedisKeyspace } from '../src/keyspace.js';
import { FakeRedis, FixtureEventSource } from './test-helpers.js';

function testConfig(overrides: Partial<RegistrySyncConfig> = {}): RegistrySyncConfig {
  return {
    substrateWsUrl: 'ws://unused',
    redisUrl: 'redis://unused',
    logLevel: 'info',
    redisKeyPrefix: 'registry-sync:v1',
    healthPort: 3011,
    maxRetries: 3,
    retryBackoffMs: 1,
    nonceTtlSeconds: 900,
    ...overrides
  };
}

describe('registry sync service processing', () => {
  it('applies projection once for replayed finalized event (idempotent)', async () => {
    const redis = new FakeRedis();
    const keys = createRedisKeyspace('registry-sync:v1');
    const source = new FixtureEventSource([
      {
        blockHeight: 200,
        eventIndex: 0,
        section: 'registry',
        method: 'AuthorizationCreated',
        sensorAddress: 'sensor-a'
      },
      {
        blockHeight: 200,
        eventIndex: 0,
        section: 'registry',
        method: 'AuthorizationCreated',
        sensorAddress: 'sensor-a'
      }
    ]);

    const service = createRegistrySyncService({
      config: testConfig(),
      redis,
      eventSource: source,
      enableHealthServer: false
    });

    await service.start();
    await service.stop();

    const sensor = await redis.hgetall(keys.sensorState('sensor-a'));
    expect(sensor.enabled).toBe('true');
    expect(service.getMetrics().updateCount).toBe(1);
    expect(await redis.get(keys.cursorHeight)).toBe('200');
  });

  it('retries transient redis failure and succeeds', async () => {
    const redis = new FakeRedis();
    redis.failHsetTimes = 1;

    const source = new FixtureEventSource([
      {
        blockHeight: 201,
        eventIndex: 1,
        section: 'registry',
        method: 'AuthorizationUpdated',
        sensorAddress: 'sensor-b'
      }
    ]);

    const service = createRegistrySyncService({
      config: testConfig(),
      redis,
      eventSource: source,
      enableHealthServer: false
    });

    await service.start();
    await service.stop();

    expect(service.getMetrics().retryCount).toBe(1);
    expect(service.getMetrics().updateCount).toBe(1);
  });

  it('routes exhausted failures to dlq and continues cursor progress', async () => {
    const redis = new FakeRedis();
    const keys = createRedisKeyspace('registry-sync:v1');
    const source = new FixtureEventSource([
      {
        blockHeight: 202,
        eventIndex: 2,
        section: 'registry',
        method: 'AuthorizationUpdated'
      },
      {
        blockHeight: 203,
        eventIndex: 0,
        section: 'registry',
        method: 'AuthorizationCreated',
        sensorAddress: 'sensor-d'
      }
    ]);

    const service = createRegistrySyncService({
      config: testConfig({ maxRetries: 2 }),
      redis,
      eventSource: source,
      enableHealthServer: false
    });

    await service.start();
    await service.stop();

    expect(service.getMetrics().dlqCount).toBe(1);
    expect(await redis.get(keys.cursorHeight)).toBe('203');
    expect(redis.getList(keys.dlqEvents)).toHaveLength(1);
  });

  it('resumes from persisted cursor on restart', async () => {
    const redis = new FakeRedis();
    const keys = createRedisKeyspace('registry-sync:v1');
    await redis.set(keys.cursorHeight, '299');

    const source = new FixtureEventSource([
      {
        blockHeight: 299,
        eventIndex: 0,
        section: 'registry',
        method: 'AuthorizationCreated',
        sensorAddress: 'sensor-old'
      },
      {
        blockHeight: 300,
        eventIndex: 0,
        section: 'registry',
        method: 'AuthorizationCreated',
        sensorAddress: 'sensor-new'
      }
    ]);

    const service = createRegistrySyncService({
      config: testConfig(),
      redis,
      eventSource: source,
      enableHealthServer: false
    });

    await service.start();
    await service.stop();

    const oldSensor = await redis.hgetall(keys.sensorState('sensor-old'));
    const newSensor = await redis.hgetall(keys.sensorState('sensor-new'));

    expect(oldSensor.sensor_address).toBeUndefined();
    expect(newSensor.sensor_address).toBe('sensor-new');
    expect(await redis.get(keys.cursorHeight)).toBe('300');
  });
});
