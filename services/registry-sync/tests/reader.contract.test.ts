import { describe, expect, it } from 'vitest';
import { createRedisKeyspace } from '../src/keyspace.js';
import { RedisRegistryReader } from '../src/reader.js';
import { FakeRedis } from './test-helpers.js';

describe('authorizer read contract from redis projection', () => {
  it('reads sensor status/public key and nonce state from projected schema', async () => {
    const redis = new FakeRedis();
    const keys = createRedisKeyspace('registry-sync:v1');

    await redis.hset(keys.sensorState('sensor-1'), {
      sensor_address: 'sensor-1',
      public_key: 'pk-1',
      enabled: 'true',
      updated_at_block: '100',
      updated_at_event: '100:0'
    });

    const reader = new RedisRegistryReader(redis, 'registry-sync:v1', 900);
    const record = await reader.getSensorRecord('sensor-1');

    expect(record).toEqual({
      sensorAddress: 'sensor-1',
      publicKey: 'pk-1',
      enabled: true
    });

    expect(await reader.isNonceSeen('sensor-1', 'n-1')).toBe(false);
    await reader.rememberNonce('sensor-1', 'n-1');
    expect(await reader.isNonceSeen('sensor-1', 'n-1')).toBe(true);
  });
});
