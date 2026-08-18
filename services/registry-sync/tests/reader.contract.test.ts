import { describe, expect, it } from 'vitest';
import { createRedisKeyspace } from '../src/keyspace.js';
import { RedisRegistryReader } from '../src/reader.js';
import { FakeRedis } from './test-helpers.js';

function toHex(bytes: Uint8Array): string {
  return Buffer.from(bytes).toString('hex');
}

describe('endpoint read contract from redis projection', () => {
  it('reads sensor status/public key and nonce state from projected schema', async () => {
    const redis = new FakeRedis();
    const keys = createRedisKeyspace('registry-sync:v1');

    const sensorId = Buffer.from('sensor-1', 'utf-8');
    const sensorIdHex = toHex(sensorId);
    const nonce1 = Buffer.from('n-1', 'utf-8');

    await redis.hset(keys.sensorState(sensorIdHex), {
      sensor_id: sensorIdHex,
      enabled: 'true',
      updated_at_block: '100',
      updated_at_event: '100:0',
    });

    const reader = new RedisRegistryReader(redis, 'registry-sync:v1', 900);
    const record = await reader.getSensorRecord(sensorId);

    expect(record).toEqual({
      sensorId,
      enabled: true,
    });

    expect(await reader.isNonceSeen(sensorId, nonce1)).toBe(false);
    await reader.rememberNonce(sensorId, nonce1);
    expect(await reader.isNonceSeen(sensorId, nonce1)).toBe(true);
  });
});
