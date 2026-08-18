/**
 * Copyright 2026 Robonomics Network
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
