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
import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import { randomBytes } from 'node:crypto';
import { encodeAddress } from '@polkadot/util-crypto';
import {
  loadSensorAuthConfig,
  createSensorAuthProvider,
} from '../src/sensor-auth-factory.js';
import { InMemoryRegistryReader } from '@scp/registry-sync';

// Helper to create SS58 test addresses (using Robonomics prefix 32)
function createTestAddress(): { address: string; pubkey: Uint8Array } {
  const pubkey = randomBytes(32);
  const address = encodeAddress(pubkey, 32);
  return { address, pubkey };
}

describe('sensor auth factory', () => {
  const originalEnv = process.env;

  beforeEach(() => {
    process.env = { ...originalEnv };
  });

  afterEach(() => {
    process.env = originalEnv;
  });

  describe('loadSensorAuthConfig', () => {
    it('should default to registry-sync strategy', () => {
      delete process.env.SENSOR_AUTH_STRATEGY;

      const config = loadSensorAuthConfig();

      expect(config.strategy).toBe('registry-sync');
    });

    it('should load whitelist strategy from env', () => {
      process.env.SENSOR_AUTH_STRATEGY = 'whitelist';

      const config = loadSensorAuthConfig();

      expect(config.strategy).toBe('whitelist');
    });

    it('should default to registry-sync for invalid strategy', () => {
      process.env.SENSOR_AUTH_STRATEGY = 'invalid-strategy';

      const config = loadSensorAuthConfig();

      expect(config.strategy).toBe('registry-sync');
    });
  });

  describe('createSensorAuthProvider', () => {
    it('should create registry-sync provider', async () => {
      const sensor1 = createTestAddress();
      const sensor2 = createTestAddress();

      const mockRegistryReader = new InMemoryRegistryReader([
        { sensorId: sensor1.pubkey, enabled: true },
      ]);

      const provider = createSensorAuthProvider(
        'registry-sync',
        () => mockRegistryReader
      );

      expect(await provider.authenticate(sensor1.pubkey)).toBe(true);
      expect(await provider.authenticate(sensor2.pubkey)).toBe(false);
    });

    it('should create whitelist provider', async () => {
      const sensorA = createTestAddress();
      const sensorB = createTestAddress();
      const sensorC = createTestAddress();
      const sensorD = createTestAddress();

      process.env.WHITELIST_SENSOR_IDS = `${sensorA.address},${sensorB.address},${sensorC.address}`;

      const mockRegistryReader = new InMemoryRegistryReader([]);
      const provider = createSensorAuthProvider(
        'whitelist',
        () => mockRegistryReader
      );

      expect(await provider.authenticate(sensorA.pubkey)).toBe(true);
      expect(await provider.authenticate(sensorB.pubkey)).toBe(true);
      expect(await provider.authenticate(sensorC.pubkey)).toBe(true);
      expect(await provider.authenticate(sensorD.pubkey)).toBe(false);
    });

    it('should handle nonce management for whitelist provider', async () => {
      const sensor1 = createTestAddress();
      const nonce1 = randomBytes(32);

      process.env.WHITELIST_SENSOR_IDS = sensor1.address;

      const mockRegistryReader = new InMemoryRegistryReader([]);
      const provider = createSensorAuthProvider(
        'whitelist',
        () => mockRegistryReader
      );

      expect(await provider.isNonceSeen(sensor1.pubkey, nonce1)).toBe(false);
      await provider.rememberNonce(sensor1.pubkey, nonce1);
      expect(await provider.isNonceSeen(sensor1.pubkey, nonce1)).toBe(true);
    });

    it('should return sensor record for whitelist provider', async () => {
      const sensor1 = createTestAddress();
      const sensor2 = createTestAddress();
      const unknown = createTestAddress();

      process.env.WHITELIST_SENSOR_IDS = `${sensor1.address},${sensor2.address}`;

      const mockRegistryReader = new InMemoryRegistryReader([]);
      const provider = createSensorAuthProvider(
        'whitelist',
        () => mockRegistryReader
      );

      const record = await provider.getSensorRecord(sensor1.pubkey);
      expect(record).toEqual({
        sensorId: sensor1.pubkey,
        enabled: true,
      });

      const unknownRecord = await provider.getSensorRecord(unknown.pubkey);
      expect(unknownRecord).toBeNull();
    });
  });
});
