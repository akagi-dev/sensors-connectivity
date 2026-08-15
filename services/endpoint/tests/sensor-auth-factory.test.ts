import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import { loadSensorAuthConfig, createSensorAuthProvider } from '../src/sensor-auth-factory.js';
import { InMemoryRegistryReader } from '@scp/registry-sync';

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
      const mockRegistryReader = new InMemoryRegistryReader([
        { sensorId: 'sensor-1', enabled: true }
      ]);

      const provider = createSensorAuthProvider('registry-sync', () => mockRegistryReader);

      expect(await provider.authenticate('sensor-1')).toBe(true);
      expect(await provider.authenticate('sensor-2')).toBe(false);
    });

    it('should create whitelist provider', async () => {
      process.env.WHITELIST_SENSOR_IDS = 'sensor-a,sensor-b,sensor-c';

      const mockRegistryReader = new InMemoryRegistryReader([]);
      const provider = createSensorAuthProvider('whitelist', () => mockRegistryReader);

      expect(await provider.authenticate('sensor-a')).toBe(true);
      expect(await provider.authenticate('sensor-b')).toBe(true);
      expect(await provider.authenticate('sensor-c')).toBe(true);
      expect(await provider.authenticate('sensor-d')).toBe(false);
    });

    it('should handle nonce management for whitelist provider', async () => {
      process.env.WHITELIST_SENSOR_IDS = 'sensor-1';

      const mockRegistryReader = new InMemoryRegistryReader([]);
      const provider = createSensorAuthProvider('whitelist', () => mockRegistryReader);

      expect(await provider.isNonceSeen('sensor-1', 'nonce-1')).toBe(false);
      await provider.rememberNonce('sensor-1', 'nonce-1');
      expect(await provider.isNonceSeen('sensor-1', 'nonce-1')).toBe(true);
    });

    it('should return sensor record for whitelist provider', async () => {
      process.env.WHITELIST_SENSOR_IDS = 'sensor-1,sensor-2';

      const mockRegistryReader = new InMemoryRegistryReader([]);
      const provider = createSensorAuthProvider('whitelist', () => mockRegistryReader);

      const record = await provider.getSensorRecord('sensor-1');
      expect(record).toEqual({
        sensorId: 'sensor-1',
        enabled: true
      });

      const unknownRecord = await provider.getSensorRecord('unknown');
      expect(unknownRecord).toBeNull();
    });
  });
});
