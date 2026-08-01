import { describe, expect, it } from 'vitest';
import { loadRegistrySyncConfig } from '../src/config.js';

describe('smoke', () => {
  it('loads default configuration values', () => {
    const config = loadRegistrySyncConfig({});
    expect(config.redisKeyPrefix).toBe('registry-sync:v1');
    expect(config.maxRetries).toBe(3);
  });
});
