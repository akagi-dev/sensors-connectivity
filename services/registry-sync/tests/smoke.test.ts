import { describe, expect, it } from 'vitest';
import { loadRegistrySyncConfig } from '../src/config.js';

describe('registry-sync smoke', () => {
  it('loads default registry-sync configuration values', () => {
    const config = loadRegistrySyncConfig({});
    expect(config.redisKeyPrefix).toBe('registry-sync:v1');
    expect(config.maxRetries).toBe(3);
  });
});
