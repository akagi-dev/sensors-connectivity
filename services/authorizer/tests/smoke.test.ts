import { describe, expect, it } from 'vitest';
import { createAuthorizerApp } from '../src/index.js';
import { InMemoryRegistryReader } from '../../registry-sync/src/reader.js';

describe('authorizer smoke', () => {
  it('returns 403 for disabled/unknown sensor and exposes health and metrics counters', async () => {
    const app = createAuthorizerApp({
      registryReader: new InMemoryRegistryReader([]),
      producer: {
        async publishAuthorized() {
          return;
        }
      }
    });

    const response = await app.inject({
      method: 'POST',
      url: '/v1/telemetry',
      payload: {
        measurements: { temp: 21 },
        sensor_address: 'sensor-1',
        timestamp: '2026-01-01T00:00:00Z',
        nonce: 'n1',
        signature: '0x00'
      }
    });

    expect(response.statusCode).toBe(403);

    const health = await app.inject({ method: 'GET', url: '/health' });
    expect(health.statusCode).toBe(200);

    const metrics = await app.inject({ method: 'GET', url: '/metrics' });
    expect(metrics.statusCode).toBe(200);
    expect(metrics.json()).toEqual({
      accepted: 0,
      rejected: 1,
      kafkaErrors: 0
    });
    await app.close();
  });
});
