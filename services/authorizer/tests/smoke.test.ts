import { describe, expect, it } from 'vitest';
import { createAuthorizerApp } from '../src/index.js';
import { InMemoryRegistryReader } from '../../registry-sync/src/reader.js';

describe('authorizer smoke', () => {
  it('returns 403 for disabled/unknown sensor', async () => {
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
    await app.close();
  });
});
