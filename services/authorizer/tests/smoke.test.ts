import { describe, expect, it } from 'vitest';
import { createAuthorizerApp } from '../src/index.js';
import { InMemoryRegistryReader } from '../../registry-sync/src/reader.js';

describe('authorizer smoke', () => {
  it('returns 403 for unknown sensor, emits rejected event, and exposes health/metrics counters', async () => {
    const rejectedEvents: Array<{ reason_code: string; sensor_address?: string | undefined }> = [];
    const app = createAuthorizerApp({
      registryReader: new InMemoryRegistryReader([]),
      producer: {
        async publishAuthorized() {
          return 'event-1';
        },
        async publishRejected(payload) {
          rejectedEvents.push(payload);
          return 'event-2';
        }
      }
    });

    const response = await app.inject({
      method: 'POST',
      url: '/v1/telemetry',
      payload: {
        measurements: { temp: 21 },
        sensor_address: 'sensor-1',
        timestamp: new Date().toISOString(),
        nonce: 'n1',
        signature: '0x00'
      }
    });

    expect(response.statusCode).toBe(403);
    expect(rejectedEvents).toEqual([
    {
      sensor_address: 'sensor-1',
      reason_code: 'sensor_forbidden',
      reason_message: 'Sensor is unknown or disabled'
    }
    ]);

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

  it('returns 401 for stale timestamps before registry/signature checks', async () => {
    const rejectedEvents: Array<{ reason_code: string; sensor_address?: string | undefined }> = [];
    const app = createAuthorizerApp(
      {
        registryReader: new InMemoryRegistryReader([]),
        producer: {
          async publishAuthorized() {
            return 'event-1';
          },
          async publishRejected(payload) {
            rejectedEvents.push(payload);
            return 'event-2';
          }
        }
      },
      { timestampSkewSeconds: 300 }
    );

    const response = await app.inject({
      method: 'POST',
      url: '/v1/telemetry',
      payload: {
        measurements: { temp: 21 },
        sensor_address: 'sensor-1',
        timestamp: '2020-01-01T00:00:00Z',
        nonce: 'n1',
        signature: 'AA=='
      }
    });

    expect(response.statusCode).toBe(401);
    expect(response.json()).toEqual({ status: 'rejected', error_code: 'stale_timestamp' });
    expect(rejectedEvents).toEqual([
      {
        sensor_address: 'sensor-1',
        reason_code: 'stale_timestamp',
        reason_message: 'Timestamp outside allowed skew window'
      }
    ]);
    await app.close();
  });
});
