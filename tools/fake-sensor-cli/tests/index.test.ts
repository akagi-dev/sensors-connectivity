import { describe, expect, it } from 'vitest';
import { createFakePayload, parseFakeSensorCliOptions } from '../src/index.js';

describe('fake sensor CLI', () => {
  it('parses args with defaults', () => {
    const options = parseFakeSensorCliOptions([], {});
    expect(options).toEqual({
      endpointUrl: 'http://localhost:3000/v1/telemetry',
      sensorId: 'debug-sensor-001',
      count: 1,
      intervalMs: 1000,
      authHeader: 'authorization'
    });
  });

  it('accepts cli args and env overrides', () => {
    const options = parseFakeSensorCliOptions(
      ['--endpoint', 'http://localhost:4000/v1/telemetry', '--count=3', '--auth-token', 'secret'],
      {
        SENSOR_FAKE_SENSOR_ID: 'env-sensor',
        SENSOR_FAKE_INTERVAL_MS: '250'
      }
    );

    expect(options).toEqual({
      endpointUrl: 'http://localhost:4000/v1/telemetry',
      sensorId: 'env-sensor',
      count: 3,
      intervalMs: 250,
      authHeader: 'authorization',
      authToken: 'secret'
    });
  });

  it('creates fake payload with expected shape', () => {
    const payload = createFakePayload('sensor-a');
    expect(payload.sensor_id).toBe('sensor-a');
    expect(typeof payload.timestamp).toBe('string');
    expect(typeof payload.measurements.temperature_c).toBe('number');
    expect(typeof payload.measurements.humidity_pct).toBe('number');
  });

  it('throws for invalid numeric options', () => {
    expect(() => parseFakeSensorCliOptions(['--count', '0'], {})).toThrow('Invalid positive integer value: 0');
  });
});
