import { cryptoWaitReady, ed25519PairFromSeed, encodeAddress } from '@polkadot/util-crypto';
import { describe, expect, it } from 'vitest';
import { createFakePayload, parseFakeSensorCliOptions } from '../src/index.js';

describe('fake sensor CLI', () => {
  it('parses args with defaults', async () => {
    await cryptoWaitReady();
    const signerSeedHex = '0x0000000000000000000000000000000000000000000000000000000000000001';
    const seed = Buffer.from(signerSeedHex.slice(2), 'hex');
    const expectedSensorId = encodeAddress(ed25519PairFromSeed(seed).publicKey);

    const options = parseFakeSensorCliOptions([], {});
    expect(options).toEqual({
      endpointUrl: 'http://localhost:3000/v1/telemetry',
      sensorId: expectedSensorId,
      count: 1,
      intervalMs: 1000,
      signerSeedHex
    });
  });

  it('accepts cli args and env overrides', () => {
    const options = parseFakeSensorCliOptions(
      [
        '--endpoint',
        'http://localhost:4000/v1/telemetry',
        '--count=3',
        '--sensor-zone',
        'eu-west',
        '--signer-seed-hex',
        '0x0101010101010101010101010101010101010101010101010101010101010101'
      ],
      {
        SENSOR_FAKE_SENSOR_ID: '5FCM8VvFKfKzQnmkT7X9kDY6xMcgeGYRK8tmSbfwpXyM1CvS',
        SENSOR_FAKE_INTERVAL_MS: '250'
      }
    );

    expect(options).toEqual({
      endpointUrl: 'http://localhost:4000/v1/telemetry',
      sensorId: '5FCM8VvFKfKzQnmkT7X9kDY6xMcgeGYRK8tmSbfwpXyM1CvS',
      count: 3,
      intervalMs: 250,
      signerSeedHex: '0x0101010101010101010101010101010101010101010101010101010101010101',
      sensorZone: 'eu-west'
    });
  });

  it('creates fake payload with expected shape', () => {
    const payload = createFakePayload('5DoHTsjp9DN9KEabruKS8p8wAAVHgrEiJ9vtyjAuGEMZqpWt');
    expect(payload.sensor_id).toBe('5DoHTsjp9DN9KEabruKS8p8wAAVHgrEiJ9vtyjAuGEMZqpWt');
    expect(typeof payload.timestamp).toBe('string');
    expect(typeof payload.nonce).toBe('string');
    expect(typeof payload.measurements.temperature_c).toBe('number');
    expect(typeof payload.measurements.humidity_pct).toBe('number');
  });

  it('throws for invalid numeric options', () => {
    expect(() => parseFakeSensorCliOptions(['--count', '0'], {})).toThrow('Invalid positive integer value: 0');
  });

  it('throws when sensor id does not match signer seed', () => {
    expect(() =>
      parseFakeSensorCliOptions(
        ['--sensor-id', '5DoHTsjp9DN9KEabruKS8p8wAAVHgrEiJ9vtyjAuGEMZqpWt'],
        { SENSOR_FAKE_SIGNER_SEED_HEX: '0x0202020202020202020202020202020202020202020202020202020202020202' }
      )
    ).toThrow('sensor id does not match signer seed');
  });
});
