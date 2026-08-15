import {
  cryptoWaitReady,
  ed25519PairFromSeed,
  encodeAddress,
} from '@polkadot/util-crypto';
import { describe, expect, it } from 'vitest';
import {
  createFakeEnvelopePayload,
  decodeCoreMessageBytes,
  parseFakeSensorCliOptions,
} from '../src/index.js';

function readVarint(
  bytes: Uint8Array,
  start: number
): { value: number; nextOffset: number } {
  let value = 0;
  let shift = 0;
  let offset = start;
  while (offset < bytes.length) {
    const current = bytes[offset];
    if (current === undefined) {
      break;
    }
    value |= (current & 0x7f) << shift;
    offset += 1;
    if ((current & 0x80) === 0) {
      return { value, nextOffset: offset };
    }
    shift += 7;
  }
  throw new Error('invalid varint');
}

function firstLengthDelimitedField(
  bytes: Uint8Array,
  targetField: number
): Uint8Array {
  let offset = 0;
  while (offset < bytes.length) {
    const { value: tag, nextOffset: tagOffset } = readVarint(bytes, offset);
    offset = tagOffset;
    const fieldNumber = tag >> 3;
    const wireType = tag & 0x07;
    if (wireType === 2) {
      const { value: length, nextOffset } = readVarint(bytes, offset);
      const end = nextOffset + length;
      const value = bytes.subarray(nextOffset, end);
      if (fieldNumber === targetField) {
        return Uint8Array.from(value);
      }
      offset = end;
      continue;
    }
    if (wireType === 0) {
      offset = readVarint(bytes, offset).nextOffset;
      continue;
    }
    if (wireType === 1) {
      offset += 8;
      continue;
    }
    if (wireType === 5) {
      offset += 4;
      continue;
    }
    throw new Error(`unsupported wire type ${wireType}`);
  }
  throw new Error(`missing field ${targetField}`);
}

describe('fake sensor CLI', () => {
  it('parses args with defaults', async () => {
    await cryptoWaitReady();
    const signerSeedHex =
      '0x0000000000000000000000000000000000000000000000000000000000000001';
    const seed = Buffer.from(signerSeedHex.slice(2), 'hex');
    const expectedSensorId = encodeAddress(ed25519PairFromSeed(seed).publicKey);

    const options = parseFakeSensorCliOptions([], {});
    expect(options).toEqual({
      endpointUrl: 'http://localhost:3000/v1/telemetry',
      sensorId: expectedSensorId,
      count: 1,
      intervalMs: 1000,
      signerSeedHex,
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
        '0x0101010101010101010101010101010101010101010101010101010101010101',
      ],
      {
        SENSOR_FAKE_SENSOR_ID:
          '5FCM8VvFKfKzQnmkT7X9kDY6xMcgeGYRK8tmSbfwpXyM1CvS',
        SENSOR_FAKE_INTERVAL_MS: '250',
      }
    );

    expect(options).toEqual({
      endpointUrl: 'http://localhost:4000/v1/telemetry',
      sensorId: '5FCM8VvFKfKzQnmkT7X9kDY6xMcgeGYRK8tmSbfwpXyM1CvS',
      count: 3,
      intervalMs: 250,
      signerSeedHex:
        '0x0101010101010101010101010101010101010101010101010101010101010101',
      sensorZone: 'eu-west',
    });
  });

  it('creates fake signed protobuf envelope', () => {
    const signerSeedHex =
      '0x0101010101010101010101010101010101010101010101010101010101010101';
    const payload = createFakeEnvelopePayload(signerSeedHex);
    expect(payload.envelopeBytes.length).toBeGreaterThan(0);
    expect(payload.sensorAddress).toBe(
      encodeAddress(
        ed25519PairFromSeed(Buffer.from(signerSeedHex.slice(2), 'hex'))
          .publicKey,
        32
      )
    );
    expect(payload.nonce.length).toBe(16);

    const message = decodeCoreMessageBytes(
      firstLengthDelimitedField(payload.envelopeBytes, 4)
    );
    expect(message.metadata).toHaveLength(1);
    expect(message.urban).toHaveLength(1);

    const publicSensors = message.urban?.[0]?.sensors;
    expect(publicSensors).toHaveLength(2);
    expect(publicSensors?.[0]?.bme280?.temperature?.value).toBeTypeOf('number');
    expect(publicSensors?.[1]?.bme280?.humidity?.value).toBeTypeOf('number');
  });

  it('throws for invalid numeric options', () => {
    expect(() => parseFakeSensorCliOptions(['--count', '0'], {})).toThrow(
      'Invalid positive integer value: 0'
    );
  });

  it('throws when sensor id does not match signer seed', () => {
    expect(() =>
      parseFakeSensorCliOptions(
        ['--sensor-id', '5DoHTsjp9DN9KEabruKS8p8wAAVHgrEiJ9vtyjAuGEMZqpWt'],
        {
          SENSOR_FAKE_SIGNER_SEED_HEX:
            '0x0202020202020202020202020202020202020202020202020202020202020202',
        }
      )
    ).toThrow('sensor id does not match signer seed');
  });
});
