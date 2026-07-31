import { describe, expect, it } from 'vitest';
import { envelopeSchema } from '../src/envelope.js';
import {
  telemetryAuthorizedPayloadSchema,
  telemetryRejectedPayloadSchema,
  telemetryIpfsPublishedPayloadSchema,
  telemetryBlockchainResultPayloadSchema
} from '../src/events.js';
import { parseEnvelopeWithKnownPayload } from '../src/validation.js';

describe('contracts', () => {
  it('parses well-formed envelope + authorized payload', () => {
    const result = parseEnvelopeWithKnownPayload({
      event_id: 'evt-1',
      event_type: 'telemetry.authorized.v1',
      event_version: 'v1',
      occurred_at: '2026-01-01T00:00:00Z',
      source: 'authorizer',
      payload: {
        sensor_address: 'sensor-1',
        timestamp: '2026-01-01T00:00:00Z',
        nonce: 'nonce-1',
        measurements: { temp: 21.4 },
        signature: '0xabc'
      }
    });

    expect(result.event_type).toBe('telemetry.authorized.v1');
  });

  it('parses each supported payload schema', () => {
    expect(
      telemetryRejectedPayloadSchema.parse({
        reason_code: 'unauthorized',
        reason_message: 'bad signature'
      })
    ).toBeTruthy();

    expect(
      telemetryIpfsPublishedPayloadSchema.parse({
        cid: 'bafybeigdyrzt',
        event_count: 10
      })
    ).toBeTruthy();

    expect(
      telemetryBlockchainResultPayloadSchema.parse({
        target: 'robonomics',
        status: 'submitted',
        cid: 'bafybeigdyrzt',
        tx_hash: '0x123'
      })
    ).toBeTruthy();
  });

  it('fails malformed envelope', () => {
    expect(() =>
      envelopeSchema.parse({
        event_type: 'telemetry.authorized.v1',
        event_version: 'v1',
        occurred_at: 'not-a-date',
        source: 'authorizer',
        payload: {}
      })
    ).toThrow();
  });

  it('fails malformed payload', () => {
    expect(() =>
      telemetryAuthorizedPayloadSchema.parse({
        sensor_address: 'sensor-1',
        timestamp: '2026-01-01T00:00:00Z',
        nonce: 'nonce-1',
        measurements: 'invalid',
        signature: '0xabc'
      })
    ).toThrow();
  });
});
