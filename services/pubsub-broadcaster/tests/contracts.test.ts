import { TELEMETRY_TOPICS, validateEnvelopeWithKnownPayload } from '@scp/contracts';
import { describe, expect, it } from 'vitest';

describe('pubsub broadcaster contract compatibility', () => {
  it('accepts telemetry.authorized.v1 envelope/payload', () => {
    const result = validateEnvelopeWithKnownPayload({
      event_id: 'evt-contract-1',
      event_type: TELEMETRY_TOPICS.AUTHORIZED,
      event_version: 'v1',
      occurred_at: '2026-01-01T00:00:00Z',
      source: 'authorizer',
      payload: {
        sensor_address: 'sensor-1',
        timestamp: '2026-01-01T00:00:00Z',
        nonce: 'nonce-1',
        measurements: { temp: 20 },
        signature: '0xabc'
      }
    });

    expect(result.success).toBe(true);
  });

  it('accepts telemetry.pubsub.result.v1 envelope/payload', () => {
    const result = validateEnvelopeWithKnownPayload({
      event_id: 'evt-contract-2',
      event_type: TELEMETRY_TOPICS.PUBSUB_RESULT,
      event_version: 'v1',
      occurred_at: '2026-01-01T00:00:00Z',
      source: 'pubsub-broadcaster',
      payload: {
        status: 'submitted',
        pubsub_topic: 'telemetry/authorized/v1',
        sensor_address: 'sensor-1',
        nonce: 'nonce-1'
      }
    });

    expect(result.success).toBe(true);
  });
});
