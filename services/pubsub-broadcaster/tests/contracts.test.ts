import {
  TELEMETRY_TOPICS,
  validateEnvelopeWithKnownPayload,
} from '@scp/contracts';
import { describe, expect, it } from 'vitest';

describe('pubsub broadcaster contract compatibility', () => {
  it('accepts telemetry.authorized.v1 envelope/payload', () => {
    const result = validateEnvelopeWithKnownPayload({
      event_id: 'evt-contract-1',
      event_type: TELEMETRY_TOPICS.AUTHORIZED,
      event_version: 'v1',
      occurred_at: '2026-01-01T00:00:00Z',
      source: 'endpoint',
      payload: {
        sensor_id: Buffer.alloc(32, 1).toString('base64'),
        timestamp: Date.parse('2026-01-01T00:00:00Z'),
        nonce: Buffer.alloc(16, 2).toString('base64'),
        message: Buffer.from(JSON.stringify({ temp: 20 })).toString('base64'),
        signature: Buffer.alloc(64, 3).toString('base64'),
      },
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
        sensor_id: 'sensor-1',
        nonce: 'nonce-1',
      },
    });

    expect(result.success).toBe(true);
  });
});
