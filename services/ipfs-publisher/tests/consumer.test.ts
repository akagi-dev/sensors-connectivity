import { TELEMETRY_TOPICS } from '@scp/contracts';
import { describe, expect, it } from 'vitest';
import { decodeAuthorizedKafkaMessage } from '../src/authorized-message.js';

/** Builds a minimal authorized envelope JSON string for decoder tests. */
function authorizedEnvelopeJson(): string {
  return JSON.stringify({
    event_id: 'event-1',
    event_type: TELEMETRY_TOPICS.AUTHORIZED,
    event_version: 'v1',
    occurred_at: '2026-08-03T13:00:00.000Z',
    source: 'authorizer',
    payload: {
      sensor_id: 'sensor-1',
      timestamp: '2026-08-03T13:00:00.000Z',
      nonce: 'nonce-1',
      measurements: { temperature: 22 },
      signature: '0x01'
    }
  });
}

describe('authorized Kafka message decoder', () => {
  it('extracts the authorized payload', () => {
    expect(
      decodeAuthorizedKafkaMessage(authorizedEnvelopeJson())
    ).toMatchObject({
      sensor_id: 'sensor-1',
      nonce: 'nonce-1'
    });
  });

  it('rejects invalid JSON', () => {
    expect(() => decodeAuthorizedKafkaMessage('not-json')).toThrow(
      'not valid JSON'
    );
  });

  it('rejects envelopes that do not match the WP-00 schema', () => {
    expect(() =>
      decodeAuthorizedKafkaMessage(
        JSON.stringify({
          event_type: TELEMETRY_TOPICS.AUTHORIZED,
          payload: {}
        })
      )
    ).toThrow('does not match WP-00 envelope/payload schemas');
  });

  it('rejects authorized payloads that do not match the WP-00 schema', () => {
    const envelope = JSON.parse(authorizedEnvelopeJson()) as {
      payload: Record<string, unknown>;
    };
    envelope.payload.measurements = 'invalid';

    expect(() =>
      decodeAuthorizedKafkaMessage(JSON.stringify(envelope))
    ).toThrow('does not match WP-00 envelope/payload schemas');
  });

  it('rejects valid envelopes for unsupported event types', () => {
    expect(() =>
      decodeAuthorizedKafkaMessage(
        JSON.stringify({
          event_id: 'event-2',
          event_type: TELEMETRY_TOPICS.REJECTED,
          event_version: 'v1',
          occurred_at: '2026-08-03T13:00:00.000Z',
          source: 'authorizer',
          payload: { reason_code: 'invalid_signature' }
        })
      )
    ).toThrow('unsupported event_type');
  });
});
