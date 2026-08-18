import {
  TELEMETRY_TOPICS,
  TelemetryAuthorizedPayloadSchema,
  EnvelopeSchema,
} from '@scp/core';
import { create, toBinary, fromBinary } from '@bufbuild/protobuf';
import { describe, expect, it } from 'vitest';

describe('pubsub broadcaster contract compatibility', () => {
  it('accepts telemetry.authorized.v1 envelope/payload', () => {
    const payload = create(TelemetryAuthorizedPayloadSchema, {
      sensorId: Buffer.alloc(32, 1),
      timestamp: BigInt(Date.parse('2026-01-01T00:00:00Z')),
      nonce: Buffer.alloc(16, 2),
      message: Buffer.from(JSON.stringify({ temp: 20 })),
      signature: Buffer.alloc(64, 3),
      signedEnvelope: Buffer.alloc(100, 4),
    });

    const envelope = create(EnvelopeSchema, {
      eventId: 'evt-contract-1',
      eventType: TELEMETRY_TOPICS.AUTHORIZED,
      eventVersion: 'v1',
      occurredAt: '2026-01-01T00:00:00Z',
      source: 'endpoint',
      payload: toBinary(TelemetryAuthorizedPayloadSchema, payload),
    });

    const envelopeBytes = toBinary(EnvelopeSchema, envelope);
    const parsed = fromBinary(EnvelopeSchema, envelopeBytes);

    expect(parsed.eventType).toBe(TELEMETRY_TOPICS.AUTHORIZED);
    expect(parsed.eventId).toBe('evt-contract-1');
  });
});
