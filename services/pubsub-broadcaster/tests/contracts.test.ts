import {
  TELEMETRY_TOPICS,
  validateEnvelopeWithKnownPayload,
  createEnvelope,
  serializeEnvelope,
  serializePayloadForEventType,
  TelemetryAuthorizedPayloadSchema,
  TelemetryPubsubResultPayloadSchema,
  TelemetryPubsubResultPayload_Status,
} from '@scp/contracts';
import { create } from '@bufbuild/protobuf';
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

    const envelope = createEnvelope({
      eventId: 'evt-contract-1',
      eventType: TELEMETRY_TOPICS.AUTHORIZED,
      eventVersion: 'v1',
      occurredAt: '2026-01-01T00:00:00Z',
      source: 'endpoint',
      payload: serializePayloadForEventType(
        TELEMETRY_TOPICS.AUTHORIZED,
        payload
      ),
    });

    const result = validateEnvelopeWithKnownPayload(
      serializeEnvelope(envelope)
    );

    expect(result.success).toBe(true);
  });

  it('accepts telemetry.pubsub.result.v1 envelope/payload', () => {
    const payload = create(TelemetryPubsubResultPayloadSchema, {
      status: TelemetryPubsubResultPayload_Status.SUBMITTED,
      sensorId: Buffer.alloc(32, 1),
    });

    const envelope = createEnvelope({
      eventId: 'evt-contract-2',
      eventType: TELEMETRY_TOPICS.PUBSUB_RESULT,
      eventVersion: 'v1',
      occurredAt: '2026-01-01T00:00:00Z',
      source: 'pubsub-broadcaster',
      payload: serializePayloadForEventType(
        TELEMETRY_TOPICS.PUBSUB_RESULT,
        payload
      ),
    });

    const result = validateEnvelopeWithKnownPayload(
      serializeEnvelope(envelope)
    );

    expect(result.success).toBe(true);
  });
});
