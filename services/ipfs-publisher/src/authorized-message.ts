import {
  parseEnvelopeWithKnownPayload,
  TELEMETRY_TOPICS,
  type TelemetryAuthorizedPayload
} from '@scp/contracts';
import type { AuthorizedTelemetryEnvelope } from './batching.js';

/**
 * Decodes a Kafka message, validates its envelope and payload against the
 * WP-00 schemas, and returns an authorized telemetry envelope.
 */
export function decodeAuthorizedKafkaEnvelope(
  rawMessage: string
): AuthorizedTelemetryEnvelope {
  let decoded: unknown;
  try {
    decoded = JSON.parse(rawMessage);
  } catch {
    throw new Error('Kafka message is not valid JSON');
  }

  let envelope: ReturnType<typeof parseEnvelopeWithKnownPayload>;
  try {
    envelope = parseEnvelopeWithKnownPayload(decoded);
  } catch (error) {
    const reason = error instanceof Error ? error.message : String(error);
    throw new Error(
      `Kafka message does not match WP-00 envelope/payload schemas: ${reason}`
    );
  }

  if (envelope.event_type !== TELEMETRY_TOPICS.AUTHORIZED) {
    throw new Error(
      `Kafka message has unsupported event_type: ${String(envelope.event_type)}`
    );
  }
  return envelope as AuthorizedTelemetryEnvelope;
}

/** Decodes and validates a Kafka message, then extracts its payload. */
export function decodeAuthorizedKafkaMessage(
  rawMessage: string
): TelemetryAuthorizedPayload {
  return decodeAuthorizedKafkaEnvelope(rawMessage).payload;
}
