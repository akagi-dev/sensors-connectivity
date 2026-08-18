import { create, fromBinary, toBinary } from '@bufbuild/protobuf';
import type { Message } from '@bufbuild/protobuf';
import {
  EnvelopeSchema,
  type Envelope,
} from './generated/connectivity/v1/envelope_pb.js';
import {
  TelemetryAuthorizedPayloadSchema,
  TelemetryRejectedPayloadSchema,
  TelemetryIpfsPublishedPayloadSchema,
  TelemetryPubsubResultPayloadSchema,
  TelemetryBlockchainResultPayloadSchema,
  type TelemetryAuthorizedPayload,
  type TelemetryRejectedPayload,
  type TelemetryIpfsPublishedPayload,
  type TelemetryPubsubResultPayload,
  type TelemetryBlockchainResultPayload,
} from './generated/connectivity/v1/payload_pb.js';

export type { Envelope };
export type {
  TelemetryAuthorizedPayload,
  TelemetryRejectedPayload,
  TelemetryIpfsPublishedPayload,
  TelemetryPubsubResultPayload,
  TelemetryBlockchainResultPayload,
};

export {
  EnvelopeSchema,
  TelemetryAuthorizedPayloadSchema,
  TelemetryRejectedPayloadSchema,
  TelemetryIpfsPublishedPayloadSchema,
  TelemetryPubsubResultPayloadSchema,
  TelemetryBlockchainResultPayloadSchema,
};

const PAYLOAD_SCHEMAS = {
  'telemetry.authorized.v1': TelemetryAuthorizedPayloadSchema,
  'telemetry.rejected.v1': TelemetryRejectedPayloadSchema,
  'telemetry.pubsub.result.v1': TelemetryPubsubResultPayloadSchema,
  'telemetry.ipfs.result.v1': TelemetryIpfsPublishedPayloadSchema,
  'telemetry.blockchain.result.v1': TelemetryBlockchainResultPayloadSchema,
} as const;

export type EventTypeWithSchema = keyof typeof PAYLOAD_SCHEMAS;

type PayloadTypeForEvent<T extends EventTypeWithSchema> =
  (typeof PAYLOAD_SCHEMAS)[T] extends { prototype: infer P }
    ? P extends Message
      ? P
      : never
    : never;

/**
 * Parse and validate Envelope from binary protobuf.
 */
export function parseEnvelope(bytes: Uint8Array): Envelope {
  return fromBinary(EnvelopeSchema, bytes);
}

/**
 * Serialize Envelope to binary protobuf.
 */
export function serializeEnvelope(envelope: Envelope): Uint8Array {
  return toBinary(EnvelopeSchema, envelope);
}

/**
 * Validate envelope from binary and return result with success flag.
 */
export function validateEnvelope(
  bytes: Uint8Array
): { success: true; data: Envelope } | { success: false; error: Error } {
  try {
    const envelope = parseEnvelope(bytes);
    return { success: true, data: envelope };
  } catch (error) {
    return {
      success: false,
      error: error instanceof Error ? error : new Error(String(error)),
    };
  }
}

/**
 * Parse payload for a specific event type from binary protobuf.
 */
export function parsePayloadForEventType<T extends EventTypeWithSchema>(
  eventType: T,
  bytes: Uint8Array
): PayloadTypeForEvent<T> {
  const schema = PAYLOAD_SCHEMAS[eventType];
  if (!schema) {
    throw new Error(`Unknown event type: ${eventType}`);
  }
  return fromBinary(schema, bytes) as PayloadTypeForEvent<T>;
}

/**
 * Serialize payload for a specific event type to binary protobuf.
 */
export function serializePayloadForEventType<T extends EventTypeWithSchema>(
  eventType: T,
  payload: PayloadTypeForEvent<T>
): Uint8Array {
  const schema = PAYLOAD_SCHEMAS[eventType];
  if (!schema) {
    throw new Error(`Unknown event type: ${eventType}`);
  }
  return toBinary(schema, payload);
}

export type EnvelopeWithParsedPayload = {
  eventId: string;
  eventType: string;
  eventVersion: string;
  occurredAt: string;
  traceId?: string | undefined;
  source: string;
  payload:
    | TelemetryAuthorizedPayload
    | TelemetryRejectedPayload
    | TelemetryPubsubResultPayload
    | TelemetryIpfsPublishedPayload
    | TelemetryBlockchainResultPayload;
};

/**
 * Parse envelope and its payload together.
 */
export function parseEnvelopeWithKnownPayload(
  bytes: Uint8Array
): EnvelopeWithParsedPayload {
  const envelope = parseEnvelope(bytes);
  const eventType = envelope.eventType as EventTypeWithSchema;

  const schema = PAYLOAD_SCHEMAS[eventType];
  if (!schema) {
    throw new Error(`Unknown event_type: ${envelope.eventType}`);
  }

  const payloadMessage = fromBinary(schema, envelope.payload) as
    | TelemetryAuthorizedPayload
    | TelemetryRejectedPayload
    | TelemetryPubsubResultPayload
    | TelemetryIpfsPublishedPayload
    | TelemetryBlockchainResultPayload;

  return {
    eventId: envelope.eventId,
    eventType: envelope.eventType,
    eventVersion: envelope.eventVersion,
    occurredAt: envelope.occurredAt,
    traceId: envelope.traceId,
    source: envelope.source,
    payload: payloadMessage,
  };
}

/**
 * Validate envelope and payload together, returning success/error result.
 */
export function validateEnvelopeWithKnownPayload(bytes: Uint8Array):
  | {
      success: true;
      data: EnvelopeWithParsedPayload;
    }
  | { success: false; error: Error } {
  try {
    const envelope = parseEnvelope(bytes);
    const eventType = envelope.eventType as EventTypeWithSchema;

    const schema = PAYLOAD_SCHEMAS[eventType];
    if (!schema) {
      return {
        success: false,
        error: new Error(`Unknown event_type: ${envelope.eventType}`),
      };
    }

    const payloadMessage = fromBinary(schema, envelope.payload) as
      | TelemetryAuthorizedPayload
      | TelemetryRejectedPayload
      | TelemetryPubsubResultPayload
      | TelemetryIpfsPublishedPayload
      | TelemetryBlockchainResultPayload;

    return {
      success: true,
      data: {
        eventId: envelope.eventId,
        eventType: envelope.eventType,
        eventVersion: envelope.eventVersion,
        occurredAt: envelope.occurredAt,
        traceId: envelope.traceId,
        source: envelope.source,
        payload: payloadMessage,
      },
    };
  } catch (error) {
    return {
      success: false,
      error: error instanceof Error ? error : new Error(String(error)),
    };
  }
}

/**
 * Create a new Envelope with the given parameters.
 */
export function createEnvelope(params: {
  eventId: string;
  eventType: EventTypeWithSchema;
  eventVersion: string;
  occurredAt: string;
  source: string;
  payload: Uint8Array;
  traceId?: string;
}): Envelope {
  return create(EnvelopeSchema, params);
}
