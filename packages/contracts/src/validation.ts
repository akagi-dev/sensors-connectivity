import { z } from 'zod';
import { envelopeSchema } from './envelope.js';
import { payloadSchemasByEventType, type EventTypeWithSchema } from './events.js';

export function parseEnvelope(input: unknown) {
  return envelopeSchema.parse(input);
}

export function parsePayloadForEventType<T extends EventTypeWithSchema>(
  eventType: T,
  payload: unknown
) {
  return payloadSchemasByEventType[eventType].parse(payload);
}

export function parseEnvelopeWithKnownPayload(input: unknown) {
  const envelope = parseEnvelope(input);
  const schema = payloadSchemasByEventType[
    envelope.event_type as EventTypeWithSchema
  ];

  if (!schema) {
    throw new z.ZodError([
      {
        code: z.ZodIssueCode.custom,
        path: ['event_type'],
        message: `Unknown event_type: ${envelope.event_type}`
      }
    ]);
  }

  return {
    ...envelope,
    payload: schema.parse(envelope.payload)
  };
}
