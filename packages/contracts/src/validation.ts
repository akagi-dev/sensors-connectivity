import { z } from 'zod';
import { envelopeSchema } from './envelope.js';
import { payloadSchemasByEventType, type EventTypeWithSchema } from './events.js';

export function parseEnvelope(input: unknown) {
  return envelopeSchema.parse(input);
}

export function validateEnvelope(input: unknown) {
  return envelopeSchema.safeParse(input);
}

export function parsePayloadForEventType<T extends EventTypeWithSchema>(
  eventType: T,
  payload: unknown
) {
  return payloadSchemasByEventType[eventType].parse(payload);
}

export function validatePayloadForEventType<T extends EventTypeWithSchema>(
  eventType: T,
  payload: unknown
) {
  return payloadSchemasByEventType[eventType].safeParse(payload);
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

export function validateEnvelopeWithKnownPayload(input: unknown) {
  const envelopeResult = validateEnvelope(input);
  if (!envelopeResult.success) {
    return envelopeResult;
  }

  const schema = payloadSchemasByEventType[
    envelopeResult.data.event_type as EventTypeWithSchema
  ];

  if (!schema) {
    return {
      success: false as const,
      error: new z.ZodError([
        {
          code: z.ZodIssueCode.custom,
          path: ['event_type'],
          message: `Unknown event_type: ${envelopeResult.data.event_type}`
        }
      ])
    };
  }

  const payloadResult = schema.safeParse(envelopeResult.data.payload);
  if (!payloadResult.success) {
    return payloadResult;
  }

  return {
    success: true as const,
    data: {
      ...envelopeResult.data,
      payload: payloadResult.data
    }
  };
}
