import { z } from 'zod';
import { rfc3339Schema } from './envelope.js';

export const telemetryAuthorizedPayloadSchema = z
  .object({
    sensor_address: z.string().min(1),
    timestamp: rfc3339Schema,
    nonce: z.string().min(1),
    measurements: z.record(z.unknown()),
    signature: z.string().min(1),
    extensions: z.record(z.unknown()).optional()
  })
  .strict();

export const telemetryRejectedPayloadSchema = z
  .object({
    sensor_address: z.string().min(1).optional(),
    reason_code: z.string().min(1),
    reason_message: z.string().min(1).optional(),
    extensions: z.record(z.unknown()).optional()
  })
  .strict();

export const telemetryIpfsPublishedPayloadSchema = z
  .object({
    cid: z.string().min(1),
    event_count: z.number().int().nonnegative(),
    extensions: z.record(z.unknown()).optional()
  })
  .strict();

export const telemetryBlockchainResultPayloadSchema = z
  .object({
    target: z.string().min(1),
    status: z.enum(['submitted', 'failed']),
    cid: z.string().min(1).optional(),
    tx_hash: z.string().min(1).optional(),
    error_code: z.string().min(1).optional(),
    error_message: z.string().min(1).optional(),
    extensions: z.record(z.unknown()).optional()
  })
  .strict();

export const payloadSchemasByEventType = {
  'telemetry.authorized.v1': telemetryAuthorizedPayloadSchema,
  'telemetry.rejected.v1': telemetryRejectedPayloadSchema,
  'telemetry.ipfs.published.v1': telemetryIpfsPublishedPayloadSchema,
  'telemetry.blockchain.result.v1': telemetryBlockchainResultPayloadSchema
} as const;

export type TelemetryAuthorizedPayload = z.infer<
  typeof telemetryAuthorizedPayloadSchema
>;
export type TelemetryRejectedPayload = z.infer<
  typeof telemetryRejectedPayloadSchema
>;
export type TelemetryIpfsPublishedPayload = z.infer<
  typeof telemetryIpfsPublishedPayloadSchema
>;
export type TelemetryBlockchainResultPayload = z.infer<
  typeof telemetryBlockchainResultPayloadSchema
>;

export type EventTypeWithSchema = keyof typeof payloadSchemasByEventType;
