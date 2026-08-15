import { z } from 'zod';
import {
  decodeBase64,
  MAX_NONCE_LENGTH,
  MIN_NONCE_LENGTH,
  SENSOR_ID_LENGTH,
  SIGNATURE_LENGTH,
} from './protobuf.js';

const binaryStringSchema = z
  .string()
  .min(1)
  .refine((value) => {
    try {
      decodeBase64(value);
      return true;
    } catch {
      return false;
    }
  }, 'Expected base64-encoded bytes');

const fixedLengthBinaryStringSchema = (bytes: number, name: string) =>
  binaryStringSchema.refine(
    (value) => decodeBase64(value).length === bytes,
    `${name} must be ${bytes} bytes`
  );

export const telemetryAuthorizedPayloadSchema: z.ZodTypeAny = z
  .object({
    sensor_id: fixedLengthBinaryStringSchema(SENSOR_ID_LENGTH, 'sensor_id'),
    timestamp: z.number().int().nonnegative(),
    nonce: binaryStringSchema.refine((value) => {
      const bytes = decodeBase64(value);
      return (
        bytes.length >= MIN_NONCE_LENGTH && bytes.length <= MAX_NONCE_LENGTH
      );
    }, `nonce must be ${MIN_NONCE_LENGTH}-${MAX_NONCE_LENGTH} bytes`),
    message: binaryStringSchema,
    signature: fixedLengthBinaryStringSchema(SIGNATURE_LENGTH, 'signature'),
    envelope: binaryStringSchema.optional(),
    extensions: z.record(z.unknown()).optional(),
  })
  .strict();

export const telemetryRejectedPayloadSchema: z.ZodTypeAny = z
  .object({
    sensor_id: z.string().min(1).optional(),
    reason_code: z.string().min(1),
    reason_message: z.string().min(1).optional(),
    extensions: z.record(z.unknown()).optional(),
  })
  .strict();

export const telemetryIpfsPublishedPayloadSchema: z.ZodTypeAny = z
  .object({
    cid: z.string().min(1),
    event_count: z.number().int().nonnegative(),
    extensions: z.record(z.unknown()).optional(),
  })
  .strict();

export const telemetryPubsubResultPayloadSchema: z.ZodTypeAny = z
  .object({
    status: z.enum(['submitted', 'failed']),
    pubsub_topic: z.string().min(1),
    sensor_id: z.string().min(1),
    nonce: z.string().min(1),
    error_code: z.string().min(1).optional(),
    error_message: z.string().min(1).optional(),
    extensions: z.record(z.unknown()).optional(),
  })
  .strict();

export const telemetryBlockchainResultPayloadSchema: z.ZodTypeAny = z
  .object({
    target: z.string().min(1),
    status: z.enum(['submitted', 'failed']),
    cid: z.string().min(1).optional(),
    tx_hash: z.string().min(1).optional(),
    error_code: z.string().min(1).optional(),
    error_message: z.string().min(1).optional(),
    extensions: z.record(z.unknown()).optional(),
  })
  .strict();

export const payloadSchemasByEventType: Record<string, z.ZodTypeAny> = {
  'telemetry.authorized.v1': telemetryAuthorizedPayloadSchema,
  'telemetry.rejected.v1': telemetryRejectedPayloadSchema,
  'telemetry.pubsub.result.v1': telemetryPubsubResultPayloadSchema,
  'telemetry.ipfs.result.v1': telemetryIpfsPublishedPayloadSchema,
  'telemetry.blockchain.result.v1': telemetryBlockchainResultPayloadSchema,
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
export type TelemetryPubsubResultPayload = z.infer<
  typeof telemetryPubsubResultPayloadSchema
>;
export type TelemetryBlockchainResultPayload = z.infer<
  typeof telemetryBlockchainResultPayloadSchema
>;

export type EventTypeWithSchema = keyof typeof payloadSchemasByEventType;
