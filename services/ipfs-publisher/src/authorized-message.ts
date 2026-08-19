/**
 * Copyright 2026 Robonomics Network
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
import { fromBinary } from '@bufbuild/protobuf';
import {
  EnvelopeSchema,
  TELEMETRY_TOPICS,
  TelemetryAuthorizedPayloadSchema,
  type TelemetryAuthorizedPayload,
} from '@scp/core';
import type { AuthorizedTelemetryEnvelope } from './batching.js';

/**
 * Decodes a binary protobuf Kafka message and validates that it contains an
 * authorized telemetry envelope.
 */
export function decodeAuthorizedKafkaEnvelope(
  rawMessage: Uint8Array
): AuthorizedTelemetryEnvelope {
  let envelope;
  try {
    envelope = fromBinary(EnvelopeSchema, rawMessage);
  } catch (error) {
    const reason = error instanceof Error ? error.message : String(error);
    throw new Error(
      `Kafka message is not a valid protobuf envelope: ${reason}`,
      { cause: error }
    );
  }

  if (envelope.eventType !== TELEMETRY_TOPICS.AUTHORIZED) {
    throw new Error(
      `Kafka message has unsupported event_type: ${envelope.eventType}`
    );
  }
  if (envelope.eventId.length === 0) {
    throw new Error('Kafka message has an empty event_id');
  }

  let payload: TelemetryAuthorizedPayload;
  try {
    payload = fromBinary(TelemetryAuthorizedPayloadSchema, envelope.payload);
  } catch (error) {
    const reason = error instanceof Error ? error.message : String(error);
    throw new Error(
      `Kafka message has an invalid authorized protobuf payload: ${reason}`,
      { cause: error }
    );
  }

  if (payload.sensorId.length !== 32) {
    throw new Error('Authorized payload sensor_id must contain 32 bytes');
  }
  if (payload.signedEnvelope.length === 0) {
    throw new Error('Authorized payload signed_envelope cannot be empty');
  }

  return {
    eventId: envelope.eventId,
    eventType: TELEMETRY_TOPICS.AUTHORIZED,
    eventVersion: envelope.eventVersion,
    occurredAt: envelope.occurredAt,
    source: envelope.source,
    ...(envelope.traceId ? { traceId: envelope.traceId } : {}),
    payload,
  };
}

/** Decodes and validates a Kafka message, then extracts its payload. */
export function decodeAuthorizedKafkaMessage(
  rawMessage: Uint8Array
): TelemetryAuthorizedPayload {
  return decodeAuthorizedKafkaEnvelope(rawMessage).payload;
}
