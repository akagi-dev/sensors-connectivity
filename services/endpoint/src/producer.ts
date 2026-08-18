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
import { randomUUID } from 'node:crypto';
import { create, toBinary } from '@bufbuild/protobuf';
import { Producer } from '@platformatic/kafka';
import {
  EnvelopeSchema,
  TelemetryAuthorizedPayloadSchema,
  TelemetryRejectedPayloadSchema,
  type TelemetryAuthorizedPayload,
  type TelemetryRejectedPayload,
  TELEMETRY_TOPICS,
  REJECTION_CODES,
} from '@scp/core';

/**
 * Endpoint-specific event producer for telemetry.
 */
export interface EndpointEventProducer {
  publishAuthorized(
    payload: TelemetryAuthorizedPayload,
    traceId?: string
  ): Promise<string>;
  publishRejected(
    payload: TelemetryRejectedPayload,
    traceId?: string
  ): Promise<string>;
}

/**
 * Create an endpoint event producer using Kafka's native producer with idempotence.
 */
export function createEndpointEventProducer(
  producer: Producer,
  source = 'endpoint'
): EndpointEventProducer {
  async function publish(
    topic:
      typeof TELEMETRY_TOPICS.AUTHORIZED | typeof TELEMETRY_TOPICS.REJECTED,
    key: Uint8Array,
    payload: TelemetryAuthorizedPayload | TelemetryRejectedPayload,
    traceId?: string
  ): Promise<string> {
    try {
      const payloadBytes = toBinary(
        topic === TELEMETRY_TOPICS.AUTHORIZED
          ? TelemetryAuthorizedPayloadSchema
          : TelemetryRejectedPayloadSchema,
        payload
      );

      const eventId = randomUUID();
      const envelope = create(EnvelopeSchema, {
        eventId,
        eventType: topic,
        eventVersion: 'v1',
        occurredAt: new Date().toISOString(),
        source,
        payload: payloadBytes,
        traceId,
      });

      await producer.send({
        messages: [
          {
            topic,
            key: Buffer.from(key),
            value: Buffer.from(toBinary(EnvelopeSchema, envelope)),
          },
        ],
      });

      return eventId;
    } catch (error) {
      // If all Kafka retries failed, attempt DLQ publish
      await publishToDlq(key, error, traceId);
      throw error;
    }
  }

  async function publishToDlq(
    key: Uint8Array,
    error: unknown,
    traceId?: string
  ): Promise<void> {
    try {
      const dlqPayload = create(TelemetryRejectedPayloadSchema, {
        sensorId: new Uint8Array(),
        reasonCode: REJECTION_CODES.KAFKA_PUBLISH_FAILED,
        reasonMessage:
          error instanceof Error ? error.message : 'Kafka publish failed',
      });

      const dlqPayloadBytes = toBinary(
        TelemetryRejectedPayloadSchema,
        dlqPayload
      );

      const eventId = randomUUID();
      const envelope = create(EnvelopeSchema, {
        eventId,
        eventType: TELEMETRY_TOPICS.DLQ,
        eventVersion: 'v1',
        occurredAt: new Date().toISOString(),
        source,
        payload: dlqPayloadBytes,
        traceId,
      });

      await producer.send({
        messages: [
          {
            topic: TELEMETRY_TOPICS.DLQ,
            key: Buffer.from(key),
            value: Buffer.from(toBinary(EnvelopeSchema, envelope)),
          },
        ],
      });
    } catch {
      // Best effort DLQ publish; main error is re-thrown by caller
    }
  }

  return {
    async publishAuthorized(
      payload: TelemetryAuthorizedPayload,
      traceId?: string
    ): Promise<string> {
      return publish(
        TELEMETRY_TOPICS.AUTHORIZED,
        payload.sensorId,
        payload,
        traceId
      );
    },
    async publishRejected(
      payload: TelemetryRejectedPayload,
      traceId?: string
    ): Promise<string> {
      return publish(
        TELEMETRY_TOPICS.REJECTED,
        payload.sensorId ? payload.sensorId : Buffer.from('unknown'),
        payload,
        traceId
      );
    },
  };
}
