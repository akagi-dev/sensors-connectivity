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
    key: string,
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
            key,
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
    key: string,
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
            key,
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
        Buffer.from(payload.sensorId).toString('hex'),
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
        payload.sensorId
          ? Buffer.from(payload.sensorId).toString('hex')
          : 'unknown',
        payload,
        traceId
      );
    },
  };
}
