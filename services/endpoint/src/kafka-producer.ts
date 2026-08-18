import {
  TELEMETRY_TOPICS,
  createEnvelope,
  serializeEnvelope,
  serializePayloadForEventType,
  REJECTION_CODES,
  type TelemetryAuthorizedPayload,
  type TelemetryRejectedPayload,
} from '@scp/contracts';
import { Producer } from '@platformatic/kafka';
import { randomUUID } from 'node:crypto';

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

export function createEndpointEventProducer(
  brokers: string[],
  source = 'endpoint',
  maxAttempts = 3,
  retryBackoffMs = 100
): EndpointEventProducer {
  const producer = new Producer({
    clientId: 'endpoint',
    bootstrapBrokers: brokers,
  });

  async function wait(ms: number) {
    await new Promise<void>((resolve) => {
      setTimeout(resolve, ms);
    });
  }

  async function sendEnvelope(
    topic: string,
    key: string,
    payload: TelemetryAuthorizedPayload | TelemetryRejectedPayload,
    traceId?: string
  ): Promise<string> {
    const eventId = randomUUID();
    let lastError: unknown;
    const attempts = Math.max(maxAttempts, 1);

    // Serialize payload to protobuf binary
    const payloadBytes = serializePayloadForEventType(topic, payload);

    // Create and serialize envelope
    const envelope = createEnvelope({
      eventId,
      eventType: topic,
      eventVersion: 'v1',
      occurredAt: new Date().toISOString(),
      source,
      payload: payloadBytes,
      traceId,
    });

    const envelopeBytes = serializeEnvelope(envelope);

    for (let attempt = 1; attempt <= attempts; attempt += 1) {
      try {
        await producer.send({
          messages: [
            {
              topic,
              key,
              value: Buffer.from(envelopeBytes),
            },
          ],
        });
        return eventId;
      } catch (error) {
        lastError = error;

        if (attempt < attempts) {
          await wait(Math.max(retryBackoffMs, 0));
        }
      }
    }

    // DLQ publish failure - create a simple rejected payload envelope
    try {
      const dlqPayload = {
        sensorId: new Uint8Array(),
        reasonCode: REJECTION_CODES.KAFKA_PUBLISH_FAILED,
        reasonMessage:
          lastError instanceof Error
            ? lastError.message
            : 'Kafka publish failed',
      };

      const dlqPayloadBytes = serializePayloadForEventType(
        TELEMETRY_TOPICS.REJECTED,
        dlqPayload
      );

      const dlqEnvelope = createEnvelope({
        eventId: randomUUID(),
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
            value: Buffer.from(serializeEnvelope(dlqEnvelope)),
          },
        ],
      });
    } catch {
      // Best effort DLQ publish; main error is re-thrown below.
    }

    throw lastError;
  }

  return {
    async publishAuthorized(
      payload: TelemetryAuthorizedPayload,
      traceId?: string
    ): Promise<string> {
      return sendEnvelope(
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
      return sendEnvelope(
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
