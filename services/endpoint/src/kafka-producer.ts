import {
  TELEMETRY_TOPICS,
  type TelemetryAuthorizedPayload,
  type TelemetryRejectedPayload,
} from '@scp/contracts';
import { Kafka } from 'kafkajs';
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
  const kafka = new Kafka({ clientId: 'endpoint', brokers });
  const producer = kafka.producer();
  let connected = false;
  let connectPromise: Promise<void> | undefined;

  async function ensureConnected() {
    if (!connected) {
      if (!connectPromise) {
        connectPromise = producer.connect().then(() => {
          connected = true;
        });
      }

      try {
        await connectPromise;
      } catch (error) {
        connectPromise = undefined;
        throw error;
      }
    }
  }

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

    for (let attempt = 1; attempt <= attempts; attempt += 1) {
      try {
        await ensureConnected();
        await producer.send({
          topic,
          messages: [
            {
              key,
              value: JSON.stringify({
                event_id: eventId,
                event_type: topic,
                event_version: 'v1',
                occurred_at: new Date().toISOString(),
                trace_id: traceId,
                source,
                payload,
              }),
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

    try {
      await ensureConnected();
      await producer.send({
        topic: TELEMETRY_TOPICS.DLQ,
        messages: [
          {
            key,
            value: JSON.stringify({
              event_id: randomUUID(),
              event_type: TELEMETRY_TOPICS.DLQ,
              event_version: 'v1',
              occurred_at: new Date().toISOString(),
              trace_id: traceId,
              source,
              payload: {
                failed_topic: topic,
                reason_code: 'publish_failed',
                reason_message:
                  lastError instanceof Error
                    ? lastError.message
                    : 'Kafka publish failed',
                failed_payload: payload,
              },
            }),
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
        `${payload.sensor_id}:${payload.nonce}`,
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
        `${payload.sensor_id ?? 'unknown'}:${payload.reason_code}`,
        payload,
        traceId
      );
    },
  };
}
