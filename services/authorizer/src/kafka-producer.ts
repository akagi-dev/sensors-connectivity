import { TELEMETRY_TOPICS, type TelemetryAuthorizedPayload } from '@scp/contracts';
import { Kafka } from 'kafkajs';
import { randomUUID } from 'node:crypto';

export interface AuthorizerEventProducer {
  publishAuthorized(payload: TelemetryAuthorizedPayload): Promise<void>;
}

export function createAuthorizerEventProducer(
  brokers: string[],
  source = 'authorizer'
): AuthorizerEventProducer {
  const kafka = new Kafka({ clientId: 'authorizer', brokers });
  const producer = kafka.producer();
  let connected = false;
  let connectPromise: Promise<void> | undefined;

  return {
    async publishAuthorized(payload: TelemetryAuthorizedPayload): Promise<void> {
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

      await producer.send({
        topic: TELEMETRY_TOPICS.AUTHORIZED,
        messages: [
          {
            key: `${payload.sensor_address}:${payload.nonce}`,
            value: JSON.stringify({
              event_id: randomUUID(),
              event_type: TELEMETRY_TOPICS.AUTHORIZED,
              event_version: 'v1',
              occurred_at: new Date().toISOString(),
              source,
              payload
            })
          }
        ]
      });
    }
  };
}
