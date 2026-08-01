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

  return {
    async publishAuthorized(payload: TelemetryAuthorizedPayload): Promise<void> {
      if (!connected) {
        await producer.connect();
        connected = true;
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
