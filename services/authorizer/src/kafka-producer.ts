import { TELEMETRY_TOPICS, type TelemetryAuthorizedPayload } from '@scp/contracts';
import { Kafka } from 'kafkajs';

export interface AuthorizerEventProducer {
  publishAuthorized(payload: TelemetryAuthorizedPayload): Promise<void>;
}

export function createAuthorizerEventProducer(brokers: string[]): AuthorizerEventProducer {
  const kafka = new Kafka({ clientId: 'authorizer', brokers });
  void kafka;

  return {
    async publishAuthorized(payload: TelemetryAuthorizedPayload): Promise<void> {
      // TODO: connect producer and wait for Kafka ACK before returning.
      console.log('[authorizer] stub publish', TELEMETRY_TOPICS.AUTHORIZED, payload);
    }
  };
}
