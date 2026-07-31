import { gossipsub } from '@chainsafe/libp2p-gossipsub';
import {
  InMemoryDedupStore,
  runConsumerProcessingRule,
  TELEMETRY_TOPICS,
  type RetryDlqPublisher,
  type TelemetryAuthorizedPayload
} from '@scp/contracts';
import { Kafka } from 'kafkajs';
import { createLibp2p } from 'libp2p';
import { randomUUID } from 'node:crypto';
import { fileURLToPath } from 'node:url';
import { loadPubsubBroadcasterConfig } from './config.js';

export async function startPubsubBroadcaster(): Promise<void> {
  const config = loadPubsubBroadcasterConfig();
  const kafka = new Kafka({ clientId: 'pubsub-broadcaster', brokers: ['localhost:9092'] });
  void kafka;

  // TODO: create a real libp2p node and connect to peers.
  void gossipsub;
  void createLibp2p;

  const retryDlqPublisher: RetryDlqPublisher<TelemetryAuthorizedPayload> = {
    async publishRetry(event, reason) {
      console.log('[pubsub-broadcaster] retry stub', TELEMETRY_TOPICS.RETRY, event, reason);
    },
    async publishDlq(event, reason) {
      console.log('[pubsub-broadcaster] dlq stub', TELEMETRY_TOPICS.DLQ, event, reason);
    }
  };

  await runConsumerProcessingRule(
    {
      sensor_address: 'sensor-dev-1',
      timestamp: new Date().toISOString(),
      nonce: 'nonce-dev',
      measurements: { temp: 21 },
      signature: '0x00'
    },
    {
      dedup: {
        keyType: 'event_id',
        getKeyValue: (event) => `${event.sensor_address}:${event.nonce}`,
        store: new InMemoryDedupStore()
      },
      performExternalAction: async () => {
        // TODO: publish to GossipSub topic and wait for local publish ack.
        console.log('[pubsub-broadcaster] publish stub to topic', config.pubsubTopic);
      },
      waitForConfirmation: async () => {
        // TODO: wait for publish confirmation policy.
      },
      emitResultEvent: async () => ({
        event_id: randomUUID(),
        event_type: TELEMETRY_TOPICS.PUBSUB_RESULT,
        event_version: 'v1',
        occurred_at: new Date().toISOString(),
        source: 'pubsub-broadcaster',
        payload: { status: 'submitted' }
      }),
      publishResultEvent: async (result) => {
        console.log('[pubsub-broadcaster] result stub', result);
      },
      commitOffset: async () => {
        console.log('[pubsub-broadcaster] commit offset stub');
      },
      retryDlqPublisher
    }
  );

  console.log('[pubsub-broadcaster] started in stub mode');
}

const isDirectRun = process.argv[1] === fileURLToPath(import.meta.url);
if (isDirectRun) {
  startPubsubBroadcaster().catch((error: unknown) => {
    console.error('[pubsub-broadcaster] failed to start', error);
    process.exitCode = 1;
  });
}
