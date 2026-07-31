import {
  InMemoryDedupStore,
  runConsumerProcessingRule,
  TELEMETRY_TOPICS,
  type RetryDlqPublisher,
  type TelemetryAuthorizedPayload
} from '@scp/contracts';
import { Kafka } from 'kafkajs';
import { randomUUID } from 'node:crypto';
import { fileURLToPath } from 'node:url';
import { loadIpfsPublisherConfig } from './config.js';

interface AuthorizedBatch {
  batch_id: string;
  events: TelemetryAuthorizedPayload[];
}

export async function startIpfsPublisher(): Promise<void> {
  const config = loadIpfsPublisherConfig();
  const kafka = new Kafka({ clientId: 'ipfs-publisher', brokers: ['localhost:9092'] });
  void kafka;

  const batch: AuthorizedBatch = {
    batch_id: `batch-${Date.now()}`,
    events: [
      {
        sensor_address: 'sensor-dev-1',
        timestamp: new Date().toISOString(),
        nonce: 'nonce-dev',
        measurements: { temp: 22 },
        signature: '0x00'
      }
    ]
  };

  const retryDlqPublisher: RetryDlqPublisher<AuthorizedBatch> = {
    async publishRetry(event, reason) {
      console.log('[ipfs-publisher] retry stub', TELEMETRY_TOPICS.RETRY, event.batch_id, reason);
    },
    async publishDlq(event, reason) {
      console.log('[ipfs-publisher] dlq stub', TELEMETRY_TOPICS.DLQ, event.batch_id, reason);
    }
  };

  await runConsumerProcessingRule(batch, {
    dedup: {
      keyType: 'batch_id',
      getKeyValue: (event) => event.batch_id,
      store: new InMemoryDedupStore()
    },
    performExternalAction: async () => {
      // TODO: deterministic batching + serialize object/CAR.
      console.log('[ipfs-publisher] build and publish IPFS object/CAR stub');
    },
    waitForConfirmation: async () => {
      // TODO: wait for publish/pin confirmation.
    },
    emitResultEvent: async (event) => ({
      event_id: randomUUID(),
      event_type: TELEMETRY_TOPICS.IPFS_RESULT,
      event_version: 'v1',
      occurred_at: new Date().toISOString(),
      source: 'ipfs-publisher',
      payload: {
        cid: 'bafybeidevstubcid',
        event_count: event.events.length
      }
    }),
    publishResultEvent: async (result) => {
      console.log('[ipfs-publisher] result stub', result);
    },
    commitOffset: async () => {
      console.log('[ipfs-publisher] commit offset stub');
    },
    retryDlqPublisher
  });

  console.log('[ipfs-publisher] started in stub mode', { ipfsApiUrl: config.ipfsApiUrl });
}

const isDirectRun = process.argv[1] === fileURLToPath(import.meta.url);
if (isDirectRun) {
  startIpfsPublisher().catch((error: unknown) => {
    console.error('[ipfs-publisher] failed to start', error);
    process.exitCode = 1;
  });
}
