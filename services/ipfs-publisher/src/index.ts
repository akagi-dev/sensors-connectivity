import {
  InMemoryDedupStore,
  runConsumerProcessingRule,
  TELEMETRY_TOPICS,
  type RetryDlqPublisher,
  type TelemetryAuthorizedPayload,
} from '@scp/contracts';
import { Kafka } from 'kafkajs';
import { randomUUID } from 'node:crypto';
import { fileURLToPath } from 'node:url';
import pino from 'pino';
import { loadIpfsPublisherConfig } from './config.js';

interface AuthorizedBatch {
  batch_id: string;
  events: TelemetryAuthorizedPayload[];
}

const logger = pino({
  name: 'ipfs-publisher',
  level:
    process.env.IPFS_PUBLISHER_LOG_LEVEL ?? process.env.LOG_LEVEL ?? 'info',
});

function logInfo(message: string, context?: Record<string, unknown>): void {
  logger.info(context ?? {}, message);
}

function logWarn(message: string, context?: Record<string, unknown>): void {
  logger.warn(context ?? {}, message);
}

function logError(
  message: string,
  error: unknown,
  context?: Record<string, unknown>
): void {
  logger.error(
    {
      ...(context ?? {}),
      error: error instanceof Error ? error.message : String(error),
    },
    message
  );
}

export async function startIpfsPublisher(): Promise<void> {
  const config = loadIpfsPublisherConfig();
  const kafka = new Kafka({
    clientId: 'ipfs-publisher',
    brokers: ['localhost:9092'],
  });
  void kafka;

  const batch: AuthorizedBatch = {
    batch_id: `batch-${Date.now()}`,
    events: [
      {
        sensor_id: 'sensor-dev-1',
        timestamp: Date.now(),
        nonce: 'nonce-dev',
        message: Buffer.from(JSON.stringify({ temp: 22 })).toString('base64'),
        signature: '0x00',
      },
    ],
  };

  const retryDlqPublisher: RetryDlqPublisher<AuthorizedBatch> = {
    async publishRetry(event, reason) {
      logWarn('retry stub', {
        topic: TELEMETRY_TOPICS.RETRY,
        batchId: event.batch_id,
        reason,
      });
    },
    async publishDlq(event, reason) {
      logWarn('dlq stub', {
        topic: TELEMETRY_TOPICS.DLQ,
        batchId: event.batch_id,
        reason,
      });
    },
  };

  await runConsumerProcessingRule(batch, {
    dedup: {
      keyType: 'batch_id',
      getKeyValue: (event) => event.batch_id,
      store: new InMemoryDedupStore(),
    },
    performExternalAction: async () => {
      // TODO: deterministic batching + serialize object/CAR.
      logInfo('build and publish IPFS object/CAR stub');
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
        event_count: event.events.length,
      },
    }),
    publishResultEvent: async (result) => {
      logInfo('result stub', { result });
    },
    commitOffset: async () => {
      logInfo('commit offset stub');
    },
    retryDlqPublisher,
  });

  logInfo('started in stub mode', { ipfsApiUrl: config.ipfsApiUrl });
}

const isDirectRun = process.argv[1] === fileURLToPath(import.meta.url);
if (isDirectRun) {
  startIpfsPublisher().catch((error: unknown) => {
    logError('failed to start', error);
    process.exitCode = 1;
  });
}
