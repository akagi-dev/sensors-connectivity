import { ApiPromise, WsProvider } from '@polkadot/api';
import {
  InMemoryDedupStore,
  runConsumerProcessingRule,
  TELEMETRY_TOPICS,
  type RetryDlqPublisher,
  type TelemetryIpfsPublishedPayload,
} from '@scp/contracts';
import { Kafka } from 'kafkajs';
import { randomUUID } from 'node:crypto';
import { fileURLToPath } from 'node:url';
import pino from 'pino';
import { loadBlockchainAnchorConfig } from './config.js';

const logger = pino({
  name: 'blockchain-anchor',
  level:
    process.env.BLOCKCHAIN_ANCHOR_LOG_LEVEL ?? process.env.LOG_LEVEL ?? 'info',
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

export async function startBlockchainAnchor(): Promise<void> {
  const config = loadBlockchainAnchorConfig();

  // TODO: wire kafkajs consumer group for telemetry.ipfs.result.v1.
  const kafka = new Kafka({
    clientId: 'blockchain-anchor',
    brokers: ['localhost:9092'],
  });
  void kafka;

  // TODO: wire @polkadot/api extrinsic submission and receipt handling.
  const _provider = new WsProvider(config.substrateWsUrl, 0);
  void _provider;
  void ApiPromise;

  const retryDlqPublisher: RetryDlqPublisher<TelemetryIpfsPublishedPayload> = {
    async publishRetry(event, reason) {
      logWarn('retry stub', {
        topic: TELEMETRY_TOPICS.RETRY,
        cid: event.cid,
        reason,
      });
    },
    async publishDlq(event, reason) {
      logWarn('dlq stub', {
        topic: TELEMETRY_TOPICS.DLQ,
        cid: event.cid,
        reason,
      });
    },
  };

  await runConsumerProcessingRule(
    { cid: 'bafybeidevstubcid', event_count: 1 },
    {
      dedup: {
        keyType: 'cid',
        getKeyValue: (event) => event.cid,
        store: new InMemoryDedupStore(),
      },
      performExternalAction: async (event) => {
        // TODO: submit CID-only anchor extrinsic to substrate-based Robonomics chain.
        logInfo('submit CID stub', { cid: event.cid });
      },
      waitForConfirmation: async () => {
        // TODO: wait for submission confirmation; finality/reorg handling is deferred.
      },
      emitResultEvent: async (event) => ({
        event_id: randomUUID(),
        event_type: TELEMETRY_TOPICS.BLOCKCHAIN_RESULT,
        event_version: 'v1',
        occurred_at: new Date().toISOString(),
        source: 'blockchain-anchor',
        payload: {
          target: config.target,
          status: 'submitted' as const,
          cid: event.cid,
          tx_hash: '0xstub',
        },
      }),
      publishResultEvent: async (result) => {
        logInfo('result stub', { result });
      },
      commitOffset: async () => {
        logInfo('commit offset stub');
      },
      retryDlqPublisher,
    }
  );

  logInfo('started in phase-1 CID-only stub mode', {
    substrateWsUrl: config.substrateWsUrl,
    target: config.target,
  });
}

const isDirectRun = process.argv[1] === fileURLToPath(import.meta.url);
if (isDirectRun) {
  startBlockchainAnchor().catch((error: unknown) => {
    logError('failed to start', error);
    process.exitCode = 1;
  });
}
