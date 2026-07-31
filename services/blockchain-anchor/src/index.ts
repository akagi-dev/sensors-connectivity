import { ApiPromise, WsProvider } from '@polkadot/api';
import {
  InMemoryDedupStore,
  runConsumerProcessingRule,
  TELEMETRY_TOPICS,
  type RetryDlqPublisher,
  type TelemetryIpfsPublishedPayload
} from '@scp/contracts';
import { Kafka } from 'kafkajs';
import { randomUUID } from 'node:crypto';
import { fileURLToPath } from 'node:url';
import { loadBlockchainAnchorConfig } from './config.js';

export async function startBlockchainAnchor(): Promise<void> {
  const config = loadBlockchainAnchorConfig();

  // TODO: wire kafkajs consumer group for telemetry.ipfs.published.v1.
  const kafka = new Kafka({ clientId: 'blockchain-anchor', brokers: ['localhost:9092'] });
  void kafka;

  // TODO: wire @polkadot/api extrinsic submission and receipt handling.
  const _provider = new WsProvider(config.substrateWsUrl, 0);
  void _provider;
  void ApiPromise;

  const retryDlqPublisher: RetryDlqPublisher<TelemetryIpfsPublishedPayload> = {
    async publishRetry(event, reason) {
      console.log('[blockchain-anchor] retry stub', TELEMETRY_TOPICS.RETRY, event.cid, reason);
    },
    async publishDlq(event, reason) {
      console.log('[blockchain-anchor] dlq stub', TELEMETRY_TOPICS.DLQ, event.cid, reason);
    }
  };

  await runConsumerProcessingRule(
    { cid: 'bafybeidevstubcid', event_count: 1 },
    {
      dedup: {
        keyType: 'cid',
        getKeyValue: (event) => event.cid,
        store: new InMemoryDedupStore()
      },
      performExternalAction: async (event) => {
        // TODO: submit CID-only anchor extrinsic to substrate-based Robonomics chain.
        console.log('[blockchain-anchor] submit CID stub', event.cid);
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
          tx_hash: '0xstub'
        }
      }),
      publishResultEvent: async (result) => {
        console.log('[blockchain-anchor] result stub', result);
      },
      commitOffset: async () => {
        console.log('[blockchain-anchor] commit offset stub');
      },
      retryDlqPublisher
    }
  );

  console.log('[blockchain-anchor] started in phase-1 CID-only stub mode', {
    substrateWsUrl: config.substrateWsUrl,
    target: config.target
  });
}

const isDirectRun = process.argv[1] === fileURLToPath(import.meta.url);
if (isDirectRun) {
  startBlockchainAnchor().catch((error: unknown) => {
    console.error('[blockchain-anchor] failed to start', error);
    process.exitCode = 1;
  });
}
