import { InMemoryDedupStore, TELEMETRY_TOPICS } from '@scp/contracts';
import { Kafka, type Consumer } from 'kafkajs';
import { create as createKuboRPCClient } from 'kubo-rpc-client';
import type { Server } from 'node:http';
import { decodeAuthorizedKafkaEnvelope } from './authorized-message.js';
import type { IpfsBatchClient } from './batch-publisher.js';
import { processSealedAuthorizedBatch } from './batch-processor.js';
import {
  AuthorizedBatchAccumulator,
  type SealedAuthorizedBatch
} from './batching.js';
import { loadIpfsPublisherConfig, type IpfsPublisherConfig } from './config.js';
import { nextKafkaOffset } from './kafka-offset.js';
import { logError, logInfo } from './logger.js';
import {
  createIpfsPublisherMetrics,
  startIpfsPublisherHealthServer,
  type IpfsPublisherMetrics
} from './metrics.js';
import {
  createKafkaIpfsResultPublisher,
  type IpfsResultPublisher
} from './result-publisher.js';

export interface IpfsPublisherDependencies {
  ipfs?: IpfsBatchClient;
  createConsumer?: () => Consumer;
  resultPublisher?: IpfsResultPublisher;
  createHealthServer?: (metrics: IpfsPublisherMetrics, port: number) => Server;
  now?: () => number;
}

export interface IpfsPublisherService {
  start(): Promise<void>;
  stop(): Promise<void>;
  isStarted(): boolean;
  getMetrics(): Readonly<IpfsPublisherMetrics>;
}

/** Creates a lifecycle-managed Kafka consumer for telemetry.authorized.v1. */
export function createIpfsPublisherService(
  config: IpfsPublisherConfig = loadIpfsPublisherConfig(),
  dependencies: IpfsPublisherDependencies = {}
): IpfsPublisherService {
  const kafka = new Kafka({
    clientId: 'ipfs-publisher',
    brokers: config.kafkaBrokers
  });
  const consumer =
    dependencies.createConsumer?.() ??
    kafka.consumer({ groupId: config.consumerGroupId });
  const ipfs =
    dependencies.ipfs ??
    (createKuboRPCClient(config.ipfsApiUrl) as unknown as IpfsBatchClient);
  const resultPublisher =
    dependencies.resultPublisher ??
    createKafkaIpfsResultPublisher(kafka.producer());
  const createHealthServer =
    dependencies.createHealthServer ?? startIpfsPublisherHealthServer;
  const dedupStore = new InMemoryDedupStore();
  const metrics = createIpfsPublisherMetrics();
  const batchAccumulator = new AuthorizedBatchAccumulator({
    maxEvents: config.batchMaxEvents,
    maxWaitMs: config.batchMaxWaitMs
  });
  const pendingBatchProcessing = new Set<Promise<void>>();
  let started = false;
  let runPromise: Promise<void> | undefined;
  let flushTimer: NodeJS.Timeout | undefined;
  let healthServer: Server | undefined;

  /**
   * Starts processing a sealed batch and tracks it for graceful shutdown.
   */
  const processBatch = (batch: SealedAuthorizedBatch): Promise<void> => {
    const promise = processSealedAuthorizedBatch(batch, {
      ipfs,
      dedupStore,
      resultPublisher,
      ipfsRetry: {
        maxAttempts: config.maxRetries,
        backoffMs: config.retryBackoffMs
      },
      commitOffset: async () => {
        const offset = nextKafkaOffset(batch.lastOffset);
        await consumer.commitOffsets([
          {
            topic: batch.topic,
            partition: batch.partition,
            offset
          }
        ]);
        logInfo('Kafka batch offset committed', {
          batchId: batch.batchId,
          topic: batch.topic,
          partition: batch.partition,
          lastOffset: batch.lastOffset,
          committedOffset: offset
        });
      },
      metrics,
      ...(dependencies.now ? { now: dependencies.now } : {})
    }).then((status) => {
      logInfo('authorized Kafka batch handled', {
        batchId: batch.batchId,
        topic: batch.topic,
        partition: batch.partition,
        firstOffset: batch.firstOffset,
        lastOffset: batch.lastOffset,
        eventCount: batch.entries.length,
        status
      });
      if (status === 'retried' || status === 'dlq') {
        throw new Error(
          `Kafka batch ${batch.batchId} was not committed after status ${status}`
        );
      }
    });
    pendingBatchProcessing.add(promise);
    void promise.then(
      () => pendingBatchProcessing.delete(promise),
      () => pendingBatchProcessing.delete(promise)
    );
    return promise;
  };

  /** Seals and processes all batches whose maximum wait time has elapsed. */
  const flushExpiredBatches = async (): Promise<void> => {
    await Promise.all(batchAccumulator.flushExpired().map(processBatch));
  };

  return {
    async start(): Promise<void> {
      if (started) {
        logInfo('start skipped; service already started');
        return;
      }

      started = true;
      try {
        await resultPublisher.connect();
        await consumer.connect();
        await consumer.subscribe({
          topic: TELEMETRY_TOPICS.AUTHORIZED,
          fromBeginning: false
        });
        healthServer = createHealthServer(metrics, config.healthPort);
        runPromise = consumer.run({
          autoCommit: false,
          eachMessage: async ({ topic, partition, message }) => {
            const rawMessage = message.value?.toString('utf8');
            if (rawMessage === undefined) {
              throw new Error('Kafka message value is empty');
            }

            const envelope = decodeAuthorizedKafkaEnvelope(rawMessage);
            const sealedBatch = batchAccumulator.add({
              topic,
              partition,
              offset: message.offset,
              envelope
            });
            if (sealedBatch) {
              await processBatch(sealedBatch);
            }
            logInfo('authorized Kafka message buffered', {
              topic,
              partition,
              offset: message.offset,
              batchId: sealedBatch?.batchId,
              bufferedEventCount: batchAccumulator.getBufferedEventCount(),
              status: sealedBatch ? 'sealed' : 'buffered'
            });
          }
        });
        void runPromise.catch((error: unknown) => {
          logError('Kafka consumer loop failed', error);
        });
        flushTimer = setInterval(
          () => {
            void flushExpiredBatches().catch((error: unknown) => {
              logError('failed to flush expired authorized batches', error);
            });
          },
          Math.min(config.batchMaxWaitMs, 1_000)
        );
        flushTimer.unref();
        logInfo('Kafka consumer started', {
          topic: TELEMETRY_TOPICS.AUTHORIZED,
          consumerGroupId: config.consumerGroupId,
          kafkaBrokers: config.kafkaBrokers,
          batchMaxEvents: config.batchMaxEvents,
          batchMaxWaitMs: config.batchMaxWaitMs,
          maxRetries: config.maxRetries,
          retryBackoffMs: config.retryBackoffMs,
          ipfsApiUrl: config.ipfsApiUrl,
          healthPort: config.healthPort
        });
      } catch (error) {
        started = false;
        if (flushTimer) {
          clearInterval(flushTimer);
          flushTimer = undefined;
        }
        runPromise = undefined;
        if (healthServer) {
          await closeHttpServer(healthServer).catch(() => undefined);
          healthServer = undefined;
        }
        await consumer.disconnect().catch(() => undefined);
        await resultPublisher.disconnect().catch(() => undefined);
        logError('Kafka consumer failed to start', error);
        throw error;
      }
    },
    async stop(): Promise<void> {
      if (!started) {
        return;
      }

      started = false;
      if (flushTimer) {
        clearInterval(flushTimer);
        flushTimer = undefined;
      }
      await consumer.stop();
      await runPromise?.catch(() => undefined);
      await Promise.all(batchAccumulator.flushAll().map(processBatch));
      await Promise.all(pendingBatchProcessing);
      if (healthServer) {
        await closeHttpServer(healthServer);
        healthServer = undefined;
      }
      await consumer.disconnect();
      await resultPublisher.disconnect();
      runPromise = undefined;
      logInfo('Kafka consumer stopped', {
        batchCount: metrics.batchCount,
        pinCount: metrics.pinCount,
        pinLatencyMs: metrics.pinLatencyMs,
        retryCount: metrics.retryCount,
        dlqCount: metrics.dlqCount
      });
    },
    isStarted(): boolean {
      return started;
    },
    getMetrics(): Readonly<IpfsPublisherMetrics> {
      return metrics;
    }
  };
}

/** Closes an HTTP server and waits for its socket to be released. */
async function closeHttpServer(server: Server): Promise<void> {
  await new Promise<void>((resolve, reject) => {
    server.close((error) => {
      if (error) {
        reject(error);
        return;
      }
      resolve();
    });
  });
}

/** Creates and starts the IPFS publisher service with default settings. */
export async function startIpfsPublisher(
  dependencies: IpfsPublisherDependencies = {}
): Promise<IpfsPublisherService> {
  const service = createIpfsPublisherService(
    loadIpfsPublisherConfig(),
    dependencies
  );
  await service.start();
  return service;
}
