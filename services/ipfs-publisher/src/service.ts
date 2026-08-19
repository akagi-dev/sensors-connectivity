/**
 * Copyright 2026 Robonomics Network
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
import { Consumer, Producer } from '@platformatic/kafka';
import { TELEMETRY_TOPICS } from '@scp/core';
import { create as createKuboRPCClient } from 'kubo-rpc-client';
import type { Server } from 'node:http';
import { decodeAuthorizedKafkaEnvelope } from './authorized-message.js';
import type { IpfsBatchClient } from './batch-publisher.js';
import {
  InMemoryBatchDedupStore,
  processSealedAuthorizedBatch,
} from './batch-processor.js';
import {
  AuthorizedBatchAccumulator,
  type SealedAuthorizedBatch,
} from './batching.js';
import { loadIpfsPublisherConfig, type IpfsPublisherConfig } from './config.js';
import { logError, logInfo, logWarn } from './logger.js';
import {
  createIpfsPublisherMetrics,
  startIpfsPublisherHealthServer,
  type IpfsPublisherMetrics,
} from './metrics.js';
import {
  createKafkaIpfsResultPublisher,
  type IpfsResultPublisher,
} from './result-publisher.js';

interface ManualKafkaMessage {
  topic: string;
  partition: number;
  offset: bigint;
  value: Buffer | null;
  commit(): Promise<void>;
}

interface KafkaMessageStream extends AsyncIterable<ManualKafkaMessage> {
  close(force?: boolean): Promise<void>;
}

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

export function createIpfsPublisherService(
  config: IpfsPublisherConfig = loadIpfsPublisherConfig(),
  dependencies: IpfsPublisherDependencies = {}
): IpfsPublisherService {
  const consumer =
    dependencies.createConsumer?.() ??
    new Consumer({
      groupId: config.consumerGroupId,
      clientId: 'ipfs-publisher',
      bootstrapBrokers: config.kafkaBrokers,
    });
  const ipfs =
    dependencies.ipfs ??
    (createKuboRPCClient(config.ipfsApiUrl) as unknown as IpfsBatchClient);
  const resultPublisher =
    dependencies.resultPublisher ??
    createKafkaIpfsResultPublisher(
      new Producer({
        clientId: 'ipfs-publisher',
        bootstrapBrokers: config.kafkaBrokers,
        idempotent: true,
        acks: -1,
      })
    );
  const createHealthServer =
    dependencies.createHealthServer ?? startIpfsPublisherHealthServer;
  const dedupStore = new InMemoryBatchDedupStore();
  const metrics = createIpfsPublisherMetrics();
  const batches = new AuthorizedBatchAccumulator({
    maxEvents: config.batchMaxEvents,
    maxWaitMs: config.batchMaxWaitMs,
  });
  const pending = new Set<Promise<void>>();
  let started = false;
  let runPromise: Promise<void> | undefined;
  let flushTimer: NodeJS.Timeout | undefined;
  let healthServer: Server | undefined;
  let consumerStream: KafkaMessageStream | undefined;

  const processBatch = (batch: SealedAuthorizedBatch): Promise<void> => {
    const commitOffset = batch.entries.at(-1)?.commitOffset;
    if (!commitOffset) {
      return Promise.reject(
        new Error(`Kafka batch ${batch.batchId} has no commit callback`)
      );
    }
    const promise = processSealedAuthorizedBatch(batch, {
      ipfs,
      dedupStore,
      resultPublisher,
      ipfsRetry: {
        maxAttempts: config.maxRetries,
        backoffMs: config.retryBackoffMs,
      },
      commitOffset: async () => {
        await commitOffset();
        logInfo('Kafka batch offset committed', {
          batchId: batch.batchId,
          topic: batch.topic,
          partition: batch.partition,
          lastOffset: batch.lastOffset,
        });
      },
      metrics,
      ...(dependencies.now ? { now: dependencies.now } : {}),
    }).then((status) => {
      logInfo('authorized Kafka batch handled', {
        batchId: batch.batchId,
        topic: batch.topic,
        partition: batch.partition,
        firstOffset: batch.firstOffset,
        lastOffset: batch.lastOffset,
        eventCount: batch.entries.length,
        status,
      });
    });
    pending.add(promise);
    void promise.finally(() => pending.delete(promise)).catch(() => undefined);
    return promise;
  };

  const flushExpiredBatches = async (): Promise<void> => {
    await Promise.all(batches.flushExpired().map(processBatch));
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
        consumerStream = (await consumer.consume({
          topics: [TELEMETRY_TOPICS.AUTHORIZED],
          autocommit: false,
        })) as unknown as KafkaMessageStream;
        healthServer = createHealthServer(metrics, config.healthPort);
        runPromise = (async () => {
          for await (const message of consumerStream!) {
            if (!message.value) {
              logWarn(
                'received null Kafka message value; committing and skipping',
                {
                  topic: message.topic,
                  partition: message.partition,
                  offset: message.offset,
                }
              );
              await message.commit();
              continue;
            }
            const envelope = decodeAuthorizedKafkaEnvelope(message.value);
            const sealedBatch = batches.add({
              topic: message.topic,
              partition: message.partition,
              offset: message.offset.toString(),
              envelope,
              commitOffset: () => message.commit(),
            });
            if (sealedBatch) await processBatch(sealedBatch);
            logInfo('authorized Kafka message buffered', {
              topic: message.topic,
              partition: message.partition,
              offset: message.offset,
              batchId: sealedBatch?.batchId,
              bufferedEventCount: batches.getBufferedEventCount(),
              status: sealedBatch ? 'sealed' : 'buffered',
            });
          }
        })();
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
          healthPort: config.healthPort,
        });
      } catch (error) {
        started = false;
        clearFlushTimer();
        await closeHttpServerIfPresent();
        await consumerStream?.close(true).catch(() => undefined);
        try {
          await consumer.close(true);
        } catch {
          // Best-effort rollback; preserve the original startup error.
        }
        await resultPublisher.disconnect().catch(() => undefined);
        consumerStream = undefined;
        runPromise = undefined;
        logError('Kafka consumer failed to start', error);
        throw error;
      }
    },

    async stop(): Promise<void> {
      if (!started) return;
      started = false;
      clearFlushTimer();
      let firstError: unknown;
      const runCleanup = async (
        operation: () => Promise<unknown>
      ): Promise<void> => {
        try {
          await operation();
        } catch (error) {
          firstError ??= error;
        }
      };

      await runCleanup(
        async () => consumerStream?.close() ?? Promise.resolve()
      );
      await runCleanup(async () => runPromise ?? Promise.resolve());
      await runCleanup(async () =>
        Promise.all(batches.flushAll().map(processBatch))
      );
      await runCleanup(async () => Promise.all(pending));
      await runCleanup(closeHttpServerIfPresent);
      await runCleanup(async () => consumer.close(true));
      await runCleanup(async () => resultPublisher.disconnect());

      consumerStream = undefined;
      runPromise = undefined;
      logInfo('Kafka consumer stopped', {
        batchCount: metrics.batchCount,
        pinCount: metrics.pinCount,
        pinLatencyMs: metrics.pinLatencyMs,
        retryCount: metrics.retryCount,
        dlqCount: metrics.dlqCount,
      });
      if (firstError) throw firstError;
    },

    isStarted(): boolean {
      return started;
    },

    getMetrics(): Readonly<IpfsPublisherMetrics> {
      return metrics;
    },
  };

  function clearFlushTimer(): void {
    if (flushTimer) {
      clearInterval(flushTimer);
      flushTimer = undefined;
    }
  }

  async function closeHttpServerIfPresent(): Promise<void> {
    if (!healthServer) return;
    const server = healthServer;
    healthServer = undefined;
    await closeHttpServer(server);
  }
}

async function closeHttpServer(server: Server): Promise<void> {
  await new Promise<void>((resolve, reject) => {
    server.close((error) => (error ? reject(error) : resolve()));
  });
}

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
