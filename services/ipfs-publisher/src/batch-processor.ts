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
import { TELEMETRY_TOPICS, type TelemetryAuthorizedPayload } from '@scp/core';
import {
  buildDeterministicAuthorizedBatch,
  confirmAuthorizedBatchPin,
  publishAndPinAuthorizedBatch,
  type IpfsBatchClient,
} from './batch-publisher.js';
import {
  deriveAuthorizedBatchId,
  type AuthorizedBatchIdContext,
  type SealedAuthorizedBatch,
} from './batching.js';
import {
  runWithBoundedIpfsRetry,
  type IpfsRetryOptions,
} from './ipfs-retry.js';
import { logInfo, logWarn } from './logger.js';
import type { IpfsPublisherMetrics } from './metrics.js';
import {
  buildIpfsDlqEnvelope,
  buildIpfsResultEnvelope,
  type IpfsDlqReasonCode,
  type IpfsFailureContext,
  type IpfsResultPublisher,
} from './result-publisher.js';

export interface BatchDedupStore {
  has(batchId: string): boolean | Promise<boolean>;
  add(batchId: string): void | Promise<void>;
}

export class InMemoryBatchDedupStore implements BatchDedupStore {
  private readonly batchIds = new Set<string>();

  has(batchId: string): boolean {
    return this.batchIds.has(batchId);
  }

  add(batchId: string): void {
    this.batchIds.add(batchId);
  }
}

export interface AuthorizedPayloadProcessingDependencies {
  ipfs: IpfsBatchClient;
  dedupStore: BatchDedupStore;
  resultPublisher: IpfsResultPublisher;
  ipfsRetry?: Pick<IpfsRetryOptions, 'maxAttempts' | 'backoffMs'>;
  commitOffset: () => Promise<void>;
  metrics?: IpfsPublisherMetrics;
  now?: () => number;
}

export type AuthorizedBatchProcessingStatus =
  'processed' | 'duplicate' | 'retried' | 'dlq';

export async function processAuthorizedPayload(
  payload: TelemetryAuthorizedPayload,
  dependencies: AuthorizedPayloadProcessingDependencies
): Promise<AuthorizedBatchProcessingStatus> {
  return processAuthorizedEvents([payload], dependencies);
}

export async function processSealedAuthorizedBatch(
  batch: SealedAuthorizedBatch,
  dependencies: AuthorizedPayloadProcessingDependencies
): Promise<AuthorizedBatchProcessingStatus> {
  const context: AuthorizedBatchIdContext = {
    topic: batch.topic,
    partition: batch.partition,
    entries: batch.entries.map((entry) => ({
      offset: entry.offset,
      eventId: entry.envelope.eventId,
    })),
  };
  if (deriveAuthorizedBatchId(context) !== batch.batchId) {
    throw new Error('Sealed authorized batch_id does not match its entries');
  }
  return processAuthorizedEvents(
    batch.entries.map((entry) => entry.envelope.payload),
    dependencies,
    context
  );
}

async function processAuthorizedEvents(
  events: TelemetryAuthorizedPayload[],
  dependencies: AuthorizedPayloadProcessingDependencies,
  batchContext?: AuthorizedBatchIdContext
): Promise<AuthorizedBatchProcessingStatus> {
  if (dependencies.metrics) dependencies.metrics.batchCount += 1;
  const deterministicBatch = buildDeterministicAuthorizedBatch(
    events,
    batchContext
  );
  const batchId = deterministicBatch.batchId;

  if (await dependencies.dedupStore.has(batchId)) {
    await dependencies.commitOffset();
    return 'duplicate';
  }

  const retryPolicy = dependencies.ipfsRetry ?? {
    maxAttempts: 1,
    backoffMs: 0,
  };
  const now = dependencies.now ?? Date.now;
  let publishedCid: string | undefined;
  let pinStartedAt: number;
  let failureState: {
    reasonCode: IpfsDlqReasonCode;
    attempt: number;
    maxAttempts: number;
  } = {
    reasonCode: 'ipfs_publish_or_pin_failed',
    attempt: 1,
    maxAttempts: retryPolicy.maxAttempts,
  };

  try {
    pinStartedAt = now();
    publishedCid = await runWithBoundedIpfsRetry(
      () =>
        publishAndPinAuthorizedBatch(
          dependencies.ipfs,
          deterministicBatch.bytes
        ),
      {
        ...retryPolicy,
        onRetry: (error, attempt) => {
          if (dependencies.metrics) dependencies.metrics.retryCount += 1;
          failureState.attempt = attempt + 1;
          logWarn('retrying transient IPFS publication failure', {
            batchId,
            attempt,
            maxAttempts: retryPolicy.maxAttempts,
            retryBackoffMs: retryPolicy.backoffMs,
            error: errorMessage(error),
          });
        },
      }
    );
    logInfo('built and published deterministic IPFS object', {
      batchId,
      cid: publishedCid,
      eventCount: events.length,
    });

    failureState = {
      reasonCode: 'pin_confirmation_failed',
      attempt: 1,
      maxAttempts: retryPolicy.maxAttempts,
    };
    await runWithBoundedIpfsRetry(
      () => confirmAuthorizedBatchPin(dependencies.ipfs, publishedCid!),
      {
        ...retryPolicy,
        onRetry: (error, attempt) => {
          if (dependencies.metrics) dependencies.metrics.retryCount += 1;
          failureState.attempt = attempt + 1;
          logWarn('retrying transient IPFS pin confirmation failure', {
            batchId,
            cid: publishedCid,
            attempt,
            maxAttempts: retryPolicy.maxAttempts,
            retryBackoffMs: retryPolicy.backoffMs,
            error: errorMessage(error),
          });
        },
      }
    );

    const pinLatencyMs = Math.max(now() - pinStartedAt, 0);
    if (dependencies.metrics) {
      dependencies.metrics.pinCount += 1;
      dependencies.metrics.pinLatencyMs = pinLatencyMs;
      dependencies.metrics.pinLatencyTotalMs += pinLatencyMs;
    }
    logInfo('IPFS pin confirmed', { batchId, cid: publishedCid, pinLatencyMs });

    failureState = {
      reasonCode: 'result_event_failed',
      attempt: 1,
      maxAttempts: 1,
    };
    const result = buildIpfsResultEnvelope(publishedCid, events.length);
    failureState = {
      reasonCode: 'result_publish_failed',
      attempt: 1,
      maxAttempts: 1,
    };
    await dependencies.resultPublisher.publish(batchId, result);
    logInfo('IPFS result published to Kafka', {
      batchId,
      eventId: result.envelope.eventId,
      cid: publishedCid,
      eventCount: result.payload.eventCount,
    });
  } catch (error) {
    const failureContext: IpfsFailureContext = {
      topic: TELEMETRY_TOPICS.DLQ,
      reason: errorMessage(error),
      eventId: batchId,
      attempt: failureState.attempt,
      maxAttempts: failureState.maxAttempts,
      failedAt: new Date().toISOString(),
    };
    const dlq = buildIpfsDlqEnvelope(
      { batch_id: batchId, events: deterministicBatch.artifact.events },
      failureState.reasonCode,
      failureContext.reason,
      failureContext
    );
    await dependencies.resultPublisher.publishDlq(batchId, dlq);
    if (dependencies.metrics) dependencies.metrics.dlqCount += 1;
    logWarn('failed IPFS batch published to Kafka DLQ', {
      topic: TELEMETRY_TOPICS.DLQ,
      batchId,
      eventId: dlq.envelope.eventId,
      reasonCode: dlq.payload.reason_code,
      reason: failureContext.reason,
      attempt: failureContext.attempt,
      maxAttempts: failureContext.maxAttempts,
    });
    return 'dlq';
  }

  await dependencies.dedupStore.add(batchId);
  await dependencies.commitOffset();
  return 'processed';
}

function errorMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}
