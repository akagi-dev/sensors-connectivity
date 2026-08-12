import {
  runConsumerProcessingRule,
  TELEMETRY_TOPICS,
  type FailureContext,
  type InMemoryDedupStore,
  type RetryDlqPublisher,
  type TelemetryAuthorizedPayload
} from '@scp/contracts';
import {
  buildDeterministicAuthorizedBatch,
  confirmAuthorizedBatchPin,
  publishAndPinAuthorizedBatch,
  type IpfsBatchClient
} from './batch-publisher.js';
import {
  deriveAuthorizedBatchId,
  type AuthorizedBatchIdContext,
  type SealedAuthorizedBatch
} from './batching.js';
import {
  runWithBoundedIpfsRetry,
  type IpfsRetryOptions
} from './ipfs-retry.js';
import { logInfo, logWarn } from './logger.js';
import type { IpfsPublisherMetrics } from './metrics.js';
import {
  buildIpfsDlqEnvelope,
  buildIpfsResultEnvelope,
  type IpfsDlqReasonCode,
  type IpfsResultPublisher
} from './result-publisher.js';

interface AuthorizedBatch {
  batch_id: string;
  events: TelemetryAuthorizedPayload[];
}

export interface AuthorizedPayloadProcessingDependencies {
  ipfs: IpfsBatchClient;
  dedupStore: InMemoryDedupStore;
  resultPublisher: IpfsResultPublisher;
  ipfsRetry?: Pick<IpfsRetryOptions, 'maxAttempts' | 'backoffMs'>;
  commitOffset: () => Promise<void>;
  metrics?: IpfsPublisherMetrics;
  now?: () => number;
}

export type AuthorizedBatchProcessingStatus =
  'processed' | 'duplicate' | 'retried' | 'dlq';

/**
 * Runs the deterministic IPFS publication flow for one set of authorized
 * payloads.
 */
export async function processAuthorizedPayload(
  payload: TelemetryAuthorizedPayload,
  dependencies: AuthorizedPayloadProcessingDependencies
): Promise<AuthorizedBatchProcessingStatus> {
  return processAuthorizedEvents([payload], dependencies);
}

/** Runs deterministic IPFS publication for one sealed Kafka batch. */
export async function processSealedAuthorizedBatch(
  batch: SealedAuthorizedBatch,
  dependencies: AuthorizedPayloadProcessingDependencies
): Promise<AuthorizedBatchProcessingStatus> {
  const context: AuthorizedBatchIdContext = {
    topic: batch.topic,
    partition: batch.partition,
    entries: batch.entries.map((entry) => ({
      offset: entry.offset,
      eventId: entry.envelope.event_id
    }))
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

/**
 * Applies the common publication rule to validated events and optional Kafka
 * batch identity metadata.
 */
async function processAuthorizedEvents(
  events: TelemetryAuthorizedPayload[],
  dependencies: AuthorizedPayloadProcessingDependencies,
  batchContext?: AuthorizedBatchIdContext
): Promise<AuthorizedBatchProcessingStatus> {
  if (dependencies.metrics) {
    dependencies.metrics.batchCount += 1;
  }
  const batch: AuthorizedBatch = {
    batch_id: `batch-${Date.now()}`,
    events
  };
  const deterministicBatch = buildDeterministicAuthorizedBatch(
    batch.events,
    batchContext
  );
  batch.batch_id = deterministicBatch.batchId;
  let publishedCid: string | undefined;
  let pinStartedAt = 0;
  const now = dependencies.now ?? Date.now;
  const retryPolicy = dependencies.ipfsRetry ?? {
    maxAttempts: 1,
    backoffMs: 0
  };
  let failureState: {
    reasonCode: IpfsDlqReasonCode;
    attempt: number;
    maxAttempts: number;
  } = {
    reasonCode: 'ipfs_publish_or_pin_failed',
    attempt: 1,
    maxAttempts: retryPolicy.maxAttempts
  };

  const retryDlqPublisher: RetryDlqPublisher<AuthorizedBatch> = {
    async publishRetry(event, reason) {
      logWarn('retry stub', {
        topic: TELEMETRY_TOPICS.RETRY,
        batchId: event.batch_id,
        reason
      });
    },
    async publishDlq(event, reason, context) {
      const failureContext: FailureContext = {
        topic: TELEMETRY_TOPICS.DLQ,
        reason: context?.reason ?? reason,
        eventId: event.batch_id,
        attempt: failureState.attempt,
        maxAttempts: failureState.maxAttempts,
        failedAt: context?.failedAt ?? new Date().toISOString()
      };
      const envelope = buildIpfsDlqEnvelope(
        event,
        failureState.reasonCode,
        failureContext.reason,
        failureContext
      );
      await dependencies.resultPublisher.publishDlq(event.batch_id, envelope);
      if (dependencies.metrics) {
        dependencies.metrics.dlqCount += 1;
      }
      logWarn('failed IPFS batch published to Kafka DLQ', {
        topic: TELEMETRY_TOPICS.DLQ,
        batchId: event.batch_id,
        eventId: envelope.event_id,
        reasonCode: envelope.payload.reason_code,
        reason: failureContext.reason,
        attempt: failureContext.attempt,
        maxAttempts: failureContext.maxAttempts
      });
    }
  };

  return runConsumerProcessingRule(batch, {
    dedup: {
      keyType: 'batch_id',
      getKeyValue: (event) => event.batch_id,
      store: dependencies.dedupStore
    },
    performExternalAction: async () => {
      pinStartedAt = now();
      failureState = {
        reasonCode: 'ipfs_publish_or_pin_failed',
        attempt: 1,
        maxAttempts: retryPolicy.maxAttempts
      };
      publishedCid = await runWithBoundedIpfsRetry(
        () =>
          publishAndPinAuthorizedBatch(
            dependencies.ipfs,
            deterministicBatch.bytes
          ),
        {
          ...retryPolicy,
          onRetry: (error, attempt) => {
            if (dependencies.metrics) {
              dependencies.metrics.retryCount += 1;
            }
            failureState.attempt = attempt + 1;
            logWarn('retrying transient IPFS publication failure', {
              batchId: batch.batch_id,
              attempt,
              maxAttempts: retryPolicy.maxAttempts,
              retryBackoffMs: retryPolicy.backoffMs,
              error: error instanceof Error ? error.message : String(error)
            });
          }
        }
      );
      logInfo('built and published deterministic IPFS object', {
        batchId: batch.batch_id,
        cid: publishedCid,
        eventCount: batch.events.length
      });
    },
    waitForConfirmation: async () => {
      failureState = {
        reasonCode: 'pin_confirmation_failed',
        attempt: 1,
        maxAttempts: retryPolicy.maxAttempts
      };
      if (!publishedCid) {
        throw new Error('IPFS publication did not return a CID');
      }
      await runWithBoundedIpfsRetry(
        () => confirmAuthorizedBatchPin(dependencies.ipfs, publishedCid!),
        {
          ...retryPolicy,
          onRetry: (error, attempt) => {
            if (dependencies.metrics) {
              dependencies.metrics.retryCount += 1;
            }
            failureState.attempt = attempt + 1;
            logWarn('retrying transient IPFS pin confirmation failure', {
              batchId: batch.batch_id,
              cid: publishedCid,
              attempt,
              maxAttempts: retryPolicy.maxAttempts,
              retryBackoffMs: retryPolicy.backoffMs,
              error: error instanceof Error ? error.message : String(error)
            });
          }
        }
      );
      const pinLatencyMs = Math.max(now() - pinStartedAt, 0);
      if (dependencies.metrics) {
        dependencies.metrics.pinCount += 1;
        dependencies.metrics.pinLatencyMs = pinLatencyMs;
        dependencies.metrics.pinLatencyTotalMs += pinLatencyMs;
      }
      logInfo('IPFS pin confirmed', {
        batchId: batch.batch_id,
        cid: publishedCid,
        pinLatencyMs
      });
    },
    emitResultEvent: async (event) => {
      failureState = {
        reasonCode: 'result_event_failed',
        attempt: 1,
        maxAttempts: 1
      };
      if (!publishedCid) {
        throw new Error('Cannot emit IPFS result without a published CID');
      }
      return buildIpfsResultEnvelope(publishedCid, event.events.length);
    },
    publishResultEvent: async (result) => {
      failureState = {
        reasonCode: 'result_publish_failed',
        attempt: 1,
        maxAttempts: 1
      };
      await dependencies.resultPublisher.publish(batch.batch_id, result);
      logInfo('IPFS result published to Kafka', {
        batchId: batch.batch_id,
        eventId: result.event_id,
        cid: result.payload.cid,
        eventCount: result.payload.event_count
      });
    },
    commitOffset: dependencies.commitOffset,
    retryDlqPublisher
  });
}
