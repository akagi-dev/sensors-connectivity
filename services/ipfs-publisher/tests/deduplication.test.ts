import {
  InMemoryDedupStore,
  TELEMETRY_TOPICS,
  type TelemetryAuthorizedPayload
} from '@scp/contracts';
import { describe, expect, it, vi } from 'vitest';
import type { IpfsBatchClient } from '../src/batch-publisher.js';
import {
  deriveAuthorizedBatchId,
  type AuthorizedTelemetryEnvelope,
  type SealedAuthorizedBatch
} from '../src/batching.js';
import { processSealedAuthorizedBatch } from '../src/batch-processor.js';
import { createIpfsPublisherMetrics } from '../src/metrics.js';
import type { IpfsResultPublisher } from '../src/result-publisher.js';

const testCid = 'bafybeideduplicated';

/** Builds a sealed batch with a deterministic identifier for deduplication tests. */
function sealedBatch(offset: string, eventId: string): SealedAuthorizedBatch {
  const payload: TelemetryAuthorizedPayload = {
    sensor_id: `sensor-${eventId}`,
    timestamp: '2026-08-11T00:00:00.000Z',
    nonce: `nonce-${eventId}`,
    measurements: { temperature: 22 },
    signature: '0x01'
  };
  const envelope: AuthorizedTelemetryEnvelope = {
    event_id: eventId,
    event_type: TELEMETRY_TOPICS.AUTHORIZED,
    event_version: 'v1',
    occurred_at: '2026-08-11T00:00:00.000Z',
    source: 'authorizer',
    payload
  };
  const topic = TELEMETRY_TOPICS.AUTHORIZED;
  const partition = 0;
  const batchId = deriveAuthorizedBatchId({
    topic,
    partition,
    entries: [{ offset, eventId }]
  });

  return {
    batchId,
    topic,
    partition,
    firstOffset: offset,
    lastOffset: offset,
    entries: [{ topic, partition, offset, envelope }]
  };
}

/** Creates a Kubo test double with publication and pin-check counters. */
function createIpfsDouble(): {
  client: IpfsBatchClient;
  add: ReturnType<typeof vi.fn>;
  pinAdd: ReturnType<typeof vi.fn>;
  pinLs: ReturnType<typeof vi.fn>;
} {
  const add = vi.fn(async () => ({
    cid: { toString: (): string => testCid }
  }));
  const pinAdd = vi.fn(async () => ({ toString: (): string => testCid }));
  const pinLs = vi.fn(async function* () {
    yield { cid: { toString: (): string => testCid } };
  });

  return {
    client: { add, pin: { add: pinAdd, ls: pinLs } },
    add,
    pinAdd,
    pinLs
  };
}

/** Creates a Kafka result publisher test double for counting acknowledged publications. */
function createResultPublisherDouble(): {
  client: IpfsResultPublisher;
  publish: ReturnType<typeof vi.fn>;
  publishDlq: ReturnType<typeof vi.fn>;
} {
  const publish = vi.fn(async () => undefined);
  const publishDlq = vi.fn(async () => undefined);
  return {
    client: {
      async connect() {},
      async disconnect() {},
      publish,
      publishDlq
    },
    publish,
    publishDlq
  };
}

/** Creates an acknowledged manual Kafka commit test double for processing-order checks. */
function createCommitOffsetDouble(): ReturnType<typeof vi.fn> {
  return vi.fn(async () => undefined);
}

describe('authorized batch deduplication', () => {
  it('skips IPFS publication when the same batch_id is replayed', async () => {
    const ipfs = createIpfsDouble();
    const resultPublisher = createResultPublisherDouble();
    const commitOffset = createCommitOffsetDouble();
    const dependencies = {
      ipfs: ipfs.client,
      dedupStore: new InMemoryDedupStore(),
      resultPublisher: resultPublisher.client,
      commitOffset
    };
    const batch = sealedBatch('7', 'event-1');

    await expect(
      processSealedAuthorizedBatch(batch, dependencies)
    ).resolves.toBe('processed');
    await expect(
      processSealedAuthorizedBatch(batch, dependencies)
    ).resolves.toBe('duplicate');

    expect(ipfs.add).toHaveBeenCalledTimes(1);
    expect(ipfs.pinAdd).toHaveBeenCalledTimes(1);
    expect(ipfs.pinLs).toHaveBeenCalledTimes(1);
    expect(resultPublisher.publish).toHaveBeenCalledTimes(1);
    expect(resultPublisher.publishDlq).not.toHaveBeenCalled();
    expect(commitOffset).toHaveBeenCalledTimes(2);
  });

  it('processes batches with different batch_id values independently', async () => {
    const ipfs = createIpfsDouble();
    const resultPublisher = createResultPublisherDouble();
    const commitOffset = createCommitOffsetDouble();
    const dependencies = {
      ipfs: ipfs.client,
      dedupStore: new InMemoryDedupStore(),
      resultPublisher: resultPublisher.client,
      commitOffset
    };

    await processSealedAuthorizedBatch(
      sealedBatch('7', 'event-1'),
      dependencies
    );
    await processSealedAuthorizedBatch(
      sealedBatch('8', 'event-2'),
      dependencies
    );

    expect(ipfs.add).toHaveBeenCalledTimes(2);
    expect(ipfs.pinAdd).toHaveBeenCalledTimes(2);
    expect(resultPublisher.publish).toHaveBeenCalledTimes(2);
    expect(commitOffset).toHaveBeenCalledTimes(2);
  });

  it('does not mark a batch as processed after a failed publication', async () => {
    const ipfs = createIpfsDouble();
    const resultPublisher = createResultPublisherDouble();
    const commitOffset = createCommitOffsetDouble();
    ipfs.add.mockRejectedValueOnce(new Error('Kubo unavailable'));
    const dependencies = {
      ipfs: ipfs.client,
      dedupStore: new InMemoryDedupStore(),
      resultPublisher: resultPublisher.client,
      commitOffset
    };
    const batch = sealedBatch('7', 'event-1');

    await expect(
      processSealedAuthorizedBatch(batch, dependencies)
    ).resolves.toBe('dlq');
    await expect(
      processSealedAuthorizedBatch(batch, dependencies)
    ).resolves.toBe('processed');

    expect(ipfs.add).toHaveBeenCalledTimes(2);
    expect(ipfs.pinAdd).toHaveBeenCalledTimes(1);
    expect(resultPublisher.publish).toHaveBeenCalledTimes(1);
    expect(resultPublisher.publishDlq).toHaveBeenCalledTimes(1);
    expect(commitOffset).toHaveBeenCalledTimes(1);
  });

  it('retries a transient Kubo publication failure before emitting a result', async () => {
    const ipfs = createIpfsDouble();
    const resultPublisher = createResultPublisherDouble();
    const commitOffset = createCommitOffsetDouble();
    const metrics = createIpfsPublisherMetrics();
    const now = vi.fn().mockReturnValueOnce(100).mockReturnValueOnce(137);
    ipfs.add.mockRejectedValueOnce(new Error('fetch failed'));
    const dependencies = {
      ipfs: ipfs.client,
      dedupStore: new InMemoryDedupStore(),
      resultPublisher: resultPublisher.client,
      ipfsRetry: { maxAttempts: 3, backoffMs: 0 },
      commitOffset,
      metrics,
      now
    };

    await expect(
      processSealedAuthorizedBatch(
        sealedBatch('7', 'event-retry'),
        dependencies
      )
    ).resolves.toBe('processed');

    expect(ipfs.add).toHaveBeenCalledTimes(2);
    expect(ipfs.pinAdd).toHaveBeenCalledTimes(1);
    expect(resultPublisher.publish).toHaveBeenCalledTimes(1);
    expect(commitOffset).toHaveBeenCalledTimes(1);
    expect(metrics).toEqual({
      batchCount: 1,
      pinCount: 1,
      pinLatencyMs: 37,
      pinLatencyTotalMs: 37,
      retryCount: 1,
      dlqCount: 0
    });
  });

  it('publishes exhausted Kubo failures to DLQ with the actual attempt count', async () => {
    const ipfs = createIpfsDouble();
    const resultPublisher = createResultPublisherDouble();
    const commitOffset = createCommitOffsetDouble();
    const metrics = createIpfsPublisherMetrics();
    ipfs.add.mockRejectedValue(new Error('fetch failed'));
    const batch = sealedBatch('9', 'event-exhausted');

    await expect(
      processSealedAuthorizedBatch(batch, {
        ipfs: ipfs.client,
        dedupStore: new InMemoryDedupStore(),
        resultPublisher: resultPublisher.client,
        ipfsRetry: { maxAttempts: 2, backoffMs: 0 },
        commitOffset,
        metrics
      })
    ).resolves.toBe('dlq');

    expect(ipfs.add).toHaveBeenCalledTimes(2);
    expect(resultPublisher.publish).not.toHaveBeenCalled();
    expect(resultPublisher.publishDlq).toHaveBeenCalledTimes(1);
    expect(commitOffset).not.toHaveBeenCalled();
    expect(metrics).toMatchObject({
      batchCount: 1,
      pinCount: 0,
      retryCount: 1,
      dlqCount: 1
    });
    expect(resultPublisher.publishDlq.mock.calls[0]?.[0]).toBe(batch.batchId);
    expect(resultPublisher.publishDlq.mock.calls[0]?.[1]).toMatchObject({
      event_type: TELEMETRY_TOPICS.DLQ,
      payload: {
        failed_topic: TELEMETRY_TOPICS.AUTHORIZED,
        reason_code: 'ipfs_publish_or_pin_failed',
        reason_message: 'fetch failed',
        failed_event: { batch_id: batch.batchId },
        context: {
          eventId: batch.batchId,
          attempt: 2,
          maxAttempts: 2
        }
      }
    });
  });

  it('does not report dlq status when the Kafka DLQ ACK fails', async () => {
    const ipfs = createIpfsDouble();
    const resultPublisher = createResultPublisherDouble();
    const commitOffset = createCommitOffsetDouble();
    ipfs.add.mockRejectedValue(new Error('Kubo add returned an empty CID'));
    resultPublisher.publishDlq.mockRejectedValue(
      new Error('Kafka DLQ unavailable')
    );

    await expect(
      processSealedAuthorizedBatch(sealedBatch('10', 'event-dlq-failed'), {
        ipfs: ipfs.client,
        dedupStore: new InMemoryDedupStore(),
        resultPublisher: resultPublisher.client,
        ipfsRetry: { maxAttempts: 3, backoffMs: 0 },
        commitOffset
      })
    ).rejects.toThrow('Kafka DLQ unavailable');

    expect(ipfs.add).toHaveBeenCalledTimes(1);
    expect(resultPublisher.publishDlq).toHaveBeenCalledTimes(1);
    expect(commitOffset).not.toHaveBeenCalled();
  });

  it('does not commit the Kafka offset when the result-event ACK fails', async () => {
    const ipfs = createIpfsDouble();
    const resultPublisher = createResultPublisherDouble();
    const commitOffset = createCommitOffsetDouble();
    resultPublisher.publish.mockRejectedValue(
      new Error('Kafka result unavailable')
    );

    await expect(
      processSealedAuthorizedBatch(sealedBatch('11', 'event-result-failed'), {
        ipfs: ipfs.client,
        dedupStore: new InMemoryDedupStore(),
        resultPublisher: resultPublisher.client,
        commitOffset
      })
    ).resolves.toBe('dlq');

    expect(resultPublisher.publish).toHaveBeenCalledTimes(1);
    expect(resultPublisher.publishDlq).toHaveBeenCalledTimes(1);
    expect(commitOffset).not.toHaveBeenCalled();
  });

  it('retries a transient pin confirmation failure without adding the artifact again', async () => {
    const ipfs = createIpfsDouble();
    const resultPublisher = createResultPublisherDouble();
    const commitOffset = createCommitOffsetDouble();
    ipfs.pinLs.mockImplementationOnce(async function* () {
      yield await Promise.reject(new Error('Kubo did not confirm pin'));
    });
    const dependencies = {
      ipfs: ipfs.client,
      dedupStore: new InMemoryDedupStore(),
      resultPublisher: resultPublisher.client,
      ipfsRetry: { maxAttempts: 2, backoffMs: 0 },
      commitOffset
    };

    await expect(
      processSealedAuthorizedBatch(
        sealedBatch('8', 'event-pin-retry'),
        dependencies
      )
    ).resolves.toBe('processed');

    expect(ipfs.add).toHaveBeenCalledTimes(1);
    expect(ipfs.pinAdd).toHaveBeenCalledTimes(1);
    expect(ipfs.pinLs).toHaveBeenCalledTimes(2);
    expect(resultPublisher.publish).toHaveBeenCalledTimes(1);
    expect(commitOffset).toHaveBeenCalledTimes(1);
  });
});
