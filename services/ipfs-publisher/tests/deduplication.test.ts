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
import { TELEMETRY_TOPICS } from '@scp/core';
import { describe, expect, it, vi } from 'vitest';
import type { IpfsBatchClient } from '../src/batch-publisher.js';
import {
  InMemoryBatchDedupStore,
  processSealedAuthorizedBatch,
} from '../src/batch-processor.js';
import {
  deriveAuthorizedBatchId,
  type SealedAuthorizedBatch,
} from '../src/batching.js';
import { createIpfsPublisherMetrics } from '../src/metrics.js';
import type { IpfsResultPublisher } from '../src/result-publisher.js';
import { authorizedEnvelope } from './helpers.js';

const cid = 'bafybeideduplicated';

function sealedBatch(offset = '7', eventId = 'event-1'): SealedAuthorizedBatch {
  const topic = TELEMETRY_TOPICS.AUTHORIZED;
  const partition = 0;
  return {
    batchId: deriveAuthorizedBatchId({
      topic,
      partition,
      entries: [{ offset, eventId }],
    }),
    topic,
    partition,
    firstOffset: offset,
    lastOffset: offset,
    entries: [
      { topic, partition, offset, envelope: authorizedEnvelope(eventId) },
    ],
  };
}

function ipfsDouble(): {
  client: IpfsBatchClient;
  add: ReturnType<typeof vi.fn>;
  pinLs: ReturnType<typeof vi.fn>;
} {
  const add = vi.fn(async () => ({ cid: { toString: () => cid } }));
  const pinLs = vi.fn(async function* () {
    yield { cid: { toString: () => cid } };
  });
  return {
    client: {
      add,
      pin: {
        add: vi.fn(async () => ({ toString: () => cid })),
        ls: pinLs,
      },
    },
    add,
    pinLs,
  };
}

function resultDouble(): {
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
      publishDlq,
    },
    publish,
    publishDlq,
  };
}

describe('authorized batch processing', () => {
  it('deduplicates only after result ACK and commits replayed batches', async () => {
    const ipfs = ipfsDouble();
    const results = resultDouble();
    const commitOffset = vi.fn(async () => undefined);
    const dependencies = {
      ipfs: ipfs.client,
      dedupStore: new InMemoryBatchDedupStore(),
      resultPublisher: results.client,
      commitOffset,
    };
    const batch = sealedBatch();
    await expect(
      processSealedAuthorizedBatch(batch, dependencies)
    ).resolves.toBe('processed');
    await expect(
      processSealedAuthorizedBatch(batch, dependencies)
    ).resolves.toBe('duplicate');
    expect(ipfs.add).toHaveBeenCalledOnce();
    expect(results.publish).toHaveBeenCalledOnce();
    expect(commitOffset).toHaveBeenCalledTimes(2);
  });

  it('retries transient IPFS failures and records metrics', async () => {
    const ipfs = ipfsDouble();
    ipfs.add.mockRejectedValueOnce(new Error('fetch failed'));
    const results = resultDouble();
    const metrics = createIpfsPublisherMetrics();
    await expect(
      processSealedAuthorizedBatch(sealedBatch(), {
        ipfs: ipfs.client,
        dedupStore: new InMemoryBatchDedupStore(),
        resultPublisher: results.client,
        commitOffset: vi.fn(async () => undefined),
        ipfsRetry: { maxAttempts: 2, backoffMs: 0 },
        metrics,
      })
    ).resolves.toBe('processed');
    expect(ipfs.add).toHaveBeenCalledTimes(2);
    expect(metrics.retryCount).toBe(1);
    expect(metrics.pinCount).toBe(1);
  });

  it('returns a normal DLQ status after an acknowledged exhausted failure', async () => {
    const ipfs = ipfsDouble();
    ipfs.add.mockRejectedValue(new Error('fetch failed'));
    const results = resultDouble();
    const commitOffset = vi.fn(async () => undefined);
    await expect(
      processSealedAuthorizedBatch(sealedBatch(), {
        ipfs: ipfs.client,
        dedupStore: new InMemoryBatchDedupStore(),
        resultPublisher: results.client,
        commitOffset,
        ipfsRetry: { maxAttempts: 2, backoffMs: 0 },
      })
    ).resolves.toBe('dlq');
    expect(results.publishDlq).toHaveBeenCalledOnce();
    expect(results.publishDlq.mock.calls[0]![1].payload.context.attempt).toBe(
      2
    );
    expect(commitOffset).not.toHaveBeenCalled();
  });

  it('rejects when the DLQ broker acknowledgement fails', async () => {
    const ipfs = ipfsDouble();
    ipfs.add.mockRejectedValue(new Error('terminal failure'));
    const results = resultDouble();
    results.publishDlq.mockRejectedValue(new Error('Kafka DLQ unavailable'));
    await expect(
      processSealedAuthorizedBatch(sealedBatch(), {
        ipfs: ipfs.client,
        dedupStore: new InMemoryBatchDedupStore(),
        resultPublisher: results.client,
        commitOffset: vi.fn(async () => undefined),
      })
    ).rejects.toThrow('Kafka DLQ unavailable');
  });
});
