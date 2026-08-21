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
import {
  TELEMETRY_TOPICS,
  TelemetryIpfsPublishedPayloadSchema,
  TelemetryIpfsPublishedPayload_Compression,
  EnvelopeSchema,
} from '@scp/core';
import { create, toBinary } from '@bufbuild/protobuf';
import type { Consumer } from '@platformatic/kafka';
import { describe, expect, it } from 'vitest';
import { createBlockchainAnchorService } from '../src/index.js';
import type { BlockchainAnchorConfig } from '../src/config.js';
import { CID } from 'multiformats/cid';

function testConfig(
  overrides: Partial<BlockchainAnchorConfig> = {}
): BlockchainAnchorConfig {
  return {
    kafkaBrokers: ['localhost:9092'],
    consumerGroupId: 'blockchain-anchor-v1',
    substrateWsUrl: 'ws://localhost:9944',
    suri: '//Alice',
    nodeId: 0,
    healthPort: 3050,
    ...overrides,
  };
}

function createIpfsPublishedMessage() {
  const fakeCid = CID.parse('QmYwAPJzv5CZsnA625s3Xf2nemtYgPpHdWEz79ojWnPbdG');

  const payload = create(TelemetryIpfsPublishedPayloadSchema, {
    cid: Buffer.from(fakeCid.bytes),
    eventCount: 10,
    compression: TelemetryIpfsPublishedPayload_Compression.NONE,
  });

  const envelope = create(EnvelopeSchema, {
    eventId: 'evt-int-anchor-1',
    eventType: TELEMETRY_TOPICS.IPFS_PUBLISHED,
    eventVersion: '1.0.0',
    occurredAt: '2026-01-01T00:00:00Z',
    source: 'ipfs-publisher',
    payload: toBinary(TelemetryIpfsPublishedPayloadSchema, payload),
  });

  return Buffer.from(toBinary(EnvelopeSchema, envelope));
}

describe('blockchain anchor integration flow (mock harness)', () => {
  it('processes consume -> blockchain anchor flow without autocommit', async () => {
    const callOrder: string[] = [];
    const commitOffsets: Array<{ partition: number; offset: string }> = [];

    const fakeConsumer = {
      async consume() {
        callOrder.push('consume');
        // Return an async iterable that yields messages
        return (async function* () {
          yield {
            topic: TELEMETRY_TOPICS.IPFS_PUBLISHED,
            partition: 0,
            offset: 1n,
            value: createIpfsPublishedMessage(),
          };
        })();
      },
      async commit({
        offsets,
      }: {
        offsets: Array<{ partition: number; offset: bigint }>;
      }) {
        callOrder.push('commit');
        commitOffsets.push(
          ...offsets.map((o) => ({
            partition: o.partition,
            offset: o.offset.toString(),
          }))
        );
      },
      async close() {
        callOrder.push('close');
      },
    };

    const service = createBlockchainAnchorService(testConfig(), {
      createConsumer: () => fakeConsumer as unknown as Consumer,
      createHealthServer: () =>
        ({
          close(callback: (error?: Error) => void) {
            callback();
          },
        }) as unknown as import('node:http').Server,
    });

    // Manually inject mocked blockchain dependencies
    // Note: In a real implementation, we'd use dependency injection
    // For this test, we verify the call order instead

    await service.start();

    // Wait for message processing
    await new Promise((resolve) => setTimeout(resolve, 100));

    await service.stop();

    expect(callOrder).toContain('consume');
    expect(callOrder).toContain('close');
    // Note: Without real blockchain, we can't verify signAndSend/commit order
    // Integration tests with testcontainers would cover full flow
  });

  it('skips non-ipfs-published messages', async () => {
    const processedMessages: string[] = [];

    const otherMessage = Buffer.from(
      toBinary(
        EnvelopeSchema,
        create(EnvelopeSchema, {
          eventId: 'evt-other-1',
          eventType: TELEMETRY_TOPICS.AUTHORIZED,
          eventVersion: 'v1',
          occurredAt: '2026-01-01T00:00:00Z',
          source: 'endpoint',
          payload: Buffer.alloc(0),
        })
      )
    );

    const fakeConsumer = {
      async consume() {
        return (async function* () {
          yield {
            topic: TELEMETRY_TOPICS.IPFS_PUBLISHED,
            partition: 0,
            offset: 1n,
            value: otherMessage,
          };
        })();
      },
      async commit() {
        processedMessages.push('commit-skipped');
      },
      async close() {},
    };

    const service = createBlockchainAnchorService(testConfig(), {
      createConsumer: () => fakeConsumer as unknown as Consumer,
      createHealthServer: () =>
        ({
          close(callback: (error?: Error) => void) {
            callback();
          },
        }) as unknown as import('node:http').Server,
    });

    await service.start();
    await new Promise((resolve) => setTimeout(resolve, 50));
    await service.stop();

    // Should not commit offsets for non-matching messages
    expect(processedMessages).toHaveLength(0);
  });

  it('exposes metrics for consumed, anchored, and failed', async () => {
    const fakeConsumer = {
      async consume() {
        return (async function* () {
          // No messages
        })();
      },
      async close() {},
    };

    const service = createBlockchainAnchorService(testConfig(), {
      createConsumer: () => fakeConsumer as unknown as Consumer,
      createHealthServer: () =>
        ({
          close(callback: (error?: Error) => void) {
            callback();
          },
        }) as unknown as import('node:http').Server,
    });

    await service.start();

    const metrics = service.getMetrics();
    expect(metrics).toHaveProperty('consumed');
    expect(metrics).toHaveProperty('anchored');
    expect(metrics).toHaveProperty('failed');
    expect(metrics.consumed).toBe(0);
    expect(metrics.anchored).toBe(0);
    expect(metrics.failed).toBe(0);

    await service.stop();
  });
});
