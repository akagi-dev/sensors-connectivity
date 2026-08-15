import { TELEMETRY_TOPICS } from '@scp/contracts';
import type { Consumer } from 'kafkajs';
import { describe, expect, it } from 'vitest';
import { createPubsubBroadcasterService } from '../src/index.js';
import type { PubsubBroadcasterConfig } from '../src/config.js';

interface FakeBatchPayload {
  batch: {
    topic: string;
    partition: number;
    highWatermark: string;
    messages: Array<{ offset: string; value: Buffer }>;
  };
  resolveOffset: (offset: string) => void;
  heartbeat: () => Promise<void>;
  isRunning: () => boolean;
  isStale: () => boolean;
}

function testConfig(
  overrides: Partial<PubsubBroadcasterConfig> = {}
): PubsubBroadcasterConfig {
  return {
    kafkaBrokers: ['localhost:9092'],
    consumerGroupId: 'pubsub-broadcaster-v1',
    source: 'pubsub-broadcaster',
    healthPort: 3020,
    maxRetries: 2,
    retryBackoffMs: 0,
    pubsubTopic: 'telemetry/authorized/v1',
    reservedPeers: [],
    ...overrides,
  };
}

const authorizedMessage = JSON.stringify({
  event_id: 'evt-int-1',
  event_type: TELEMETRY_TOPICS.AUTHORIZED,
  event_version: 'v1',
  occurred_at: '2026-01-01T00:00:00Z',
  source: 'endpoint',
  payload: {
    sensor_id: Buffer.alloc(32, 1).toString('base64'),
    timestamp: Date.parse('2026-01-01T00:00:00Z'),
    nonce: Buffer.alloc(16, 2).toString('base64'),
    message: Buffer.from(JSON.stringify({ temp: 25 })).toString('base64'),
    signature: Buffer.alloc(64, 3).toString('base64'),
  },
});

describe('pubsub broadcaster integration flow (mock harness)', () => {
  it('processes consume -> publish -> result event -> commit in order', async () => {
    const callOrder: string[] = [];
    const reservedPeerCalls: string[][] = [];

    const fakeConsumer = {
      async connect() {},
      async subscribe() {},
      async stop() {},
      async disconnect() {},
      async commitOffsets() {
        callOrder.push('commit');
      },
      async run({
        eachBatch,
      }: {
        eachBatch: (payload: FakeBatchPayload) => Promise<void>;
      }) {
        await eachBatch({
          batch: {
            topic: TELEMETRY_TOPICS.AUTHORIZED,
            partition: 0,
            highWatermark: '2',
            messages: [{ offset: '1', value: Buffer.from(authorizedMessage) }],
          },
          resolveOffset() {},
          heartbeat: async () => {},
          isRunning: () => true,
          isStale: () => false,
        });
      },
    };

    const service = createPubsubBroadcasterService(
      testConfig({
        reservedPeers: ['/ip4/127.0.0.1/tcp/4001/p2p/12D3KooWReserved'],
      }),
      {
        createConsumer: () => fakeConsumer as unknown as Consumer,
        createPublisher: () => ({
          async connect() {},
          async disconnect() {},
          async publish(topic) {
            if (topic === TELEMETRY_TOPICS.PUBSUB_RESULT) {
              callOrder.push('result');
            }
          },
        }),
        createPubsubClient: async () => ({
          async start() {},
          async stop() {},
          async connectReservedPeers(peers) {
            reservedPeerCalls.push([...peers]);
          },
          async publish() {
            callOrder.push('publish');
          },
        }),
        createHealthServer: () =>
          ({
            close(callback: (error?: Error) => void) {
              callback();
            },
          }) as unknown as import('node:http').Server,
      }
    );

    await service.start();
    await service.stop();

    expect(reservedPeerCalls).toEqual([
      ['/ip4/127.0.0.1/tcp/4001/p2p/12D3KooWReserved'],
    ]);
    expect(callOrder).toEqual(['publish', 'result', 'commit']);
  });

  it('routes exhausted transient failures to DLQ and commits afterward', async () => {
    const callOrder: string[] = [];

    const fakeConsumer = {
      async connect() {},
      async subscribe() {},
      async stop() {},
      async disconnect() {},
      async commitOffsets() {
        callOrder.push('commit');
      },
      async run({
        eachBatch,
      }: {
        eachBatch: (payload: FakeBatchPayload) => Promise<void>;
      }) {
        await eachBatch({
          batch: {
            topic: TELEMETRY_TOPICS.AUTHORIZED,
            partition: 0,
            highWatermark: '1',
            messages: [{ offset: '0', value: Buffer.from(authorizedMessage) }],
          },
          resolveOffset() {},
          heartbeat: async () => {},
          isRunning: () => true,
          isStale: () => false,
        });
      },
    };

    const service = createPubsubBroadcasterService(
      testConfig({ maxRetries: 2 }),
      {
        createConsumer: () => fakeConsumer as unknown as Consumer,
        createPublisher: () => ({
          async connect() {},
          async disconnect() {},
          async publish(topic) {
            callOrder.push(topic);
          },
        }),
        createPubsubClient: async () => ({
          async start() {},
          async stop() {},
          async connectReservedPeers() {},
          async publish() {
            const error = new Error('network timeout') as Error & {
              retriable: boolean;
            };
            error.retriable = true;
            throw error;
          },
        }),
        createHealthServer: () =>
          ({
            close(callback: (error?: Error) => void) {
              callback();
            },
          }) as unknown as import('node:http').Server,
      }
    );

    await service.start();
    await service.stop();

    expect(callOrder).toEqual([
      TELEMETRY_TOPICS.RETRY,
      TELEMETRY_TOPICS.DLQ,
      'commit',
    ]);
  });
});
