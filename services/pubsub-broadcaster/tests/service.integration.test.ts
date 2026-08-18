import {
  TELEMETRY_TOPICS,
  createEnvelope,
  serializeEnvelope,
  serializePayloadForEventType,
  TelemetryAuthorizedPayloadSchema,
} from '@scp/contracts';
import { create } from '@bufbuild/protobuf';
import type { Consumer } from '@platformatic/kafka';
import { describe, expect, it } from 'vitest';
import { createPubsubBroadcasterService } from '../src/index.js';
import type { PubsubBroadcasterConfig } from '../src/config.js';

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
    ipfsApiUrl: 'http://localhost:5001',
    reservedPeers: [],
    ...overrides,
  };
}

function createAuthorizedMessage() {
  const payload = create(TelemetryAuthorizedPayloadSchema, {
    sensorId: Buffer.alloc(32, 1),
    timestamp: BigInt(Date.parse('2026-01-01T00:00:00Z')),
    nonce: Buffer.alloc(16, 2),
    message: Buffer.from(JSON.stringify({ temp: 25 })),
    signature: Buffer.alloc(64, 3),
    signedEnvelope: Buffer.alloc(100, 4),
  });

  const envelope = createEnvelope({
    eventId: 'evt-int-1',
    eventType: TELEMETRY_TOPICS.AUTHORIZED,
    eventVersion: 'v1',
    occurredAt: '2026-01-01T00:00:00Z',
    source: 'endpoint',
    payload: serializePayloadForEventType(TELEMETRY_TOPICS.AUTHORIZED, payload),
  });

  return serializeEnvelope(envelope);
}

const authorizedMessage = createAuthorizedMessage();

describe('pubsub broadcaster integration flow (mock harness)', () => {
  it('processes consume -> publish -> result event -> commit in order', async () => {
    const callOrder: string[] = [];

    const fakeConsumer = {
      async consume({ _topics }: { topics: string[]; autocommit: boolean }) {
        // Return an async iterable that yields messages
        return (async function* () {
          yield {
            topic: TELEMETRY_TOPICS.AUTHORIZED,
            partition: 0,
            offset: 1n,
            value: authorizedMessage,
          };
        })();
      },
      async commit() {
        callOrder.push('commit');
      },
      async disconnect() {},
    };

    const service = createPubsubBroadcasterService(testConfig({}), {
      createConsumer: () =>
        fakeConsumer as unknown as Consumer<Buffer, Buffer, Buffer, Buffer>,
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
    });

    await service.start();
    // Give it time to process the message
    await new Promise((resolve) => setTimeout(resolve, 100));
    await service.stop();

    expect(callOrder).toEqual(['publish', 'result', 'commit']);
  });

  it('routes exhausted transient failures to DLQ and commits afterward', async () => {
    const callOrder: string[] = [];

    const fakeConsumer = {
      async consume({ _topics }: { topics: string[]; autocommit: boolean }) {
        return (async function* () {
          yield {
            topic: TELEMETRY_TOPICS.AUTHORIZED,
            partition: 0,
            offset: 0n,
            value: authorizedMessage,
          };
        })();
      },
      async commit() {
        callOrder.push('commit');
      },
      async disconnect() {},
    };

    const service = createPubsubBroadcasterService(
      testConfig({ maxRetries: 2 }),
      {
        createConsumer: () =>
          fakeConsumer as unknown as Consumer<Buffer, Buffer, Buffer, Buffer>,
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
    // Give it time to process the message
    await new Promise((resolve) => setTimeout(resolve, 100));
    await service.stop();

    expect(callOrder).toEqual([TELEMETRY_TOPICS.RETRY, TELEMETRY_TOPICS.DLQ]);
  });
});
