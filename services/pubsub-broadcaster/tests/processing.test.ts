import { InMemoryRetryCounterStore, TELEMETRY_TOPICS } from '@scp/contracts';
import { describe, expect, it } from 'vitest';
import {
  processAuthorizedEnvelope,
  type AuthorizedTelemetryMessage,
} from '../src/index.js';
import type { PubsubBroadcasterConfig } from '../src/config.js';

function makeMessage(eventId: string): AuthorizedTelemetryMessage {
  return {
    eventId,
    eventType: TELEMETRY_TOPICS.AUTHORIZED,
    traceId: 'trace-1',
    payload: {
      sensorId: Buffer.alloc(32, 1),
      timestamp: BigInt(Date.parse('2026-01-01T00:00:00Z')),
      nonce: Buffer.alloc(16, 2),
      message: Buffer.from(JSON.stringify({ temp: 21 })),
      signature: Buffer.alloc(64, 3),
      signedEnvelope: Buffer.alloc(100, 4),
    },
  };
}

function makeConfig(
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

describe('pubsub broadcaster processing', () => {
  it('maps authorized payload to configured pubsub topic and emits result event', async () => {
    const publishedTopics: string[] = [];
    const resultTopics: string[] = [];
    const metrics = {
      consumed: 0,
      processed: 0,
      publishSuccess: 0,
      publishFailure: 0,
      retryCount: 0,
      dlqCount: 0,
      consumerLag: 0,
    };

    const status = await processAuthorizedEnvelope(
      makeMessage('evt-success'),
      async () => {},
      {
        config: makeConfig(),
        pubsub: {
          async start() {},
          async stop() {},
          async publish(topic) {
            publishedTopics.push(topic);
          },
        },
        publisher: {
          async connect() {},
          async disconnect() {},
          async publish(topic) {
            resultTopics.push(topic);
          },
        },
        idempotencyStore: {
          async has() {
            return false;
          },
          async mark() {},
        },
        retryStore: new InMemoryRetryCounterStore(),
        metrics,
      }
    );

    expect(status).toBe('processed');
    expect(publishedTopics).toEqual(['telemetry/authorized/v1']);
    expect(resultTopics).toEqual([TELEMETRY_TOPICS.PUBSUB_RESULT]);
    expect(metrics.publishSuccess).toBe(1);
  });

  it('classifies non-transient publish errors as terminal and emits failed result without retry', async () => {
    const publishedTopics: string[] = [];
    const metrics = {
      consumed: 0,
      processed: 0,
      publishSuccess: 0,
      publishFailure: 0,
      retryCount: 0,
      dlqCount: 0,
      consumerLag: 0,
    };

    const status = await processAuthorizedEnvelope(
      makeMessage('evt-terminal'),
      async () => {},
      {
        config: makeConfig(),
        pubsub: {
          async start() {},
          async stop() {},
          async publish() {
            throw new Error('signature format invalid');
          },
        },
        publisher: {
          async connect() {},
          async disconnect() {},
          async publish(topic, _key, _envelope) {
            publishedTopics.push(topic);
            // Result envelope should be published even on failure
          },
        },
        idempotencyStore: {
          async has() {
            return false;
          },
          async mark() {},
        },
        retryStore: new InMemoryRetryCounterStore(),
        metrics,
      }
    );

    expect(status).toBe('processed');
    expect(publishedTopics).toEqual([TELEMETRY_TOPICS.PUBSUB_RESULT]);
    expect(metrics.retryCount).toBe(0);
    expect(metrics.publishFailure).toBe(1);
  });

  it('retries transient errors and routes exhausted attempts to DLQ', async () => {
    const publishedTopics: string[] = [];
    let commitCount = 0;
    const metrics = {
      consumed: 0,
      processed: 0,
      publishSuccess: 0,
      publishFailure: 0,
      retryCount: 0,
      dlqCount: 0,
      consumerLag: 0,
    };

    const status = await processAuthorizedEnvelope(
      makeMessage('evt-retry'),
      async () => {
        commitCount += 1;
      },
      {
        config: makeConfig({ maxRetries: 2 }),
        pubsub: {
          async start() {},
          async stop() {},
          async publish() {
            const error = new Error('network timeout') as Error & {
              retriable: boolean;
            };
            error.retriable = true;
            throw error;
          },
        },
        publisher: {
          async connect() {},
          async disconnect() {},
          async publish(topic) {
            publishedTopics.push(topic);
          },
        },
        idempotencyStore: {
          async has() {
            return false;
          },
          async mark() {},
        },
        retryStore: new InMemoryRetryCounterStore(),
        metrics,
      }
    );

    expect(status).toBe('dlq');
    expect(publishedTopics).toEqual([
      TELEMETRY_TOPICS.RETRY,
      TELEMETRY_TOPICS.DLQ,
    ]);
    expect(metrics.retryCount).toBe(1);
    expect(metrics.dlqCount).toBe(1);
    expect(commitCount).toBe(0);
  });

  it('is idempotent for repeated event_id processing', async () => {
    const processedEventIds = new Set<string>();
    let publishCount = 0;
    let commitCount = 0;
    const metrics = {
      consumed: 0,
      processed: 0,
      publishSuccess: 0,
      publishFailure: 0,
      retryCount: 0,
      dlqCount: 0,
      consumerLag: 0,
    };

    const context = {
      config: makeConfig(),
      pubsub: {
        async start() {},
        async stop() {},
        async publish() {
          publishCount += 1;
        },
      },
      publisher: {
        async connect() {},
        async disconnect() {},
        async publish() {},
      },
      idempotencyStore: {
        async has(eventId: string) {
          return processedEventIds.has(eventId);
        },
        async mark(eventId: string) {
          processedEventIds.add(eventId);
        },
      },
      retryStore: new InMemoryRetryCounterStore(),
      metrics,
    };

    await processAuthorizedEnvelope(
      makeMessage('evt-dup'),
      async () => {
        commitCount += 1;
      },
      context
    );
    const secondStatus = await processAuthorizedEnvelope(
      makeMessage('evt-dup'),
      async () => {
        commitCount += 1;
      },
      context
    );

    expect(secondStatus).toBe('duplicate');
    expect(publishCount).toBe(1);
    expect(commitCount).toBe(2);
  });
});
