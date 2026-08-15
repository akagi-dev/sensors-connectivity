import { InMemoryRetryCounterStore, TELEMETRY_TOPICS } from '@scp/contracts';
import { describe, expect, it } from 'vitest';
import { processAuthorizedEnvelope, type AuthorizedTelemetryEnvelope } from '../src/index.js';
import type { PubsubBroadcasterConfig } from '../src/config.js';

function makeEnvelope(eventId: string): AuthorizedTelemetryEnvelope {
  return {
    event_id: eventId,
    event_type: TELEMETRY_TOPICS.AUTHORIZED,
    event_version: 'v1',
    occurred_at: '2026-01-01T00:00:00Z',
    trace_id: 'trace-1',
    source: 'endpoint',
    payload: {
      sensor_id: 'sensor-1',
      timestamp: '2026-01-01T00:00:00Z',
      nonce: 'nonce-1',
      measurements: { temp: 21 },
      signature: '0xabc'
    }
  };
}

function makeConfig(overrides: Partial<PubsubBroadcasterConfig> = {}): PubsubBroadcasterConfig {
  return {
    kafkaBrokers: ['localhost:9092'],
    consumerGroupId: 'pubsub-broadcaster-v1',
    source: 'pubsub-broadcaster',
    healthPort: 3020,
    maxRetries: 2,
    retryBackoffMs: 0,
    pubsubTopic: 'telemetry/authorized/v1',
    reservedPeers: [],
    ...overrides
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
      consumerLag: 0
    };

    const status = await processAuthorizedEnvelope(
      makeEnvelope('evt-success'),
      async () => {},
      {
        config: makeConfig(),
        pubsub: {
          async start() {},
          async stop() {},
          async connectReservedPeers() {},
          async publish(topic) {
            publishedTopics.push(topic);
          }
        },
        publisher: {
          async connect() {},
          async disconnect() {},
          async publish(topic) {
            resultTopics.push(topic);
          }
        },
        idempotencyStore: {
          async has() {
            return false;
          },
          async mark() {}
        },
        retryStore: new InMemoryRetryCounterStore(),
        metrics
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
      consumerLag: 0
    };

    const status = await processAuthorizedEnvelope(
      makeEnvelope('evt-terminal'),
      async () => {},
      {
        config: makeConfig(),
        pubsub: {
          async start() {},
          async stop() {},
          async connectReservedPeers() {},
          async publish() {
            throw new Error('signature format invalid');
          }
        },
        publisher: {
          async connect() {},
          async disconnect() {},
          async publish(topic, _key, envelope) {
            publishedTopics.push(topic);
            if (topic === TELEMETRY_TOPICS.PUBSUB_RESULT) {
              expect(envelope.payload).toMatchObject({ status: 'failed' });
            }
          }
        },
        idempotencyStore: {
          async has() {
            return false;
          },
          async mark() {}
        },
        retryStore: new InMemoryRetryCounterStore(),
        metrics
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
      consumerLag: 0
    };

    const status = await processAuthorizedEnvelope(
      makeEnvelope('evt-retry'),
      async () => {
        commitCount += 1;
      },
      {
        config: makeConfig({ maxRetries: 2 }),
        pubsub: {
          async start() {},
          async stop() {},
          async connectReservedPeers() {},
          async publish() {
            const error = new Error('network timeout') as Error & { retriable: boolean };
            error.retriable = true;
            throw error;
          }
        },
        publisher: {
          async connect() {},
          async disconnect() {},
          async publish(topic) {
            publishedTopics.push(topic);
          }
        },
        idempotencyStore: {
          async has() {
            return false;
          },
          async mark() {}
        },
        retryStore: new InMemoryRetryCounterStore(),
        metrics
      }
    );

    expect(status).toBe('dlq');
    expect(publishedTopics).toEqual([TELEMETRY_TOPICS.RETRY, TELEMETRY_TOPICS.DLQ]);
    expect(metrics.retryCount).toBe(1);
    expect(metrics.dlqCount).toBe(1);
    expect(commitCount).toBe(1);
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
      consumerLag: 0
    };

    const context = {
      config: makeConfig(),
      pubsub: {
        async start() {},
        async stop() {},
        async connectReservedPeers() {},
        async publish() {
          publishCount += 1;
        }
      },
      publisher: {
        async connect() {},
        async disconnect() {},
        async publish() {}
      },
      idempotencyStore: {
        async has(eventId: string) {
          return processedEventIds.has(eventId);
        },
        async mark(eventId: string) {
          processedEventIds.add(eventId);
        }
      },
      retryStore: new InMemoryRetryCounterStore(),
      metrics
    };

    await processAuthorizedEnvelope(makeEnvelope('evt-dup'), async () => {
      commitCount += 1;
    }, context);
    const secondStatus = await processAuthorizedEnvelope(makeEnvelope('evt-dup'), async () => {
      commitCount += 1;
    }, context);

    expect(secondStatus).toBe('duplicate');
    expect(publishCount).toBe(1);
    expect(commitCount).toBe(2);
  });
});
