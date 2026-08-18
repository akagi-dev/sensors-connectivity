import {
  TELEMETRY_TOPICS,
  TelemetryAuthorizedPayloadSchema,
  EnvelopeSchema,
} from '@scp/core';
import { create, toBinary } from '@bufbuild/protobuf';
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
    pubsubTopic: 'telemetry/authorized/v1',
    ipfsApiUrl: 'http://localhost:5001',
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

  const envelope = create(EnvelopeSchema, {
    eventId: 'evt-int-1',
    eventType: TELEMETRY_TOPICS.AUTHORIZED,
    eventVersion: 'v1',
    occurredAt: '2026-01-01T00:00:00Z',
    source: 'endpoint',
    payload: toBinary(TelemetryAuthorizedPayloadSchema, payload),
  });

  return Buffer.from(toBinary(EnvelopeSchema, envelope));
}

const authorizedMessage = createAuthorizedMessage();

describe('pubsub broadcaster integration flow (mock harness)', () => {
  it('processes consume -> publish flow with autocommit', async () => {
    const callOrder: string[] = [];

    const fakeConsumer = {
      async consume() {
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
      async close() {},
    };

    const service = createPubsubBroadcasterService(testConfig({}), {
      createConsumer: () => fakeConsumer as unknown as Consumer,
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

    expect(callOrder).toEqual(['publish']);
    const metrics = service.getMetrics();
    expect(metrics.consumed).toBe(1);
    expect(metrics.publishSuccess).toBe(1);
    expect(metrics.publishFailure).toBe(0);
  });

  it('handles publish failures gracefully without retry', async () => {
    const callOrder: string[] = [];

    const fakeConsumer = {
      async consume() {
        return (async function* () {
          yield {
            topic: TELEMETRY_TOPICS.AUTHORIZED,
            partition: 0,
            offset: 0n,
            value: authorizedMessage,
          };
        })();
      },
      async close() {},
    };

    const service = createPubsubBroadcasterService(testConfig({}), {
      createConsumer: () => fakeConsumer as unknown as Consumer,
      createPubsubClient: async () => ({
        async start() {},
        async stop() {},
        async publish() {
          callOrder.push('publish-attempt');
          throw new Error('network timeout');
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

    expect(callOrder).toEqual(['publish-attempt']);
    const metrics = service.getMetrics();
    expect(metrics.consumed).toBe(1);
    expect(metrics.publishSuccess).toBe(0);
    expect(metrics.publishFailure).toBe(1);
  });
});
