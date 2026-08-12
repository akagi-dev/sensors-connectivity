import { TELEMETRY_TOPICS } from '@scp/contracts';
import type { Consumer, EachMessagePayload } from 'kafkajs';
import type { Server } from 'node:http';
import { describe, expect, it } from 'vitest';
import type { IpfsBatchClient } from '../src/batch-publisher.js';
import { startIpfsPublisher } from '../src/service.js';
import type {
  IpfsResultEnvelope,
  IpfsResultPublisher
} from '../src/result-publisher.js';

const testCid = 'bafybeideterministic';

/** Creates an authorized envelope for the consumer smoke test. */
function authorizedEnvelope(eventId: string): string {
  return JSON.stringify({
    event_id: eventId,
    event_type: TELEMETRY_TOPICS.AUTHORIZED,
    event_version: 'v1',
    occurred_at: '2026-08-03T13:00:00.000Z',
    source: 'authorizer',
    payload: {
      sensor_id: `sensor-${eventId}`,
      timestamp: '2026-08-03T13:00:00.000Z',
      nonce: `nonce-${eventId}`,
      measurements: { temperature: 22 },
      signature: '0x01'
    }
  });
}

describe('ipfs-publisher Kafka consumer smoke', () => {
  it('subscribes, batches authorized messages, and shuts down', async () => {
    const calls: string[] = [];
    let publishedArtifact:
      { batch_id?: string; event_count?: number } | undefined;
    let publishedResult:
      { batchId: string; envelope: IpfsResultEnvelope } | undefined;
    const fakeIpfs: IpfsBatchClient = {
      async add(content) {
        calls.push('ipfs.add');
        publishedArtifact = JSON.parse(
          new TextDecoder().decode(content)
        ) as typeof publishedArtifact;
        return { cid: { toString: () => testCid } };
      },
      pin: {
        async add() {
          calls.push('pin.add');
          return { toString: () => testCid };
        },
        async *ls() {
          calls.push('pin.ls');
          yield { cid: { toString: () => testCid } };
        }
      }
    };
    const fakeResultPublisher: IpfsResultPublisher = {
      async connect() {
        calls.push('result.connect');
      },
      async disconnect() {
        calls.push('result.disconnect');
      },
      async publish(batchId, envelope) {
        calls.push('result.publish');
        publishedResult = { batchId, envelope };
      },
      async publishDlq() {
        calls.push('result.publishDlq');
      }
    };
    const fakeConsumer = {
      async connect() {
        calls.push('consumer.connect');
      },
      async subscribe(options: { topic: string }) {
        calls.push(`consumer.subscribe:${options.topic}`);
      },
      async run(options: {
        autoCommit?: boolean;
        eachMessage: (payload: EachMessagePayload) => Promise<void>;
      }) {
        calls.push(`consumer.run:autoCommit=${String(options.autoCommit)}`);
        for (const [offset, eventId] of [
          ['7', 'event-1'],
          ['8', 'event-2']
        ] as const) {
          const encodedEnvelope = authorizedEnvelope(eventId);
          await options.eachMessage({
            topic: TELEMETRY_TOPICS.AUTHORIZED,
            partition: 0,
            message: {
              key: null,
              value: Buffer.from(encodedEnvelope),
              timestamp: '0',
              attributes: 0,
              offset,
              size: Buffer.byteLength(encodedEnvelope)
            },
            heartbeat: async () => {},
            pause: () => () => {}
          });
        }
      },
      async stop() {
        calls.push('consumer.stop');
      },
      async commitOffsets(
        offsets: Array<{ topic: string; partition: number; offset: string }>
      ) {
        for (const offset of offsets) {
          calls.push(
            `consumer.commit:${offset.topic}:${offset.partition}:${offset.offset}`
          );
        }
      },
      async disconnect() {
        calls.push('consumer.disconnect');
      }
    };

    const service = await startIpfsPublisher({
      ipfs: fakeIpfs,
      createConsumer: () => fakeConsumer as unknown as Consumer,
      resultPublisher: fakeResultPublisher,
      createHealthServer: (_metrics, port) => {
        calls.push(`health.listen:${port}`);
        return {
          close(callback: (error?: Error) => void) {
            calls.push('health.close');
            callback();
          }
        } as unknown as Server;
      }
    });
    expect(service.isStarted()).toBe(true);

    await service.stop();

    expect(service.isStarted()).toBe(false);
    expect(service.getMetrics()).toMatchObject({
      batchCount: 1,
      pinCount: 1,
      retryCount: 0,
      dlqCount: 0
    });
    expect(publishedArtifact?.event_count).toBe(2);
    expect(calls.filter((call) => call === 'ipfs.add')).toHaveLength(1);
    expect(calls.slice(0, 4)).toEqual([
      'result.connect',
      'consumer.connect',
      `consumer.subscribe:${TELEMETRY_TOPICS.AUTHORIZED}`,
      'health.listen:3040'
    ]);
    expect(calls).toContain('consumer.run:autoCommit=false');
    expect(calls).toEqual(
      expect.arrayContaining([
        'ipfs.add',
        'consumer.stop',
        'health.close',
        'consumer.disconnect',
        'pin.add',
        'pin.ls',
        'result.publish',
        `consumer.commit:${TELEMETRY_TOPICS.AUTHORIZED}:0:9`,
        'result.disconnect'
      ])
    );
    expect(calls.indexOf('ipfs.add')).toBeLessThan(calls.indexOf('pin.add'));
    expect(calls.indexOf('pin.add')).toBeLessThan(calls.indexOf('pin.ls'));
    expect(calls.indexOf('pin.ls')).toBeLessThan(
      calls.indexOf('result.publish')
    );
    expect(calls.indexOf('result.publish')).toBeLessThan(
      calls.indexOf(`consumer.commit:${TELEMETRY_TOPICS.AUTHORIZED}:0:9`)
    );
    expect(
      calls.indexOf(`consumer.commit:${TELEMETRY_TOPICS.AUTHORIZED}:0:9`)
    ).toBeLessThan(calls.indexOf('consumer.disconnect'));
    expect(calls.indexOf('consumer.stop')).toBeLessThan(
      calls.indexOf('consumer.disconnect')
    );
    expect(calls.indexOf('pin.ls')).toBeLessThan(
      calls.indexOf('consumer.disconnect')
    );
    expect(calls.indexOf('consumer.disconnect')).toBeLessThan(
      calls.indexOf('result.disconnect')
    );
    expect(publishedResult).toMatchObject({
      batchId: publishedArtifact?.batch_id,
      envelope: {
        event_type: TELEMETRY_TOPICS.IPFS_RESULT,
        event_version: 'v1',
        source: 'ipfs-publisher',
        payload: { cid: testCid, event_count: 2 }
      }
    });
  });
});
