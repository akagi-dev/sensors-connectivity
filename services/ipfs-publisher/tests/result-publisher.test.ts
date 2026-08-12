import {
  parseEnvelopeWithKnownPayload,
  TELEMETRY_TOPICS
} from '@scp/contracts';
import type { Producer } from 'kafkajs';
import { describe, expect, it, vi } from 'vitest';
import {
  buildIpfsDlqEnvelope,
  buildIpfsResultEnvelope,
  createKafkaIpfsResultPublisher
} from '../src/result-publisher.js';

describe('IPFS Kafka result publisher', () => {
  it('builds a WP-00 compatible result envelope', () => {
    const envelope = buildIpfsResultEnvelope('bafy-result', 2, {
      eventId: 'result-event-1',
      occurredAt: '2026-08-11T00:00:00.000Z'
    });

    expect(parseEnvelopeWithKnownPayload(envelope)).toEqual(envelope);
    expect(envelope).toEqual({
      event_id: 'result-event-1',
      event_type: TELEMETRY_TOPICS.IPFS_RESULT,
      event_version: 'v1',
      occurred_at: '2026-08-11T00:00:00.000Z',
      source: 'ipfs-publisher',
      payload: { cid: 'bafy-result', event_count: 2 }
    });
  });

  it('publishes the envelope with batch_id as Kafka key and waits for send', async () => {
    const connect = vi.fn(async () => undefined);
    const disconnect = vi.fn(async () => undefined);
    const send = vi.fn(async () => []);
    const publisher = createKafkaIpfsResultPublisher({
      connect,
      disconnect,
      send
    } as unknown as Producer);
    const envelope = buildIpfsResultEnvelope('bafy-result', 2, {
      eventId: 'result-event-1',
      occurredAt: '2026-08-11T00:00:00.000Z'
    });

    await publisher.connect();
    await publisher.publish('batch-v1-result', envelope);
    await publisher.disconnect();

    expect(connect).toHaveBeenCalledTimes(1);
    expect(send).toHaveBeenCalledWith({
      topic: TELEMETRY_TOPICS.IPFS_RESULT,
      messages: [
        {
          key: 'batch-v1-result',
          value: JSON.stringify(envelope)
        }
      ]
    });
    expect(disconnect).toHaveBeenCalledTimes(1);
  });

  it('propagates a Kafka send failure to the processing rule', async () => {
    const send = vi.fn(async () => {
      throw new Error('Kafka unavailable');
    });
    const publisher = createKafkaIpfsResultPublisher({
      connect: vi.fn(async () => undefined),
      disconnect: vi.fn(async () => undefined),
      send
    } as unknown as Producer);

    await expect(
      publisher.publish(
        'batch-v1-result',
        buildIpfsResultEnvelope('bafy-result', 1)
      )
    ).rejects.toThrow('Kafka unavailable');
  });

  it('publishes an exhausted batch to DLQ with failure context', async () => {
    const send = vi.fn(async () => []);
    const publisher = createKafkaIpfsResultPublisher({
      connect: vi.fn(async () => undefined),
      disconnect: vi.fn(async () => undefined),
      send
    } as unknown as Producer);
    const envelope = buildIpfsDlqEnvelope(
      { batch_id: 'batch-v1-failed', events: [] },
      'ipfs_publish_or_pin_failed',
      'fetch failed',
      {
        topic: TELEMETRY_TOPICS.DLQ,
        reason: 'fetch failed',
        eventId: 'batch-v1-failed',
        attempt: 3,
        maxAttempts: 3,
        failedAt: '2026-08-11T00:00:00.000Z'
      },
      {
        eventId: 'dlq-event-1',
        occurredAt: '2026-08-11T00:00:01.000Z'
      }
    );

    await publisher.publishDlq('batch-v1-failed', envelope);

    expect(send).toHaveBeenCalledWith({
      topic: TELEMETRY_TOPICS.DLQ,
      messages: [
        {
          key: 'batch-v1-failed',
          value: JSON.stringify(envelope)
        }
      ]
    });
  });
});
