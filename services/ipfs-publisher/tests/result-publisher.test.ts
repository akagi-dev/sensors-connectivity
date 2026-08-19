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
import { fromBinary } from '@bufbuild/protobuf';
import type { Producer } from '@platformatic/kafka';
import {
  EnvelopeSchema,
  TELEMETRY_TOPICS,
  TelemetryIpfsPublishedPayloadSchema,
} from '@scp/core';
import { describe, expect, it, vi } from 'vitest';
import {
  buildIpfsDlqEnvelope,
  buildIpfsResultEnvelope,
  createKafkaIpfsResultPublisher,
} from '../src/result-publisher.js';

describe('IPFS Kafka result publisher', () => {
  it('builds the current protobuf result envelope', () => {
    const result = buildIpfsResultEnvelope('bafy-result', 2, {
      eventId: 'result-event-1',
      occurredAt: '2026-08-11T00:00:00.000Z',
    });
    expect(result.envelope).toMatchObject({
      eventId: 'result-event-1',
      eventType: TELEMETRY_TOPICS.IPFS_PUBLISHED,
      eventVersion: 'v1',
      source: 'ipfs-publisher',
    });
    expect(new TextDecoder().decode(result.payload.cid)).toBe('bafy-result');
    expect(result.payload.eventCount).toBe(2);
    expect(
      fromBinary(TelemetryIpfsPublishedPayloadSchema, result.envelope.payload)
    ).toEqual(result.payload);
  });

  it('publishes protobuf bytes with batch_id as the Kafka key', async () => {
    const send = vi.fn(async () => undefined);
    const close = vi.fn(async () => undefined);
    const publisher = createKafkaIpfsResultPublisher({
      send,
      close,
    } as unknown as Producer);
    const result = buildIpfsResultEnvelope('bafy-result', 2);
    await publisher.publish('batch-v1-result', result);
    const message = send.mock.calls[0]![0].messages[0]!;
    expect(message.topic).toBe(TELEMETRY_TOPICS.IPFS_PUBLISHED);
    expect(message.key).toEqual(Buffer.from('batch-v1-result'));
    const decoded = fromBinary(EnvelopeSchema, message.value);
    expect(decoded).toMatchObject({
      eventId: result.envelope.eventId,
      eventType: TELEMETRY_TOPICS.IPFS_PUBLISHED,
    });
    expect([...decoded.payload]).toEqual([...result.envelope.payload]);
    await publisher.disconnect();
    expect(close).toHaveBeenCalledOnce();
  });

  it('publishes detailed DLQ context inside a protobuf envelope', async () => {
    const send = vi.fn(async () => undefined);
    const publisher = createKafkaIpfsResultPublisher({
      send,
      close: vi.fn(async () => undefined),
    } as unknown as Producer);
    const dlq = buildIpfsDlqEnvelope(
      { batch_id: 'batch-v1-failed', events: [] },
      'ipfs_publish_or_pin_failed',
      'fetch failed',
      {
        topic: TELEMETRY_TOPICS.DLQ,
        reason: 'fetch failed',
        eventId: 'batch-v1-failed',
        attempt: 3,
        maxAttempts: 3,
        failedAt: '2026-08-11T00:00:00.000Z',
      }
    );
    await publisher.publishDlq('batch-v1-failed', dlq);
    const message = send.mock.calls[0]![0].messages[0]!;
    expect(message.topic).toBe(TELEMETRY_TOPICS.DLQ);
    const decoded = fromBinary(EnvelopeSchema, message.value);
    expect(decoded.eventType).toBe(TELEMETRY_TOPICS.DLQ);
    expect(JSON.parse(new TextDecoder().decode(decoded.payload))).toEqual(
      dlq.payload
    );
  });
});
