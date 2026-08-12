import { TELEMETRY_TOPICS } from '@scp/contracts';
import { describe, expect, it } from 'vitest';
import {
  AuthorizedBatchAccumulator,
  deriveAuthorizedBatchId,
  type AuthorizedBatchEntry,
  type AuthorizedTelemetryEnvelope
} from '../src/batching.js';

/** Creates a valid authorized Kafka entry for batching tests. */
function entry(
  offset: string,
  eventId: string,
  partition = 0
): AuthorizedBatchEntry {
  const envelope: AuthorizedTelemetryEnvelope = {
    event_id: eventId,
    event_type: TELEMETRY_TOPICS.AUTHORIZED,
    event_version: 'v1',
    occurred_at: '2026-08-04T12:00:00.000Z',
    source: 'authorizer',
    payload: {
      sensor_id: `sensor-${eventId}`,
      timestamp: '2026-08-04T12:00:00.000Z',
      nonce: `nonce-${eventId}`,
      measurements: { temperature: 22 },
      signature: '0x01'
    }
  };
  return {
    topic: TELEMETRY_TOPICS.AUTHORIZED,
    partition,
    offset,
    envelope
  };
}

describe('authorized batch ID derivation', () => {
  it('is stable for an identical closed batch', () => {
    const context = {
      topic: TELEMETRY_TOPICS.AUTHORIZED,
      partition: 2,
      entries: [
        { offset: '9007199254740993', eventId: 'event-1' },
        { offset: '9007199254740994', eventId: 'event-2' }
      ]
    };

    expect(deriveAuthorizedBatchId(context)).toBe(
      deriveAuthorizedBatchId(structuredClone(context))
    );
  });

  it('changes when Kafka position or event identity changes', () => {
    const first = {
      topic: TELEMETRY_TOPICS.AUTHORIZED,
      partition: 0,
      entries: [{ offset: '1', eventId: 'event-1' }]
    };

    expect(
      deriveAuthorizedBatchId({
        ...first,
        partition: 1
      })
    ).not.toBe(deriveAuthorizedBatchId(first));
    expect(
      deriveAuthorizedBatchId({
        ...first,
        entries: [{ offset: '1', eventId: 'event-2' }]
      })
    ).not.toBe(deriveAuthorizedBatchId(first));
  });
});

describe('authorized batch accumulator', () => {
  it('seals per-partition batches at the maximum event count', () => {
    const accumulator = new AuthorizedBatchAccumulator({
      maxEvents: 2,
      maxWaitMs: 1_000
    });

    expect(accumulator.add(entry('2', 'event-2'), 0)).toBeUndefined();
    const sealed = accumulator.add(entry('1', 'event-1'), 1);

    expect(sealed).toMatchObject({
      partition: 0,
      firstOffset: '1',
      lastOffset: '2'
    });
    expect(sealed?.entries.map((current) => current.offset)).toEqual([
      '1',
      '2'
    ]);
    expect(Object.isFrozen(sealed?.entries)).toBe(true);
    expect(accumulator.getBufferedEventCount()).toBe(0);
  });

  it('keeps partitions independent and flushes batches after max wait', () => {
    const accumulator = new AuthorizedBatchAccumulator({
      maxEvents: 10,
      maxWaitMs: 100
    });
    accumulator.add(entry('1', 'event-1', 0), 1_000);
    accumulator.add(entry('5', 'event-5', 1), 1_050);

    expect(accumulator.flushExpired(1_099)).toEqual([]);
    expect(accumulator.flushExpired(1_100)).toHaveLength(1);
    expect(accumulator.getBufferedEventCount()).toBe(1);
    expect(accumulator.flushAll()).toHaveLength(1);
  });

  it('rejects duplicate offsets within an open partition batch', () => {
    const accumulator = new AuthorizedBatchAccumulator({
      maxEvents: 3,
      maxWaitMs: 100
    });
    accumulator.add(entry('1', 'event-1'));

    expect(() => accumulator.add(entry('1', 'event-2'))).toThrow(
      'Duplicate Kafka offset'
    );
  });
});
