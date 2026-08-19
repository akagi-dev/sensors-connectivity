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
import { TELEMETRY_TOPICS } from '@scp/core';
import { describe, expect, it } from 'vitest';
import {
  AuthorizedBatchAccumulator,
  deriveAuthorizedBatchId,
  type AuthorizedBatchEntry,
} from '../src/batching.js';
import { authorizedEnvelope } from './helpers.js';

function entry(
  offset: string,
  eventId: string,
  partition = 0
): AuthorizedBatchEntry {
  return {
    topic: TELEMETRY_TOPICS.AUTHORIZED,
    partition,
    offset,
    envelope: authorizedEnvelope(eventId, Number(offset) || 1),
  };
}

describe('authorized batch identity and accumulation', () => {
  it('derives a stable ID without losing 64-bit offsets', () => {
    const context = {
      topic: TELEMETRY_TOPICS.AUTHORIZED,
      partition: 2,
      entries: [
        { offset: '9007199254740993', eventId: 'event-1' },
        { offset: '9007199254740994', eventId: 'event-2' },
      ],
    };
    expect(deriveAuthorizedBatchId(context)).toBe(
      deriveAuthorizedBatchId(structuredClone(context))
    );
  });

  it('sorts and seals batches independently per partition', () => {
    const accumulator = new AuthorizedBatchAccumulator({
      maxEvents: 2,
      maxWaitMs: 100,
    });
    expect(accumulator.add(entry('2', 'event-2'), 0)).toBeUndefined();
    const sealed = accumulator.add(entry('1', 'event-1'), 1);
    expect(sealed?.entries.map((item) => item.offset)).toEqual(['1', '2']);
    accumulator.add(entry('5', 'event-5', 1), 1_000);
    expect(accumulator.flushExpired(1_100)).toHaveLength(1);
  });

  it('preserves the manual commit callback while cloning binary payloads', () => {
    const commitOffset = async (): Promise<void> => {};
    const accumulator = new AuthorizedBatchAccumulator({
      maxEvents: 1,
      maxWaitMs: 100,
    });
    const sealed = accumulator.add({ ...entry('1', 'event-1'), commitOffset });
    expect(sealed?.entries[0]?.commitOffset).toBe(commitOffset);
    expect(sealed?.entries[0]?.envelope.payload.sensorId).not.toBe(
      entry('1', 'event-1').envelope.payload.sensorId
    );
  });

  it('rejects duplicate offsets in an open partition', () => {
    const accumulator = new AuthorizedBatchAccumulator({
      maxEvents: 3,
      maxWaitMs: 100,
    });
    accumulator.add(entry('1', 'event-1'));
    expect(() => accumulator.add(entry('1', 'event-2'))).toThrow(
      'Duplicate Kafka offset'
    );
  });
});
