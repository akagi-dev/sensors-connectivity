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
import { TELEMETRY_TOPICS, type TelemetryAuthorizedPayload } from '@scp/core';
import { canonicalize } from 'json-canonicalize';
import { createHash } from 'node:crypto';

export interface AuthorizedTelemetryEnvelope {
  eventId: string;
  eventType: typeof TELEMETRY_TOPICS.AUTHORIZED;
  eventVersion: string;
  occurredAt: string;
  traceId?: string;
  source: string;
  payload: TelemetryAuthorizedPayload;
}

export interface AuthorizedBatchEntry {
  topic: string;
  partition: number;
  offset: string;
  envelope: AuthorizedTelemetryEnvelope;
  commitOffset?: () => Promise<void>;
}

export interface AuthorizedBatchIdContext {
  topic: string;
  partition: number;
  entries: ReadonlyArray<{ offset: string; eventId: string }>;
}

export interface SealedAuthorizedBatch {
  batchId: string;
  topic: string;
  partition: number;
  firstOffset: string;
  lastOffset: string;
  entries: readonly AuthorizedBatchEntry[];
}

export interface AuthorizedBatchAccumulatorOptions {
  maxEvents: number;
  maxWaitMs: number;
}

interface OpenAuthorizedBatch {
  openedAt: number;
  entries: AuthorizedBatchEntry[];
}

export function deriveAuthorizedBatchId(
  context: AuthorizedBatchIdContext
): string {
  validateContext(context);
  const descriptor = {
    schema_version: 'telemetry-ipfs-batch-descriptor/v1',
    topic: context.topic,
    partition: context.partition,
    first_offset: context.entries[0]!.offset,
    last_offset: context.entries.at(-1)!.offset,
    events: context.entries.map((entry) => ({
      offset: entry.offset,
      event_id: entry.eventId,
    })),
  };
  const bytes = new TextEncoder().encode(canonicalize(descriptor));
  return 'batch-v1-' + createHash('sha256').update(bytes).digest('hex');
}

export class AuthorizedBatchAccumulator {
  private readonly openBatches = new Map<string, OpenAuthorizedBatch>();

  constructor(private readonly options: AuthorizedBatchAccumulatorOptions) {
    if (!Number.isInteger(options.maxEvents) || options.maxEvents < 1) {
      throw new Error('Batch maxEvents must be a positive integer');
    }
    if (!Number.isInteger(options.maxWaitMs) || options.maxWaitMs < 1) {
      throw new Error('Batch maxWaitMs must be a positive integer');
    }
  }

  add(
    entry: AuthorizedBatchEntry,
    receivedAt: number = Date.now()
  ): SealedAuthorizedBatch | undefined {
    validateEntry(entry);
    const key = partitionKey(entry.topic, entry.partition);
    const openBatch = this.openBatches.get(key) ?? {
      openedAt: receivedAt,
      entries: [],
    };
    if (openBatch.entries.some((current) => current.offset === entry.offset)) {
      throw new Error(
        `Duplicate Kafka offset ${entry.offset} for ${entry.topic} partition ${entry.partition}`
      );
    }
    openBatch.entries.push(entry);
    openBatch.entries.sort(compareEntriesByOffset);
    this.openBatches.set(key, openBatch);
    return openBatch.entries.length >= this.options.maxEvents
      ? this.seal(key, openBatch)
      : undefined;
  }

  flushExpired(now: number = Date.now()): SealedAuthorizedBatch[] {
    const sealed: SealedAuthorizedBatch[] = [];
    for (const [key, batch] of this.openBatches) {
      if (now - batch.openedAt >= this.options.maxWaitMs) {
        sealed.push(this.seal(key, batch));
      }
    }
    return sealed;
  }

  flushAll(): SealedAuthorizedBatch[] {
    return [...this.openBatches.entries()].map(([key, batch]) =>
      this.seal(key, batch)
    );
  }

  getBufferedEventCount(): number {
    return [...this.openBatches.values()].reduce(
      (count, batch) => count + batch.entries.length,
      0
    );
  }

  private seal(key: string, batch: OpenAuthorizedBatch): SealedAuthorizedBatch {
    this.openBatches.delete(key);
    const entries = Object.freeze(batch.entries.map(cloneEntry));
    const first = entries[0]!;
    const last = entries.at(-1)!;
    const context = {
      topic: first.topic,
      partition: first.partition,
      entries: entries.map((entry) => ({
        offset: entry.offset,
        eventId: entry.envelope.eventId,
      })),
    };
    return {
      batchId: deriveAuthorizedBatchId(context),
      topic: first.topic,
      partition: first.partition,
      firstOffset: first.offset,
      lastOffset: last.offset,
      entries,
    };
  }
}

function cloneEntry(entry: AuthorizedBatchEntry): AuthorizedBatchEntry {
  return {
    ...entry,
    envelope: {
      ...entry.envelope,
      payload: {
        ...entry.envelope.payload,
        sensorId: new Uint8Array(entry.envelope.payload.sensorId),
        signedEnvelope: new Uint8Array(entry.envelope.payload.signedEnvelope),
      },
    },
  };
}

function validateContext(context: AuthorizedBatchIdContext): void {
  if (context.topic.length === 0)
    throw new Error('Batch topic cannot be empty');
  if (!Number.isInteger(context.partition) || context.partition < 0) {
    throw new Error('Batch partition must be a non-negative integer');
  }
  if (context.entries.length === 0)
    throw new Error('Cannot derive an ID for an empty batch');
  let previousOffset: bigint | undefined;
  for (const entry of context.entries) {
    const offset = parseOffset(entry.offset);
    if (entry.eventId.length === 0)
      throw new Error('Batch event_id cannot be empty');
    if (previousOffset !== undefined && offset <= previousOffset) {
      throw new Error('Batch offsets must be strictly increasing');
    }
    previousOffset = offset;
  }
}

function validateEntry(entry: AuthorizedBatchEntry): void {
  if (entry.envelope.eventType !== TELEMETRY_TOPICS.AUTHORIZED) {
    throw new Error(
      `Unsupported batch event type: ${entry.envelope.eventType}`
    );
  }
  validateContext({
    topic: entry.topic,
    partition: entry.partition,
    entries: [{ offset: entry.offset, eventId: entry.envelope.eventId }],
  });
}

function parseOffset(offset: string): bigint {
  if (!/^\d+$/.test(offset)) throw new Error(`Invalid Kafka offset: ${offset}`);
  return BigInt(offset);
}

function compareEntriesByOffset(
  first: AuthorizedBatchEntry,
  second: AuthorizedBatchEntry
): number {
  const a = parseOffset(first.offset);
  const b = parseOffset(second.offset);
  return a < b ? -1 : a > b ? 1 : 0;
}

function partitionKey(topic: string, partition: number): string {
  return `${topic}:${partition}`;
}
