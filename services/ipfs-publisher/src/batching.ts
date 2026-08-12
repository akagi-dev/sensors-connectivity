import {
  TELEMETRY_TOPICS,
  type Envelope,
  type TelemetryAuthorizedPayload
} from '@scp/contracts';
import { canonicalize } from 'json-canonicalize';
import { createHash } from 'node:crypto';

export type AuthorizedTelemetryEnvelope = Envelope & {
  event_type: typeof TELEMETRY_TOPICS.AUTHORIZED;
  payload: TelemetryAuthorizedPayload;
};

export interface AuthorizedBatchEntry {
  topic: string;
  partition: number;
  offset: string;
  envelope: AuthorizedTelemetryEnvelope;
}

export interface AuthorizedBatchIdContext {
  topic: string;
  partition: number;
  entries: ReadonlyArray<{
    offset: string;
    eventId: string;
  }>;
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

/** Derives a stable batch identifier from Kafka positions and ordered event identifiers. */
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
      event_id: entry.eventId
    }))
  };
  const bytes = new TextEncoder().encode(canonicalize(descriptor));
  return 'batch-v1-' + createHash('sha256').update(bytes).digest('hex');
}

/**
 * Accumulates authorized events separately for each Kafka partition and seals
 * immutable batches when the configured size or wait-time limit is reached.
 */
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

  /** Adds an event and returns a sealed batch when the size limit is reached. */
  add(
    entry: AuthorizedBatchEntry,
    receivedAt: number = Date.now()
  ): SealedAuthorizedBatch | undefined {
    validateEntry(entry);
    const key = partitionKey(entry.topic, entry.partition);
    const openBatch = this.openBatches.get(key) ?? {
      openedAt: receivedAt,
      entries: []
    };
    if (openBatch.entries.some((current) => current.offset === entry.offset)) {
      throw new Error(
        `Duplicate Kafka offset ${entry.offset} for ${entry.topic} partition ${entry.partition}`
      );
    }

    openBatch.entries.push(entry);
    openBatch.entries.sort(compareEntriesByOffset);
    this.openBatches.set(key, openBatch);

    if (openBatch.entries.length >= this.options.maxEvents) {
      return this.seal(key, openBatch);
    }
    return undefined;
  }

  /** Seals and returns batches whose maximum wait time has elapsed. */
  flushExpired(now: number = Date.now()): SealedAuthorizedBatch[] {
    const sealed: SealedAuthorizedBatch[] = [];
    for (const [key, openBatch] of this.openBatches) {
      if (now - openBatch.openedAt >= this.options.maxWaitMs) {
        sealed.push(this.seal(key, openBatch));
      }
    }
    return sealed;
  }

  /** Seals all open batches, for example during graceful shutdown. */
  flushAll(): SealedAuthorizedBatch[] {
    return [...this.openBatches.entries()].map(([key, batch]) =>
      this.seal(key, batch)
    );
  }

  /** Returns the current number of buffered events across all partitions. */
  getBufferedEventCount(): number {
    let count = 0;
    for (const batch of this.openBatches.values()) {
      count += batch.entries.length;
    }
    return count;
  }

  private seal(
    key: string,
    openBatch: OpenAuthorizedBatch
  ): SealedAuthorizedBatch {
    this.openBatches.delete(key);
    const entries = Object.freeze(
      openBatch.entries.map((entry) => structuredClone(entry))
    );
    const first = entries[0]!;
    const last = entries.at(-1)!;
    const context = {
      topic: first.topic,
      partition: first.partition,
      entries: entries.map((entry) => ({
        offset: entry.offset,
        eventId: entry.envelope.event_id
      }))
    };

    return {
      batchId: deriveAuthorizedBatchId(context),
      topic: first.topic,
      partition: first.partition,
      firstOffset: first.offset,
      lastOffset: last.offset,
      entries
    };
  }
}

/** Validates Kafka metadata used to derive a deterministic batch identifier. */
function validateContext(context: AuthorizedBatchIdContext): void {
  if (context.topic.length === 0) {
    throw new Error('Batch topic cannot be empty');
  }
  if (!Number.isInteger(context.partition) || context.partition < 0) {
    throw new Error('Batch partition must be a non-negative integer');
  }
  if (context.entries.length === 0) {
    throw new Error('Cannot derive an ID for an empty batch');
  }

  let previousOffset: bigint | undefined;
  for (const entry of context.entries) {
    const offset = parseOffset(entry.offset);
    if (entry.eventId.length === 0) {
      throw new Error('Batch event_id cannot be empty');
    }
    if (previousOffset !== undefined && offset <= previousOffset) {
      throw new Error('Batch offsets must be strictly increasing');
    }
    previousOffset = offset;
  }
}

/** Validates an event before adding it to the open batch for its partition. */
function validateEntry(entry: AuthorizedBatchEntry): void {
  if (entry.envelope.event_type !== TELEMETRY_TOPICS.AUTHORIZED) {
    throw new Error(
      `Unsupported batch event type: ${entry.envelope.event_type}`
    );
  }
  validateContext({
    topic: entry.topic,
    partition: entry.partition,
    entries: [{ offset: entry.offset, eventId: entry.envelope.event_id }]
  });
}

/** Parses a Kafka offset as an integer without losing 64-bit precision. */
function parseOffset(offset: string): bigint {
  if (!/^\d+$/.test(offset)) {
    throw new Error(`Invalid Kafka offset: ${offset}`);
  }
  return BigInt(offset);
}

/** Orders entries by Kafka offset using arbitrary-precision integers. */
function compareEntriesByOffset(
  first: AuthorizedBatchEntry,
  second: AuthorizedBatchEntry
): number {
  const firstOffset = parseOffset(first.offset);
  const secondOffset = parseOffset(second.offset);
  return firstOffset < secondOffset ? -1 : firstOffset > secondOffset ? 1 : 0;
}

/** Creates a stable Map key for a Kafka topic partition. */
function partitionKey(topic: string, partition: number): string {
  return `${topic}:${partition}`;
}
