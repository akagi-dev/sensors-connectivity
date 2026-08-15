import { TELEMETRY_TOPICS } from './topics.js';

export type DedupKey = 'event_id' | 'batch_id' | 'cid';

export interface DedupStore {
  has(keyType: DedupKey, keyValue: string): Promise<boolean>;
  setProcessed(keyType: DedupKey, keyValue: string): Promise<void>;
}

export interface RetryDlqPublisher<TEvent> {
  publishRetry(
    event: TEvent,
    reason: string,
    context?: FailureContext
  ): Promise<void>;
  publishDlq(
    event: TEvent,
    reason: string,
    context?: FailureContext
  ): Promise<void>;
}

export interface FailureContext {
  topic: typeof TELEMETRY_TOPICS.RETRY | typeof TELEMETRY_TOPICS.DLQ;
  reason: string;
  eventId: string | undefined;
  attempt: number;
  maxAttempts: number;
  failedAt: string;
}

export interface RetryCounterStore {
  getAttempts(eventId: string): Promise<number>;
  setAttempts(eventId: string, attempts: number): Promise<void>;
  clearAttempts(eventId: string): Promise<void>;
}

export interface BoundedRetryPolicy<TEvent> {
  maxAttempts: number;
  getEventId: (event: TEvent) => string | undefined;
  store: RetryCounterStore;
}

export interface EventIdempotencyHook<TEvent> {
  getEventId: (event: TEvent) => string | undefined;
  hasProcessed: (eventId: string) => Promise<boolean>;
  markProcessed: (eventId: string) => Promise<void>;
}

export interface ConsumerFlowHandlers<TEvent, TResultEvent> {
  dedup?: {
    keyType: DedupKey;
    getKeyValue: (event: TEvent) => string | undefined;
    store: DedupStore;
  };
  maxRetries?: number;
  retryPolicy?: BoundedRetryPolicy<TEvent>;
  idempotency?: EventIdempotencyHook<TEvent>;
  performExternalAction: (event: TEvent) => Promise<void>;
  waitForConfirmation: (event: TEvent) => Promise<void>;
  emitResultEvent: (event: TEvent) => Promise<TResultEvent>;
  publishResultEvent: (resultEvent: TResultEvent) => Promise<void>;
  commitOffset: () => Promise<void>;
  retryDlqPublisher: RetryDlqPublisher<TEvent>;
}

export async function runConsumerProcessingRule<TEvent, TResultEvent>(
  event: TEvent,
  handlers: ConsumerFlowHandlers<TEvent, TResultEvent>
): Promise<'processed' | 'duplicate' | 'retried' | 'dlq'> {
  const maxAttempts =
    handlers.retryPolicy?.maxAttempts ?? handlers.maxRetries ?? 3;
  const eventIdFromRetryPolicy = handlers.retryPolicy?.getEventId(event);

  if (handlers.dedup) {
    const keyValue = handlers.dedup.getKeyValue(event);
    if (keyValue) {
      const alreadyProcessed = await handlers.dedup.store.has(
        handlers.dedup.keyType,
        keyValue
      );
      if (alreadyProcessed) {
        await handlers.commitOffset();
        return 'duplicate';
      }
    }
  }

  if (handlers.idempotency) {
    const eventId = handlers.idempotency.getEventId(event);
    if (eventId && (await handlers.idempotency.hasProcessed(eventId))) {
      if (handlers.retryPolicy && eventIdFromRetryPolicy)
        await handlers.retryPolicy.store.clearAttempts(eventIdFromRetryPolicy);
      await handlers.commitOffset();
      return 'duplicate';
    }
  }

  try {
    // Ordered guardrail: consume -> side effect -> confirmation -> result event -> offset commit
    await handlers.performExternalAction(event);
    await handlers.waitForConfirmation(event);
    const resultEvent = await handlers.emitResultEvent(event);
    await handlers.publishResultEvent(resultEvent);

    if (handlers.dedup) {
      const keyValue = handlers.dedup.getKeyValue(event);
      if (keyValue) {
        await handlers.dedup.store.setProcessed(
          handlers.dedup.keyType,
          keyValue
        );
      }
    }

    if (handlers.idempotency) {
      const eventId = handlers.idempotency.getEventId(event);
      if (eventId) {
        await handlers.idempotency.markProcessed(eventId);
      }
    }

    if (handlers.retryPolicy && eventIdFromRetryPolicy) {
      await handlers.retryPolicy.store.clearAttempts(eventIdFromRetryPolicy);
    }

    await handlers.commitOffset();
    return 'processed';
  } catch (error) {
    const reason =
      error instanceof Error ? error.message : 'Unknown processing error';
    const attempt = await nextAttempt(
      eventIdFromRetryPolicy,
      handlers.retryPolicy
    );

    if (
      handlers.retryPolicy &&
      eventIdFromRetryPolicy &&
      attempt < maxAttempts
    ) {
      const context: FailureContext = {
        topic: TELEMETRY_TOPICS.RETRY,
        reason,
        eventId: eventIdFromRetryPolicy,
        attempt,
        maxAttempts,
        failedAt: new Date().toISOString(),
      };
      await handlers.retryDlqPublisher.publishRetry(
        event,
        `${TELEMETRY_TOPICS.RETRY}:${reason}`,
        context
      );
      return 'retried';
    }

    const context: FailureContext = {
      topic: TELEMETRY_TOPICS.DLQ,
      reason,
      eventId: eventIdFromRetryPolicy,
      attempt,
      maxAttempts,
      failedAt: new Date().toISOString(),
    };
    await handlers.retryDlqPublisher.publishDlq(
      event,
      `${TELEMETRY_TOPICS.DLQ}:${reason}`,
      context
    );
    return 'dlq';
  }
}

async function nextAttempt<TEvent>(
  eventId: string | undefined,
  retryPolicy: BoundedRetryPolicy<TEvent> | undefined
): Promise<number> {
  if (!retryPolicy || !eventId) {
    return 1;
  }

  const previousAttempts = await retryPolicy.store.getAttempts(eventId);
  const currentAttempt = previousAttempts + 1;
  await retryPolicy.store.setAttempts(eventId, currentAttempt);
  return currentAttempt;
}

export class InMemoryDedupStore implements DedupStore {
  private readonly seen = new Set<string>();

  async has(keyType: DedupKey, keyValue: string): Promise<boolean> {
    return this.seen.has(`${keyType}:${keyValue}`);
  }

  async setProcessed(keyType: DedupKey, keyValue: string): Promise<void> {
    this.seen.add(`${keyType}:${keyValue}`);
  }
}

export class InMemoryRetryCounterStore implements RetryCounterStore {
  private readonly attempts = new Map<string, number>();

  async getAttempts(eventId: string): Promise<number> {
    return this.attempts.get(eventId) ?? 0;
  }

  async setAttempts(eventId: string, attempts: number): Promise<void> {
    this.attempts.set(eventId, attempts);
  }

  async clearAttempts(eventId: string): Promise<void> {
    this.attempts.delete(eventId);
  }
}
