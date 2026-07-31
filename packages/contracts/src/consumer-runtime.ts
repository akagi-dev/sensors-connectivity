import { TELEMETRY_TOPICS } from './topics.js';

export type DedupKey = 'event_id' | 'batch_id' | 'cid';

export interface DedupStore {
  has(keyType: DedupKey, keyValue: string): Promise<boolean>;
  setProcessed(keyType: DedupKey, keyValue: string): Promise<void>;
}

export interface RetryDlqPublisher<TEvent> {
  publishRetry(event: TEvent, reason: string): Promise<void>;
  publishDlq(event: TEvent, reason: string): Promise<void>;
}

export interface ConsumerFlowHandlers<TEvent, TResultEvent> {
  dedup?: {
    keyType: DedupKey;
    getKeyValue: (event: TEvent) => string | undefined;
    store: DedupStore;
  };
  maxRetries?: number;
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
  const retryLimit = handlers.maxRetries ?? 3;

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

  try {
    await handlers.performExternalAction(event);
    await handlers.waitForConfirmation(event);
    const resultEvent = await handlers.emitResultEvent(event);
    await handlers.publishResultEvent(resultEvent);

    if (handlers.dedup) {
      const keyValue = handlers.dedup.getKeyValue(event);
      if (keyValue) {
        await handlers.dedup.store.setProcessed(handlers.dedup.keyType, keyValue);
      }
    }

    await handlers.commitOffset();
    return 'processed';
  } catch (error) {
    const reason = error instanceof Error ? error.message : 'Unknown processing error';

    if (retryLimit > 0) {
      // TODO: implement bounded retry counters in persistent store and move to retry topic.
      await handlers.retryDlqPublisher.publishRetry(
        event,
        `${TELEMETRY_TOPICS.RETRY}:${reason}`
      );
      return 'retried';
    }

    // TODO: emit telemetry.dlq.v1 with enriched failure context.
    await handlers.retryDlqPublisher.publishDlq(
      event,
      `${TELEMETRY_TOPICS.DLQ}:${reason}`
    );
    return 'dlq';
  }
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
