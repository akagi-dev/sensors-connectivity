# `@scp/contracts`

Shared v1 integration contracts and consumer runtime primitives for the telemetry pipeline.

## Import topic constants

```ts
import { TELEMETRY_TOPICS } from '@scp/contracts';

console.log(TELEMETRY_TOPICS.AUTHORIZED); // telemetry.authorized.v1
```

## Validate envelopes and payloads

```ts
import { validateEnvelopeWithKnownPayload } from '@scp/contracts';

const result = validateEnvelopeWithKnownPayload(message);
if (!result.success) {
  // reject / dead-letter invalid contract payloads
}
```

## Wire bounded retry + DLQ in a consumer

```ts
import {
  InMemoryRetryCounterStore,
  runConsumerProcessingRule,
  TELEMETRY_TOPICS
} from '@scp/contracts';

await runConsumerProcessingRule(event, {
  retryPolicy: {
    maxAttempts: 3,
    getEventId: (current) => current.event_id,
    store: new InMemoryRetryCounterStore()
  },
  performExternalAction: async () => {},
  waitForConfirmation: async () => {},
  emitResultEvent: async () => ({ event_type: TELEMETRY_TOPICS.PUBSUB_RESULT }),
  publishResultEvent: async () => {},
  commitOffset: async () => {},
  retryDlqPublisher: {
    async publishRetry(_event, _reason, _context) {},
    async publishDlq(_event, _reason, _context) {}
  }
});
```
