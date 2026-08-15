import { describe, expect, it } from 'vitest';
import { envelopeSchema } from '../src/envelope.js';
import {
  telemetryAuthorizedPayloadSchema,
  telemetryRejectedPayloadSchema,
  telemetryPubsubResultPayloadSchema,
  telemetryIpfsPublishedPayloadSchema,
  telemetryBlockchainResultPayloadSchema
} from '../src/events.js';
import {
  InMemoryRetryCounterStore,
  runConsumerProcessingRule
} from '../src/consumer-runtime.js';
import { TELEMETRY_TOPICS } from '../src/topics.js';
import {
  parseEnvelopeWithKnownPayload,
  validateEnvelope,
  validateEnvelopeWithKnownPayload,
  validatePayloadForEventType
} from '../src/validation.js';
import { createSignedEnvelope, toSignedEnvelopeBytes, validateSignedEnvelope } from '../src/protobuf.js';

describe('contracts', () => {
  it('parses well-formed envelope + authorized payload', () => {
    const result = parseEnvelopeWithKnownPayload({
      event_id: 'evt-1',
      event_type: 'telemetry.authorized.v1',
      event_version: 'v1',
      occurred_at: '2026-01-01T00:00:00Z',
      source: 'endpoint',
      payload: {
        sensor_id: Buffer.alloc(32, 1).toString('base64'),
        timestamp: Date.now(),
        nonce: Buffer.alloc(16, 2).toString('base64'),
        message: Buffer.from('payload').toString('base64'),
        signature: Buffer.alloc(64, 3).toString('base64')
      }
    });

    expect(result.event_type).toBe('telemetry.authorized.v1');
  });

  it('parses each supported payload schema', () => {
    expect(
      telemetryRejectedPayloadSchema.parse({
        reason_code: 'unauthorized',
        reason_message: 'bad signature'
      })
    ).toBeTruthy();

    expect(
      telemetryPubsubResultPayloadSchema.parse({
        status: 'submitted',
        pubsub_topic: 'telemetry/authorized/v1',
        sensor_id: 'sensor-1',
        nonce: 'nonce-1'
      })
    ).toBeTruthy();

    expect(
      telemetryIpfsPublishedPayloadSchema.parse({
        cid: 'bafybeigdyrzt',
        event_count: 10
      })
    ).toBeTruthy();

    expect(
      telemetryBlockchainResultPayloadSchema.parse({
        target: 'robonomics',
        status: 'submitted',
        cid: 'bafybeigdyrzt',
        tx_hash: '0x123'
      })
    ).toBeTruthy();
  });

  it('uses strict envelope fields', () => {
    expect(() =>
      envelopeSchema.parse({
        event_id: 'evt-1',
        event_type: 'telemetry.authorized.v1',
        event_version: 'v1',
        occurred_at: '2026-01-01T00:00:00Z',
        source: 'endpoint',
        payload: {},
        extra_field: true
      })
    ).toThrow();
  });

  it('fails malformed envelope', () => {
    expect(() =>
      envelopeSchema.parse({
        event_type: 'telemetry.authorized.v1',
        event_version: 'v1',
        occurred_at: 'not-a-date',
        source: 'endpoint',
        payload: {}
      })
    ).toThrow();
  });

  it('fails malformed payload', () => {
    expect(() =>
      telemetryAuthorizedPayloadSchema.parse({
        sensor_id: 'sensor-1',
        timestamp: Date.now(),
        nonce: Buffer.alloc(16, 2).toString('base64'),
        message: 'invalid',
        signature: '0xabc'
      })
    ).toThrow();
  });

  it('exposes exact stable topic constants', () => {
    expect(TELEMETRY_TOPICS.AUTHORIZED).toBe('telemetry.authorized.v1');
    expect(TELEMETRY_TOPICS.REJECTED).toBe('telemetry.rejected.v1');
    expect(TELEMETRY_TOPICS.PUBSUB_RESULT).toBe('telemetry.pubsub.result.v1');
    expect(TELEMETRY_TOPICS.IPFS_RESULT).toBe('telemetry.ipfs.result.v1');
    expect(TELEMETRY_TOPICS.BLOCKCHAIN_RESULT).toBe('telemetry.blockchain.result.v1');
    expect(TELEMETRY_TOPICS.RETRY).toBe('telemetry.retry.v1');
    expect(TELEMETRY_TOPICS.DLQ).toBe('telemetry.dlq.v1');
    expect(Object.isFrozen(TELEMETRY_TOPICS)).toBe(true);
  });

  it('provides non-throwing envelope/payload validation helpers', () => {
    const envelopeResult = validateEnvelope({
      event_id: 'evt-1',
      event_type: 'telemetry.authorized.v1',
      event_version: 'v1',
      occurred_at: '2026-01-01T00:00:00Z',
      source: 'endpoint',
      payload: {
        sensor_id: Buffer.alloc(32, 1).toString('base64'),
        timestamp: Date.now(),
        nonce: Buffer.alloc(16, 2).toString('base64'),
        message: Buffer.from('payload').toString('base64'),
        signature: Buffer.alloc(64, 3).toString('base64')
      }
    });
    expect(envelopeResult.success).toBe(true);

    const payloadResult = validatePayloadForEventType('telemetry.rejected.v1', {
      reason_code: 'invalid_signature'
    });
    expect(payloadResult.success).toBe(true);

    const unknownTypeResult = validateEnvelopeWithKnownPayload({
      event_id: 'evt-2',
      event_type: 'telemetry.unknown.v1',
      event_version: 'v1',
      occurred_at: '2026-01-01T00:00:00Z',
      source: 'endpoint',
      payload: {}
    });
    expect(unknownTypeResult.success).toBe(false);
  });

  it('validates signed envelope protobuf bytes', () => {
    const envelope = createSignedEnvelope({
      sensorId: Buffer.alloc(32, 1),
      timestamp: BigInt(Date.now()),
      nonce: Buffer.alloc(16, 2),
      message: Buffer.from('abc'),
      signature: Buffer.alloc(64, 4)
    });
    const parsed = validateSignedEnvelope(toSignedEnvelopeBytes(envelope));
    expect(parsed.sensorId.length).toBe(32);
    expect(parsed.signature.length).toBe(64);
  });

  it('enforces ordered processing and commit-after-result guardrail', async () => {
    const calls: string[] = [];
    const status = await runConsumerProcessingRule(
      { event_id: 'evt-1' },
      {
        performExternalAction: async () => {
          calls.push('side-effect');
        },
        waitForConfirmation: async () => {
          calls.push('confirmation');
        },
        emitResultEvent: async () => {
          calls.push('emit-result');
          return { ok: true };
        },
        publishResultEvent: async () => {
          calls.push('publish-result');
        },
        commitOffset: async () => {
          calls.push('commit');
        },
        retryDlqPublisher: {
          async publishRetry() {
            calls.push('retry');
          },
          async publishDlq() {
            calls.push('dlq');
          }
        }
      }
    );

    expect(status).toBe('processed');
    expect(calls).toEqual([
      'side-effect',
      'confirmation',
      'emit-result',
      'publish-result',
      'commit'
    ]);
  });

  it('does not commit offset when result publish fails', async () => {
    const calls: string[] = [];
    const status = await runConsumerProcessingRule(
      { event_id: 'evt-1' },
      {
        retryPolicy: {
          maxAttempts: 1,
          getEventId: (event) => event.event_id,
          store: new InMemoryRetryCounterStore()
        },
        performExternalAction: async () => {
          calls.push('side-effect');
        },
        waitForConfirmation: async () => {
          calls.push('confirmation');
        },
        emitResultEvent: async () => {
          calls.push('emit-result');
          return { ok: true };
        },
        publishResultEvent: async () => {
          calls.push('publish-result');
          throw new Error('result publish failed');
        },
        commitOffset: async () => {
          calls.push('commit');
        },
        retryDlqPublisher: {
          async publishRetry() {
            calls.push('retry');
          },
          async publishDlq() {
            calls.push('dlq');
          }
        }
      }
    );

    expect(status).toBe('dlq');
    expect(calls).not.toContain('commit');
    expect(calls).toContain('dlq');
  });

  it('applies bounded retry policy and routes exhausted attempts to DLQ', async () => {
    const retryStore = new InMemoryRetryCounterStore();
    const retryCalls: number[] = [];
    const dlqCalls: number[] = [];
    const event = { event_id: 'evt-retry-1' };

    const runFailingAttempt = () =>
      runConsumerProcessingRule(event, {
        retryPolicy: {
          maxAttempts: 2,
          getEventId: (current) => current.event_id,
          store: retryStore
        },
        performExternalAction: async () => {
          throw new Error('transient');
        },
        waitForConfirmation: async () => {
          throw new Error('unreachable');
        },
        emitResultEvent: async () => ({ ok: true }),
        publishResultEvent: async () => {},
        commitOffset: async () => {},
        retryDlqPublisher: {
          async publishRetry(_current, _reason, context) {
            retryCalls.push(context?.attempt ?? -1);
          },
          async publishDlq(_current, _reason, context) {
            dlqCalls.push(context?.attempt ?? -1);
          }
        }
      });

    await expect(runFailingAttempt()).resolves.toBe('retried');
    await expect(runFailingAttempt()).resolves.toBe('dlq');
    expect(retryCalls).toEqual([1]);
    expect(dlqCalls).toEqual([2]);
  });

  it('routes directly to DLQ when retry attempts cannot be tracked', async () => {
    const calls: string[] = [];
    const status = await runConsumerProcessingRule(
      { event_id: undefined },
      {
        maxRetries: 3,
        performExternalAction: async () => {
          throw new Error('transient');
        },
        waitForConfirmation: async () => {
          throw new Error('unreachable');
        },
        emitResultEvent: async () => ({ ok: true }),
        publishResultEvent: async () => {},
        commitOffset: async () => {
          calls.push('commit');
        },
        retryDlqPublisher: {
          async publishRetry() {
            calls.push('retry');
          },
          async publishDlq() {
            calls.push('dlq');
          }
        }
      }
    );

    expect(status).toBe('dlq');
    expect(calls).toEqual(['dlq']);
  });

  it('supports event-level idempotency hook by event_id', async () => {
    const calls: string[] = [];
    const status = await runConsumerProcessingRule(
      { event_id: 'evt-dup-1' },
      {
        idempotency: {
          getEventId: (event) => event.event_id,
          hasProcessed: async () => true,
          markProcessed: async () => {
            calls.push('mark');
          }
        },
        performExternalAction: async () => {
          calls.push('side-effect');
        },
        waitForConfirmation: async () => {
          calls.push('confirmation');
        },
        emitResultEvent: async () => ({ ok: true }),
        publishResultEvent: async () => {
          calls.push('publish');
        },
        commitOffset: async () => {
          calls.push('commit');
        },
        retryDlqPublisher: {
          async publishRetry() {},
          async publishDlq() {}
        }
      }
    );

    expect(status).toBe('duplicate');
    expect(calls).toEqual(['commit']);
  });
});
