import { describe, expect, it } from 'vitest';
import { create, toBinary } from '@bufbuild/protobuf';
import {
  InMemoryRetryCounterStore,
  runConsumerProcessingRule,
} from '../src/consumer-runtime.js';
import { TELEMETRY_TOPICS } from '../src/topics.js';
import {
  parseEnvelope,
  createEnvelope,
  serializeEnvelope,
  parsePayloadForEventType,
} from '../src/validation.js';
import { validateSignedEnvelope } from '../src/utils.js';
import {
  TelemetryAuthorizedPayloadSchema,
  TelemetryRejectedPayloadSchema,
  TelemetryPubsubResultPayloadSchema,
  TelemetryIpfsPublishedPayloadSchema,
  TelemetryBlockchainResultPayloadSchema,
  TelemetryPubsubResultPayload_Status,
  TelemetryBlockchainResultPayload_Status,
} from '../src/generated/connectivity/v1/payload_pb.js';
import { SignedEnvelopeSchema } from '@buf/airalab_sensors-social-proto.bufbuild_es/crypto/v1/envelope_pb.js';

describe('contracts', () => {
  it('creates and parses well-formed envelope + authorized payload', () => {
    const signedEnvelopeBytes = Buffer.alloc(100, 1);
    const payload = create(TelemetryAuthorizedPayloadSchema, {
      sensorId: Buffer.alloc(32, 1),
      signedEnvelope: signedEnvelopeBytes,
    });
    const payloadBytes = toBinary(TelemetryAuthorizedPayloadSchema, payload);

    const envelope = createEnvelope({
      eventId: 'evt-1',
      eventType: TELEMETRY_TOPICS.AUTHORIZED,
      source: 'endpoint',
      payload: payloadBytes,
    });

    const envelopeBytes = serializeEnvelope(envelope);
    const parsed = parseEnvelope(envelopeBytes);

    expect(parsed.eventType).toBe(TELEMETRY_TOPICS.AUTHORIZED);
    expect(parsed.eventId).toBe('evt-1');
  });

  it('exposes exact stable topic constants', () => {
    expect(TELEMETRY_TOPICS.AUTHORIZED).toBe('telemetry.authorized.v1');
    expect(TELEMETRY_TOPICS.REJECTED).toBe('telemetry.rejected.v1');
    expect(TELEMETRY_TOPICS.PUBSUB_RESULT).toBe('telemetry.pubsub.result.v1');
    expect(TELEMETRY_TOPICS.IPFS_RESULT).toBe('telemetry.ipfs.result.v1');
    expect(TELEMETRY_TOPICS.BLOCKCHAIN_RESULT).toBe(
      'telemetry.blockchain.result.v1'
    );
    expect(TELEMETRY_TOPICS.RETRY).toBe('telemetry.retry.v1');
    expect(TELEMETRY_TOPICS.DLQ).toBe('telemetry.dlq.v1');
    expect(Object.isFrozen(TELEMETRY_TOPICS)).toBe(true);
  });

  it('validates signed envelope protobuf bytes', () => {
    const envelope = create(SignedEnvelopeSchema, {
      sensorId: Buffer.alloc(32, 1),
      timestamp: BigInt(Date.now()),
      nonce: Buffer.alloc(16, 2),
      message: Buffer.from('abc'),
      signature: Buffer.alloc(64, 4),
    });
    const envelopeBytes = toBinary(SignedEnvelopeSchema, envelope);
    const parsed = validateSignedEnvelope(envelopeBytes);
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
          },
        },
      }
    );

    expect(status).toBe('processed');
    expect(calls).toEqual([
      'side-effect',
      'confirmation',
      'emit-result',
      'publish-result',
      'commit',
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
          store: new InMemoryRetryCounterStore(),
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
          },
        },
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
          store: retryStore,
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
          },
        },
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
        retryPolicy: {
          maxAttempts: 3,
          getEventId: (event) => event.event_id,
          store: new InMemoryRetryCounterStore(),
        },
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
          },
        },
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
          },
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
          async publishDlq() {},
        },
      }
    );

    expect(status).toBe('duplicate');
    expect(calls).toEqual(['commit']);
  });

  it('parses payload for rejected event type', () => {
    const payload = create(TelemetryRejectedPayloadSchema, {
      sensorId: Buffer.alloc(32, 1),
      reasonCode: 1,
      reasonMessage: 'Test rejection',
    });
    const payloadBytes = toBinary(TelemetryRejectedPayloadSchema, payload);

    const parsed = parsePayloadForEventType(
      TELEMETRY_TOPICS.REJECTED,
      payloadBytes
    );

    expect(parsed.reasonCode).toBe(1);
    expect(parsed.reasonMessage).toBe('Test rejection');
  });

  it('parses payload for pubsub result event type', () => {
    const payload = create(TelemetryPubsubResultPayloadSchema, {
      status: 1, // SUBMITTED
      sensorId: Buffer.alloc(32, 2),
    });
    const payloadBytes = toBinary(TelemetryPubsubResultPayloadSchema, payload);

    const parsed = parsePayloadForEventType(
      TELEMETRY_TOPICS.PUBSUB_RESULT,
      payloadBytes
    );

    expect(parsed.status).toBe(1); // SUBMITTED
    expect(parsed.sensorId.length).toBe(32);
    // Verify the enum constants are available
    expect(TelemetryPubsubResultPayload_Status.SUBMITTED).toBe(1);
  });

  it('parses payload for IPFS published event type', () => {
    const payload = create(TelemetryIpfsPublishedPayloadSchema, {
      cid: Buffer.from('Qm...'),
      eventCount: 5,
    });
    const payloadBytes = toBinary(TelemetryIpfsPublishedPayloadSchema, payload);

    const parsed = parsePayloadForEventType(
      TELEMETRY_TOPICS.IPFS_RESULT,
      payloadBytes
    );

    expect(parsed.eventCount).toBe(5);
    expect(parsed.cid.length).toBeGreaterThan(0);
  });

  it('parses payload for blockchain result event type', () => {
    const payload = create(TelemetryBlockchainResultPayloadSchema, {
      status: 1, // SUBMITTED
      cid: Buffer.from('Qm...'),
    });
    const payloadBytes = toBinary(
      TelemetryBlockchainResultPayloadSchema,
      payload
    );

    const parsed = parsePayloadForEventType(
      TELEMETRY_TOPICS.BLOCKCHAIN_RESULT,
      payloadBytes
    );

    expect(parsed.status).toBe(1); // SUBMITTED
    expect(parsed.cid.length).toBeGreaterThan(0);
    // Verify the enum constants are available
    expect(TelemetryBlockchainResultPayload_Status.SUBMITTED).toBe(1);
  });
});
