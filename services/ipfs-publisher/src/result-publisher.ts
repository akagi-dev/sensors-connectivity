import {
  parseEnvelope,
  parseEnvelopeWithKnownPayload,
  TELEMETRY_TOPICS,
  type FailureContext,
  type TelemetryAuthorizedPayload,
  type TelemetryIpfsPublishedPayload
} from '@scp/contracts';
import type { Producer } from 'kafkajs';
import { randomUUID } from 'node:crypto';

export interface IpfsResultEnvelope {
  event_id: string;
  event_type: typeof TELEMETRY_TOPICS.IPFS_RESULT;
  event_version: 'v1';
  occurred_at: string;
  source: 'ipfs-publisher';
  payload: TelemetryIpfsPublishedPayload;
}

export interface IpfsResultEnvelopeOptions {
  eventId?: string;
  occurredAt?: string;
}

export type IpfsDlqReasonCode =
  | 'ipfs_publish_or_pin_failed'
  | 'pin_confirmation_failed'
  | 'result_event_failed'
  | 'result_publish_failed';

export interface IpfsFailedBatch {
  batch_id: string;
  events: TelemetryAuthorizedPayload[];
}

export interface IpfsDlqEnvelope {
  event_id: string;
  event_type: typeof TELEMETRY_TOPICS.DLQ;
  event_version: 'v1';
  occurred_at: string;
  source: 'ipfs-publisher';
  payload: {
    failed_topic: typeof TELEMETRY_TOPICS.AUTHORIZED;
    reason_code: IpfsDlqReasonCode;
    reason_message: string;
    failed_event: IpfsFailedBatch;
    context: FailureContext;
  };
}

export interface IpfsResultPublisher {
  connect(): Promise<void>;
  disconnect(): Promise<void>;
  publish(batchId: string, envelope: IpfsResultEnvelope): Promise<void>;
  publishDlq(batchId: string, envelope: IpfsDlqEnvelope): Promise<void>;
}

/** Builds and validates the canonical WP-00 IPFS publication result envelope. */
export function buildIpfsResultEnvelope(
  cid: string,
  eventCount: number,
  options: IpfsResultEnvelopeOptions = {}
): IpfsResultEnvelope {
  const envelope: IpfsResultEnvelope = {
    event_id: options.eventId ?? randomUUID(),
    event_type: TELEMETRY_TOPICS.IPFS_RESULT,
    event_version: 'v1',
    occurred_at: options.occurredAt ?? new Date().toISOString(),
    source: 'ipfs-publisher',
    payload: {
      cid,
      event_count: eventCount
    }
  };
  const validated = parseEnvelopeWithKnownPayload(envelope);
  if (validated.event_type !== TELEMETRY_TOPICS.IPFS_RESULT) {
    throw new Error('IPFS result envelope has an unexpected event_type');
  }

  return envelope;
}

/** Builds and validates a WP-00 DLQ envelope with the original batch and exhausted-error context. */
export function buildIpfsDlqEnvelope(
  failedBatch: IpfsFailedBatch,
  reasonCode: IpfsDlqReasonCode,
  reasonMessage: string,
  context: FailureContext,
  options: IpfsResultEnvelopeOptions = {}
): IpfsDlqEnvelope {
  if (context.topic !== TELEMETRY_TOPICS.DLQ) {
    throw new Error('IPFS DLQ context must target telemetry.dlq.v1');
  }
  const envelope: IpfsDlqEnvelope = {
    event_id: options.eventId ?? randomUUID(),
    event_type: TELEMETRY_TOPICS.DLQ,
    event_version: 'v1',
    occurred_at: options.occurredAt ?? new Date().toISOString(),
    source: 'ipfs-publisher',
    payload: {
      failed_topic: TELEMETRY_TOPICS.AUTHORIZED,
      reason_code: reasonCode,
      reason_message: reasonMessage,
      failed_event: failedBatch,
      context
    }
  };
  parseEnvelope(envelope);
  return envelope;
}

/** Creates a Kafka result and DLQ publisher that uses batch_id as the key and waits for broker ACKs. */
export function createKafkaIpfsResultPublisher(
  producer: Producer
): IpfsResultPublisher {
  let connected = false;
  let connectPromise: Promise<void> | undefined;

  /** Establishes the single shared Kafka producer connection. */
  async function ensureConnected(): Promise<void> {
    if (connected) {
      return;
    }
    if (!connectPromise) {
      connectPromise = producer.connect().then(() => {
        connected = true;
      });
    }

    try {
      await connectPromise;
    } catch (error) {
      connectPromise = undefined;
      throw error;
    }
  }

  return {
    connect: ensureConnected,
    async disconnect(): Promise<void> {
      if (connectPromise) {
        try {
          await connectPromise;
        } catch {
          connectPromise = undefined;
          return;
        }
      }
      if (!connected) {
        return;
      }

      await producer.disconnect();
      connected = false;
      connectPromise = undefined;
    },
    async publish(
      batchId: string,
      envelope: IpfsResultEnvelope
    ): Promise<void> {
      if (batchId.length === 0) {
        throw new Error('Cannot publish an IPFS result without batch_id');
      }
      const validated = parseEnvelopeWithKnownPayload(envelope);
      if (validated.event_type !== TELEMETRY_TOPICS.IPFS_RESULT) {
        throw new Error('Cannot publish a non-IPFS result envelope');
      }

      await ensureConnected();
      await producer.send({
        topic: TELEMETRY_TOPICS.IPFS_RESULT,
        messages: [
          {
            key: batchId,
            value: JSON.stringify(validated)
          }
        ]
      });
    },
    async publishDlq(
      batchId: string,
      envelope: IpfsDlqEnvelope
    ): Promise<void> {
      if (batchId.length === 0) {
        throw new Error('Cannot publish an IPFS DLQ record without batch_id');
      }
      const validated = parseEnvelope(envelope);
      if (validated.event_type !== TELEMETRY_TOPICS.DLQ) {
        throw new Error('Cannot publish a non-DLQ envelope');
      }

      await ensureConnected();
      await producer.send({
        topic: TELEMETRY_TOPICS.DLQ,
        messages: [
          {
            key: batchId,
            value: JSON.stringify(validated)
          }
        ]
      });
    }
  };
}
