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
import { create, toBinary } from '@bufbuild/protobuf';
import type { Producer } from '@platformatic/kafka';
import {
  EnvelopeSchema,
  TELEMETRY_TOPICS,
  TelemetryIpfsPublishedPayloadSchema,
  type Envelope,
  type TelemetryIpfsPublishedPayload,
} from '@scp/core';
import { canonicalize } from 'json-canonicalize';
import { randomUUID } from 'node:crypto';
import type { SerializedAuthorizedPayload } from './batch-publisher.js';

export interface IpfsResultEnvelope {
  envelope: Envelope;
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

export interface IpfsFailureContext {
  topic: typeof TELEMETRY_TOPICS.DLQ;
  reason: string;
  eventId: string;
  attempt: number;
  maxAttempts: number;
  failedAt: string;
}

export interface IpfsFailedBatch {
  batch_id: string;
  events: SerializedAuthorizedPayload[];
}

export interface IpfsDlqPayload {
  failed_topic: typeof TELEMETRY_TOPICS.AUTHORIZED;
  reason_code: IpfsDlqReasonCode;
  reason_message: string;
  failed_event: IpfsFailedBatch;
  context: IpfsFailureContext;
}

export interface IpfsDlqEnvelope {
  envelope: Envelope;
  payload: IpfsDlqPayload;
}

export interface IpfsResultPublisher {
  connect(): Promise<void>;
  disconnect(): Promise<void>;
  publish(batchId: string, envelope: IpfsResultEnvelope): Promise<void>;
  publishDlq(batchId: string, envelope: IpfsDlqEnvelope): Promise<void>;
}

export function buildIpfsResultEnvelope(
  cid: string,
  eventCount: number,
  options: IpfsResultEnvelopeOptions = {}
): IpfsResultEnvelope {
  if (cid.trim().length === 0)
    throw new Error('IPFS result CID cannot be empty');
  if (
    !Number.isInteger(eventCount) ||
    eventCount < 1 ||
    eventCount > 0xffffffff
  ) {
    throw new Error('IPFS result event_count must be a positive uint32');
  }
  const payload = create(TelemetryIpfsPublishedPayloadSchema, {
    cid: new TextEncoder().encode(cid),
    eventCount,
  });
  const envelope = create(EnvelopeSchema, {
    eventId: options.eventId ?? randomUUID(),
    eventType: TELEMETRY_TOPICS.IPFS_PUBLISHED,
    eventVersion: 'v1',
    occurredAt: options.occurredAt ?? new Date().toISOString(),
    source: 'ipfs-publisher',
    payload: toBinary(TelemetryIpfsPublishedPayloadSchema, payload),
  });
  return { envelope, payload };
}

export function buildIpfsDlqEnvelope(
  failedBatch: IpfsFailedBatch,
  reasonCode: IpfsDlqReasonCode,
  reasonMessage: string,
  context: IpfsFailureContext,
  options: IpfsResultEnvelopeOptions = {}
): IpfsDlqEnvelope {
  if (failedBatch.batch_id.length === 0)
    throw new Error('IPFS DLQ batch_id cannot be empty');
  if (context.topic !== TELEMETRY_TOPICS.DLQ) {
    throw new Error('IPFS DLQ context must target telemetry.dlq.v1');
  }
  const payload: IpfsDlqPayload = {
    failed_topic: TELEMETRY_TOPICS.AUTHORIZED,
    reason_code: reasonCode,
    reason_message: reasonMessage,
    failed_event: failedBatch,
    context,
  };
  const envelope = create(EnvelopeSchema, {
    eventId: options.eventId ?? randomUUID(),
    eventType: TELEMETRY_TOPICS.DLQ,
    eventVersion: 'v1',
    occurredAt: options.occurredAt ?? new Date().toISOString(),
    source: 'ipfs-publisher',
    payload: new TextEncoder().encode(canonicalize(payload)),
  });
  return { envelope, payload };
}

export function createKafkaIpfsResultPublisher(
  producer: Producer
): IpfsResultPublisher {
  return {
    async connect(): Promise<void> {},
    async disconnect(): Promise<void> {
      await producer.close();
    },
    async publish(batchId, result): Promise<void> {
      validateBatchId(batchId);
      if (result.envelope.eventType !== TELEMETRY_TOPICS.IPFS_PUBLISHED) {
        throw new Error('Cannot publish a non-IPFS result envelope');
      }
      await producer.send({
        messages: [
          {
            topic: TELEMETRY_TOPICS.IPFS_PUBLISHED,
            key: Buffer.from(batchId),
            value: Buffer.from(toBinary(EnvelopeSchema, result.envelope)),
          },
        ],
      });
    },
    async publishDlq(batchId, result): Promise<void> {
      validateBatchId(batchId);
      if (result.envelope.eventType !== TELEMETRY_TOPICS.DLQ) {
        throw new Error('Cannot publish a non-DLQ envelope');
      }
      await producer.send({
        messages: [
          {
            topic: TELEMETRY_TOPICS.DLQ,
            key: Buffer.from(batchId),
            value: Buffer.from(toBinary(EnvelopeSchema, result.envelope)),
          },
        ],
      });
    },
  };
}

function validateBatchId(batchId: string): void {
  if (batchId.length === 0) throw new Error('Cannot publish without batch_id');
}
