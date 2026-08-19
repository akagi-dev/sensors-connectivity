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
import {
  EnvelopeSchema,
  TELEMETRY_TOPICS,
  TelemetryAuthorizedPayloadSchema,
} from '@scp/core';
import { describe, expect, it } from 'vitest';
import {
  decodeAuthorizedKafkaEnvelope,
  decodeAuthorizedKafkaMessage,
} from '../src/authorized-message.js';
import { authorizedEnvelopeBytes, authorizedPayload } from './helpers.js';

describe('authorized Kafka message decoder', () => {
  it('decodes the current protobuf envelope and payload', () => {
    const decoded = decodeAuthorizedKafkaEnvelope(authorizedEnvelopeBytes());
    expect(decoded.eventId).toBe('event-1');
    expect(decoded.eventType).toBe(TELEMETRY_TOPICS.AUTHORIZED);
    expect(
      decodeAuthorizedKafkaMessage(authorizedEnvelopeBytes()).sensorId
    ).toEqual(Buffer.alloc(32, 1));
  });

  it('rejects malformed protobuf and unsupported event types', () => {
    expect(() =>
      decodeAuthorizedKafkaMessage(Buffer.from('not-protobuf'))
    ).toThrow('not a valid protobuf envelope');
    const envelope = create(EnvelopeSchema, {
      eventId: 'event-2',
      eventType: TELEMETRY_TOPICS.REJECTED,
      eventVersion: 'v1',
      occurredAt: '2026-08-11T00:00:00.000Z',
      source: 'endpoint',
      payload: toBinary(TelemetryAuthorizedPayloadSchema, authorizedPayload()),
    });
    expect(() =>
      decodeAuthorizedKafkaMessage(toBinary(EnvelopeSchema, envelope))
    ).toThrow('unsupported event_type');
  });

  it('rejects empty IDs and invalid current payload fields', () => {
    const payload = create(TelemetryAuthorizedPayloadSchema, {
      sensorId: Buffer.alloc(31),
      signedEnvelope: Buffer.from([1]),
    });
    const envelope = create(EnvelopeSchema, {
      eventId: '',
      eventType: TELEMETRY_TOPICS.AUTHORIZED,
      eventVersion: 'v1',
      occurredAt: '2026-08-11T00:00:00.000Z',
      source: 'endpoint',
      payload: toBinary(TelemetryAuthorizedPayloadSchema, payload),
    });
    expect(() =>
      decodeAuthorizedKafkaMessage(toBinary(EnvelopeSchema, envelope))
    ).toThrow('empty event_id');
    envelope.eventId = 'event-3';
    expect(() =>
      decodeAuthorizedKafkaMessage(toBinary(EnvelopeSchema, envelope))
    ).toThrow('32 bytes');
  });
});
