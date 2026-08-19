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
  type TelemetryAuthorizedPayload,
} from '@scp/core';
import type { AuthorizedTelemetryEnvelope } from '../src/batching.js';

export function authorizedPayload(seed = 1): TelemetryAuthorizedPayload {
  return create(TelemetryAuthorizedPayloadSchema, {
    sensorId: Buffer.alloc(32, seed),
    signedEnvelope: Buffer.from([seed, seed + 1, seed + 2]),
  });
}

export function authorizedEnvelope(
  eventId = 'event-1',
  seed = 1
): AuthorizedTelemetryEnvelope {
  return {
    eventId,
    eventType: TELEMETRY_TOPICS.AUTHORIZED,
    eventVersion: 'v1',
    occurredAt: '2026-08-11T00:00:00.000Z',
    source: 'endpoint',
    payload: authorizedPayload(seed),
  };
}

export function authorizedEnvelopeBytes(eventId = 'event-1', seed = 1): Buffer {
  const payload = authorizedPayload(seed);
  const envelope = create(EnvelopeSchema, {
    eventId,
    eventType: TELEMETRY_TOPICS.AUTHORIZED,
    eventVersion: 'v1',
    occurredAt: '2026-08-11T00:00:00.000Z',
    source: 'endpoint',
    payload: toBinary(TelemetryAuthorizedPayloadSchema, payload),
  });
  return Buffer.from(toBinary(EnvelopeSchema, envelope));
}
