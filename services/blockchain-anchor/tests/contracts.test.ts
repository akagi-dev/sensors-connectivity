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
import {
  TELEMETRY_TOPICS,
  TelemetryIpfsPublishedPayloadSchema,
  TelemetryIpfsPublishedPayload_Compression,
  EnvelopeSchema,
} from '@scp/core';
import { create, toBinary, fromBinary } from '@bufbuild/protobuf';
import { describe, expect, it } from 'vitest';
import { CID } from 'multiformats/cid';

describe('blockchain anchor contract compatibility', () => {
  it('accepts telemetry.ipfs.published.v1 envelope/payload as input', () => {
    const fakeCid = CID.parse('QmYwAPJzv5CZsnA625s3Xf2nemtYgPpHdWEz79ojWnPbdG');

    const payload = create(TelemetryIpfsPublishedPayloadSchema, {
      cid: Buffer.from(fakeCid.bytes),
      eventCount: 10,
      compression: TelemetryIpfsPublishedPayload_Compression.NONE,
    });

    const envelope = create(EnvelopeSchema, {
      eventId: 'evt-anchor-1',
      eventType: TELEMETRY_TOPICS.IPFS_PUBLISHED,
      eventVersion: '1.0.0',
      occurredAt: '2026-01-01T00:00:00Z',
      source: 'ipfs-publisher',
      payload: toBinary(TelemetryIpfsPublishedPayloadSchema, payload),
    });

    const envelopeBytes = toBinary(EnvelopeSchema, envelope);
    const parsed = fromBinary(EnvelopeSchema, envelopeBytes);

    expect(parsed.eventType).toBe(TELEMETRY_TOPICS.IPFS_PUBLISHED);
    expect(parsed.eventId).toBe('evt-anchor-1');

    const payloadParsed = fromBinary(
      TelemetryIpfsPublishedPayloadSchema,
      parsed.payload
    );
    expect(payloadParsed.eventCount).toBe(10);
    expect(payloadParsed.compression).toBe(
      TelemetryIpfsPublishedPayload_Compression.NONE
    );

    // Verify CID can be decoded
    const decodedCid = CID.decode(payloadParsed.cid);
    expect(decodedCid.toString()).toBe(fakeCid.toString());
  });

  it('handles CID v0 format', () => {
    const cidV0 = CID.parse('QmYwAPJzv5CZsnA625s3Xf2nemtYgPpHdWEz79ojWnPbdG');

    const payload = create(TelemetryIpfsPublishedPayloadSchema, {
      cid: Buffer.from(cidV0.bytes),
      eventCount: 5,
      compression: TelemetryIpfsPublishedPayload_Compression.NONE,
    });

    const decodedCid = CID.decode(payload.cid);
    expect(decodedCid.version).toBe(0);
    expect(decodedCid.toString()).toBe(cidV0.toString());
  });

  it('handles CID v1 format', () => {
    const cidV1 = CID.parse(
      'bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi'
    );

    const payload = create(TelemetryIpfsPublishedPayloadSchema, {
      cid: Buffer.from(cidV1.bytes),
      eventCount: 20,
      compression: TelemetryIpfsPublishedPayload_Compression.XZ,
    });

    const decodedCid = CID.decode(payload.cid);
    expect(decodedCid.version).toBe(1);
    expect(decodedCid.toString()).toBe(cidV1.toString());
  });

  it('accepts compressed batch events', () => {
    const fakeCid = CID.parse(
      'bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi'
    );

    const payload = create(TelemetryIpfsPublishedPayloadSchema, {
      cid: Buffer.from(fakeCid.bytes),
      eventCount: 100,
      compression: TelemetryIpfsPublishedPayload_Compression.XZ,
    });

    const envelope = create(EnvelopeSchema, {
      eventId: 'evt-anchor-xz-1',
      eventType: TELEMETRY_TOPICS.IPFS_PUBLISHED,
      eventVersion: '1.0.0',
      occurredAt: '2026-01-01T00:00:00Z',
      source: 'ipfs-publisher',
      payload: toBinary(TelemetryIpfsPublishedPayloadSchema, payload),
    });

    const envelopeBytes = toBinary(EnvelopeSchema, envelope);
    const parsed = fromBinary(EnvelopeSchema, envelopeBytes);

    const payloadParsed = fromBinary(
      TelemetryIpfsPublishedPayloadSchema,
      parsed.payload
    );

    expect(payloadParsed.eventCount).toBe(100);
    expect(payloadParsed.compression).toBe(
      TelemetryIpfsPublishedPayload_Compression.XZ
    );
  });

  it('CID bytes can be converted to string for blockchain payload', () => {
    const cidString = 'QmYwAPJzv5CZsnA625s3Xf2nemtYgPpHdWEz79ojWnPbdG';
    const cid = CID.parse(cidString);

    const payload = create(TelemetryIpfsPublishedPayloadSchema, {
      cid: Buffer.from(cid.bytes),
      eventCount: 1,
      compression: TelemetryIpfsPublishedPayload_Compression.NONE,
    });

    // Simulate what blockchain anchor does: decode and toString
    const decodedCid = CID.decode(payload.cid);
    const cidStringOutput = decodedCid.toString();

    expect(cidStringOutput).toBe(cidString);

    // Verify UTF-8 encoding for blockchain
    const utf8Bytes = Buffer.from(cidStringOutput, 'utf-8');
    expect(utf8Bytes.toString('utf-8')).toBe(cidString);
  });

  it('envelope contains trace_id for distributed tracing', () => {
    const fakeCid = CID.parse('QmYwAPJzv5CZsnA625s3Xf2nemtYgPpHdWEz79ojWnPbdG');

    const payload = create(TelemetryIpfsPublishedPayloadSchema, {
      cid: Buffer.from(fakeCid.bytes),
      eventCount: 1,
      compression: TelemetryIpfsPublishedPayload_Compression.NONE,
    });

    const envelope = create(EnvelopeSchema, {
      eventId: 'evt-anchor-trace-1',
      eventType: TELEMETRY_TOPICS.IPFS_PUBLISHED,
      eventVersion: '1.0.0',
      occurredAt: '2026-01-01T00:00:00Z',
      traceId: 'trace-123-456',
      source: 'ipfs-publisher',
      payload: toBinary(TelemetryIpfsPublishedPayloadSchema, payload),
    });

    const envelopeBytes = toBinary(EnvelopeSchema, envelope);
    const parsed = fromBinary(EnvelopeSchema, envelopeBytes);

    expect(parsed.traceId).toBe('trace-123-456');
  });

  it('compression enum values match protobuf definition', () => {
    expect(TelemetryIpfsPublishedPayload_Compression.NONE).toBe(0);
    expect(TelemetryIpfsPublishedPayload_Compression.XZ).toBe(1);
  });
});
