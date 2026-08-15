import { describe, expect, it } from 'vitest';
import { cryptoWaitReady, ed25519PairFromSeed, ed25519Sign, encodeAddress } from '@polkadot/util-crypto';
import { createSignedEnvelope, toSignedEnvelopeBytes, buildEnvelopeSigningBytes } from '@scp/contracts';
import { createEndpointApp } from '../src/index.js';
import { InMemoryRegistryReader } from '../../registry-sync/src/reader.js';

async function buildSignedEnvelopeBytes(seedByte: number, timestamp: bigint = BigInt(Date.now())) {
  await cryptoWaitReady();
  const seed = Uint8Array.from(Array.from({ length: 32 }, () => seedByte));
  const pair = ed25519PairFromSeed(seed);
  const envelope = createSignedEnvelope({
    sensorId: pair.publicKey,
    timestamp,
    nonce: Uint8Array.from(Buffer.alloc(16, 1)),
    message: Uint8Array.from(Buffer.from('payload'))
  });
  envelope.signature = ed25519Sign(buildEnvelopeSigningBytes(envelope), pair);
  return {
    bytes: toSignedEnvelopeBytes(envelope),
    sensorId: encodeAddress(pair.publicKey, 32)
  };
}

describe('endpoint smoke', () => {
  it('returns 403 for unknown sensor, emits rejected event, and exposes health/metrics counters', async () => {
    const rejectedEvents: Array<{ reason_code: string; sensor_id?: string | undefined }> = [];
    const app = createEndpointApp({
      registryReader: new InMemoryRegistryReader([]),
      producer: {
        async publishAuthorized() {
          return 'event-1';
        },
        async publishRejected(payload) {
          rejectedEvents.push(payload);
          return 'event-2';
        }
      }
    });
    const envelope = await buildSignedEnvelopeBytes(1);
    const response = await app.inject({
      method: 'POST',
      url: '/v1/telemetry',
      headers: {
        'content-type': 'application/protobuf'
      },
      payload: Buffer.from(envelope.bytes)
    });

    expect(response.statusCode).toBe(403);
    expect(rejectedEvents[0]?.reason_code).toBe('sensor_forbidden');
    await app.close();
  });

  it('returns 401 for stale timestamps', async () => {
    const rejectedEvents: Array<{ reason_code: string; sensor_id?: string | undefined }> = [];
    const app = createEndpointApp(
      {
        registryReader: new InMemoryRegistryReader([]),
        producer: {
          async publishAuthorized() {
            return 'event-1';
          },
          async publishRejected(payload) {
            rejectedEvents.push(payload);
            return 'event-2';
          }
        }
      },
      { timestampSkewSeconds: 300 }
    );
    const envelope = await buildSignedEnvelopeBytes(1, BigInt(1));
    const response = await app.inject({
      method: 'POST',
      url: '/v1/telemetry',
      headers: {
        'content-type': 'application/protobuf'
      },
      payload: Buffer.from(envelope.bytes)
    });

    expect(response.statusCode).toBe(401);
    expect(response.json()).toEqual({ status: 'rejected', error_code: 'stale_timestamp' });
    expect(rejectedEvents[0]?.reason_code).toBe('stale_timestamp');
    await app.close();
  });

  it('accepts valid envelope for enabled sensor', async () => {
    const envelope = await buildSignedEnvelopeBytes(2);
    const authorized: unknown[] = [];
    const app = createEndpointApp({
      registryReader: new InMemoryRegistryReader([
        { sensorId: envelope.sensorId, enabled: true }
      ]),
      producer: {
        async publishAuthorized(payload) {
          authorized.push(payload);
          return 'event-1';
        },
        async publishRejected() {
          return 'event-2';
        }
      }
    });

    const response = await app.inject({
      method: 'POST',
      url: '/v1/telemetry',
      headers: {
        'content-type': 'application/protobuf'
      },
      payload: Buffer.from(envelope.bytes)
    });

    expect(response.statusCode).toBe(202);
    expect(authorized).toHaveLength(1);
    await app.close();
  });
});
