import { cryptoWaitReady, ed25519PairFromSeed, ed25519Sign, encodeAddress } from '@polkadot/util-crypto';
import { createHash } from 'node:crypto';
import { describe, expect, it } from 'vitest';
import { canonicalize } from 'json-canonicalize';
import { verifyTelemetrySignature } from '../src/signature.js';

function telemetryHashHex(
  measurements: Record<string, unknown>,
  timestamp: string,
  nonce: string,
  sensorAddress: string
): string {
  const canonicalMeasurements = canonicalize(measurements);
  const concatenated = `${canonicalMeasurements}${timestamp}${nonce}${sensorAddress}`;
  return createHash('sha256').update(new TextEncoder().encode(concatenated)).digest('hex');
}

describe('verifyTelemetrySignature', () => {
  it('verifies signature with SS58 signer address', async () => {
    await cryptoWaitReady();
    const seed = Uint8Array.from(Array.from({ length: 32 }, (_, i) => i + 1));
    const pair = ed25519PairFromSeed(seed);
    const signerAddress = encodeAddress(pair.publicKey, 32);
    const measurements = { temp: 21, humidity: 54 };
    const timestamp = '2026-01-01T00:00:00Z';
    const nonce = 'nonce-1';
    const sensorAddress = signerAddress;
    const hashHex = telemetryHashHex(measurements, timestamp, nonce, sensorAddress);
    const signatureBytes = ed25519Sign(Buffer.from(hashHex, 'hex'), pair);

    await expect(
      verifyTelemetrySignature({
        measurements,
        timestamp,
        nonce,
        sensorAddress,
        signature: `0x${Buffer.from(signatureBytes).toString('hex')}`,
        signerAddress
      })
    ).resolves.toBe(true);
  });

  it('returns false when timestamp changes', async () => {
    await cryptoWaitReady();
    const seed = Uint8Array.from(Array.from({ length: 32 }, (_, i) => i + 1));
    const pair = ed25519PairFromSeed(seed);
    const signerAddress = encodeAddress(pair.publicKey, 32);
    const measurements = { temp: 21 };
    const hashHex = telemetryHashHex(measurements, '2026-01-01T00:00:00Z', 'nonce-1', signerAddress);
    const signatureBytes = ed25519Sign(Buffer.from(hashHex, 'hex'), pair);

    await expect(
      verifyTelemetrySignature({
        measurements,
        timestamp: '2026-01-01T00:00:01Z',
        nonce: 'nonce-1',
        sensorAddress: signerAddress,
        signature: `0x${Buffer.from(signatureBytes).toString('hex')}`,
        signerAddress
      })
    ).resolves.toBe(false);
  });

  it('returns false for malformed signer address', async () => {
    await expect(
      verifyTelemetrySignature({
        measurements: { temp: 21 },
        timestamp: '2026-01-01T00:00:00Z',
        nonce: 'nonce-1',
        sensorAddress: '5FHneW46xGXgs5mUiveU4sbTyGBzmstN5fJQw6QvP5M4Xv4H',
        signature: '0x00',
        signerAddress: 'not-a-ss58-address'
      })
    ).resolves.toBe(false);
  });
});
