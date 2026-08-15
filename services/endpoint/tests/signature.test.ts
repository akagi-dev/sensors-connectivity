import { cryptoWaitReady, ed25519PairFromSeed, ed25519Sign, encodeAddress } from '@polkadot/util-crypto';
import { describe, expect, it } from 'vitest';
import { canonicalize } from 'json-canonicalize';
import { verifyTelemetrySignature } from '../src/signature.js';

function telemetryMessageBytes(
  measurements: Record<string, unknown>,
  timestamp: string,
  nonce: string,
  sensorId: string
): Uint8Array {
  const canonicalMeasurements = canonicalize(measurements);
  const concatenated = `${canonicalMeasurements}${timestamp}${nonce}${sensorId}`;
  return new TextEncoder().encode(concatenated);
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
    const sensorId = signerAddress;
    const messageBytes = telemetryMessageBytes(measurements, timestamp, nonce, sensorId);
    const signatureBytes = ed25519Sign(messageBytes, pair);

    await expect(
      verifyTelemetrySignature({
        measurements,
        timestamp,
        nonce,
        sensorId,
        signature: Buffer.from(signatureBytes).toString('base64'),
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
    const messageBytes = telemetryMessageBytes(measurements, '2026-01-01T00:00:00Z', 'nonce-1', signerAddress);
    const signatureBytes = ed25519Sign(messageBytes, pair);

    await expect(
      verifyTelemetrySignature({
        measurements,
        timestamp: '2026-01-01T00:00:01Z',
        nonce: 'nonce-1',
        sensorId: signerAddress,
        signature: Buffer.from(signatureBytes).toString('base64'),
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
        sensorId: '5FHneW46xGXgs5mUiveU4sbTyGBzmstN5fJQw6QvP5M4Xv4H',
        signature: 'AA==',
        signerAddress: 'not-a-ss58-address'
      })
    ).resolves.toBe(false);
  });
});
