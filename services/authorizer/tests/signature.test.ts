import { cryptoWaitReady, ed25519PairFromSeed, ed25519Sign, encodeAddress } from '@polkadot/util-crypto';
import { createHash } from 'node:crypto';
import { describe, expect, it } from 'vitest';
import { canonicalize } from 'json-canonicalize';
import { verifyTelemetrySignature } from '../src/signature.js';

function telemetryHashHex(measurements: Record<string, unknown>, nonce: string, sensorAddress: string): string {
  const canonicalMeasurements = canonicalize(measurements);
  const concatenated = `${canonicalMeasurements}${nonce}${sensorAddress}`;
  return createHash('sha256').update(new TextEncoder().encode(concatenated)).digest('hex');
}

describe('verifyTelemetrySignature', () => {
  it('verifies signature with SS58 signer address', async () => {
    await cryptoWaitReady();
    const seed = Uint8Array.from(Array.from({ length: 32 }, (_, i) => i + 1));
    const pair = ed25519PairFromSeed(seed);
    const signerAddress = encodeAddress(pair.publicKey, 32);
    const measurements = { temp: 21, humidity: 54 };
    const nonce = 'nonce-1';
    const sensorAddress = signerAddress;
    const hashHex = telemetryHashHex(measurements, nonce, sensorAddress);
    const signatureBytes = ed25519Sign(Buffer.from(hashHex, 'hex'), pair);

    await expect(
      verifyTelemetrySignature({
        measurements,
        nonce,
        sensorAddress,
        signature: `0x${Buffer.from(signatureBytes).toString('hex')}`,
        signerAddress
      })
    ).resolves.toBe(true);
  });

  it('returns false for malformed signer address', async () => {
    await expect(
      verifyTelemetrySignature({
        measurements: { temp: 21 },
        nonce: 'nonce-1',
        sensorAddress: '5FHneW46xGXgs5mUiveU4sbTyGBzmstN5fJQw6QvP5M4Xv4H',
        signature: '0x00',
        signerAddress: 'not-a-ss58-address'
      })
    ).resolves.toBe(false);
  });
});
