import { cryptoWaitReady, ed25519PairFromSeed, ed25519Sign, encodeAddress } from '@polkadot/util-crypto';
import { describe, expect, it } from 'vitest';
import { buildEnvelopeSigningBytes } from '@scp/contracts';
import { verifyTelemetrySignature } from '../src/signature.js';

describe('verifyTelemetrySignature', () => {
  it('verifies protobuf envelope signature', async () => {
    await cryptoWaitReady();
    const seed = Uint8Array.from(Array.from({ length: 32 }, (_, i) => i + 1));
    const pair = ed25519PairFromSeed(seed);
    const signerAddress = encodeAddress(pair.publicKey, 32);
    const payload = {
      sensorId: pair.publicKey,
      timestamp: BigInt(Date.now()),
      nonce: Uint8Array.from(Buffer.alloc(16, 8)),
      message: Uint8Array.from(Buffer.from('test-message'))
    };
    const signature = ed25519Sign(buildEnvelopeSigningBytes(payload), pair);

    await expect(
      verifyTelemetrySignature({
        ...payload,
        signature,
        signerAddress
      })
    ).resolves.toBe(true);
  });

  it('returns false when envelope payload changes', async () => {
    await cryptoWaitReady();
    const seed = Uint8Array.from(Array.from({ length: 32 }, (_, i) => i + 1));
    const pair = ed25519PairFromSeed(seed);
    const signerAddress = encodeAddress(pair.publicKey, 32);
    const payload = {
      sensorId: pair.publicKey,
      timestamp: BigInt(Date.now()),
      nonce: Uint8Array.from(Buffer.alloc(16, 8)),
      message: Uint8Array.from(Buffer.from('test-message'))
    };
    const signature = ed25519Sign(buildEnvelopeSigningBytes(payload), pair);

    await expect(
      verifyTelemetrySignature({
        ...payload,
        message: Uint8Array.from(Buffer.from('mutated')),
        signature,
        signerAddress
      })
    ).resolves.toBe(false);
  });
});
