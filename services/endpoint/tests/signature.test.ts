import {
  cryptoWaitReady,
  ed25519PairFromSeed,
  ed25519Sign,
} from '@polkadot/util-crypto';
import { describe, expect, it } from 'vitest';
import { SignedEnvelopeSchema } from '@buf/airalab_sensors-social-proto.bufbuild_es/crypto/v1/envelope_pb.js';
import { validateSignedEnvelope, buildEnvelopeSigningBytes } from '@scp/core';
import { create, toBinary } from '@bufbuild/protobuf';

describe('verifyTelemetrySignature', () => {
  it('verifies protobuf envelope signature', async () => {
    await cryptoWaitReady();
    const seed = Uint8Array.from(Array.from({ length: 32 }, (_, i) => i + 1));
    const pair = ed25519PairFromSeed(seed);
    const payload = {
      sensorId: pair.publicKey,
      timestamp: BigInt(Date.now()),
      nonce: Uint8Array.from(Buffer.alloc(16, 8)),
      message: Uint8Array.from(Buffer.from('test-message')),
    };
    const signature = ed25519Sign(buildEnvelopeSigningBytes(payload), pair);
    const envelope = create(SignedEnvelopeSchema, {
      ...payload,
      signature,
    });
    await expect(
      validateSignedEnvelope(toBinary(SignedEnvelopeSchema, envelope), true)
    ).resolves.toStrictEqual(envelope);
  });

  it('returns false when envelope payload changes', async () => {
    await cryptoWaitReady();
    const seed = Uint8Array.from(Array.from({ length: 32 }, (_, i) => i + 1));
    const pair = ed25519PairFromSeed(seed);
    const payload = {
      sensorId: pair.publicKey,
      timestamp: BigInt(Date.now()),
      nonce: Uint8Array.from(Buffer.alloc(16, 8)),
      message: Uint8Array.from(Buffer.from('test-message')),
    };
    const signature = ed25519Sign(buildEnvelopeSigningBytes(payload), pair);
    const envelope = create(SignedEnvelopeSchema, {
      ...payload,
      message: Uint8Array.from(Buffer.from('mutated')),
      signature,
    });

    await expect(
      validateSignedEnvelope(toBinary(SignedEnvelopeSchema, envelope), true)
    ).rejects.toThrow('bad signature');
  });
});
