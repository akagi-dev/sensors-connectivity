import { decodeAddress, ed25519Verify } from '@polkadot/util-crypto';
import { buildEnvelopeSigningBytes } from '@scp/contracts';

export interface SignatureVerificationInput {
  sensorId: Uint8Array;
  timestamp: bigint;
  nonce: Uint8Array;
  message: Uint8Array;
  signature: Uint8Array;
  signerAddress: string;
}

export async function verifyTelemetrySignature(
  input: SignatureVerificationInput
): Promise<boolean> {
  try {
    return ed25519Verify(
      buildEnvelopeSigningBytes(input),
      input.signature,
      decodeAddress(input.signerAddress)
    );
  } catch {
    return false;
  }
}
