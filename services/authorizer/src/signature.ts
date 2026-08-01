import { decodeAddress, ed25519Verify } from '@polkadot/util-crypto';
import { canonicalize } from 'json-canonicalize';

export interface SignatureVerificationInput {
  measurements: Record<string, unknown>;
  timestamp: string;
  nonce: string;
  sensorAddress: string;
  signature: string;
  signerAddress: string;
}

function bytesFromUtf8(input: string): Uint8Array {
  return new TextEncoder().encode(input);
}

function bytesFromBase64(input: string): Uint8Array {
  const normalized = input.trim();
  if (normalized.length === 0) {
    throw new Error('Expected non-empty base64 input');
  }
  return Uint8Array.from(Buffer.from(normalized, 'base64'));
}

export async function verifyTelemetrySignature(
  input: SignatureVerificationInput
): Promise<boolean> {
  try {
    const canonicalMeasurements = canonicalize(input.measurements);
    const concatenated = `${canonicalMeasurements}${input.timestamp}${input.nonce}${input.sensorAddress}`;

    return ed25519Verify(
      bytesFromUtf8(concatenated),
      bytesFromBase64(input.signature),
      decodeAddress(input.signerAddress)
    );
  } catch {
    return false;
  }
}
