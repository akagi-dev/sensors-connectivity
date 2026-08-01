import { decodeAddress, ed25519Verify } from '@polkadot/util-crypto';
import { createHash } from 'node:crypto';
import { canonicalize } from 'json-canonicalize';

export interface SignatureVerificationInput {
  measurements: Record<string, unknown>;
  nonce: string;
  sensorAddress: string;
  signature: string;
  signerAddress: string;
}

function bytesFromUtf8(input: string): Uint8Array {
  return new TextEncoder().encode(input);
}

function bytesFromHex(input: string): Uint8Array {
  const normalized = input.startsWith('0x') ? input.slice(2) : input;
  if (normalized.length === 0 || normalized.length % 2 !== 0 || !/^[0-9a-fA-F]+$/.test(normalized)) {
    throw new Error('Expected even-length hex input');
  }
  const pairs = normalized.match(/.{1,2}/g) ?? [];
  return new Uint8Array(pairs.map((pair) => Number.parseInt(pair, 16)));
}

export async function verifyTelemetrySignature(
  input: SignatureVerificationInput
): Promise<boolean> {
  if (input.signerAddress.startsWith('TODO_')) {
    return false;
  }

  try {
    const canonicalMeasurements = canonicalize(input.measurements);
    const concatenated = `${canonicalMeasurements}${input.nonce}${input.sensorAddress}`;
    const dataHashHex = createHash('sha256')
      .update(bytesFromUtf8(concatenated))
      .digest('hex');

    // Documented verification path: canonicalize -> concat -> SHA-256 -> Ed25519 verify.
    return ed25519Verify(
      bytesFromHex(dataHashHex),
      bytesFromHex(input.signature),
      decodeAddress(input.signerAddress)
    );
  } catch {
    return false;
  }
}
