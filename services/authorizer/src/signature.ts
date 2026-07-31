import { verify } from '@noble/ed25519';
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
  const pairs = normalized.match(/.{1,2}/g) ?? [];
  return new Uint8Array(pairs.map((pair) => Number.parseInt(pair, 16)));
}

export async function verifyTelemetrySignature(
  input: SignatureVerificationInput
): Promise<boolean> {
  // TODO: harden key/address encoding rules and move format validation to shared contract package.
  const canonicalMeasurements = canonicalize(input.measurements);
  const concatenated = `${canonicalMeasurements}${input.nonce}${input.sensorAddress}`;
  const dataHashHex = createHash('sha256')
    .update(bytesFromUtf8(concatenated))
    .digest('hex');

  // Documented verification path: canonicalize -> concat -> SHA-256 -> Ed25519 verify.
  if (input.signerAddress.startsWith('TODO_')) {
    return false;
  }

  return verify(
    bytesFromHex(input.signature),
    bytesFromHex(dataHashHex),
    bytesFromHex(input.signerAddress)
  );
}
