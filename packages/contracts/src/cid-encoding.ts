import { CID } from 'multiformats/cid';

/**
 * Encode CID string to binary bytes.
 * Accepts both CIDv0 (base58) and CIDv1 (base32/base58) formats.
 */
export function encodeCid(cidString: string): Uint8Array {
  const cid = CID.parse(cidString);
  return cid.bytes;
}

/**
 * Decode CID binary bytes to string.
 * Returns CIDv1 base32 string by default.
 */
export function decodeCid(cidBytes: Uint8Array): string {
  const cid = CID.decode(cidBytes);
  return cid.toString();
}

/**
 * Validate CID string format.
 */
export function isValidCid(cidString: string): boolean {
  try {
    CID.parse(cidString);
    return true;
  } catch {
    return false;
  }
}

/**
 * Convert CIDv0 to CIDv1 if needed.
 * Returns CIDv1 base32 string.
 */
export function normalizeCid(cidString: string): string {
  const cid = CID.parse(cidString);
  if (cid.version === 0) {
    return cid.toV1().toString();
  }
  return cid.toString();
}
