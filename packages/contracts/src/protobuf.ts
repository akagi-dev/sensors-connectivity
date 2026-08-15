import { create, fromBinary, toBinary } from '@bufbuild/protobuf';
import { encodeAddress } from '@polkadot/util-crypto';
import { SignedEnvelopeSchema, type SignedEnvelope } from './proto/crypto/v1/envelope_pb.js';

export const SENSOR_ID_LENGTH = 32;
export const SIGNATURE_LENGTH = 64;
export const MIN_NONCE_LENGTH = 16;
export const MAX_NONCE_LENGTH = 32;

export function decodeBase64OrHex(input: string): Uint8Array {
  const normalized = input.trim();
  if (normalized.length === 0) {
    throw new Error('Expected non-empty binary string');
  }
  const isHex = normalized.startsWith('0x') || /^[0-9a-fA-F]+$/.test(normalized);
  if (isHex) {
    const hex = normalized.startsWith('0x') ? normalized.slice(2) : normalized;
    if (hex.length % 2 !== 0) {
      throw new Error('Expected even-length hex string');
    }
    return Uint8Array.from(Buffer.from(hex, 'hex'));
  }
  return Uint8Array.from(Buffer.from(normalized, 'base64'));
}

export function encodeBase64(bytes: Uint8Array): string {
  return Buffer.from(bytes).toString('base64');
}

export function timestampToLeBytes(timestamp: bigint): Uint8Array {
  const out = new Uint8Array(8);
  new DataView(out.buffer).setBigUint64(0, timestamp, true);
  return out;
}

export function concatBytes(parts: Uint8Array[]): Uint8Array {
  const total = parts.reduce((sum, p) => sum + p.length, 0);
  const out = new Uint8Array(total);
  let offset = 0;
  for (const part of parts) {
    out.set(part, offset);
    offset += part.length;
  }
  return out;
}

export function buildEnvelopeSigningBytes(envelope: Pick<SignedEnvelope, 'sensorId' | 'timestamp' | 'nonce' | 'message'>): Uint8Array {
  return concatBytes([
    envelope.sensorId,
    timestampToLeBytes(envelope.timestamp),
    envelope.nonce,
    envelope.message
  ]);
}

export function validateSignedEnvelope(bytes: Uint8Array): SignedEnvelope {
  const envelope = fromBinary(SignedEnvelopeSchema, bytes);
  if (envelope.sensorId.length !== SENSOR_ID_LENGTH) {
    throw new Error(`sensor_id must be ${SENSOR_ID_LENGTH} bytes`);
  }
  if (envelope.signature.length !== SIGNATURE_LENGTH) {
    throw new Error(`signature must be ${SIGNATURE_LENGTH} bytes`);
  }
  if (envelope.nonce.length < MIN_NONCE_LENGTH || envelope.nonce.length > MAX_NONCE_LENGTH) {
    throw new Error(`nonce must be ${MIN_NONCE_LENGTH}-${MAX_NONCE_LENGTH} bytes`);
  }
  if (envelope.message.length === 0) {
    throw new Error('message must be non-empty');
  }
  return envelope;
}

export function extractSensorId(envelope: Pick<SignedEnvelope, 'sensorId'>): string {
  return encodeAddress(envelope.sensorId, 32);
}

export function toSignedEnvelopeBytes(envelope: SignedEnvelope): Uint8Array {
  return toBinary(SignedEnvelopeSchema, envelope);
}

export function createSignedEnvelope(input: {
  sensorId?: Uint8Array;
  timestamp?: bigint;
  nonce?: Uint8Array;
  message?: Uint8Array;
  signature?: Uint8Array;
}): SignedEnvelope {
  return create(SignedEnvelopeSchema, input);
}
