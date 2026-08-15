import { encodeAddress } from '@polkadot/util-crypto';

export interface SignedEnvelope {
  sensorId: Uint8Array;
  timestamp: bigint;
  nonce: Uint8Array;
  message: Uint8Array;
  signature: Uint8Array;
}

export const SENSOR_ID_LENGTH = 32;
export const SIGNATURE_LENGTH = 64;
export const MIN_NONCE_LENGTH = 16;
export const MAX_NONCE_LENGTH = 32;

const base64Pattern = /^(?:[A-Za-z0-9+/]{4})*(?:[A-Za-z0-9+/]{2}==|[A-Za-z0-9+/]{3}=)?$/;

export function decodeBase64(input: string): Uint8Array {
  const normalized = input.trim();
  if (normalized.length === 0) {
    throw new Error('Expected non-empty binary string');
  }
  if (!base64Pattern.test(normalized)) {
    throw new Error('Expected base64-encoded bytes');
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

function encodeVarint(value: bigint): Uint8Array {
  if (value < 0n) {
    throw new Error('Expected non-negative varint value');
  }
  const chunks: number[] = [];
  let remaining = value;
  while (remaining >= 0x80n) {
    chunks.push(Number((remaining & 0x7fn) | 0x80n));
    remaining >>= 7n;
  }
  chunks.push(Number(remaining));
  return Uint8Array.from(chunks);
}

function decodeVarint(bytes: Uint8Array, start: number): { value: bigint; nextOffset: number } {
  let value = 0n;
  let shift = 0n;
  let offset = start;
  while (offset < bytes.length) {
    const current = bytes[offset];
    if (current === undefined) {
      break;
    }
    value |= BigInt(current & 0x7f) << shift;
    offset += 1;
    if ((current & 0x80) === 0) {
      return { value, nextOffset: offset };
    }
    shift += 7n;
    if (shift >= 64n) {
      throw new Error('Varint exceeds uint64 range');
    }
  }
  throw new Error('Invalid varint encoding');
}

function encodeLengthDelimited(bytes: Uint8Array): Uint8Array {
  return concatBytes([encodeVarint(BigInt(bytes.length)), bytes]);
}

function readLengthDelimited(bytes: Uint8Array, start: number): { value: Uint8Array; nextOffset: number } {
  const { value: length, nextOffset } = decodeVarint(bytes, start);
  const size = Number(length);
  if (!Number.isSafeInteger(size) || size < 0) {
    throw new Error('Invalid length-delimited field size');
  }
  const end = nextOffset + size;
  if (end > bytes.length) {
    throw new Error('Truncated length-delimited field');
  }
  return { value: bytes.subarray(nextOffset, end), nextOffset: end };
}

function decodeSignedEnvelope(bytes: Uint8Array): SignedEnvelope {
  const envelope: SignedEnvelope = {
    sensorId: new Uint8Array(),
    timestamp: 0n,
    nonce: new Uint8Array(),
    message: new Uint8Array(),
    signature: new Uint8Array()
  };
  let offset = 0;
  while (offset < bytes.length) {
    const { value: tag, nextOffset: tagOffset } = decodeVarint(bytes, offset);
    offset = tagOffset;
    const fieldNumber = Number(tag >> 3n);
    const wireType = Number(tag & 0x07n);
    if (wireType === 2) {
      const field = readLengthDelimited(bytes, offset);
      offset = field.nextOffset;
      if (fieldNumber === 1) {
        envelope.sensorId = Uint8Array.from(field.value);
      } else if (fieldNumber === 3) {
        envelope.nonce = Uint8Array.from(field.value);
      } else if (fieldNumber === 4) {
        envelope.message = Uint8Array.from(field.value);
      } else if (fieldNumber === 5) {
        envelope.signature = Uint8Array.from(field.value);
      }
      continue;
    }
    if (fieldNumber === 2 && wireType === 0) {
      const field = decodeVarint(bytes, offset);
      envelope.timestamp = field.value;
      offset = field.nextOffset;
      continue;
    }
    if (wireType === 0) {
      offset = decodeVarint(bytes, offset).nextOffset;
      continue;
    }
    if (wireType === 1) {
      if (offset + 8 > bytes.length) {
        throw new Error('Truncated fixed64 field');
      }
      offset += 8;
      continue;
    }
    if (wireType === 5) {
      if (offset + 4 > bytes.length) {
        throw new Error('Truncated fixed32 field');
      }
      offset += 4;
      continue;
    }
    throw new Error(`Unsupported protobuf wire type: ${wireType}`);
  }
  return envelope;
}

export function validateSignedEnvelope(bytes: Uint8Array): SignedEnvelope {
  const envelope = decodeSignedEnvelope(bytes);
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
  return concatBytes([
    Uint8Array.from([0x0a]),
    encodeLengthDelimited(envelope.sensorId),
    Uint8Array.from([0x10]),
    encodeVarint(envelope.timestamp),
    Uint8Array.from([0x1a]),
    encodeLengthDelimited(envelope.nonce),
    Uint8Array.from([0x22]),
    encodeLengthDelimited(envelope.message),
    Uint8Array.from([0x2a]),
    encodeLengthDelimited(envelope.signature)
  ]);
}

export function createSignedEnvelope(input: {
  sensorId?: Uint8Array;
  timestamp?: bigint;
  nonce?: Uint8Array;
  message?: Uint8Array;
  signature?: Uint8Array;
}): SignedEnvelope {
  return {
    sensorId: input.sensorId ?? new Uint8Array(),
    timestamp: input.timestamp ?? 0n,
    nonce: input.nonce ?? new Uint8Array(),
    message: input.message ?? new Uint8Array(),
    signature: input.signature ?? new Uint8Array()
  };
}
