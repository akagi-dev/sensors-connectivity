import {
  telemetryAuthorizedPayloadSchema,
  type TelemetryAuthorizedPayload
} from '@scp/contracts';
import { canonicalize } from 'json-canonicalize';
import { createHash } from 'node:crypto';
import {
  deriveAuthorizedBatchId,
  type AuthorizedBatchIdContext
} from './batching.js';

export interface AuthorizedBatchArtifact {
  schema_version: 'telemetry-ipfs-batch/v1';
  batch_id: string;
  event_count: number;
  events: TelemetryAuthorizedPayload[];
}

export interface DeterministicAuthorizedBatch {
  batchId: string;
  artifact: AuthorizedBatchArtifact;
  bytes: Uint8Array;
}

export interface IpfsAddOptions {
  cidVersion: 1;
  hashAlg: 'sha2-256';
  rawLeaves: true;
  chunker: 'size-262144';
  wrapWithDirectory: false;
  pin: false;
}

export interface IpfsBatchClient {
  add(
    content: Uint8Array,
    options: IpfsAddOptions
  ): Promise<{ cid: { toString(): string } }>;
  pin: {
    add(
      cid: string,
      options: { recursive: true }
    ): Promise<{ toString(): string }>;
    ls(options: {
      paths: string[];
      type: 'all';
    }): AsyncIterable<{ cid: { toString(): string } }>;
  };
}

/**
 * Validates authorized payloads and produces stable canonical JSON bytes.
 * When Kafka identity metadata is available, derives the batch identifier from
 * its canonical descriptor; otherwise, uses the payload descriptor.
 */
export function buildDeterministicAuthorizedBatch(
  events: readonly TelemetryAuthorizedPayload[],
  batchContext?: AuthorizedBatchIdContext
): DeterministicAuthorizedBatch {
  if (events.length === 0) {
    throw new Error('Cannot build an empty authorized telemetry batch');
  }

  const validatedEvents = events.map((event, index) => {
    const result = telemetryAuthorizedPayloadSchema.safeParse(event);
    if (!result.success) {
      throw new Error(
        `Invalid authorized telemetry payload at index ${index}: ${result.error.message}`
      );
    }
    return result.data;
  });
  if (batchContext && batchContext.entries.length !== validatedEvents.length) {
    throw new Error(
      'Batch identity entry count must match authorized payload count'
    );
  }
  const descriptor = {
    schema_version: 'telemetry-ipfs-batch-descriptor/v1',
    event_count: validatedEvents.length,
    events: validatedEvents
  } as const;
  const descriptorBytes = new TextEncoder().encode(canonicalize(descriptor));
  const batchId = batchContext
    ? deriveAuthorizedBatchId(batchContext)
    : 'batch-v1-' + createHash('sha256').update(descriptorBytes).digest('hex');
  const artifact: AuthorizedBatchArtifact = {
    schema_version: 'telemetry-ipfs-batch/v1',
    batch_id: batchId,
    event_count: validatedEvents.length,
    events: validatedEvents
  };

  return {
    batchId,
    artifact,
    bytes: new TextEncoder().encode(canonicalize(artifact))
  };
}

/**
 * Adds canonical batch bytes to Kubo with fixed CID options and explicitly pins
 * the resulting CID before returning it.
 */
export async function publishAndPinAuthorizedBatch(
  ipfs: IpfsBatchClient,
  batchBytes: Uint8Array
): Promise<string> {
  if (batchBytes.length === 0) {
    throw new Error('Cannot publish an empty IPFS batch artifact');
  }

  const added = await ipfs.add(batchBytes, {
    cidVersion: 1,
    hashAlg: 'sha2-256',
    rawLeaves: true,
    chunker: 'size-262144',
    wrapWithDirectory: false,
    pin: false
  });
  const cid = added.cid.toString();
  if (cid.length === 0) {
    throw new Error('Kubo add returned an empty CID');
  }

  const pinnedCid = (await ipfs.pin.add(cid, { recursive: true })).toString();
  if (pinnedCid !== cid) {
    throw new Error(
      `Kubo pinned unexpected CID ${pinnedCid} instead of ${cid}`
    );
  }

  return cid;
}

/** Confirms that a published CID exists in Kubo's local pin set. */
export async function confirmAuthorizedBatchPin(
  ipfs: IpfsBatchClient,
  cid: string
): Promise<void> {
  if (cid.length === 0) {
    throw new Error('Cannot confirm an empty CID');
  }

  for await (const pin of ipfs.pin.ls({ paths: [cid], type: 'all' })) {
    if (pin.cid.toString() === cid) {
      return;
    }
  }

  throw new Error(`Kubo did not confirm pin for CID ${cid}`);
}
