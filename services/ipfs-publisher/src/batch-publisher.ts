/**
 * Copyright 2026 Robonomics Network
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
import type { TelemetryAuthorizedPayload } from '@scp/core';
import { canonicalize } from 'json-canonicalize';
import { createHash } from 'node:crypto';
import {
  deriveAuthorizedBatchId,
  type AuthorizedBatchIdContext,
} from './batching.js';

export interface SerializedAuthorizedPayload {
  sensor_id: string;
  signed_envelope: string;
}

export interface AuthorizedBatchArtifact {
  schema_version: 'telemetry-ipfs-batch/v1';
  batch_id: string;
  event_count: number;
  events: SerializedAuthorizedPayload[];
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

export function buildDeterministicAuthorizedBatch(
  events: readonly TelemetryAuthorizedPayload[],
  batchContext?: AuthorizedBatchIdContext
): DeterministicAuthorizedBatch {
  if (events.length === 0) {
    throw new Error('Cannot build an empty authorized telemetry batch');
  }
  const validatedEvents = events.map((event, index) => {
    if (event.sensorId.length !== 32) {
      throw new Error(
        `Invalid authorized telemetry payload at index ${index}: sensor_id must contain 32 bytes`
      );
    }
    if (event.signedEnvelope.length === 0) {
      throw new Error(
        `Invalid authorized telemetry payload at index ${index}: signed_envelope cannot be empty`
      );
    }
    return {
      sensor_id: Buffer.from(event.sensorId).toString('base64'),
      signed_envelope: Buffer.from(event.signedEnvelope).toString('base64'),
    };
  });
  if (batchContext && batchContext.entries.length !== validatedEvents.length) {
    throw new Error(
      'Batch identity entry count must match authorized payload count'
    );
  }
  const descriptor = {
    schema_version: 'telemetry-ipfs-batch-descriptor/v1',
    event_count: validatedEvents.length,
    events: validatedEvents,
  } as const;
  const descriptorBytes = new TextEncoder().encode(canonicalize(descriptor));
  const batchId = batchContext
    ? deriveAuthorizedBatchId(batchContext)
    : 'batch-v1-' + createHash('sha256').update(descriptorBytes).digest('hex');
  const artifact: AuthorizedBatchArtifact = {
    schema_version: 'telemetry-ipfs-batch/v1',
    batch_id: batchId,
    event_count: validatedEvents.length,
    events: validatedEvents,
  };
  return {
    batchId,
    artifact,
    bytes: new TextEncoder().encode(canonicalize(artifact)),
  };
}

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
    pin: false,
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

export async function confirmAuthorizedBatchPin(
  ipfs: IpfsBatchClient,
  cid: string
): Promise<void> {
  if (cid.length === 0) {
    throw new Error('Cannot confirm an empty CID');
  }
  for await (const pin of ipfs.pin.ls({ paths: [cid], type: 'all' })) {
    if (pin.cid.toString() === cid) return;
  }
  throw new Error(`Kubo did not confirm pin for CID ${cid}`);
}
