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
import { canonicalize } from 'json-canonicalize';
import { describe, expect, it, vi } from 'vitest';
import {
  buildDeterministicAuthorizedBatch,
  confirmAuthorizedBatchPin,
  publishAndPinAuthorizedBatch,
  type IpfsBatchClient,
} from '../src/batch-publisher.js';
import { authorizedPayload } from './helpers.js';

const cid = 'bafybeibatch';

function ipfsDouble(pins: string[] = [cid]): {
  client: IpfsBatchClient;
  add: ReturnType<typeof vi.fn>;
  pinAdd: ReturnType<typeof vi.fn>;
} {
  const add = vi.fn(async () => ({ cid: { toString: () => cid } }));
  const pinAdd = vi.fn(async () => ({ toString: () => cid }));
  return {
    client: {
      add,
      pin: {
        add: pinAdd,
        async *ls() {
          for (const pin of pins) yield { cid: { toString: () => pin } };
        },
      },
    },
    add,
    pinAdd,
  };
}

describe('deterministic authorized batch', () => {
  it('serializes current protobuf byte fields as canonical base64 JSON', () => {
    const result = buildDeterministicAuthorizedBatch(
      [authorizedPayload(1), authorizedPayload(2)],
      {
        topic: 'telemetry.authorized.v1',
        partition: 0,
        entries: [
          { offset: '7', eventId: 'event-1' },
          { offset: '8', eventId: 'event-2' },
        ],
      }
    );
    expect(JSON.parse(new TextDecoder().decode(result.bytes))).toEqual(
      result.artifact
    );
    expect(new TextDecoder().decode(result.bytes)).toBe(
      canonicalize(result.artifact)
    );
    expect(result.artifact.events[0]).toEqual({
      sensor_id: Buffer.alloc(32, 1).toString('base64'),
      signed_envelope: Buffer.from([1, 2, 3]).toString('base64'),
    });
  });

  it('is deterministic and validates current payload constraints', () => {
    expect(
      buildDeterministicAuthorizedBatch([authorizedPayload()]).bytes
    ).toEqual(buildDeterministicAuthorizedBatch([authorizedPayload()]).bytes);
    expect(() => buildDeterministicAuthorizedBatch([])).toThrow('empty');
    expect(() =>
      buildDeterministicAuthorizedBatch([
        { ...authorizedPayload(), sensorId: Buffer.alloc(31) },
      ])
    ).toThrow('32 bytes');
    expect(() =>
      buildDeterministicAuthorizedBatch([
        { ...authorizedPayload(), signedEnvelope: Buffer.alloc(0) },
      ])
    ).toThrow('cannot be empty');
  });
});

describe('IPFS publication', () => {
  it('adds fixed bytes, pins the CID, and confirms the pin', async () => {
    const test = ipfsDouble();
    const bytes = Buffer.from([1, 2, 3]);
    await expect(
      publishAndPinAuthorizedBatch(test.client, bytes)
    ).resolves.toBe(cid);
    expect(test.add).toHaveBeenCalledWith(bytes, {
      cidVersion: 1,
      hashAlg: 'sha2-256',
      rawLeaves: true,
      chunker: 'size-262144',
      wrapWithDirectory: false,
      pin: false,
    });
    expect(test.pinAdd).toHaveBeenCalledWith(cid, { recursive: true });
    await expect(
      confirmAuthorizedBatchPin(test.client, cid)
    ).resolves.toBeUndefined();
    await expect(
      confirmAuthorizedBatchPin(ipfsDouble([]).client, cid)
    ).rejects.toThrow('did not confirm pin');
  });
});
