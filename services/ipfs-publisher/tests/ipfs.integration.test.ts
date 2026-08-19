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
import { create } from '@bufbuild/protobuf';
import {
  TelemetryAuthorizedPayloadSchema,
  type TelemetryAuthorizedPayload,
} from '@scp/core';
import { create as createKuboRPCClient } from 'kubo-rpc-client';
import { describe, expect, it } from 'vitest';
import {
  buildDeterministicAuthorizedBatch,
  confirmAuthorizedBatchPin,
  publishAndPinAuthorizedBatch,
  type IpfsBatchClient,
} from '../src/batch-publisher.js';

const runIpfsIntegration = process.env.IPFS_PUBLISHER_IPFS_INTEGRATION === '1';

type IpfsIntegrationClient = IpfsBatchClient & {
  cat(cid: string): AsyncIterable<Uint8Array>;
};

describe.runIf(runIpfsIntegration)('ipfs-publisher Kubo integration', () => {
  it('publishes, pins, and reads back the deterministic artifact', async () => {
    const client = createKuboRPCClient(
      process.env.IPFS_API_URL ?? 'http://127.0.0.1:5001'
    ) as unknown as IpfsIntegrationClient;
    const event: TelemetryAuthorizedPayload = create(
      TelemetryAuthorizedPayloadSchema,
      {
        sensorId: Buffer.alloc(32, 1),
        signedEnvelope: Buffer.from('integration-envelope'),
      }
    );
    const batch = buildDeterministicAuthorizedBatch([event]);

    const cid = await publishAndPinAuthorizedBatch(client, batch.bytes);

    expect(cid).toMatch(/^b[a-z2-7]+$/);
    await expect(
      confirmAuthorizedBatchPin(client, cid)
    ).resolves.toBeUndefined();

    const publishedBytes: number[] = [];
    for await (const chunk of client.cat(cid)) {
      publishedBytes.push(...chunk);
    }
    expect(new Uint8Array(publishedBytes)).toEqual(batch.bytes);
  });
});
