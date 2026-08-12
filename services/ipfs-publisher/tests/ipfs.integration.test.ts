import type { TelemetryAuthorizedPayload } from '@scp/contracts';
import { create as createKuboRPCClient } from 'kubo-rpc-client';
import { describe, expect, it } from 'vitest';
import {
  buildDeterministicAuthorizedBatch,
  confirmAuthorizedBatchPin,
  publishAndPinAuthorizedBatch,
  type IpfsBatchClient
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
    const event: TelemetryAuthorizedPayload = {
      sensor_id: 'integration-sensor',
      timestamp: '2026-08-11T00:00:00.000Z',
      nonce: 'ipfs-publication-integration',
      measurements: { temperature: 22 },
      signature: '0x01'
    };
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
