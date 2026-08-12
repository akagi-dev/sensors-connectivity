import type { TelemetryAuthorizedPayload } from '@scp/contracts';
import { canonicalize } from 'json-canonicalize';
import { describe, expect, it, vi } from 'vitest';
import {
  buildDeterministicAuthorizedBatch,
  confirmAuthorizedBatchPin,
  publishAndPinAuthorizedBatch,
  type IpfsBatchClient
} from '../src/batch-publisher.js';

const testCid = 'bafybeibatch';

/** Creates valid authorized telemetry payloads for determinism tests. */
function payload(
  measurements: Record<string, unknown> = { humidity: 45, temperature: 22 }
): TelemetryAuthorizedPayload {
  return {
    sensor_id: 'sensor-1',
    timestamp: '2026-08-03T13:00:00.000Z',
    nonce: 'nonce-1',
    measurements,
    signature: '0x01'
  };
}

/** Creates a Kubo test double and exposes its add and pin calls. */
function createIpfsDouble(
  pinEntries: string[] = [testCid],
  addedCid = testCid,
  pinnedCid = testCid
): {
  client: IpfsBatchClient;
  add: ReturnType<typeof vi.fn>;
  pinAdd: ReturnType<typeof vi.fn>;
} {
  const add = vi.fn(async () => ({
    cid: { toString: (): string => addedCid }
  }));
  const pinAdd = vi.fn(async () => ({ toString: (): string => pinnedCid }));
  return {
    client: {
      add,
      pin: {
        add: pinAdd,
        async *ls() {
          for (const entry of pinEntries) {
            yield { cid: { toString: () => entry } };
          }
        }
      }
    },
    add,
    pinAdd
  };
}

describe('deterministic authorized batch', () => {
  it('encodes validated batch payloads as canonical JSON bytes', () => {
    const events = [
      payload({ humidity: 45, temperature: 22 }),
      {
        ...payload({ pressure: 1012 }),
        sensor_id: 'sensor-2',
        nonce: 'nonce-2'
      }
    ];
    const result = buildDeterministicAuthorizedBatch(events, {
      topic: 'telemetry.authorized.v1',
      partition: 1,
      entries: [
        { offset: '7', eventId: 'event-1' },
        { offset: '8', eventId: 'event-2' }
      ]
    });
    const encoded = new TextDecoder().decode(result.bytes);

    expect(JSON.parse(encoded)).toEqual({
      schema_version: 'telemetry-ipfs-batch/v1',
      batch_id: result.batchId,
      event_count: 2,
      events
    });
    expect(encoded).toBe(canonicalize(result.artifact));
    expect(result.artifact.events).toEqual(events);
  });

  it('produces the same ID and bytes for equivalent key insertion orders', () => {
    const first = buildDeterministicAuthorizedBatch([
      payload({ humidity: 45, temperature: 22 })
    ]);
    const second = buildDeterministicAuthorizedBatch([
      payload({ temperature: 22, humidity: 45 })
    ]);

    expect(second.batchId).toBe(first.batchId);
    expect(second.bytes).toEqual(first.bytes);
    expect(first.artifact.event_count).toBe(1);
  });

  it('changes the batch ID when event order changes', () => {
    const first = payload();
    const second = { ...payload(), sensor_id: 'sensor-2', nonce: 'nonce-2' };

    expect(buildDeterministicAuthorizedBatch([first, second]).batchId).not.toBe(
      buildDeterministicAuthorizedBatch([second, first]).batchId
    );
  });

  it('rejects empty and invalid batches', () => {
    expect(() => buildDeterministicAuthorizedBatch([])).toThrow('empty');
    expect(() =>
      buildDeterministicAuthorizedBatch([{ ...payload(), signature: '' }])
    ).toThrow('Invalid authorized telemetry payload');
    expect(() =>
      buildDeterministicAuthorizedBatch([payload()], {
        topic: 'telemetry.authorized.v1',
        partition: 0,
        entries: [
          { offset: '1', eventId: 'event-1' },
          { offset: '2', eventId: 'event-2' }
        ]
      })
    ).toThrow('entry count');
  });
});

describe('IPFS batch publication', () => {
  it('adds canonical bytes with stable options and pins the CID', async () => {
    const { client, add, pinAdd } = createIpfsDouble();
    const bytes = new Uint8Array([1, 2, 3]);

    await expect(publishAndPinAuthorizedBatch(client, bytes)).resolves.toBe(
      testCid
    );
    expect(add).toHaveBeenCalledWith(bytes, {
      cidVersion: 1,
      hashAlg: 'sha2-256',
      rawLeaves: true,
      chunker: 'size-262144',
      wrapWithDirectory: false,
      pin: false
    });
    expect(pinAdd).toHaveBeenCalledWith(testCid, { recursive: true });
  });

  it('confirms a present pin and rejects a missing pin', async () => {
    await expect(
      confirmAuthorizedBatchPin(createIpfsDouble().client, testCid)
    ).resolves.toBeUndefined();
    await expect(
      confirmAuthorizedBatchPin(createIpfsDouble([]).client, testCid)
    ).rejects.toThrow('did not confirm pin');
  });

  it('rejects an empty artifact before calling Kubo', async () => {
    const { client, add, pinAdd } = createIpfsDouble();

    await expect(
      publishAndPinAuthorizedBatch(client, new Uint8Array())
    ).rejects.toThrow('empty IPFS batch artifact');
    expect(add).not.toHaveBeenCalled();
    expect(pinAdd).not.toHaveBeenCalled();
  });

  it('does not pin when Kubo add fails or returns an empty CID', async () => {
    const failedAdd = createIpfsDouble();
    failedAdd.add.mockRejectedValueOnce(new Error('fetch failed'));

    await expect(
      publishAndPinAuthorizedBatch(failedAdd.client, new Uint8Array([1]))
    ).rejects.toThrow('fetch failed');
    expect(failedAdd.pinAdd).not.toHaveBeenCalled();

    const emptyCid = createIpfsDouble([testCid], '');
    await expect(
      publishAndPinAuthorizedBatch(emptyCid.client, new Uint8Array([1]))
    ).rejects.toThrow('empty CID');
    expect(emptyCid.pinAdd).not.toHaveBeenCalled();
  });

  it('rejects a pin response for a different CID', async () => {
    const { client, pinAdd } = createIpfsDouble(
      [testCid],
      testCid,
      'bafybeidifferent'
    );

    await expect(
      publishAndPinAuthorizedBatch(client, new Uint8Array([1]))
    ).rejects.toThrow('pinned unexpected CID');
    expect(pinAdd).toHaveBeenCalledWith(testCid, { recursive: true });
  });
});
