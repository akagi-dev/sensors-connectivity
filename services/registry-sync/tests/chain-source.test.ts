import { describe, expect, it } from 'vitest';
import { decodeAddress, encodeAddress } from '@polkadot/util-crypto';
import { normalizeRegistryEvents } from '../src/chain-source.js';

describe('normalizeRegistryEvents', () => {
  it('parses finalized registry event payload fields', () => {
    const records = [
      {
        event: {
          section: 'registry',
          method: 'AuthorizationUpdated',
          data: {
            toJSON: () => ({
              sensor_id: 'sensor-1',
              enabled: true,
            }),
          },
        },
        phase: {
          isApplyExtrinsic: true,
          asApplyExtrinsic: { toNumber: () => 3 },
        },
      },
    ];

    const normalized = normalizeRegistryEvents(records as never, 120);

    expect(normalized).toEqual([
      {
        blockHeight: 120,
        eventIndex: 0,
        section: 'registry',
        method: 'AuthorizationUpdated',
        sensorId: 'sensor-1',
        enabled: true,
        rawData: {
          sensor_id: 'sensor-1',
          enabled: true,
        },
      },
    ]);
  });

  it('filters non-registry events', () => {
    const records = [
      {
        event: {
          section: 'balances',
          method: 'Transfer',
          data: { toJSON: () => ({ from: 'a', to: 'b' }) },
        },
        phase: {
          isApplyExtrinsic: false,
          asApplyExtrinsic: { toNumber: () => 0 },
        },
      },
    ];

    expect(normalizeRegistryEvents(records as never, 121)).toEqual([]);
  });

  it('expands rws.NewDevices addresses into eligible events', () => {
    const fixtureA = '4CvP46mxFm54eBbTMFayHK7n38MaXo7gCbq7KCHSd28xrWSJ';
    const fixtureB = '4F43fMXAe3Y9uKWqZoEZwxKCgTdhJ4aLS4NDkXKLjdnCz9jC';
    const expectedA = encodeAddress(decodeAddress(fixtureA), 32);
    const expectedB = encodeAddress(decodeAddress(fixtureB), 32);

    const records = [
      {
        event: {
          section: 'rws',
          method: 'NewDevices',
          data: {
            toJSON: () => ({
              devices: [fixtureA, fixtureB],
            }),
          },
        },
        phase: {
          isApplyExtrinsic: true,
          asApplyExtrinsic: { toNumber: () => 4 },
        },
      },
    ];

    const normalized = normalizeRegistryEvents(records as never, 140);
    expect(normalized).toHaveLength(2);
    expect(normalized[0]?.sensorId).toBe(expectedA);
    expect(normalized[1]?.sensorId).toBe(expectedB);
    expect(normalized[0]?.enabled).toBe(true);
    expect(normalized[1]?.enabled).toBe(true);
  });
});
