import { describe, expect, it } from 'vitest';
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
              sensor_address: 'sensor-1',
              enabled: true
            })
          }
        },
        phase: {
          isApplyExtrinsic: true,
          asApplyExtrinsic: { toNumber: () => 3 }
        }
      }
    ];

    const normalized = normalizeRegistryEvents(records as never, 120);

    expect(normalized).toEqual([
      {
        blockHeight: 120,
        eventIndex: 0,
        section: 'registry',
        method: 'AuthorizationUpdated',
        sensorAddress: 'sensor-1',
        enabled: true,
        rawData: {
          sensor_address: 'sensor-1',
          enabled: true
        }
      }
    ]);
  });

  it('filters non-registry events', () => {
    const records = [
      {
        event: {
          section: 'balances',
          method: 'Transfer',
          data: { toJSON: () => ({ from: 'a', to: 'b' }) }
        },
        phase: {
          isApplyExtrinsic: false,
          asApplyExtrinsic: { toNumber: () => 0 }
        }
      }
    ];

    expect(normalizeRegistryEvents(records as never, 121)).toEqual([]);
  });

  it('expands rws.NewDevices addresses into eligible events', () => {
    const records = [
      {
        event: {
          section: 'rws',
          method: 'NewDevices',
          data: {
            toJSON: () => ({
              devices: [
                '5GrwvaEF5zXb26Fz9rcQpDWS57CtERHpNehXCPcNoHGKutQY',
                '5FHneW46xGXgs5mUiveU4sbTyGBzmstN5fJQw6QvP5M4Xv4H'
              ]
            })
          }
        },
        phase: {
          isApplyExtrinsic: true,
          asApplyExtrinsic: { toNumber: () => 4 }
        }
      }
    ];

    const normalized = normalizeRegistryEvents(records as never, 140);
    expect(normalized).toHaveLength(2);
    expect(normalized[0]?.sensorAddress).toBe('5GrwvaEF5zXb26Fz9rcQpDWS57CtERHpNehXCPcNoHGKutQY');
    expect(normalized[1]?.sensorAddress).toBe('5FHneW46xGXgs5mUiveU4sbTyGBzmstN5fJQw6QvP5M4Xv4H');
    expect(normalized[0]?.enabled).toBe(true);
    expect(normalized[1]?.enabled).toBe(true);
  });
});
