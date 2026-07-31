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
              public_key: 'pk-1',
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
        eventIndex: 3,
        section: 'registry',
        method: 'AuthorizationUpdated',
        sensorAddress: 'sensor-1',
        publicKey: 'pk-1',
        enabled: true,
        rawData: {
          sensor_address: 'sensor-1',
          public_key: 'pk-1',
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
});
