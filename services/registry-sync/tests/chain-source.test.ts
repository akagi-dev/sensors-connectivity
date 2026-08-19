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
