import { describe, expect, it } from 'vitest';
import { createRedisKeyspace, mapRegistryEventToUpdate, toChainEventId } from '../src/keyspace.js';

describe('keyspace and mapping', () => {
  it('computes normalized redis keys', () => {
    const keys = createRedisKeyspace('registry-sync:v1');
    expect(keys.sensorState('sensor-1')).toBe('registry-sync:v1:sensor:sensor-1');
    expect(keys.nonceState('sensor-1', 'nonce-1')).toBe('registry-sync:v1:nonce:sensor-1:nonce-1');
    expect(keys.cursorHeight).toBe('registry-sync:v1:cursor:finalized-height');
  });

  it('maps create and disable authorization events', () => {
    const created = mapRegistryEventToUpdate({
      blockHeight: 10,
      eventIndex: 2,
      section: 'registry',
      method: 'AuthorizationCreated',
      sensorId: 'sensor-1'
    });

    const disabled = mapRegistryEventToUpdate({
      blockHeight: 11,
      eventIndex: 0,
      section: 'registry',
      method: 'AuthorizationDisabled',
      sensorId: 'sensor-1'
    });

    expect(created).toEqual({ sensorId: 'sensor-1', enabled: true });
    expect(disabled).toEqual({ sensorId: 'sensor-1', enabled: false });
  });

  it('maps rws.NewDevices addresses as eligible records', () => {
    const projected = mapRegistryEventToUpdate({
      blockHeight: 50,
      eventIndex: 1,
      section: 'rws',
      method: 'NewDevices',
      sensorId: 'sensor-eligible'
    });

    expect(projected).toEqual({
      sensorId: 'sensor-eligible',
      enabled: true
    });
  });


  it('returns null when enablement cannot be inferred', () => {
    const projected = mapRegistryEventToUpdate({
      blockHeight: 51,
      eventIndex: 2,
      section: 'registry',
      method: 'AuthorizationObserved',
      sensorId: 'sensor-unknown'
    });

    expect(projected).toBeNull();
  });

  it('uses chain identity as idempotency key', () => {
    expect(toChainEventId({ blockHeight: 42, eventIndex: 7 })).toBe('42:7');
  });
});
