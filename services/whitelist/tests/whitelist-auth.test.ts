import { describe, it, expect } from 'vitest';
import { WhitelistAuth } from '../src/whitelist-auth.js';

describe('WhitelistAuth', () => {
  it('should authenticate sensors in the whitelist', async () => {
    const auth = new WhitelistAuth(['sensor-1', 'sensor-2', 'sensor-3']);

    expect(await auth.authenticate('sensor-1')).toBe(true);
    expect(await auth.authenticate('sensor-2')).toBe(true);
    expect(await auth.authenticate('sensor-3')).toBe(true);
  });

  it('should reject sensors not in the whitelist', async () => {
    const auth = new WhitelistAuth(['sensor-1', 'sensor-2']);

    expect(await auth.authenticate('sensor-3')).toBe(false);
    expect(await auth.authenticate('unknown-sensor')).toBe(false);
  });

  it('should handle empty whitelist', async () => {
    const auth = new WhitelistAuth([]);

    expect(await auth.authenticate('sensor-1')).toBe(false);
    expect(await auth.authenticate('any-sensor')).toBe(false);
  });

  it('should track seen nonces', async () => {
    const auth = new WhitelistAuth(['sensor-1']);

    expect(await auth.isNonceSeen('sensor-1', 'nonce-1')).toBe(false);

    await auth.rememberNonce('sensor-1', 'nonce-1');

    expect(await auth.isNonceSeen('sensor-1', 'nonce-1')).toBe(true);
  });

  it('should track nonces per sensor', async () => {
    const auth = new WhitelistAuth(['sensor-1', 'sensor-2']);

    await auth.rememberNonce('sensor-1', 'nonce-1');

    expect(await auth.isNonceSeen('sensor-1', 'nonce-1')).toBe(true);
    expect(await auth.isNonceSeen('sensor-2', 'nonce-1')).toBe(false);
  });

  it('should return list of allowed sensors', () => {
    const allowedSensors = ['sensor-1', 'sensor-2', 'sensor-3'];
    const auth = new WhitelistAuth(allowedSensors);

    const result = auth.getAllowedSensors();
    expect(result).toHaveLength(3);
    expect(result).toContain('sensor-1');
    expect(result).toContain('sensor-2');
    expect(result).toContain('sensor-3');
  });

  it('should handle duplicate sensor IDs in constructor', () => {
    const auth = new WhitelistAuth(['sensor-1', 'sensor-1', 'sensor-2']);

    const result = auth.getAllowedSensors();
    expect(result).toHaveLength(2);
    expect(result).toContain('sensor-1');
    expect(result).toContain('sensor-2');
  });
});
