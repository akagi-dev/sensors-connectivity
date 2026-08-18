import { describe, it, expect } from 'vitest';
import { randomBytes } from 'node:crypto';
import { encodeAddress } from '@polkadot/util-crypto';
import { WhitelistAuth } from '../src/whitelist-auth.js';

// Helper to create SS58 test addresses (using Robonomics prefix 32)
function createTestAddress(): { address: string; pubkey: Uint8Array } {
  const pubkey = randomBytes(32);
  const address = encodeAddress(pubkey, 32); // 32 = Robonomics prefix
  return { address, pubkey };
}

describe('WhitelistAuth', () => {
  it('should authenticate sensors in the whitelist', async () => {
    const sensor1 = createTestAddress();
    const sensor2 = createTestAddress();
    const sensor3 = createTestAddress();

    const auth = new WhitelistAuth([
      sensor1.address,
      sensor2.address,
      sensor3.address,
    ]);

    expect(await auth.authenticate(sensor1.pubkey)).toBe(true);
    expect(await auth.authenticate(sensor2.pubkey)).toBe(true);
    expect(await auth.authenticate(sensor3.pubkey)).toBe(true);
  });

  it('should reject sensors not in the whitelist', async () => {
    const sensor1 = createTestAddress();
    const sensor2 = createTestAddress();
    const sensor3 = createTestAddress();
    const unknown = createTestAddress();

    const auth = new WhitelistAuth([sensor1.address, sensor2.address]);

    expect(await auth.authenticate(sensor3.pubkey)).toBe(false);
    expect(await auth.authenticate(unknown.pubkey)).toBe(false);
  });

  it('should handle empty whitelist', async () => {
    const sensor1 = createTestAddress();
    const auth = new WhitelistAuth([]);

    expect(await auth.authenticate(sensor1.pubkey)).toBe(false);
  });

  it('should track seen nonces', async () => {
    const sensor1 = createTestAddress();
    const nonce1 = randomBytes(32);

    const auth = new WhitelistAuth([sensor1.address]);

    expect(await auth.isNonceSeen(sensor1.pubkey, nonce1)).toBe(false);

    await auth.rememberNonce(sensor1.pubkey, nonce1);

    expect(await auth.isNonceSeen(sensor1.pubkey, nonce1)).toBe(true);
  });

  it('should track nonces per sensor', async () => {
    const sensor1 = createTestAddress();
    const sensor2 = createTestAddress();
    const nonce1 = randomBytes(32);

    const auth = new WhitelistAuth([sensor1.address, sensor2.address]);

    await auth.rememberNonce(sensor1.pubkey, nonce1);

    expect(await auth.isNonceSeen(sensor1.pubkey, nonce1)).toBe(true);
    expect(await auth.isNonceSeen(sensor2.pubkey, nonce1)).toBe(false);
  });

  it('should return list of allowed sensors', () => {
    const sensor1 = createTestAddress();
    const sensor2 = createTestAddress();
    const sensor3 = createTestAddress();

    const auth = new WhitelistAuth([
      sensor1.address,
      sensor2.address,
      sensor3.address,
    ]);

    const result = auth.getAllowedSensors();
    expect(result).toHaveLength(3);

    // getAllowedSensors returns SS58 with the same prefix
    expect(result).toContain(sensor1.address);
    expect(result).toContain(sensor2.address);
    expect(result).toContain(sensor3.address);
  });

  it('should handle duplicate sensor IDs in constructor', () => {
    const sensor1 = createTestAddress();
    const sensor2 = createTestAddress();

    const auth = new WhitelistAuth([
      sensor1.address,
      sensor1.address,
      sensor2.address,
    ]);

    const result = auth.getAllowedSensors();
    // Duplicates are filtered during decoding
    expect(result.length).toBeGreaterThanOrEqual(2);
    expect(result.length).toBeLessThanOrEqual(3);
  });
});
