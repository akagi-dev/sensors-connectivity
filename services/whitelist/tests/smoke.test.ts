import { describe, it, expect } from 'vitest';
import { randomBytes } from 'node:crypto';
import { encodeAddress } from '@polkadot/util-crypto';
import { WhitelistAuth } from '../src/index.js';

describe('whitelist smoke', () => {
  it('should create whitelist auth instance', () => {
    // Create a valid SS58 address (Robonomics prefix 32)
    const pubkey = randomBytes(32);
    const address = encodeAddress(pubkey, 32);

    const auth = new WhitelistAuth([address]);
    expect(auth).toBeDefined();

    const allowedSensors = auth.getAllowedSensors();
    expect(allowedSensors).toHaveLength(1);
    expect(allowedSensors[0]).toBe(address);
  });
});
