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
