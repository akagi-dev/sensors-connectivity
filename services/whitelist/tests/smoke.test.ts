import { describe, it, expect } from 'vitest';
import { WhitelistAuth } from '../src/index.js';

describe('whitelist smoke', () => {
  it('should create whitelist auth instance', () => {
    const auth = new WhitelistAuth(['sensor-1']);
    expect(auth).toBeDefined();
    expect(auth.getAllowedSensors()).toEqual(['sensor-1']);
  });
});
