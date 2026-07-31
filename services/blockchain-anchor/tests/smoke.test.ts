import { describe, expect, it } from 'vitest';
import { startBlockchainAnchor } from '../src/index.js';

describe('blockchain-anchor smoke', () => {
  it('starts in stub mode', async () => {
    await startBlockchainAnchor();
    expect(true).toBe(true);
  });
});
