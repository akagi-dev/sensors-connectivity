import { describe, expect, it } from 'vitest';
import { startIpfsPublisher } from '../src/index.js';

describe('ipfs-publisher smoke', () => {
  it('starts in stub mode', async () => {
    await startIpfsPublisher();
    expect(true).toBe(true);
  });
});
