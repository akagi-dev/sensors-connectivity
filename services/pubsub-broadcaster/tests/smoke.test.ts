import { describe, expect, it } from 'vitest';
import { startPubsubBroadcaster } from '../src/index.js';

describe('pubsub-broadcaster smoke', () => {
  it('starts in stub mode', async () => {
    await startPubsubBroadcaster();
    expect(true).toBe(true);
  });
});
