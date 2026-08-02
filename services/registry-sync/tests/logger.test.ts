import { describe, expect, it } from 'vitest';
import { logDebug } from '../src/logger.js';

describe('registry-sync logger helpers', () => {
  it('supports debug logs without throwing', () => {
    expect(() => {
      logDebug('registry-sync debug test log', { eventId: '1:0' });
    }).not.toThrow();
  });
});
