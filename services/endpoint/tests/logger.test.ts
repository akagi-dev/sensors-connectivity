import { describe, expect, it } from 'vitest';
import { logDebug } from '../src/index.js';

describe('endpoint logger helpers', () => {
  it('supports debug logs without throwing', () => {
    expect(() => {
      logDebug('endpoint debug test log', { trace_id: 'test-trace' });
    }).not.toThrow();
  });
});
