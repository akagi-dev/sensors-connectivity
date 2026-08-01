import { describe, expect, it } from 'vitest';
import { logDebug } from '../src/index.js';

describe('authorizer logger helpers', () => {
  it('supports debug logs without throwing', () => {
    expect(() => {
      logDebug('authorizer debug test log', { trace_id: 'test-trace' });
    }).not.toThrow();
  });
});
