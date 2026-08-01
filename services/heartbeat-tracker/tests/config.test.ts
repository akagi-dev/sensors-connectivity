import { describe, expect, it } from 'vitest';
import { loadHeartbeatTrackerConfig } from '../src/config.js';

describe('heartbeat tracker config', () => {
  it('uses 30000ms as default online window', () => {
    const config = loadHeartbeatTrackerConfig({});

    expect(config.onlineWindowMs).toBe(30000);
  });

  it('respects HEARTBEAT_TRACKER_ONLINE_WINDOW_MS override', () => {
    const config = loadHeartbeatTrackerConfig({ HEARTBEAT_TRACKER_ONLINE_WINDOW_MS: '45000' });

    expect(config.onlineWindowMs).toBe(45000);
  });
});
